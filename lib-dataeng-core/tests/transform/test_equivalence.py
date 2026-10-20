"""Equivalência pandas × PySpark nos mesmos casos do motor pandas (Tarefa 9).

Os testes `@pytest.mark.spark` precisam de PySpark e Java e rodam no CI (Linux); aqui são pulados.
Sem Spark, este arquivo prova a importação tardia e a tradução dos formatos de data para regex.
"""

from __future__ import annotations

import datetime as dt
import math
import os
import re
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from dataeng_core.base.errors import ConfigError, DataEngError
from dataeng_core.base.pipeline import TransformOp
from dataeng_core.transform.engine_pandas import _DATE_FORMATS, _TS_FORMATS, apply_transform
from dataeng_core.transform.engine_spark import (
    ISO_PATTERNS,
    DatePattern,
    apply_transform_spark,
    pandas_to_spark,
    spark_to_pandas,
    strptime_pattern,
)

WS_PY = r"[\s\x1c-\x1f]"  # a mesma classe que WS_JAVA, na sintaxe do Python


def _tipar(coluna: str, tipo: str, **extra: str) -> dict[str, Any]:
    return {"tipar": {coluna: {"tipo": tipo, **extra}}}


_FULL_EMPTY_OPS = [
    {"renomear": {"a": "x"}},
    _tipar("x", "decimal", separador_decimal=","),
    {"normalizar": {"b": ["trim"]}},
    {"deduplicar": ["x"]},
    {"rejeitar_se": [{"coluna": "x", "regra": "nao_nulo"}]},
]

# (id, colunas -> valores em texto, operações): os casos de tests/transform/test_engine_pandas.py
CASES: list[tuple[str, dict[str, list[str | None]], list[dict[str, Any]]]] = [
    (
        "renomear_e_decimal_com_virgula",
        {"vlr": ["1.234,56", "10,5", "7", None, "-0,25"]},
        [{"renomear": {"vlr": "valor"}}, _tipar("valor", "decimal", separador_decimal=",")],
    ),
    ("decimal_com_ponto", {"v": ["1,234.5", "2", "3", "4.5", "+7", " 1 234.5 "]}, [_tipar("v", "decimal")]),
    (
        "decimal_rejeita_ambiguos",
        {"v": ["1,5", "nan", "inf", "1.2.3", "abc", "1e3", "1" * 400, "12,34.5"]},
        [_tipar("v", "decimal")],
    ),
    (
        "falha_de_conversao_preserva_original",
        {"id": ["1", "2x", "3"], "vlr": ["1,5", "2,5", "abc"], "nome": ["a", "b", "c"]},
        [{"renomear": {"vlr": "valor"}}, _tipar("id", "inteiro"), _tipar("valor", "decimal", separador_decimal=",")],
    ),
    (
        "duas_colunas_falham_uma_rejeicao",
        {"a": ["x"], "b": ["y"]},
        [{"tipar": {"a": {"tipo": "inteiro"}, "b": {"tipo": "inteiro"}}}],
    ),
    (
        "inteiro_variantes",
        {
            "n": [
                "007",
                " 12 ",
                "12.0",
                "1.5",
                "99999999999999999999",
                None,
                "",
                "-9223372036854775808",
                "9223372036854775807",
                "9223372036854775808",
                "-0",
                "+5",
            ]
        },
        [_tipar("n", "inteiro")],
    ),
    ("inteiro_com_separador", {"n": ["1.234", "5,0", "5,5"]}, [_tipar("n", "inteiro", separador_decimal=",")]),
    ("texto_preserva_zeros_e_brancos", {"cod": ["00123", None, "  x ", ""]}, [_tipar("cod", "texto")]),
    (
        "data_brasileira_iso",
        {
            "d": [
                "31/01/2024",
                "2024-02-29",
                "05-03-2024",
                None,
                "31/02/2024",
                "xx",
                "1/2/2024",
                "29/02/2023",
                "0000-01-01",
                "01.02.2024",
            ]
        },
        [_tipar("d", "data")],
    ),
    ("data_com_formato", {"d": ["01/02/2024", "2024-02-01", " 1/02/2024"]}, [_tipar("d", "data", formato="%d/%m/%Y")]),
    (
        "timestamp_utc",
        {
            "t": [
                "2024-01-31T10:00:00Z",
                "2024-01-31 10:00:00-03:00",
                "31/01/2024 10:30",
                "2024-01-31",
                "lixo",
                None,
                "31/12/9999 10:00",
                "2024-01-31T10:00:00.5+05:30",
                "20240131T1015",
                "2024-01-31T25:00",
                "31-01-2024 10:30:15",
            ]
        },
        [_tipar("t", "timestamp")],
    ),
    (
        "timestamp_com_formato",
        {"t": ["31/01/2024 10:30:15", "31/01/2024 10:30", "31/01/2024 10:30:60"]},
        [_tipar("t", "timestamp", formato="%d/%m/%Y %H:%M:%S")],
    ),
    ("booleano", {"b": ["Sim", "não", "TRUE", "0", "1", "talvez", None, "  v ", "NÃO"]}, [_tipar("b", "booleano")]),
    ("coluna_toda_nula", {"v": [None, None]}, [_tipar("v", "decimal")]),
    (
        "normalizar_cnpj",
        {"cnpj": [" 12.345.678/0001-95", None, "00.000.000/0001-00"]},
        [{"normalizar": {"cnpj": ["apenas_digitos"]}}],
    ),
    (
        "normalizar_em_ordem",
        {"n": ["  Foo   Bar \t baz ", None, " x  y "]},
        [{"normalizar": {"n": ["trim", "colapsar_espacos", "maiusculas"]}}],
    ),
    (
        "normalizar_minusculas",
        {"n": ["  Foo   Bar \t baz ", None]},
        [{"normalizar": {"n": ["colapsar_espacos", "minusculas"]}}],
    ),
    ("deduplicar_mantem_a_primeira", {"k": ["a", "b", "a", "a"], "v": ["1", "2", "3", "4"]}, [{"deduplicar": ["k"]}]),
    (
        "deduplicar_chave_composta_e_nulos",
        {"a": ["x", "x", "x", None, None], "b": ["1", "1", "2", "1", "1"]},
        [{"deduplicar": ["a", "b"]}],
    ),
    (
        "rejeitar_nulo_regex_faixa",
        {"cnpj": ["12345678000195", None, "  ", "abc", "12345678000196"], "v": ["5", "5", "5", "5", "500"]},
        [
            _tipar("v", "decimal"),
            {"rejeitar_se": [{"coluna": "cnpj", "regra": "nao_nulo"}]},
            {"rejeitar_se": [{"coluna": "cnpj", "regra": "regex", "padrao": r"^\d{14}$"}]},
            {"rejeitar_se": [{"coluna": "v", "regra": "faixa", "minimo": 0, "maximo": 100}]},
        ],
    ),
    (
        "faixa_inclusiva_e_nulos_passam",
        {"v": ["0", "10", "-1", "11", None]},
        [_tipar("v", "decimal"), {"rejeitar_se": [{"coluna": "v", "regra": "faixa", "minimo": 0, "maximo": 10}]}],
    ),
    (
        "faixa_unilateral",
        {"v": ["0", "10", "-1", "11", None]},
        [_tipar("v", "decimal"), {"rejeitar_se": [{"coluna": "v", "regra": "faixa", "minimo": 5}]}],
    ),
    (
        "faixa_em_texto_nao_numerico",
        {"v": ["3", "abc", "1,5"]},
        [{"rejeitar_se": [{"coluna": "v", "regra": "faixa", "minimo": 0}]}],
    ),
    (
        "ordem_trim_antes_de_deduplicar",
        {"a": [" 1 ", "1", "2"]},
        [{"normalizar": {"a": ["trim"]}}, {"deduplicar": ["a"]}],
    ),
    (
        "ordem_deduplicar_antes_de_trim",
        {"a": [" 1 ", "1", "2"]},
        [{"deduplicar": ["a"]}, {"normalizar": {"a": ["trim"]}}],
    ),
    (
        "rejeitada_preserva_original_apos_normalizar",
        {"c": [" ab ", " 12 "]},
        [{"normalizar": {"c": ["trim"]}}, {"rejeitar_se": [{"coluna": "c", "regra": "regex", "padrao": r"^\d+$"}]}],
    ),
    ("troca_de_nomes", {"a": ["1"], "b": ["2"]}, [{"renomear": {"a": "b", "b": "a"}}]),
    ("vazio", {"a": [], "b": []}, _FULL_EMPTY_OPS),
    (
        "conversao_apos_filtro",
        {"a": ["x", "1", "2"], "b": ["5", "6", "7"]},
        [_tipar("a", "inteiro"), _tipar("b", "inteiro"), {"normalizar": {"b": ["trim"]}}],
    ),
    (
        "regras_sobre_colunas_tipadas",
        {"n": ["1", "20", None, "x"], "f": ["sim", "não", None, "sim"]},
        [
            _tipar("n", "inteiro"),
            _tipar("f", "booleano"),
            {"normalizar": {"f": ["maiusculas"]}},
            {
                "rejeitar_se": [
                    {"coluna": "n", "regra": "faixa", "maximo": 10},
                    {"coluna": "f", "regra": "regex", "padrao": "^TRUE$"},
                ]
            },
        ],
    ),
]


def _ops(raw: list[dict[str, Any]]) -> list[TransformOp]:
    return [TransformOp(**op) for op in raw]


def _frame(data: dict[str, list[str | None]]) -> pd.DataFrame:
    return pd.DataFrame({k: pd.Series(v, dtype=object) for k, v in data.items()})


def _norm(v: Any) -> Any:
    if v is None or v is pd.NA or v is pd.NaT:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    item = getattr(v, "item", None)
    return item() if callable(item) else v


def _table(df: pd.DataFrame) -> tuple[list[str], list[tuple[Any, ...]]]:
    values = [tuple(_norm(v) for v in r) for r in df.astype(object).itertuples(index=False, name=None)]
    return [str(c) for c in df.columns], sorted(values, key=repr)


# ------------------------------------------------------------------------------------- sem Spark


def test_spark_engine_import_is_lazy(tmp_path: Path) -> None:
    code = (
        "import sys\n"
        "sys.modules['pyspark'] = None\n"
        "import dataeng_core, dataeng_core.steps, dataeng_core.jobs.ingest_spark\n"
        "import dataeng_core.transform.engine_spark as es\n"
        "from dataeng_core.base.errors import ConfigError\n"
        "assert [m for m in sys.modules if m.startswith('pyspark') and sys.modules[m] is not None] == []\n"
        "try:\n"
        "    es.apply_transform_spark(object(), [], project_root='.')\n"
        "except ConfigError as exc:\n"
        "    print('ERRO:', exc.detail)\n"
    )
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    proc = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, encoding="utf-8", timeout=120, env=env
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == (
        "ERRO: o motor Spark exige o PySpark: instale o extra [spark] (pip install 'dataeng-core[spark]') "
        "e um Java compatível (JAVA_HOME ou java no PATH)"
    )
    # PySpark "instalado" (pacote falso no PYTHONPATH): importar a biblioteca não o importa
    (tmp_path / "pyspark").mkdir()
    (tmp_path / "pyspark" / "__init__.py").write_text("", encoding="utf-8")
    env["PYTHONPATH"] = os.pathsep.join([str(tmp_path), env.get("PYTHONPATH", "")])
    code = (
        "import sys\n"
        "import dataeng_core, dataeng_core.steps, dataeng_core.jobs.ingest_spark, dataeng_core.transform.engine_spark\n"
        "print(sorted(m for m in sys.modules if m == 'pyspark' or m.startswith('pyspark.')))\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, encoding="utf-8", timeout=120, env=env
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "[]"


def _match(pattern: DatePattern, text: str) -> dict[str, str] | None:
    """Simula em Python o que o Spark faz com a regex: primeira correspondência no início, cobrindo tudo."""
    regex = pattern.regex.replace(r"[\p{IsWhite_Space}\x1c-\x1f]", WS_PY)
    m = re.match(regex, text)
    if m is None or m.group(0) != text:
        return None
    return {k: "".join(m.group(i) or "" for i in idx) for k, idx in pattern.groups.items()}


_STRPTIME_SAMPLES = [
    "31/01/2024",
    "1/2/2024",
    " 1/02/2024",
    "31/02/2024",
    "2024-01-31",
    "2024-1-5",
    "05-03-2024",
    "01.02.2024",
    "31/01/2024 10:30",
    "31/01/2024 10:30:15",
    "31/01/2024  9:05:07",
    "31/01/2024\t10:30",
    "31-01-2024 10:30:15",
    "31/01/2024 10:30:60",
    "31/01/2024 x",
    "31/01/24",
    "24-01-31 10:30:15.25",
    "lixo",
]


@pytest.mark.parametrize(
    "fmt",
    [*_DATE_FORMATS, *_TS_FORMATS, "%d/%m/%y", "%y-%m-%d %H:%M:%S.%f", "%Y%m%d", "%d de %m de %Y", "%Y-%m-%dT%H:%M"],
)
def test_strptime_pattern_matches_like_python(fmt: str) -> None:
    pattern = strptime_pattern(fmt, ws=WS_PY)
    extra = ["20240131", "31 de 01 de 2024", "31 DE 01 de 2024", "2024-01-31T 9:05", "2024-01-31T09:05"]
    for text in [*_STRPTIME_SAMPLES, *extra]:
        try:
            expected: dt.datetime | None = dt.datetime.strptime(text, fmt)
        except ValueError:
            expected = None
        parts = _match(pattern, text)
        if expected is None:
            continue  # casar e ser data inválida (31/02) é recusado pela validação no Spark
        assert parts is not None, (fmt, text)
        if "Y" in parts:
            year = int(parts["Y"])
        elif "y" in parts:
            year = int(parts["y"]) + (2000 if int(parts["y"]) <= 68 else 1900)
        else:
            year = 1900
        got = (
            year,
            int(parts.get("m", "1").replace(" ", "")),
            int(parts.get("d", "1").replace(" ", "")),
            int((parts.get("H") or "0").replace(" ", "")),
            int(parts.get("M") or "0"),
            int(parts.get("S") or "0"),
            int((parts.get("f") or "0").ljust(6, "0")[:6]),
        )
        assert got == (
            expected.year,
            expected.month,
            expected.day,
            expected.hour,
            expected.minute,
            expected.second,
            expected.microsecond,
        ), (fmt, text)


def test_strptime_pattern_refuses_text_python_refuses() -> None:
    for fmt in _DATE_FORMATS:
        pattern = strptime_pattern(fmt, ws=WS_PY)
        for text in ("2024-1-5x", "31/01/2024 x", "lixo", "31/01/24"):
            if _match(pattern, text) is None:
                with pytest.raises(ValueError):
                    dt.datetime.strptime(text, fmt)


def _iso_value(parts: dict[str, str]) -> dt.datetime:
    base = dt.datetime(
        int(parts["Y"]),
        int(parts["m"]),
        int(parts["d"]),
        int(parts["H"] or 0),
        int(parts["M"] or 0),
        int(parts["S"] or 0),
        int((parts["f"] or "0")[:6].ljust(6, "0")),
        tzinfo=dt.UTC,
    )
    offset = int(parts["tzh"] or 0) * 3600 + int(parts["tzm"] or 0) * 60 + int(parts["tzsec"] or 0)
    return base - dt.timedelta(seconds=-offset if parts["tzs"] == "-" else offset)


def test_iso_patterns_agree_with_fromisoformat() -> None:
    supported = [
        "2024-01-31",
        "20240131",
        "2024-01-31T10",
        "2024-01-31T10:15",
        "2024-01-31 10:15:30",
        "2024-01-31T10:15:30.5",
        "2024-01-31T10:15:30,123456789",
        "2024-01-31T10:15:30Z",
        "2024-01-31T10:15:30-03:00",
        "2024-01-31T10:15:30+0530",
        "2024-01-31T10+03",
        "2024-01-31T101530",
        "20240131T1015",
        "2024-01-31x10:00",
        "2024-01-31T10:15:30+05:30:15",
    ]
    for text in supported:
        expected = dt.datetime.fromisoformat(text)
        expected = expected.replace(tzinfo=dt.UTC) if expected.tzinfo is None else expected.astimezone(dt.UTC)
        found = [p for p in (_match(pat, text) for pat in ISO_PATTERNS) if p is not None]
        assert len(found) == 1, text
        assert _iso_value(found[0]) == expected, text
    # recusados pelos dois (o Spark ainda valida 25h, mês 13 etc. depois da regex)
    for text in ("lixo", "2024-01", "2024/01/31", "31-01-2024", "2024-01-31T", "2024-01-31T10:1"):
        assert all(_match(pat, text) is None for pat in ISO_PATTERNS), text
        with pytest.raises(ValueError):
            dt.datetime.fromisoformat(text)


@pytest.mark.parametrize(("fmt", "fragment"), [("%d/%b/%Y", "%b"), ("%Y-%m-%d %z", "%z"), ("%d/%m/%", "'%'")])
def test_unsupported_directive_is_config_error(fmt: str, fragment: str) -> None:
    with pytest.raises(ConfigError, match=re.escape(fragment)) as exc:
        strptime_pattern(fmt)
    assert "%Y %y %m %d %H %M %S %f" in str(exc.value)


def test_repeated_year_directive_is_config_error() -> None:
    with pytest.raises(ConfigError, match="repetida"):
        strptime_pattern("%Y %y")


# ------------------------------------------------------------------------------------- com Spark


@pytest.fixture(params=[False, True], ids=["ansi_desligado", "ansi_ligado"])
def spark_ansi(spark: Any, request: pytest.FixtureRequest) -> Iterator[Any]:
    spark.conf.set("spark.sql.ansi.enabled", "true" if request.param else "false")
    yield spark


@pytest.mark.spark
@pytest.mark.parametrize(("data", "ops"), [pytest.param(d, o, id=i) for i, d, o in CASES])
def test_pandas_and_spark_engines_agree(
    spark_ansi: Any, data: dict[str, list[str | None]], ops: list[dict[str, Any]]
) -> None:
    df = _frame(data)
    expected = apply_transform(df, _ops(ops), project_root=Path("."))
    got = apply_transform_spark(pandas_to_spark(spark_ansi, df), _ops(ops), project_root=Path("."))
    assert _table(spark_to_pandas(got.rows)) == _table(expected.rows)
    assert _table(spark_to_pandas(got.rejected)) == _table(expected.rejected)
    assert got.duplicates_removed == expected.duplicates_removed


@pytest.mark.spark
def test_spark_rows_keep_input_order_and_dtypes(spark: Any) -> None:
    df = _frame({"id": ["3", "1", "x", "2"], "v": ["1,5", None, "2", "abc"], "d": ["01/02/2024", None, None, "x"]})
    ops = [
        {"tipar": {"id": {"tipo": "inteiro"}, "v": {"tipo": "decimal", "separador_decimal": ","}}},
        _tipar("d", "data"),
    ]
    expected = apply_transform(df, _ops(ops), project_root=Path("."))
    got = spark_to_pandas(apply_transform_spark(pandas_to_spark(spark, df), _ops(ops), project_root=Path(".")).rows)
    assert got["id"].tolist() == expected.rows["id"].tolist() == [3, 1]
    assert {c: str(t) for c, t in got.dtypes.items()} == {"id": "Int64", "v": "Float64", "d": "object"}
    assert got["d"].tolist()[0] == dt.date(2024, 2, 1)


@pytest.mark.spark
@pytest.mark.parametrize(
    "ops",
    [
        [{"renomear": {"x": "y"}}],
        [_tipar("x", "texto")],
        [{"normalizar": {"x": ["trim"]}}],
        [{"deduplicar": ["x"]}],
        [{"rejeitar_se": [{"coluna": "x", "regra": "nao_nulo"}]}],
        [{"renomear": {"a": "b"}}],
        [{"deduplicar": []}],
        [{"rejeitar_se": [{"coluna": "a", "regra": "regex", "padrao": "("}]}],
        [_tipar("a", "decimal", separador_decimal=";")],
    ],
)
def test_spark_engine_errors_match_pandas(spark: Any, ops: list[dict[str, Any]]) -> None:
    df = _frame({"a": ["1"], "b": ["2"]})
    with pytest.raises(ConfigError) as pandas_exc:
        apply_transform(df, _ops(ops), project_root=Path("."))
    with pytest.raises(ConfigError) as spark_exc:
        apply_transform_spark(pandas_to_spark(spark, df), _ops(ops), project_root=Path("."))
    assert spark_exc.value.detail == pandas_exc.value.detail


def _hook(tmp_path: Path, body: str) -> Path:
    (tmp_path / "h.py").write_text("from pyspark.sql import functions as F\n\n\n" + body, encoding="utf-8")
    return tmp_path


def _spark_run(spark: Any, root: Path, data: dict[str, list[str | None]], *ops: dict[str, Any]) -> Any:
    return apply_transform_spark(pandas_to_spark(spark, _frame(data)), _ops(list(ops)), project_root=root)


@pytest.mark.spark
def test_spark_hook_runs_between_ops(spark: Any, tmp_path: Path) -> None:
    root = _hook(tmp_path, "def dobrar(df):\n    return df.withColumn('v', F.col('v') * 2)\n")
    res = _spark_run(
        spark,
        root,
        {"v": ["1", "2"]},
        _tipar("v", "inteiro"),
        {"hook": "h:dobrar"},
        {"rejeitar_se": [{"coluna": "v", "regra": "faixa", "maximo": 2}]},
    )
    assert spark_to_pandas(res.rows)["v"].tolist() == [2]
    assert spark_to_pandas(res.rejected)["v"].tolist() == ["2"]  # a linha original (texto)


@pytest.mark.spark
def test_spark_hook_that_filters_keeps_originals(spark: Any, tmp_path: Path) -> None:
    root = _hook(tmp_path, "def pares(df):\n    return df.filter(F.col('v').cast('int') % 2 == 0)\n")
    res = _spark_run(
        spark,
        root,
        {"v": ["1", "2", "3", "4"]},
        {"hook": "h:pares"},
        {"rejeitar_se": [{"coluna": "v", "regra": "regex", "padrao": "^4$"}]},
    )
    assert spark_to_pandas(res.rows)["v"].tolist() == ["4"]
    assert spark_to_pandas(res.rejected)["v"].tolist() == ["2"]


@pytest.mark.spark
def test_spark_hook_that_drops_row_id_and_adds_rows(spark: Any, tmp_path: Path) -> None:
    root = _hook(
        tmp_path,
        "def mais(df):\n"
        "    extra = df.sparkSession.createDataFrame([('x',)], 'v string')\n"
        "    return df.select('v').unionByName(extra)\n",
    )
    res = _spark_run(
        spark,
        root,
        {"v": ["1", "2"]},
        {"hook": "h:mais"},
        {"rejeitar_se": [{"coluna": "v", "regra": "regex", "padrao": "^1$"}]},
    )
    assert spark_to_pandas(res.rows)["v"].tolist() == ["1"]
    assert sorted(spark_to_pandas(res.rejected)["v"].tolist()) == ["2", "x"]
    assert "_dataeng_row_id" not in res.rows.columns and "_dataeng_row_id" not in res.rejected.columns


@pytest.mark.spark
def test_spark_hook_errors_are_data_errors_without_message(spark: Any, tmp_path: Path) -> None:
    root = _hook(
        tmp_path,
        "def ruim(df):\n    return 3\n\n\n"
        "def explode(df):\n    raise ValueError('segredo-123')\n\n\n"
        "def preguicoso(df):\n"
        "    from pyspark.sql.functions import udf\n\n"
        "    @udf('string')\n"
        "    def boom(v):\n"
        "        raise ValueError('segredo-456')\n\n"
        "    return df.withColumn('v', boom('v'))\n",
    )
    with pytest.raises(DataEngError, match="DataFrame do PySpark"):
        _spark_run(spark, root, {"v": ["1"]}, {"hook": "h:ruim"})
    with pytest.raises(DataEngError) as exc:
        _spark_run(spark, root, {"v": ["1"]}, {"hook": "h:explode"})
    assert "ValueError" in str(exc.value) and "segredo-123" not in str(exc.value) and "h:explode" in str(exc.value)
    with pytest.raises(DataEngError) as exc:
        _spark_run(spark, root, {"v": ["1"]}, {"hook": "h:preguicoso"})
    assert "falhou ao ser executado" in str(exc.value) and "segredo-456" not in str(exc.value)


@pytest.mark.spark
def test_spark_reserved_row_id_column_is_config_error(spark: Any) -> None:
    df = pandas_to_spark(spark, _frame({"_dataeng_row_id": ["1"]}))
    with pytest.raises(ConfigError, match="reservada"):
        apply_transform_spark(df, [], project_root=Path("."))
