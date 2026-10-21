"""Motor Spark sem Spark: tamanho das expressões geradas e validações que não tocam nos dados.

Usa o PySpark falso de `fake_pyspark.py`, que só monta o plano. Tetos (por operação `tipar` de UMA coluna,
somando todas as expressões que o motor passa ao DataFrame, cada reuso de subárvore contado de novo):
veja `CEILINGS`. A implementação anterior (uma expressão única por coluna) gerava, para `timestamp` sem
`formato`, uma expressão de 73 mil nós com 4,5 mil `regexp_extract`: estouraria o limite de 64 KB do codegen.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
from tests.transform.fake_pyspark import REGEX_OPS, count_ops, install, size

from dataeng_core.base.errors import ConfigError, DataEngError
from dataeng_core.base.pipeline import TransformOp
from dataeng_core.transform.engine_spark import apply_transform_spark

# Teto por coluna: (maior expressão em nós, total de operações de regex), cada reuso de subárvore contado de novo.
# Medido nesta versão (nós/regex): timestamp 83/9, timestamp com formato 83/3, data 83/6, decimal 32/5,
# inteiro 122/4, inteiro com separador 32/5, booleano 30/2, texto 9/2. Tetos com folga de no máximo 2x.
# A versão anterior: timestamp 68.155/≈4.500 regexp_extract, data 4.604, timestamp com formato 8.244, inteiro 45 regex.
CEILINGS: list[tuple[str, dict[str, Any], int, int]] = [
    ("timestamp", {"tipo": "timestamp"}, 160, 16),
    ("timestamp_formato", {"tipo": "timestamp", "formato": "%d/%m/%Y %H:%M:%S"}, 160, 6),
    ("data", {"tipo": "data"}, 160, 12),
    ("data_formato", {"tipo": "data", "formato": "%d/%m/%Y"}, 160, 6),
    ("decimal", {"tipo": "decimal", "separador_decimal": ","}, 64, 10),
    ("inteiro", {"tipo": "inteiro"}, 240, 8),
    ("inteiro_separador", {"tipo": "inteiro", "separador_decimal": ","}, 64, 10),
    ("booleano", {"tipo": "booleano"}, 60, 4),
    ("texto", {"tipo": "texto"}, 20, 4),
]


def _measure(monkeypatch: pytest.MonkeyPatch, spec: dict[str, Any]) -> tuple[int, int]:
    fake = install(monkeypatch)
    df = fake.frame({"t": "StringType", "outra": "StringType"})
    start = len(fake.log)
    apply_transform_spark(df, [TransformOp(tipar={"t": spec})], project_root=Path("."))
    exprs = [e for _, e in fake.log[start:]]
    return max(size(e) for e in exprs), sum(count_ops(e, REGEX_OPS) for e in exprs)


@pytest.mark.parametrize(("spec", "max_nodes", "max_regex"), [pytest.param(*c[1:], id=c[0]) for c in CEILINGS])
def test_expression_size_per_column_stays_under_ceiling(
    monkeypatch: pytest.MonkeyPatch, spec: dict[str, Any], max_nodes: int, max_regex: int
) -> None:
    nodes, regex = _measure(monkeypatch, spec)
    assert nodes <= max_nodes, f"{spec}: maior expressão com {nodes} nós (teto {max_nodes})"
    assert regex <= max_regex, f"{spec}: {regex} operações de regex (teto {max_regex})"


def _ops(*raw: dict[str, Any]) -> list[TransformOp]:
    return [TransformOp(**op) for op in raw]


def test_temporary_columns_are_removed(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = install(monkeypatch)
    df = fake.frame({"t": "StringType", "d": "StringType", "n": "StringType"})
    ops = _ops({"tipar": {"t": {"tipo": "timestamp"}, "d": {"tipo": "data"}, "n": {"tipo": "inteiro"}}})
    res = apply_transform_spark(df, ops, project_root=Path("."))
    assert res.rows.columns == ["t", "d", "n"]
    assert res.rejected.columns == ["t", "d", "n", "_motivo"]


@pytest.mark.parametrize(
    ("ops", "fragment"),
    [
        ([{"tipar": {"t": {"tipo": "data", "formato": "%d/%b/%Y"}}}], "'%b'"),
        ([{"tipar": {"t": {"tipo": "timestamp", "formato": "%Y-%m-%d %z"}}}], "'%z'"),
        ([{"tipar": {"t": {"tipo": "decimal", "separador_decimal": ";"}}}], "separador_decimal"),
        ([{"rejeitar_se": [{"coluna": "t", "regra": "regex", "padrao": "("}]}], "padrão inválido"),
    ],
)
def test_configuration_errors_happen_before_touching_the_data(
    monkeypatch: pytest.MonkeyPatch, ops: list[dict[str, Any]], fragment: str
) -> None:
    fake = install(monkeypatch)
    df = fake.frame({"t": "StringType"})
    # erro na SEGUNDA operação: mesmo assim nada é executado (nem o localCheckpoint da entrada)
    with pytest.raises(ConfigError, match=re.escape(fragment)):
        apply_transform_spark(df, _ops({"normalizar": {"t": ["trim"]}}, *ops), project_root=Path("."))
    assert df.actions == [] and fake.log == []


@pytest.mark.parametrize("column", ["_dataeng_x", "_DATAENG_y", "_dataeng_parte"])
def test_reserved_prefix_in_input_is_config_error(monkeypatch: pytest.MonkeyPatch, column: str) -> None:
    fake = install(monkeypatch)
    with pytest.raises(ConfigError, match="prefixo reservado"):
        apply_transform_spark(fake.frame({"a": "StringType", column: "StringType"}), [], project_root=Path("."))


def test_rename_refuses_case_only_collisions_and_reserved_names(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = install(monkeypatch)
    df = fake.frame({"a": "StringType", "b": "StringType"})
    with pytest.raises(ConfigError) as exc:
        apply_transform_spark(df, _ops({"renomear": {"a": "B"}}), project_root=Path("."))
    assert "só diferem em maiúsculas/minúsculas" in exc.value.detail and "'B'" in exc.value.detail
    with pytest.raises(ConfigError, match="prefixo reservado"):
        apply_transform_spark(df, _ops({"renomear": {"a": "_dataeng_a"}}), project_root=Path("."))
    # duplicada exata: o mesmo texto do motor pandas
    with pytest.raises(ConfigError) as exc:
        apply_transform_spark(df, _ops({"renomear": {"a": "b"}}), project_root=Path("."))
    assert exc.value.detail == "operação 'renomear' geraria colunas duplicadas: 'b'"
    # troca de nomes continua permitida
    res = apply_transform_spark(df, _ops({"renomear": {"a": "b", "b": "a"}}), project_root=Path("."))
    assert res.rows.columns == ["b", "a"]


class _NotAFrame:
    columns = ["v"]  # tem colunas, mas não é DataFrame (sem toPandas)


def test_hook_output_is_checked_by_duck_typing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import sys

    fake = install(monkeypatch)
    df = fake.frame({"v": "StringType"})
    hooks = [
        "def mesmo(df):",
        "    return df",
        "def falso(df):",
        "    from tests.transform.test_engine_spark_plan import _NotAFrame",
        "    return _NotAFrame()",
        "def reservado(df):",
        "    from pyspark.sql import functions as F",
        "    return df.withColumn('_dataeng_x', F.col('v'))",
    ]
    (tmp_path / "h.py").write_text(chr(10).join(hooks) + chr(10), encoding="utf-8")
    monkeypatch.setattr(sys, "path", [*sys.path])
    res = apply_transform_spark(df, _ops({"hook": "h:mesmo"}), project_root=tmp_path)
    assert res.rows.columns == ["v"]  # um DataFrame de outra classe (ex.: Spark Connect) é aceito
    with pytest.raises(DataEngError, match="DataFrame do PySpark"):
        apply_transform_spark(df, _ops({"hook": "h:falso"}), project_root=tmp_path)
    with pytest.raises(ConfigError, match="prefixo reservado"):
        apply_transform_spark(df, _ops({"hook": "h:reservado"}), project_root=tmp_path)
