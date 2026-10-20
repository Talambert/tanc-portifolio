from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from dataeng_core.base.errors import ConfigError, DataEngError
from dataeng_core.base.pipeline import TransformOp
from dataeng_core.transform.engine_pandas import TransformResult, apply_transform


def _run(df: pd.DataFrame, *ops: dict[str, Any], root: Path = Path(".")) -> TransformResult:
    return apply_transform(df, [TransformOp(**op) for op in ops], project_root=root)


def _tipar(coluna: str, tipo: str, **extra: str) -> dict[str, Any]:
    return {"tipar": {coluna: {"tipo": tipo, **extra}}}


def _obj(values: list[Any]) -> pd.Series:
    return pd.Series(values, dtype=object)


def test_rename_and_cast_decimal_with_comma() -> None:
    df = pd.DataFrame({"vlr": ["1.234,56", "10,5", "7", None, "-0,25"]})
    res = _run(df, {"renomear": {"vlr": "valor"}}, _tipar("valor", "decimal", separador_decimal=","))
    assert list(res.rows.columns) == ["valor"]
    assert str(res.rows["valor"].dtype) == "Float64"
    assert res.rows["valor"].tolist()[:3] == [1234.56, 10.5, 7.0]
    assert pd.isna(res.rows["valor"].iloc[3])
    assert res.rows["valor"].iloc[4] == -0.25
    assert res.rejected.empty


def test_decimal_with_dot_and_comma_thousands_and_json_numbers() -> None:
    df = pd.DataFrame({"v": _obj(["1,234.5", "2", 3, 4.5])})
    res = _run(df, _tipar("v", "decimal"))
    assert res.rows["v"].tolist() == [1234.5, 2.0, 3.0, 4.5]


def test_decimal_rejects_ambiguous_grouping_nan_and_bool() -> None:
    df = pd.DataFrame({"v": _obj(["1,5", "nan", "inf", "1.2.3", True, "abc", "1e3"])})
    res = _run(df, _tipar("v", "decimal"))
    assert res.rows.empty
    assert len(res.rejected) == 7


def test_cast_failure_rejects_row_with_reason_and_keeps_original() -> None:
    df = pd.DataFrame({"id": ["1", "2x", "3"], "vlr": ["1,5", "2,5", "abc"], "nome": ["a", "b", "c"]})
    res = _run(
        df,
        {"renomear": {"vlr": "valor"}},
        _tipar("id", "inteiro"),
        _tipar("valor", "decimal", separador_decimal=","),
    )
    assert res.rows["id"].tolist() == [1]
    assert str(res.rows["id"].dtype) == "Int64"
    assert res.rows["nome"].tolist() == ["a"]
    assert len(res.rejected) == 2
    # a linha original (com o nome de coluna original e o valor ruim) é preservada
    assert res.rejected["id"].tolist() == ["2x", "3"]
    assert res.rejected["vlr"].tolist() == ["2,5", "abc"]
    motivos = res.rejected["_motivo"].tolist()
    assert "id" in motivos[0] and "inteiro" in motivos[0]
    assert "valor" in motivos[1] and "decimal" in motivos[1]
    # o motivo nunca ecoa o valor da célula
    assert "2x" not in motivos[0] and "abc" not in motivos[1]


def test_row_failing_two_columns_is_rejected_once_with_both_reasons() -> None:
    df = pd.DataFrame({"a": ["x"], "b": ["y"]})
    res = _run(df, {"tipar": {"a": {"tipo": "inteiro"}, "b": {"tipo": "inteiro"}}})
    assert len(res.rejected) == 1
    assert "'a'" in res.rejected["_motivo"].iloc[0] and "'b'" in res.rejected["_motivo"].iloc[0]


def test_cast_integer_variants() -> None:
    df = pd.DataFrame({"n": _obj(["007", " 12 ", "12.0", "1.5", "99999999999999999999", None, 5, 2.0, 2.5, ""])})
    res = _run(df, _tipar("n", "inteiro"))
    assert str(res.rows["n"].dtype) == "Int64"
    assert res.rows["n"].dropna().tolist() == [7, 12, 12, 5, 2]
    assert res.rows["n"].isna().sum() == 2  # None e "" são nulos
    assert len(res.rejected) == 3  # 1.5, 2.5 e o estouro de int64


def test_integer_with_decimal_separator_accepts_whole_values_only() -> None:
    df = pd.DataFrame({"n": ["1.234", "5,0", "5,5"]})
    res = _run(df, _tipar("n", "inteiro", separador_decimal=","))
    assert res.rows["n"].tolist() == [1234, 5]
    assert len(res.rejected) == 1


def test_cast_text_preserves_leading_zeros_and_nulls() -> None:
    df = pd.DataFrame({"cod": _obj(["00123", None, "  x "]), "n": _obj([5, 6, None])})
    res = _run(df, _tipar("cod", "texto"), _tipar("n", "texto"))
    assert res.rows["cod"].tolist()[0] == "00123"
    assert res.rows["cod"].tolist()[2] == "  x "
    assert pd.isna(res.rows["cod"].iloc[1])
    assert pd.api.types.is_string_dtype(res.rows["cod"])
    assert res.rows["n"].iloc[0] == "5"
    assert pd.isna(res.rows["n"].iloc[2])


def test_cast_date_brazilian_and_iso_and_explicit_format() -> None:
    df = pd.DataFrame({"d": ["31/01/2024", "2024-02-29", "05-03-2024", None, "31/02/2024", "xx"]})
    res = _run(df, _tipar("d", "data"))
    assert res.rows["d"].tolist()[:3] == [dt.date(2024, 1, 31), dt.date(2024, 2, 29), dt.date(2024, 3, 5)]
    assert pd.isna(res.rows["d"].iloc[3])
    assert len(res.rejected) == 2

    res = _run(pd.DataFrame({"d": ["01/02/2024", "2024-02-01"]}), _tipar("d", "data", formato="%d/%m/%Y"))
    assert res.rows["d"].tolist() == [dt.date(2024, 2, 1)]
    assert len(res.rejected) == 1


def test_cast_date_from_datetime_objects() -> None:
    df = pd.DataFrame({"d": _obj([dt.datetime(2024, 1, 2, 3, 4), dt.date(2024, 5, 6)])})
    res = _run(df, _tipar("d", "data"))
    assert res.rows["d"].tolist() == [dt.date(2024, 1, 2), dt.date(2024, 5, 6)]


def test_cast_timestamp_is_utc() -> None:
    df = pd.DataFrame(
        {"t": ["2024-01-31T10:00:00Z", "2024-01-31 10:00:00-03:00", "31/01/2024 10:30", "2024-01-31", "lixo", None]}
    )
    res = _run(df, _tipar("t", "timestamp"))
    assert len(res.rejected) == 1
    vals = res.rows["t"]
    assert str(vals.dtype).endswith("UTC]")
    assert vals.iloc[0] == pd.Timestamp("2024-01-31 10:00", tz="UTC")
    assert vals.iloc[1] == pd.Timestamp("2024-01-31 13:00", tz="UTC")
    assert vals.iloc[2] == pd.Timestamp("2024-01-31 10:30", tz="UTC")
    assert vals.iloc[3] == pd.Timestamp("2024-01-31", tz="UTC")
    assert pd.isna(vals.iloc[4])


def test_cast_boolean() -> None:
    df = pd.DataFrame({"b": _obj(["Sim", "não", "TRUE", "0", "1", "talvez", None, True, 2])})
    res = _run(df, _tipar("b", "booleano"))
    assert str(res.rows["b"].dtype) == "boolean"
    assert res.rows["b"].tolist()[:5] == [True, False, True, False, True]
    assert pd.isna(res.rows["b"].iloc[5])
    assert res.rows["b"].iloc[6]
    assert len(res.rejected) == 2


def test_all_null_column_casts_without_rejection() -> None:
    df = pd.DataFrame({"v": [None, None]})
    res = _run(df, _tipar("v", "decimal"))
    assert len(res.rows) == 2 and res.rejected.empty
    assert str(res.rows["v"].dtype) == "Float64"


def test_unsupported_decimal_separator_is_config_error() -> None:
    with pytest.raises(ConfigError, match="separador_decimal"):
        _run(pd.DataFrame({"v": ["1"]}), _tipar("v", "decimal", separador_decimal=";"))


def test_normalize_cnpj_with_leading_space_to_digits() -> None:
    df = pd.DataFrame({"cnpj": [" 12.345.678/0001-95", None, "00.000.000/0001-00"]})
    res = _run(df, {"normalizar": {"cnpj": ["apenas_digitos"]}})
    assert res.rows["cnpj"].tolist()[0] == "12345678000195"
    assert res.rows["cnpj"].tolist()[2] == "00000000000100"  # zeros à esquerda preservados
    assert pd.isna(res.rows["cnpj"].iloc[1])


def test_normalize_text_functions_in_order() -> None:
    df = pd.DataFrame({"n": ["  Foo   Bar \t baz ", None]})
    res = _run(df, {"normalizar": {"n": ["trim", "colapsar_espacos", "maiusculas"]}})
    assert res.rows["n"].iloc[0] == "FOO BAR BAZ"
    res = _run(df, {"normalizar": {"n": ["colapsar_espacos", "minusculas"]}})
    assert res.rows["n"].iloc[0] == " foo bar baz "
    assert pd.isna(res.rows["n"].iloc[1])


def test_normalize_digits_from_numeric_values_does_not_append_decimal_zero() -> None:
    df = pd.DataFrame({"c": _obj([123, 45.0, "7"])})
    res = _run(df, {"normalizar": {"c": ["apenas_digitos"]}})
    assert res.rows["c"].tolist() == ["123", "45", "7"]


def test_dedup_keeps_first_and_counts_removed() -> None:
    df = pd.DataFrame({"k": ["a", "b", "a", "a"], "v": [1, 2, 3, 4]})
    res = _run(df, {"deduplicar": ["k"]})
    assert res.rows["k"].tolist() == ["a", "b"]
    assert res.rows["v"].tolist() == [1, 2]
    assert res.duplicates_removed == 2
    assert res.rejected.empty


def test_dedup_composite_key_and_null_keys() -> None:
    df = pd.DataFrame({"a": ["x", "x", "x", None, None], "b": [1, 1, 2, 1, 1]})
    res = _run(df, {"deduplicar": ["a", "b"]})
    assert len(res.rows) == 3
    assert res.duplicates_removed == 2


def test_dedup_with_empty_key_list_is_config_error() -> None:
    with pytest.raises(ConfigError, match="deduplicar"):
        _run(pd.DataFrame({"a": ["x"]}), {"deduplicar": []})


def test_reject_if_null_regex_and_range() -> None:
    df = pd.DataFrame(
        {
            "cnpj": ["12345678000195", None, "  ", "abc", "12345678000196"],
            "v": ["5", "5", "5", "5", "500"],
        }
    )
    res = _run(
        df,
        _tipar("v", "decimal"),
        {"rejeitar_se": [{"coluna": "cnpj", "regra": "nao_nulo"}]},
        {"rejeitar_se": [{"coluna": "cnpj", "regra": "regex", "padrao": r"^\d{14}$"}]},
        {"rejeitar_se": [{"coluna": "v", "regra": "faixa", "minimo": 0, "maximo": 100}]},
    )
    assert res.rows["cnpj"].tolist() == ["12345678000195"]
    motivos = res.rejected["_motivo"].tolist()
    assert len(motivos) == 4
    assert "cnpj" in motivos[0] and "nulo" in motivos[0]
    assert "nulo" in motivos[1]
    assert "regex" in motivos[2] and "cnpj" in motivos[2]
    assert "faixa" in motivos[3] and "'v'" in motivos[3]
    # linha original preservada (v ainda em texto)
    assert res.rejected["v"].tolist() == ["5", "5", "5", "500"]
    assert "abc" not in motivos[2]


def test_range_bounds_inclusive_one_sided_and_nulls_pass() -> None:
    df = pd.DataFrame({"v": [0.0, 10.0, -1.0, 11.0, None]})
    res = _run(df, {"rejeitar_se": [{"coluna": "v", "regra": "faixa", "minimo": 0, "maximo": 10}]})
    assert res.rows["v"].dropna().tolist() == [0.0, 10.0]
    assert len(res.rows) == 3  # o nulo passa (use nao_nulo para exigir)
    res = _run(df, {"rejeitar_se": [{"coluna": "v", "regra": "faixa", "minimo": 5}]})
    assert res.rows["v"].dropna().tolist() == [10.0, 11.0]


def test_range_on_non_numeric_text_rejects_row_instead_of_crashing() -> None:
    df = pd.DataFrame({"v": ["3", "abc", "1,5"]})
    res = _run(df, {"rejeitar_se": [{"coluna": "v", "regra": "faixa", "minimo": 0}]})
    assert res.rows["v"].tolist() == ["3"]
    assert len(res.rejected) == 2


def test_invalid_regex_is_config_error() -> None:
    with pytest.raises(ConfigError, match="regex"):
        _run(pd.DataFrame({"v": ["a"]}), {"rejeitar_se": [{"coluna": "v", "regra": "regex", "padrao": "("}]})


def test_ops_apply_in_declared_order() -> None:
    df = pd.DataFrame({"a": [" 1 ", "1", "2"]})
    # trim antes de deduplicar: " 1 " e "1" colidem
    res = _run(df, {"normalizar": {"a": ["trim"]}}, {"deduplicar": ["a"]})
    assert res.rows["a"].tolist() == ["1", "2"]
    # deduplicar antes de trim: nada colide
    res = _run(df, {"deduplicar": ["a"]}, {"normalizar": {"a": ["trim"]}})
    assert res.rows["a"].tolist() == ["1", "1", "2"]
    # renomear antes de usar o novo nome; o nome antigo deixa de existir
    with pytest.raises(ConfigError, match="'a'"):
        _run(df, {"renomear": {"a": "b"}}, {"normalizar": {"a": ["trim"]}})


def test_rejected_rows_keep_original_values_even_after_normalize() -> None:
    df = pd.DataFrame({"c": [" ab ", " 12 "]})
    res = _run(
        df, {"normalizar": {"c": ["trim"]}}, {"rejeitar_se": [{"coluna": "c", "regra": "regex", "padrao": r"^\d+$"}]}
    )
    assert res.rows["c"].tolist() == ["12"]
    assert res.rejected["c"].tolist() == [" ab "]


def test_missing_column_names_column_and_available() -> None:
    df = pd.DataFrame({"a": ["1"]})
    for op in (
        {"renomear": {"x": "y"}},
        _tipar("x", "texto"),
        {"normalizar": {"x": ["trim"]}},
        {"deduplicar": ["x"]},
        {"rejeitar_se": [{"coluna": "x", "regra": "nao_nulo"}]},
    ):
        with pytest.raises(ConfigError) as exc:
            _run(df, op)
        assert "'x'" in str(exc.value) and "disponíveis: a" in str(exc.value)


def test_rename_to_existing_name_is_config_error() -> None:
    with pytest.raises(ConfigError, match="duplicad"):
        _run(pd.DataFrame({"a": ["1"], "b": ["2"]}), {"renomear": {"a": "b"}})


def test_swap_rename_works() -> None:
    res = _run(pd.DataFrame({"a": ["1"], "b": ["2"]}), {"renomear": {"a": "b", "b": "a"}})
    assert res.rows["b"].tolist() == ["1"] and res.rows["a"].tolist() == ["2"]


def test_empty_dataframe_passes_through() -> None:
    df = pd.DataFrame({"a": _obj([]), "b": _obj([])})
    res = _run(
        df,
        {"renomear": {"a": "x"}},
        _tipar("x", "decimal", separador_decimal=","),
        {"normalizar": {"b": ["trim"]}},
        {"deduplicar": ["x"]},
        {"rejeitar_se": [{"coluna": "x", "regra": "nao_nulo"}]},
    )
    assert len(res.rows) == 0 and len(res.rejected) == 0
    assert list(res.rows.columns) == ["x", "b"]
    assert str(res.rows["x"].dtype) == "Float64"
    assert "_motivo" in res.rejected.columns
    assert res.duplicates_removed == 0


def test_dataframe_without_columns_and_rows_passes_through() -> None:
    res = _run(pd.DataFrame(), _tipar("x", "texto"), {"deduplicar": ["x"]})
    assert len(res.rows) == 0 and len(res.rejected) == 0


def test_does_not_mutate_input_and_returns_a_copy() -> None:
    df = pd.DataFrame({"a": ["1", "2"]})
    res = _run(df, {"normalizar": {"a": ["trim"]}}, _tipar("a", "inteiro"))
    assert df["a"].tolist() == ["1", "2"]
    assert res.rows["a"].tolist() == [1, 2]
    res0 = apply_transform(df, [], project_root=Path("."))
    assert res0.rows.equals(df) and res0.rows is not df


def test_indexes_are_reset_after_rejections() -> None:
    df = pd.DataFrame({"a": ["x", "1", "y", "2"]})
    res = _run(df, _tipar("a", "inteiro"))
    assert res.rows.index.tolist() == [0, 1]
    assert res.rejected.index.tolist() == [0, 1]


def test_hook_runs_between_ops(tmp_path: Path) -> None:
    (tmp_path / "custom").mkdir()
    (tmp_path / "custom" / "__init__.py").write_text("")
    (tmp_path / "custom" / "hooks.py").write_text(
        "def dobrar(df):\n    df = df.copy()\n    df['v'] = df['v'] * 2\n    return df\n"
    )
    df = pd.DataFrame({"v": ["1", "2"]})
    res = _run(
        df,
        _tipar("v", "inteiro"),
        {"hook": "custom.hooks:dobrar"},
        {"rejeitar_se": [{"coluna": "v", "regra": "faixa", "maximo": 2}]},
        root=tmp_path,
    )
    assert res.rows["v"].tolist() == [2]
    # o gancho recebeu inteiros; a linha rejeitada depois dele preserva a original (texto "2")
    assert res.rejected["v"].tolist() == ["2"]


def test_hook_that_returns_non_dataframe_or_raises_is_data_error(tmp_path: Path) -> None:
    (tmp_path / "h.py").write_text(
        "def ruim(df):\n    return 3\n\ndef explode(df):\n    raise ValueError('segredo-123')\n"
    )
    df = pd.DataFrame({"v": ["1"]})
    with pytest.raises(DataEngError, match="DataFrame"):
        _run(df, {"hook": "h:ruim"}, root=tmp_path)
    with pytest.raises(DataEngError) as exc:
        _run(df, {"hook": "h:explode"}, root=tmp_path)
    assert "ValueError" in str(exc.value)
    assert "segredo-123" not in str(exc.value)
    assert "h:explode" in str(exc.value)


def test_hook_that_filters_rows_keeps_following_rejections_consistent(tmp_path: Path) -> None:
    (tmp_path / "h.py").write_text("def so_pares(df):\n    return df[df['v'].astype(int) % 2 == 0]\n")
    df = pd.DataFrame({"v": ["1", "2", "3", "4"]})
    res = _run(
        df, {"hook": "h:so_pares"}, {"rejeitar_se": [{"coluna": "v", "regra": "regex", "padrao": "^4$"}]}, root=tmp_path
    )
    assert res.rows["v"].tolist() == ["4"]
    assert res.rejected["v"].tolist() == ["2"]


def test_hook_reset_index_does_not_misattribute_originals(tmp_path: Path) -> None:
    (tmp_path / "h.py").write_text(
        "def so_pares(df):\n    return df[df['v'].astype(int) % 2 == 0].reset_index(drop=True)\n"
    )
    df = pd.DataFrame({"v": ["1", "2", "3", "4"]})
    res = _run(
        df, {"hook": "h:so_pares"}, {"rejeitar_se": [{"coluna": "v", "regra": "regex", "padrao": "^4$"}]}, root=tmp_path
    )
    assert res.rejected["v"].tolist() == ["2"]


def _hook_run(tmp_path: Path, body: str, df: pd.DataFrame, *ops: dict[str, Any]) -> TransformResult:
    (tmp_path / "h.py").write_text(f"def f(df):\n{body}\n", encoding="utf-8")
    return _run(df, {"hook": "h:f"}, *ops, root=tmp_path)


_ONLY_3 = {"rejeitar_se": [{"coluna": "v", "regra": "regex", "padrao": "^3$"}]}


def test_hook_reorder_with_reset_index_keeps_correct_originals(tmp_path: Path) -> None:
    df = pd.DataFrame({"v": ["1", "2", "3"], "o": ["a", "b", "c"]})
    res = _hook_run(tmp_path, "    return df.sort_values('v', ascending=False).reset_index(drop=True)", df, _ONLY_3)
    assert res.rows["v"].tolist() == ["3"]
    assert res.rejected["v"].tolist() == ["2", "1"]
    assert res.rejected["o"].tolist() == ["b", "a"]
    assert "_dataeng_row_id" not in res.rows.columns and "_dataeng_row_id" not in res.rejected.columns


def test_hook_reorder_and_filter(tmp_path: Path) -> None:
    df = pd.DataFrame({"v": ["1", "2", "3", "4"], "o": ["a", "b", "c", "d"]})
    res = _hook_run(
        tmp_path,
        "    return df[df['v'] != '1'].sort_values('v', ascending=False).reset_index(drop=True)",
        df,
        {"rejeitar_se": [{"coluna": "v", "regra": "regex", "padrao": "^(3|4)$"}]},
    )
    assert res.rows["v"].tolist() == ["4", "3"]
    assert res.rejected["o"].tolist() == ["b"]


def test_hook_that_adds_new_rows_and_drops_row_id_column(tmp_path: Path) -> None:
    df = pd.DataFrame({"v": ["1", "2"]})
    body = "    import pandas as pd\n    return pd.concat([df, pd.DataFrame({'v': ['x']})], ignore_index=True)"
    res = _hook_run(tmp_path, body, df, {"rejeitar_se": [{"coluna": "v", "regra": "regex", "padrao": "^1$"}]})
    assert res.rows["v"].tolist() == ["1"]
    assert sorted(res.rejected["v"].tolist()) == ["2", "x"]

    # gancho que seleciona colunas (descarta o identificador interno): ainda correto
    res = _hook_run(tmp_path, "    return df[['v']].iloc[::-1].reset_index(drop=True)", df, _ONLY_3)
    assert res.rejected["v"].tolist() == ["2", "1"]


def test_hook_that_duplicates_rows(tmp_path: Path) -> None:
    df = pd.DataFrame({"v": ["1", "3"]})
    res = _hook_run(
        tmp_path,
        "    import pandas as pd\n    return pd.concat([df, df], ignore_index=True)",
        df,
        {"rejeitar_se": [{"coluna": "v", "regra": "regex", "padrao": "^1$"}]},
    )
    assert res.rows["v"].tolist() == ["1", "1"]
    assert res.rejected["v"].tolist() == ["3", "3"]


def test_hook_that_changes_nothing(tmp_path: Path) -> None:
    df = pd.DataFrame({"v": ["1", "2", "3"]})
    res = _hook_run(
        tmp_path, "    return df", df, {"rejeitar_se": [{"coluna": "v", "regra": "regex", "padrao": "^2$"}]}
    )
    assert res.rows["v"].tolist() == ["2"]
    assert res.rejected["v"].tolist() == ["1", "3"]
    assert list(res.rows.columns) == ["v"]


def test_timestamp_year_out_of_pandas_range_goes_to_rejected_without_crash() -> None:
    df = pd.DataFrame({"t": ["31/12/9999 10:00", "2024-01-01T00:00:00Z"]})
    res = _run(df, _tipar("t", "timestamp"))
    assert len(res.rows) == 1 and len(res.rejected) == 1
    assert res.rows["t"].iloc[0] == pd.Timestamp("2024-01-01", tz="UTC")
    assert "timestamp" in res.rejected["_motivo"].iloc[0]


def test_cast_after_filter_raises_no_warning() -> None:
    import warnings

    df = pd.DataFrame({"a": ["x", "1", "2"], "b": ["5", "6", "7"]})
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        res = _run(df, _tipar("a", "inteiro"), _tipar("b", "inteiro"), {"normalizar": {"b": ["trim"]}})
    assert res.rows["b"].tolist() == ["6", "7"]
