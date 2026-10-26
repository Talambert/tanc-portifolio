from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import pytest

from dataeng_core.base.errors import QualityError
from dataeng_core.base.pipeline import QualityCheck
from dataeng_core.quality.checks import CheckResult, raise_if_blocking, run_checks


def _check(tipo: str, colunas: list[str], **extra: Any) -> QualityCheck:
    return QualityCheck(tipo=tipo, colunas=colunas, **extra)  # type: ignore[arg-type]


def _one(df: pd.DataFrame, check: QualityCheck) -> CheckResult:
    (result,) = run_checks(df, [check])
    return result


def test_not_null_approves_and_fails_with_exact_rate() -> None:
    ok = _one(pd.DataFrame({"a": [1, 2]}), _check("nao_nulo", ["a"]))
    assert (ok.nome, ok.coluna, ok.aprovado, ok.taxa, ok.limiar, ok.bloqueante) == (
        "nao_nulo",
        "a",
        True,
        1.0,
        1.0,
        False,
    )
    bad = _one(pd.DataFrame({"a": [1, None, 3, None]}), _check("nao_nulo", ["a"]))
    assert bad.aprovado is False
    assert bad.taxa == 0.5


def test_not_null_with_several_columns_yields_one_result_per_column() -> None:
    df = pd.DataFrame({"a": [1, None], "b": [1, 2]})
    results = run_checks(df, [_check("nao_nulo", ["a", "b"])])
    assert [(r.coluna, r.taxa, r.aprovado) for r in results] == [("a", 0.5, False), ("b", 1.0, True)]


def test_unique_approves_and_fails_with_exact_rate() -> None:
    assert _one(pd.DataFrame({"a": [1, 2, 3]}), _check("unico", ["a"])).aprovado is True
    bad = _one(pd.DataFrame({"a": [1, 1, 2, 3]}), _check("unico", ["a"]))
    assert bad.aprovado is False
    assert bad.taxa == 0.75  # 1 linha repetida em 4


def test_unique_over_composite_key() -> None:
    df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "y", "x"]})
    assert _one(df, _check("unico", ["a"])).taxa == pytest.approx(2 / 3)
    composite = _one(df, _check("unico", ["a", "b"]))
    assert composite.aprovado is True
    assert composite.coluna == "a, b"
    dup = pd.DataFrame({"a": [1, 1], "b": ["x", "x"]})
    assert _one(dup, _check("unico", ["a", "b"])).taxa == 0.5


def test_unique_treats_null_keys_as_distinct() -> None:
    df = pd.DataFrame({"a": [None, None, 1.0]})
    assert _one(df, _check("unico", ["a"])).aprovado is True


def test_range_approves_and_fails_with_exact_rate() -> None:
    check = _check("faixa", ["v"], minimo=0, maximo=10)
    assert _one(pd.DataFrame({"v": [0, 5, 10]}), check).aprovado is True
    bad = _one(pd.DataFrame({"v": [-1, 5, 11, 7]}), check)
    assert bad.aprovado is False
    assert bad.taxa == 0.5


def test_range_with_only_one_bound() -> None:
    df = pd.DataFrame({"v": [1, 5, 9]})
    assert _one(df, _check("faixa", ["v"], minimo=5)).taxa == pytest.approx(2 / 3)
    assert _one(df, _check("faixa", ["v"], maximo=5)).taxa == pytest.approx(2 / 3)


def test_range_on_non_numeric_column_is_execution_error() -> None:
    with pytest.raises(QualityError, match="'v'.*exige coluna numérica"):
        run_checks(pd.DataFrame({"v": ["a", "b"]}), [_check("faixa", ["v"], minimo=0)])


def test_set_approves_and_fails_with_exact_rate() -> None:
    check = _check("conjunto", ["s"], valores=["A", "B"])
    assert _one(pd.DataFrame({"s": ["A", "B", "A"]}), check).aprovado is True
    bad = _one(pd.DataFrame({"s": ["A", "Z", "B", "Y"]}), check)
    assert bad.aprovado is False
    assert bad.taxa == 0.5


def test_pattern_approves_and_fails_with_exact_rate() -> None:
    check = _check("formato", ["c"], padrao=r"\d{3}")
    assert _one(pd.DataFrame({"c": ["123", "456"]}), check).aprovado is True
    bad = _one(pd.DataFrame({"c": ["123", "12", "1234", "abc"]}), check)
    assert bad.taxa == 0.25  # casamento da string inteira


def test_pattern_non_string_values_fail() -> None:
    check = _check("formato", ["c"], padrao=r"\d+")
    assert _one(pd.DataFrame({"c": [1, 2]}), check).taxa == 0.0


def test_invalid_regex_is_execution_error() -> None:
    with pytest.raises(QualityError, match="padrão.*inválid"):
        run_checks(pd.DataFrame({"c": ["a"]}), [_check("formato", ["c"], padrao="(")])


def test_nulls_are_ignored_by_value_checks() -> None:
    df = pd.DataFrame({"v": [1.0, None, 1.0], "s": ["x", None, "y"]})
    assert _one(df, _check("faixa", ["v"], minimo=0)).taxa == 1.0
    assert _one(df, _check("conjunto", ["v"], valores=[1])).taxa == 1.0
    assert _one(df, _check("formato", ["s"], padrao=".")).taxa == 1.0
    # mas o nulo continua contando para `nao_nulo`
    assert _one(df, _check("nao_nulo", ["v"])).taxa == pytest.approx(2 / 3)


def test_threshold_allows_partial_failures() -> None:
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7, 8, 9, None]})
    assert _one(df, _check("nao_nulo", ["a"], limiar=0.9)).aprovado is True
    r = _one(df, _check("nao_nulo", ["a"], limiar=0.95))
    assert r.aprovado is False
    assert r.limiar == 0.95


def test_empty_dataframe_is_vacuously_approved(caplog: pytest.LogCaptureFixture) -> None:
    df = pd.DataFrame({"a": pd.Series([], dtype="float64")})
    with caplog.at_level(logging.WARNING):
        results = run_checks(df, [_check("nao_nulo", ["a"]), _check("unico", ["a"])])
    assert [(r.aprovado, r.taxa) for r in results] == [(True, 1.0), (True, 1.0)]
    assert "tabela vazia" in caplog.text


def test_missing_column_is_execution_error_not_failure() -> None:
    df = pd.DataFrame({"a": [1]})
    with pytest.raises(QualityError, match="'zz' não existe") as exc:
        run_checks(df, [_check("nao_nulo", ["a"]), _check("unico", ["a", "zz"])])
    assert "não pôde ser executada" in str(exc.value)


def test_error_messages_do_not_leak_data_values() -> None:
    df = pd.DataFrame({"v": ["segredo-123", "outro"]})
    with pytest.raises(QualityError) as exc:
        run_checks(df, [_check("faixa", ["v"], minimo=0)])
    assert "segredo" not in str(exc.value)
    results = run_checks(df, [_check("conjunto", ["v"], valores=["x"], bloqueante=True)])
    with pytest.raises(QualityError) as exc2:
        raise_if_blocking(results)
    assert "segredo" not in str(exc2.value)


def test_run_checks_preserves_order_and_flags() -> None:
    df = pd.DataFrame({"a": [1, 2], "b": [1, 1]})
    results = run_checks(df, [_check("unico", ["b"], bloqueante=True), _check("nao_nulo", ["a"])])
    assert [(r.nome, r.coluna, r.bloqueante) for r in results] == [("unico", "b", True), ("nao_nulo", "a", False)]


def test_blocking_failure_raises_and_non_blocking_does_not() -> None:
    df = pd.DataFrame({"a": [1, 1]})
    non_blocking = run_checks(df, [_check("unico", ["a"])])
    assert non_blocking[0].aprovado is False
    raise_if_blocking(non_blocking)  # não levanta

    blocking = run_checks(
        df,
        [_check("unico", ["a"], bloqueante=True), _check("nao_nulo", ["a"], bloqueante=True)],
    )
    with pytest.raises(QualityError) as exc:
        raise_if_blocking(blocking)
    msg = str(exc.value)
    assert "unico" in msg and "'a'" in msg and "0.5" in msg
    assert "nao_nulo" not in msg  # só as reprovadas


def test_blocking_that_passes_does_not_raise() -> None:
    raise_if_blocking(run_checks(pd.DataFrame({"a": [1]}), [_check("unico", ["a"], bloqueante=True)]))


def test_range_on_empty_object_column_is_vacuous() -> None:
    df = pd.DataFrame({"v": pd.Series([], dtype="object")})
    r = _one(df, _check("faixa", ["v"], minimo=0))
    assert (r.taxa, r.aprovado) == (1.0, True)


def test_range_on_all_null_object_column_is_vacuous() -> None:
    r = _one(pd.DataFrame({"v": pd.Series([None, None], dtype="object")}), _check("faixa", ["v"], minimo=0))
    assert (r.taxa, r.aprovado) == (1.0, True)


def test_range_on_object_column_with_numbers_and_none() -> None:
    df = pd.DataFrame({"v": pd.Series([1, None, 3, -5], dtype="object")})
    assert _one(df, _check("faixa", ["v"], minimo=0)).taxa == pytest.approx(2 / 3)


def test_range_on_object_column_with_text_errors_without_leaking() -> None:
    df = pd.DataFrame({"v": pd.Series([1, "abc"], dtype="object")})
    with pytest.raises(QualityError, match="'v'.*exige coluna numérica") as exc:
        run_checks(df, [_check("faixa", ["v"], minimo=0)])
    assert "abc" not in str(exc.value)


def test_range_on_boolean_columns_is_execution_error() -> None:
    for s in (pd.Series([True, False]), pd.Series([True, False], dtype="object")):
        with pytest.raises(QualityError, match="exige coluna numérica"):
            run_checks(pd.DataFrame({"v": s}), [_check("faixa", ["v"], minimo=0)])


def test_nullable_and_object_dtypes_are_handled() -> None:
    df = pd.DataFrame(
        {
            "i": pd.array([1, None, 1], dtype="Int64"),
            "s": pd.array(["ab", None, "ab"], dtype="string"),
            "b": pd.array([True, None, True], dtype="boolean"),
            "o": pd.Series(["ab", None, "ab"], dtype="object"),
        }
    )
    for col in df.columns:
        assert _one(df, _check("nao_nulo", [col])).taxa == pytest.approx(2 / 3)
        assert _one(df, _check("unico", [col])).taxa == pytest.approx(2 / 3)  # nulo não é repetido
    assert _one(df, _check("faixa", ["i"], minimo=1, maximo=1)).taxa == 1.0
    assert _one(df, _check("conjunto", ["i"], valores=[1])).taxa == 1.0
    assert _one(df, _check("conjunto", ["b"], valores=[True])).taxa == 1.0
    assert _one(df, _check("conjunto", ["s"], valores=["ab"])).taxa == 1.0
    assert _one(df, _check("formato", ["s"], padrao="ab")).taxa == 1.0
    assert _one(df, _check("formato", ["o"], padrao="ab")).taxa == 1.0
    assert _one(df, _check("formato", ["o"], padrao="x")).taxa == 0.0


def test_threshold_zero_always_approves() -> None:
    df = pd.DataFrame({"a": [None, None]})
    r = _one(df, _check("nao_nulo", ["a"], limiar=0.0))
    assert (r.taxa, r.aprovado, r.limiar) == (0.0, True, 0.0)


def test_unknown_check_type_is_actionable_error() -> None:
    bogus = QualityCheck.model_construct(tipo="inventada", colunas=["a"], limiar=1.0, bloqueante=False)
    with pytest.raises(QualityError, match="'inventada'.*nao_nulo"):
        run_checks(pd.DataFrame({"a": [1]}), [bogus])


def test_duplicate_column_names_are_execution_error() -> None:
    df = pd.DataFrame([[1, 2]], columns=["a", "a"])
    with pytest.raises(QualityError, match="'a'.*duplicad"):
        run_checks(df, [_check("nao_nulo", ["a"])])
