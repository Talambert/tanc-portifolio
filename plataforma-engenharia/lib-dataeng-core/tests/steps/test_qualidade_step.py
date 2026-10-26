from __future__ import annotations

import dataclasses
import json
import logging
from typing import Any

import pandas as pd
import pytest
from tests.steps.conftest import MakeCtx
from tests.steps.helpers import pipeline, rows

from dataeng_core.base.errors import SqlError
from dataeng_core.steps import StepContext, run_step
from dataeng_core.steps.registry import registered_types

SECRET_VALUE = "VALOR-SENSIVEL-777"
CONTROL = "silver._controle_execucoes"


def _step(*checks: dict[str, Any]) -> Any:
    p = pipeline({"id": "q", "tipo": "qualidade", "verificacoes": list(checks)})
    return p, p.etapas[0]


def _load(ctx: StepContext, table: str, data: dict[str, list[Any]]) -> None:
    ctx.warehouse.load(table, pd.DataFrame(data), mode="append")


def _control(ctx: StepContext) -> list[tuple[Any, ...]]:
    return rows(ctx, f"SELECT etapa, status, qualidade, detalhe FROM {CONTROL} ORDER BY registrado_em")


class SpyWarehouse:
    """Delega ao warehouse real e registra as consultas; `fail` faz a consulta levantar a exceção dada."""

    def __init__(self, inner: Any, fail: Exception | None = None) -> None:
        self.inner = inner
        self.queries: list[str] = []
        self.fail = fail

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def query(self, sql: str) -> pd.DataFrame:
        self.queries.append(sql)
        if self.fail is not None:
            raise self.fail
        return self.inner.query(sql)  # type: ignore[no-any-return]


def test_quality_type_is_registered() -> None:
    assert "qualidade" in registered_types()


def test_quality_step_records_results_and_fails_when_blocking(make_ctx: MakeCtx) -> None:
    p, step = _step(
        {"nao_nulo": "id", "tabela": "silver.vendas", "bloqueante": True},
        {"conjunto": "produto", "valores": ["gasolina", "etanol"], "tabela": "silver.vendas", "limiar": 0.9},
    )
    ctx = make_ctx(p)
    _load(ctx, "silver.vendas", {"id": ["1", None, "3"], "produto": ["gasolina", "etanol", SECRET_VALUE]})

    result = run_step(step, ctx)

    assert result.status == "falha"
    assert [(q.nome, q.coluna, q.aprovado, q.bloqueante) for q in result.quality] == [
        ("nao_nulo", "id", False, True),
        ("conjunto", "produto", False, False),
    ]
    assert result.rows_read == 3
    assert "fase verificação" in result.detail and "referência" in result.detail
    assert "tabela 'silver.vendas'" in result.detail and "nao_nulo em 'id'" in result.detail
    assert "taxa 0.6667" in result.detail and "limiar 1" in result.detail
    ((etapa, status, qualidade, detalhe),) = _control(ctx)
    assert (etapa, status) == ("q", "falha")
    saved = json.loads(qualidade)
    assert [(q["nome"], q["coluna"], q["aprovado"], q["bloqueante"]) for q in saved] == [
        ("nao_nulo", "id", False, True),
        ("conjunto", "produto", False, False),
    ]
    assert saved[0]["taxa"] == pytest.approx(2 / 3) and saved[0]["limiar"] == 1.0
    assert detalhe == result.detail


def test_quality_failure_never_contains_row_values(make_ctx: MakeCtx, caplog: pytest.LogCaptureFixture) -> None:
    p, step = _step(
        {"conjunto": "produto", "valores": ["gasolina"], "tabela": "silver.vendas", "bloqueante": True},
        {"formato": "produto", "padrao": "[a-z]+", "tabela": "silver.vendas"},
    )
    ctx = make_ctx(p)
    _load(ctx, "silver.vendas", {"produto": ["gasolina", SECRET_VALUE]})
    with caplog.at_level(logging.DEBUG):
        result = run_step(step, ctx)
    everything = result.detail + caplog.text + repr(_control(ctx)) + repr(result.quality)
    assert result.status == "falha"
    assert SECRET_VALUE not in everything


def test_non_blocking_failure_is_ok_with_warning_in_detail_and_log(
    make_ctx: MakeCtx, caplog: pytest.LogCaptureFixture
) -> None:
    p, step = _step(
        {"nao_nulo": "id", "tabela": "silver.vendas"},
        {"faixa": "valor", "minimo": 0, "tabela": "silver.vendas", "bloqueante": True},
    )
    ctx = make_ctx(p)
    _load(ctx, "silver.vendas", {"id": ["1", None], "valor": [5.0, 7.5]})
    with caplog.at_level(logging.WARNING):
        result = run_step(step, ctx)
    assert result.status == "ok"
    assert [(q.nome, q.aprovado) for q in result.quality] == [("nao_nulo", False), ("faixa", True)]
    assert "aviso" in result.detail and "nao_nulo em 'id'" in result.detail and "silver.vendas" in result.detail
    assert "não bloqueante" in caplog.text and "nao_nulo em 'id'" in caplog.text
    assert _control(ctx)[0][1] == "ok"


def test_all_checks_pass_is_ok_without_warning(make_ctx: MakeCtx) -> None:
    p, step = _step({"nao_nulo": ["id", "produto"], "tabela": "silver.vendas", "bloqueante": True})
    ctx = make_ctx(p)
    _load(ctx, "silver.vendas", {"id": ["1", "2"], "produto": ["a", "b"]})
    result = run_step(step, ctx)
    assert result.status == "ok" and len(result.quality) == 2 and "aviso" not in result.detail
    assert result.detail == "2 verificações em 1 tabela(s) executadas"


def test_each_table_is_queried_once_with_only_the_needed_columns(make_ctx: MakeCtx) -> None:
    p, step = _step(
        {"nao_nulo": "id", "tabela": "silver.vendas"},
        {"unico": ["id", "produto"], "tabela": "silver.vendas"},
        {"nao_nulo": "a", "tabela": "silver.outra"},
    )
    ctx = make_ctx(p)
    spy = SpyWarehouse(ctx.warehouse)
    ctx = dataclasses.replace(ctx, warehouse=spy)
    _load(ctx, "silver.vendas", {"id": ["1"], "produto": ["x"], "ignorada": ["y"]})
    _load(ctx, "silver.outra", {"a": ["1"], "b": ["2"]})
    assert run_step(step, ctx).status == "ok"
    assert spy.queries == ["SELECT id, produto FROM silver.vendas", "SELECT a FROM silver.outra"]


def test_blocking_failure_still_evaluates_the_other_tables(make_ctx: MakeCtx) -> None:
    p, step = _step(
        {"nao_nulo": "id", "tabela": "silver.a", "bloqueante": True},
        {"nao_nulo": "id", "tabela": "silver.b", "bloqueante": True},
    )
    ctx = make_ctx(p)
    _load(ctx, "silver.a", {"id": [None]})
    _load(ctx, "silver.b", {"id": [None]})
    result = run_step(step, ctx)
    assert result.status == "falha" and len(result.quality) == 2
    assert "tabela 'silver.a'" in result.detail and "tabela 'silver.b'" in result.detail


def test_missing_table_is_an_actionable_failure(make_ctx: MakeCtx) -> None:
    p, step = _step({"nao_nulo": "id", "tabela": "silver.nao_existe"})
    ctx = make_ctx(p)
    result = run_step(step, ctx)
    assert result.status == "falha"
    assert "fase consulta" in result.detail and "'silver.nao_existe' não existe" in result.detail
    assert "referência" in result.detail
    assert _control(ctx)[0][1] == "falha"


def test_unexecutable_check_fails_with_actionable_detail(make_ctx: MakeCtx) -> None:
    p, step = _step({"faixa": "produto", "minimo": 0, "tabela": "silver.vendas"})
    ctx = make_ctx(p)
    _load(ctx, "silver.vendas", {"produto": ["gasolina"]})
    result = run_step(step, ctx)
    assert result.status == "falha"
    assert "fase verificação" in result.detail and "não pôde ser executada" in result.detail
    assert "'produto'" in result.detail and "referência" in result.detail
    assert _control(ctx)[0][1] == "falha"


def test_missing_column_fails_in_the_query_phase_naming_the_columns(make_ctx: MakeCtx) -> None:
    p, step = _step({"nao_nulo": "inexistente", "tabela": "silver.vendas"})
    ctx = make_ctx(p)
    _load(ctx, "silver.vendas", {"id": ["1"]})
    result = run_step(step, ctx)
    assert result.status == "falha"
    assert "fase consulta" in result.detail and "(inexistente)" in result.detail and "referência" in result.detail


def test_invalid_regex_fails_instead_of_passing(make_ctx: MakeCtx) -> None:
    p, step = _step({"formato": "produto", "padrao": "([a-z", "tabela": "silver.vendas"})
    ctx = make_ctx(p)
    _load(ctx, "silver.vendas", {"produto": ["a"]})
    result = run_step(step, ctx)
    assert result.status == "falha" and "não pôde ser executada" in result.detail


def test_column_case_may_differ_from_the_yaml(make_ctx: MakeCtx) -> None:
    p, step = _step({"nao_nulo": "ID", "tabela": "silver.vendas", "bloqueante": True})
    ctx = make_ctx(p)
    _load(ctx, "silver.vendas", {"id": ["1", "2"]})
    result = run_step(step, ctx)
    assert result.status == "ok" and result.quality[0].coluna == "ID" and result.quality[0].taxa == 1.0


def test_unsafe_identifiers_are_refused_before_any_query(make_ctx: MakeCtx) -> None:
    for check in (
        {"nao_nulo": "id", "tabela": "silver.vendas; DROP TABLE silver.vendas"},
        {"nao_nulo": "id) FROM silver.vendas --", "tabela": "silver.vendas"},
        {"nao_nulo": "id", "tabela": "a.b.c"},
    ):
        p, step = _step({"nao_nulo": "id", "tabela": "silver.vendas"}, check)
        ctx = make_ctx(p)
        spy = SpyWarehouse(ctx.warehouse)
        ctx = dataclasses.replace(ctx, warehouse=spy)
        _load(ctx, "silver.vendas", {"id": ["1"]})
        result = run_step(step, ctx)
        assert result.status == "falha" and "fase validação" in result.detail, check
        assert spy.queries == [], check
        assert ctx.warehouse.table_exists("silver.vendas")


def test_warehouse_failure_detail_has_class_and_reference_but_not_the_message(
    make_ctx: MakeCtx, caplog: pytest.LogCaptureFixture
) -> None:
    p, step = _step({"nao_nulo": "id", "tabela": "silver.vendas"})
    ctx = make_ctx(p)
    _load(ctx, "silver.vendas", {"id": ["1"]})
    boom = SqlError(f"erro perto de '{SECRET_VALUE}'", operation="warehouse.query")
    real = ctx
    ctx = dataclasses.replace(ctx, warehouse=SpyWarehouse(ctx.warehouse, fail=boom))
    with caplog.at_level(logging.DEBUG):
        result = run_step(step, ctx)
    assert result.status == "falha"
    assert "SqlError" in result.detail and boom.reference in result.detail and "fase consulta" in result.detail
    assert SECRET_VALUE not in result.detail + caplog.text + repr(_control(real))


def test_unexpected_warehouse_exception_is_class_only(make_ctx: MakeCtx) -> None:
    p, step = _step({"nao_nulo": "id", "tabela": "silver.vendas"})
    ctx = make_ctx(p)
    _load(ctx, "silver.vendas", {"id": ["1"]})
    ctx = dataclasses.replace(ctx, warehouse=SpyWarehouse(ctx.warehouse, fail=RuntimeError(SECRET_VALUE)))
    result = run_step(step, ctx)
    assert result.status == "falha" and "RuntimeError" in result.detail and SECRET_VALUE not in result.detail


def test_empty_table_passes_by_vacuity(make_ctx: MakeCtx, caplog: pytest.LogCaptureFixture) -> None:
    p, step = _step({"nao_nulo": "id", "tabela": "silver.vazia", "bloqueante": True})
    ctx = make_ctx(p)
    ctx.warehouse.execute_ddl("CREATE TABLE silver.vazia (id VARCHAR)")
    with caplog.at_level(logging.WARNING):
        result = run_step(step, ctx)
    assert result.status == "ok" and result.rows_read == 0 and result.quality[0].taxa == 1.0
    assert "vacuidade" in caplog.text


def test_non_blocking_warnings_of_every_table_are_reported(make_ctx: MakeCtx) -> None:
    p, step = _step(
        {"nao_nulo": "id", "tabela": "silver.a"},
        {"nao_nulo": "id", "tabela": "silver.b"},
    )
    ctx = make_ctx(p)
    _load(ctx, "silver.a", {"id": [None]})
    _load(ctx, "silver.b", {"id": [None]})
    result = run_step(step, ctx)
    assert result.status == "ok" and result.detail.count("nao_nulo em 'id'") == 2
    assert "tabela 'silver.a'" in result.detail and "tabela 'silver.b'" in result.detail


def test_columns_differing_only_by_case_are_selected_once(make_ctx: MakeCtx) -> None:
    p, step = _step(
        {"nao_nulo": "ID", "tabela": "silver.vendas"},
        {"unico": "id", "tabela": "silver.vendas"},
    )
    ctx = make_ctx(p)
    spy = SpyWarehouse(ctx.warehouse)
    ctx = dataclasses.replace(ctx, warehouse=spy)
    _load(ctx, "silver.vendas", {"id": ["1", "2"]})
    assert run_step(step, ctx).status == "ok"
    assert spy.queries == ["SELECT ID FROM silver.vendas"]
