from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from tests.steps.conftest import MakeCtx
from tests.steps.helpers import CONTROL, RID1, FlakyWarehouse, environment, ingest_step, pipeline, rows

import dataeng_core.steps.registry as registry
from dataeng_core.base.errors import ConfigError, SourceError
from dataeng_core.base.pipeline import Step
from dataeng_core.steps import (
    StepContext,
    StepResult,
    build_context,
    register_step_type,
    run_pipeline,
    run_step,
)


@pytest.fixture(autouse=True)
def isolated_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cada teste registra tipos numa cópia do registro."""
    monkeypatch.setattr(registry, "_RUNNERS", dict(registry._RUNNERS))


def _sql(step_id: str, **kw: Any) -> dict[str, Any]:
    return {"id": step_id, "tipo": "sql", "sql": [f"sql/{step_id}.sql"], **kw}


def _statuses(ctx: StepContext) -> list[tuple[str, str]]:
    return [(str(e), str(s)) for e, s in rows(ctx, f"SELECT etapa, status FROM {CONTROL} ORDER BY registrado_em")]


def test_ingestao_is_registered_when_package_is_imported() -> None:
    assert "ingestao" in registry.registered_types()


def test_unknown_step_type_raises_config_error(make_ctx: MakeCtx) -> None:
    registry._RUNNERS.pop("sql", None)  # tipos de etapas futuras: garantir que não estão registrados
    pipe = pipeline(_sql("a"))
    ctx = make_ctx(pipe)
    with pytest.raises(ConfigError) as err:
        run_step(pipe.get("a"), ctx)
    assert "tipo de etapa 'sql'" in str(err.value) and "ingestao" in str(err.value)
    assert ctx.warehouse.table_exists(CONTROL) is False

    with pytest.raises(ConfigError):
        run_step(SimpleNamespace(id="x", tipo="inexistente"), ctx)  # type: ignore[arg-type]


def test_register_step_type_adds_new_runner(make_ctx: MakeCtx) -> None:
    calls: list[tuple[str, str]] = []

    def runner(step: Step, ctx: StepContext) -> StepResult:
        calls.append((step.id, ctx.run_id))
        return StepResult(step.id, "ok", rows_written=7, detail="feito")

    register_step_type("sql", runner)
    pipe = pipeline(_sql("a"))
    ctx = make_ctx(pipe)

    result = run_step(pipe.get("a"), ctx)

    assert calls == [("a", RID1)]
    assert (result.status, result.rows_written) == ("ok", 7)
    assert rows(ctx, f"SELECT run_id, etapa, status, linhas_gravadas, detalhe FROM {CONTROL}") == [
        (RID1, "a", "ok", 7, "feito")
    ]


def test_register_step_type_rejects_invalid_arguments() -> None:
    with pytest.raises(ConfigError):
        register_step_type("", lambda s, c: StepResult(s.id, "ok"))
    with pytest.raises(ConfigError):
        register_step_type("sql", "nao_chamavel")  # type: ignore[arg-type]


def test_run_pipeline_stops_after_first_failure(make_ctx: MakeCtx) -> None:
    executed: list[str] = []

    def sql_runner(step: Step, ctx: StepContext) -> StepResult:
        executed.append(step.id)
        return StepResult(step.id, "ok")

    def view_runner(step: Step, ctx: StepContext) -> StepResult:
        executed.append(step.id)
        raise SourceError("tabela de origem indisponível", operation="teste")

    register_step_type("sql", sql_runner)
    register_step_type("view", view_runner)
    pipe = pipeline(
        _sql("c", depende_de=["b"]),
        _sql("a", depende_de=[]),
        {"id": "b", "tipo": "view", "sql": "sql/b.sql", "depende_de": ["a"]},
        _sql("d", depende_de=["c"]),
    )
    ctx = make_ctx(pipe)

    results = run_pipeline(pipe, ctx)

    assert executed == ["a", "b"]
    assert [(r.step_id, r.status) for r in results] == [
        ("a", "ok"),
        ("b", "falha"),
        ("c", "nao_executada"),
        ("d", "nao_executada"),
    ]
    assert "tabela de origem indisponível" in results[1].detail
    assert "'b' falhou" in results[2].detail
    assert _statuses(ctx) == [("a", "ok"), ("b", "falha"), ("c", "nao_executada"), ("d", "nao_executada")]


def test_run_pipeline_only_runs_selected_steps_in_order(make_ctx: MakeCtx) -> None:
    executed: list[str] = []
    register_step_type("sql", lambda s, c: executed.append(s.id) or StepResult(s.id, "ok"))  # type: ignore[func-returns-value]
    pipe = pipeline(_sql("a"), _sql("b"), _sql("c"))
    ctx = make_ctx(pipe)
    results = run_pipeline(pipe, ctx, only=["c", "a"])
    assert executed == ["a", "c"]
    assert [r.step_id for r in results] == ["a", "c"]

    with pytest.raises(ConfigError, match="'zzz' não existe"):
        run_pipeline(pipe, ctx, only=["zzz"])
    with pytest.raises(ConfigError):
        run_pipeline(pipe, ctx, only="a")
    assert executed == ["a", "c"]


def test_run_pipeline_unknown_type_fails_before_running_anything(make_ctx: MakeCtx) -> None:
    executed: list[str] = []
    register_step_type("sql", lambda s, c: executed.append(s.id) or StepResult(s.id, "ok"))  # type: ignore[func-returns-value]
    registry._RUNNERS.pop("view", None)
    pipe = pipeline(_sql("a"), {"id": "b", "tipo": "view", "sql": "sql/b.sql"})
    ctx = make_ctx(pipe)
    with pytest.raises(ConfigError, match="'view'"):
        run_pipeline(pipe, ctx)
    assert executed == []


def test_runner_exception_becomes_failure_without_raw_message(make_ctx: MakeCtx) -> None:
    def runner(step: Step, ctx: StepContext) -> StepResult:
        raise ValueError("cnpj 12345678000199 inválido")

    register_step_type("sql", runner)
    pipe = pipeline(_sql("a"))
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("a"), ctx)
    assert result.status == "falha"
    assert "ValueError" in result.detail and "12345678000199" not in result.detail
    assert _statuses(ctx) == [("a", "falha")]


def test_runner_returning_wrong_type_is_failure(make_ctx: MakeCtx) -> None:
    register_step_type("sql", lambda s, c: None)  # type: ignore[arg-type,return-value]
    pipe = pipeline(_sql("a"))
    result = run_step(pipe.get("a"), make_ctx(pipe))
    assert result.status == "falha" and "StepResult" in result.detail


def test_control_write_failure_never_leaves_step_ok(make_ctx: MakeCtx) -> None:
    register_step_type("sql", lambda s, c: StepResult(s.id, "ok"))
    pipe = pipeline(_sql("a"))
    ctx = make_ctx(pipe)
    ctx.warehouse = FlakyWarehouse(ctx.warehouse, CONTROL, times=1)  # type: ignore[assignment]
    result = run_step(pipe.get("a"), ctx)
    assert result.status == "falha"
    assert "tabela de controle não foi gravada" in result.detail


def test_build_context(project: Path) -> None:
    pipe = pipeline(ingest_step())
    env = environment()
    ctx = build_context(pipe, env, project_root=project)
    try:
        assert ctx.run_id and ctx.params["run_id"] == ctx.run_id
        assert ctx.params["dataset_silver"] == "silver"
        assert Path(ctx.env.pasta_local) == (project / ".local").resolve()  # relativo à raiz, não ao cwd
        assert ctx.pubsub is None and ctx.project_root == project.resolve()
        ctx.storage.write("gs://b/x.txt", b"1")
        assert (project / ".local" / "b" / "x.txt").read_bytes() == b"1"
    finally:
        ctx.warehouse.close()  # type: ignore[attr-defined]

    for bad in ["", "../x", "a/b", "run id", "a:b", "x" * 129]:
        with pytest.raises(ConfigError):
            build_context(pipe, env, project_root=project, run_id=bad)
