"""Registro de tipos de etapa e executor (`run_step`, `run_pipeline`).

Cada módulo de tipo de etapa se registra ao ser importado (`register_step_type`); `dataeng_core.steps`
importa todos. O executor grava cada resultado na tabela de controle, inclusive falhas e etapas não
executadas, e converte qualquer erro do runner em `StepResult(status="falha")` com texto acionável.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import replace

from dataeng_core.base.errors import ConfigError, DataEngError
from dataeng_core.base.pipeline import Pipeline, Step
from dataeng_core.base.runtime import get_logger, run_id_var
from dataeng_core.steps.context import StepContext, StepResult
from dataeng_core.steps.control import record_result

log = get_logger(__name__)

StepRunner = Callable[[Step, StepContext], StepResult]

_RUNNERS: dict[str, StepRunner] = {}


def register_step_type(tipo: str, runner: StepRunner) -> None:
    """Associa um tipo de etapa (`step.tipo`) ao seu runner; registrar de novo substitui o anterior."""
    if not isinstance(tipo, str) or not tipo.strip():
        raise ConfigError("o tipo de etapa deve ser um texto não vazio.", operation="register_step_type")
    if not callable(runner):
        raise ConfigError(f"o runner do tipo '{tipo}' não é chamável.", operation="register_step_type")
    _RUNNERS[tipo] = runner


def registered_types() -> list[str]:
    return sorted(_RUNNERS)


def _runner_for(step: Step) -> StepRunner:
    tipo = getattr(step, "tipo", None)
    runner = _RUNNERS.get(tipo) if isinstance(tipo, str) else None
    if runner is None:
        raise ConfigError(
            f"tipo de etapa '{tipo}' (etapa '{getattr(step, 'id', '?')}') não tem executor registrado; "
            f"tipos disponíveis: {', '.join(registered_types()) or 'nenhum'}",
            operation="run_step",
        )
    return runner


def _record(ctx: StepContext, result: StepResult) -> StepResult:
    """Grava no controle. Se a gravação falhar, a etapa não pode ficar como "ok"."""
    problem: str | None = None
    try:
        record_result(ctx.warehouse, ctx.env, result, ctx.run_id)
    except DataEngError as exc:
        problem = str(exc)
    except Exception as exc:  # o warehouse deveria levantar DataEngError; não deixar escapar exceção crua
        problem = f"erro inesperado ({type(exc).__name__})"
    if problem is None:
        return result
    log.error(f"etapa '{result.step_id}': falha ao gravar a tabela de controle: {problem}")
    if result.status != "ok":
        return result
    return replace(
        result,
        status="falha",
        detail=f"os dados foram processados, mas a tabela de controle não foi gravada ({problem}); "
        "reexecute a etapa (a carga é idempotente)",
    )


def run_step(step: Step, ctx: StepContext) -> StepResult:
    """Executa uma etapa, grava o resultado no controle e o devolve.

    Tipo sem executor levanta `ConfigError` (nada é executado nem gravado). Erros do runner viram
    `status="falha"`: `DataEngError` com sua mensagem; outras exceções só com o tipo (a mensagem
    delas pode conter valores dos dados).
    """
    runner = _runner_for(step)
    token = run_id_var.set(ctx.run_id)
    try:
        try:
            result = runner(step, ctx)
            if not isinstance(result, StepResult):
                result = StepResult(
                    step.id, "falha", detail=f"o executor do tipo '{step.tipo}' não devolveu um StepResult"
                )
        except DataEngError as exc:
            result = StepResult(step.id, "falha", detail=str(exc))
        except Exception as exc:
            result = StepResult(
                step.id,
                "falha",
                detail=f"erro inesperado no executor do tipo '{step.tipo}' ({type(exc).__name__}); veja o log",
            )
        result = _record(ctx, result)
        level = log.info if result.status == "ok" else log.error
        level(
            f"etapa '{step.id}' ({step.tipo}): {result.status}; lidas={result.rows_read} "
            f"gravadas={result.rows_written} rejeitadas={result.rows_rejected}"
            + (f"; {result.detail}" if result.detail and result.status != "ok" else "")
        )
        return result
    finally:
        run_id_var.reset(token)


def run_pipeline(pipeline: Pipeline, ctx: StepContext, *, only: Sequence[str] | None = None) -> list[StepResult]:
    """Executa as etapas na ordem de `ordered_steps` (ou só as de `only`, na mesma ordem).

    Para na primeira falha: as seguintes ficam `nao_executada` (e também vão para o controle).
    Ids desconhecidos em `only` e tipos sem executor levantam `ConfigError` antes de executar qualquer etapa.
    """
    steps = pipeline.ordered_steps()
    if only is not None:
        if isinstance(only, str):
            raise ConfigError("`only` deve ser uma lista de ids de etapa, não um texto.", operation="run_pipeline")
        for step_id in only:
            pipeline.get(step_id)
        wanted = set(only)
        steps = [s for s in steps if s.id in wanted]
    for step in steps:
        _runner_for(step)

    results: list[StepResult] = []
    failed: str | None = None
    for step in steps:
        if failed is not None:
            skipped = StepResult(step.id, "nao_executada", detail=f"não executada porque a etapa '{failed}' falhou")
            token = run_id_var.set(ctx.run_id)
            try:
                results.append(_record(ctx, skipped))
            finally:
                run_id_var.reset(token)
            continue
        result = run_step(step, ctx)
        results.append(result)
        if result.status != "ok":
            failed = step.id
    return results
