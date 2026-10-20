"""Etapa `python`: chama `funcao(ctx)` (`"modulo:funcao"`, resolvida a partir da raiz do projeto).

A função recebe o `StepContext` (storage, warehouse, parâmetros, run_id...) e devolve `StepResult` ou `None`
(`None` vira `ok`). Um `StepResult` devolvido é gravado com o id da etapa, mesmo que traga outro.

Falha da função: o resultado (e portanto o CLI e a tabela de controle) traz só a CLASSE da exceção e uma
referência; NUNCA a mensagem dela, que pode citar valores dos dados. O traceback, com os segredos redigidos e a
mesma referência, vai para o log.
"""

from __future__ import annotations

import dataclasses
import secrets as _secrets

from dataeng_core.base.errors import ConfigError, DataEngError
from dataeng_core.base.pipeline import PythonStep, Step
from dataeng_core.base.runtime import redact
from dataeng_core.steps.context import StepContext, StepResult
from dataeng_core.steps.registry import log_unexpected, register_step_type, step_secrets
from dataeng_core.transform.hooks import resolve_hook


def _run_python_step(step: Step, ctx: StepContext) -> StepResult:
    if not isinstance(step, PythonStep):
        raise ConfigError(f"a etapa '{step.id}' não é do tipo python", operation="python")
    fn = resolve_hook(step.funcao, project_root=ctx.project_root)  # ConfigError acionável se não existir
    secrets = step_secrets(step)
    try:
        returned = fn(ctx)  # type: ignore[arg-type]  # o gancho da etapa recebe o contexto, não um DataFrame
    except Exception as exc:
        reference = exc.reference if isinstance(exc, DataEngError) else _secrets.token_hex(4)
        log_unexpected(step.id, exc, secrets, where=f" na função {step.funcao} [referência {reference}]")
        return StepResult(
            step.id,
            "falha",
            detail=(
                f"a função {step.funcao} falhou ({type(exc).__name__}); referência {reference}; "
                "veja o log com essa referência, corrija a função e reexecute a etapa"
            ),
        )
    if returned is None:
        return StepResult(step.id, "ok", detail=f"função {step.funcao} executada")
    if not isinstance(returned, StepResult):
        return StepResult(
            step.id,
            "falha",
            detail=f"a função {step.funcao} devolveu {type(returned).__name__}; ela deve devolver StepResult ou None",
        )
    return dataclasses.replace(returned, step_id=step.id, detail=redact(returned.detail, secrets))


register_step_type("python", _run_python_step)
