"""Etapas do pipeline: contexto, executor, controle e os tipos de etapa.

Cada módulo de tipo de etapa se registra ao ser importado; este pacote importa todos.
"""

from dataeng_core.steps import ingestao  # noqa: F401  (registra o tipo "ingestao")
from dataeng_core.steps.context import StepContext, StepResult, build_context
from dataeng_core.steps.control import record_result
from dataeng_core.steps.registry import StepRunner, register_step_type, run_pipeline, run_step

__all__ = [
    "StepContext",
    "StepResult",
    "StepRunner",
    "build_context",
    "record_result",
    "register_step_type",
    "run_pipeline",
    "run_step",
]
