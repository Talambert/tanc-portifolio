"""Etapas do pipeline: contexto, executor, controle e os tipos de etapa.

Cada módulo de tipo de etapa se registra ao ser importado; este pacote importa todos.
"""

# noqa: F401 abaixo: os módulos registram "ingestao", "sql", "view", "python" e "spark" ao serem importados
from dataeng_core.steps import ingestao, python_step, spark_step, sql_steps  # noqa: F401
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
