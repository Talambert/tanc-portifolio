"""Planejador de tarefas: traduz o `Pipeline` em especificações neutras (sem Airflow, sem Google).

Fase 1 (Tarefa 16): só etapas `ingestao`. Os demais tipos levantam `ConfigError` até as Tarefas 20 a 22.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from dataeng_core.base.errors import ConfigError
from dataeng_core.base.pipeline import IngestaoStep, Pipeline

TaskKind = Literal["cloud_run_job", "dataproc_batch", "bigquery_job"]

# Run id da execução lógica da DAG: expressão Jinja do Airflow (ex.: 20261019T060000). Estável entre as
# tentativas (retries) da mesma execução e começa com AAAAMMDD, como `validate_run_id` exige.
RUN_ID_TEMPLATE = "{{ ts_nodash }}"

_KIND_BY_MOTOR: dict[str, TaskKind] = {"cloud_run": "cloud_run_job", "dataproc": "dataproc_batch"}


@dataclass
class TaskSpec:
    task_id: str
    kind: TaskKind
    command: list[str]
    depends_on: list[str] = field(default_factory=list)
    resources: dict[str, Any] = field(default_factory=dict)


def plan_tasks(pipeline: Pipeline, *, config_path: str, ambiente: str = "prod") -> list[TaskSpec]:
    """Uma tarefa por etapa, em ordem topológica estável; `depends_on` segue `depende_de`."""
    tasks: list[TaskSpec] = []
    for step in pipeline.ordered_steps():
        if not isinstance(step, IngestaoStep):
            raise ConfigError(
                f"o tipo de etapa '{step.tipo}' (etapa '{step.id}') ainda não é planejado nesta versão; "
                "só etapas do tipo 'ingestao' viram tarefas da DAG.",
                operation="plan_tasks",
            )
        motor = step.motor or "cloud_run"
        tasks.append(
            TaskSpec(
                task_id=step.id,
                kind=_KIND_BY_MOTOR[motor],
                command=[
                    "python",
                    "-m",
                    "dataeng_core.jobs.run_step",
                    "--config",
                    config_path,
                    "--etapa",
                    step.id,
                    "--ambiente",
                    ambiente,
                    "--run-id",
                    RUN_ID_TEMPLATE,
                ],
                depends_on=list(step.depende_de),
                resources=dict(pipeline.execucao_padrao.get(motor, {})),
            )
        )
    return tasks
