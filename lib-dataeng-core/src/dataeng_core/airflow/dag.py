"""Montador de DAG. O Airflow só é importado DENTRO de `build_dag` (extra `[airflow]`).

As funções auxiliares abaixo montam apenas dicionários e textos, sem Airflow, e por isso são testáveis aqui.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dataeng_core.airflow.planner import TaskSpec, plan_tasks
from dataeng_core.base.errors import ConfigError
from dataeng_core.base.params import Environment, load_environment
from dataeng_core.base.pipeline import load_pipeline

# Argumentos que o job do Cloud Run recebe: o ENTRYPOINT da imagem é `python -m dataeng_core.jobs.run_step`
# e a execução só pode sobrescrever `args` (por isso se descartam os 3 primeiros itens do comando planejado).
_ENTRYPOINT_ARGS = 3
# Ponto de entrada do lote do Dataproc: a imagem/arquivo real é definido pela Tarefa 21 (executor Spark).
DEFAULT_DATAPROC_MAIN = "local:///opt/dataeng/run_step_entrypoint.py"
DEFAULT_DATAPROC_RUNTIME = "2.2"


def _slug(text: str, limit: int) -> str:
    return re.sub(r"[^a-z0-9-]+", "-", text.lower()).strip("-")[:limit].strip("-")


def cloud_run_job_name(pipeline_name: str) -> str:
    """Nome do Cloud Run Job (um por pipeline): minúsculas, dígitos e '-'."""
    return _slug(pipeline_name, 63)


def cloud_run_overrides(spec: TaskSpec) -> dict[str, Any]:
    """`overrides` da execução do job: argumentos da etapa e, se houver, o timeout (cpu/memória são do job)."""
    overrides: dict[str, Any] = {"container_overrides": [{"args": spec.command[_ENTRYPOINT_ARGS:]}]}
    if "timeout" in spec.resources:
        overrides["timeout"] = f"{int(spec.resources['timeout'])}s"
    return overrides


def dataproc_batch_id(pipeline_name: str, task_id: str) -> str:
    """Prefixo do batch_id + execução (`ts_nodash` em minúsculas): estável entre retries da mesma execução."""
    prefix = _slug(f"{pipeline_name}-{task_id}", 40) or "dataeng"
    return prefix + "-{{ ts_nodash | lower }}"


def dataproc_batch(spec: TaskSpec, env: Environment) -> dict[str, Any]:
    """Corpo do lote do Dataproc Serverless (`runtime` e `executores` vêm de `execucao_padrao.dataproc`)."""
    batch: dict[str, Any] = {
        "pyspark_batch": {
            "main_python_file_uri": spec.resources.get("main_python_file_uri", DEFAULT_DATAPROC_MAIN),
            "args": spec.command[_ENTRYPOINT_ARGS:],
        },
        "runtime_config": {"version": str(spec.resources.get("runtime", DEFAULT_DATAPROC_RUNTIME))},
    }
    if "executores" in spec.resources:
        batch["runtime_config"]["properties"] = {"spark.executor.instances": str(spec.resources["executores"])}
    if env.service_account:
        batch["environment_config"] = {"execution_config": {"service_account": env.service_account}}
    return batch


def _environment(config_path: str, ambiente: str) -> Environment:
    pasta = Path(config_path).resolve().parent
    root = pasta.parent if pasta.name == "config" else pasta
    return load_environment(root / "config" / "ambientes" / f"{ambiente}.yaml")


def build_dag(
    config_path: str,
    *,
    ambiente: str = "prod",
    extra_tasks: Callable[[Any], None] | None = None,
    **dag_kwargs: Any,
) -> Any:
    """Cria a DAG do pipeline: uma tarefa por etapa e dependências `>>` conforme `depende_de`.

    `extra_tasks(dag)` é chamado dentro do contexto da DAG, depois das tarefas da biblioteca.
    `dag_kwargs` vai para `DAG(...)` e prevalece sobre os padrões (dag_id, schedule, start_date, catchup).
    """
    try:
        try:
            from airflow.sdk import DAG  # Airflow 3
        except ImportError:
            from airflow import DAG  # Airflow 2.x
        from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
        from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
    except ImportError as exc:
        raise ConfigError(
            "o Airflow (ou o provider do Google) não está instalado; instale o extra com "
            '`pip install "dataeng-core[airflow]"` (em Linux: o Airflow não roda no Windows).',
            operation="build_dag",
        ) from exc

    pipeline = load_pipeline(Path(config_path), ambiente=ambiente)
    env = _environment(config_path, ambiente)
    specs = plan_tasks(pipeline, config_path=config_path, ambiente=ambiente)

    options: dict[str, Any] = {
        "dag_id": pipeline.nome,
        "schedule": pipeline.agenda,
        "start_date": datetime(2024, 1, 1, tzinfo=UTC),
        "catchup": False,
    }
    options.update(dag_kwargs)
    with DAG(**options) as dag:
        tasks: dict[str, Any] = {}
        for spec in specs:
            if spec.kind == "cloud_run_job":
                tasks[spec.task_id] = CloudRunExecuteJobOperator(
                    task_id=spec.task_id,
                    project_id=env.projeto_gcp,
                    region=env.regiao,
                    job_name=cloud_run_job_name(pipeline.nome),
                    overrides=cloud_run_overrides(spec),
                )
            elif spec.kind == "dataproc_batch":
                tasks[spec.task_id] = DataprocCreateBatchOperator(
                    task_id=spec.task_id,
                    project_id=env.projeto_gcp,
                    region=env.regiao,
                    batch=dataproc_batch(spec, env),
                    batch_id=dataproc_batch_id(pipeline.nome, spec.task_id),
                )
            else:
                raise ConfigError(f"tipo de tarefa '{spec.kind}' ainda não é montado", operation="build_dag")
        for spec in specs:
            for dep in spec.depends_on:
                tasks[dep] >> tasks[spec.task_id]
        if extra_tasks is not None:
            extra_tasks(dag)
    return dag
