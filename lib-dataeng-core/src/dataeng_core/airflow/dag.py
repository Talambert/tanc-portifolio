"""Montador de DAG. O Airflow só é importado DENTRO de `build_dag` (extra `[airflow]`).

As funções auxiliares abaixo montam apenas dicionários e textos, sem Airflow, e por isso são testáveis aqui.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dataeng_core.airflow.planner import TaskSpec, plan_tasks
from dataeng_core.base.errors import ConfigError
from dataeng_core.base.params import Environment
from dataeng_core.jobs.run_step import load_project

# Argumentos que o job do Cloud Run recebe: o ENTRYPOINT da imagem é `python -m dataeng_core.jobs.run_step`
# e a execução só pode sobrescrever `args` (por isso se descartam os 3 primeiros itens do comando planejado).
_ENTRYPOINT_ARGS = 3
# Ponto de entrada do lote do Dataproc: a imagem/arquivo real é definido pela Tarefa 21 (executor Spark).
DEFAULT_DATAPROC_MAIN = "local:///opt/dataeng/run_step_entrypoint.py"
DEFAULT_DATAPROC_RUNTIME = "2.2"


def _slug(text: str, limit: int) -> str:
    return re.sub(r"[^a-z0-9-]+", "-", text.lower()).strip("-")[:limit].strip("-")


def cloud_run_job_name(pipeline_name: str) -> str:
    """Nome do Cloud Run Job, derivado de `pipeline.nome` (um job por pipeline): minúsculas, dígitos e '-'.

    A infraestrutura (Tarefa 17) precisa criar o job com EXATAMENTE este nome.
    """
    name = _slug(pipeline_name, 63)
    if not name:
        raise ConfigError(
            f"o nome do pipeline '{pipeline_name}' não gera um nome válido de Cloud Run Job; "
            "use letras ou dígitos no campo `nome`.",
            operation="cloud_run_job_name",
        )
    return name


def cloud_run_overrides(spec: TaskSpec) -> dict[str, Any]:
    """`overrides` da execução do job: argumentos da etapa e, se houver, o timeout (cpu/memória são do job)."""
    overrides: dict[str, Any] = {"container_overrides": [{"args": spec.command[_ENTRYPOINT_ARGS:]}]}
    if "timeout" in spec.resources:
        try:
            overrides["timeout"] = f"{int(spec.resources['timeout'])}s"
        except (TypeError, ValueError) as exc:
            raise ConfigError(
                f"`timeout` inválido em execucao_padrao.cloud_run: {spec.resources['timeout']!r}; "
                "informe os segundos como número inteiro (ex.: 3600).",
                operation="cloud_run_overrides",
            ) from exc
    return overrides


def dataproc_batch_id(pipeline_name: str, task_id: str) -> str:
    """Template do `batch_id`: `<prefixo>-<hash6>-<ts_nodash em minúsculas>-<tentativa>`.

    Inclui a tentativa (`ti.try_number`) para que um retry não reaproveite o batch que falhou; o `--run-id`
    segue estável (`ts_nodash`), então a carga continua idempotente. Cabe sempre em 63 caracteres, começa por
    letra minúscula e termina em dígito; o hash de `nome:task_id` evita colisão entre ids longos truncados.
    """
    base = _slug(f"{pipeline_name}-{task_id}", 200)
    if not base or not base[0].isalpha():
        base = "b-" + base
    base = base[:30].strip("-") or "b"
    digest = hashlib.sha1(f"{pipeline_name}:{task_id}".encode()).hexdigest()[:6]
    return f"{base}-{digest}-{{{{ ts_nodash | lower }}}}-{{{{ ti.try_number }}}}"


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


def build_dag(
    config_path: str,
    *,
    ambiente: str = "prod",
    extra_tasks: Callable[[Any], None] | None = None,
    job_config_path: str | None = None,
    **dag_kwargs: Any,
) -> Any:
    """Cria a DAG do pipeline: uma tarefa por etapa e dependências `>>` conforme `depende_de`.

    `extra_tasks(dag)` é chamado dentro do contexto da DAG, depois das tarefas da biblioteca.
    `config_path`: onde o parse da DAG lê o yaml (use caminho absoluto resolvido a partir do `__file__` do
    arquivo da DAG). `job_config_path`: caminho do mesmo arquivo DENTRO da imagem do job (relativo à WORKDIR ou
    absoluto Linux); vai em `--config`. Sem ele, usa-se `config_path`.

    De `execucao_padrao` a montagem consome só `cloud_run.timeout` (override da execução) e
    `dataproc.runtime`, `dataproc.executores` e `dataproc.main_python_file_uri`; as demais chaves (cpu, memoria...)
    são ignoradas aqui: pertencem à definição do job. O Cloud Run Job deve se chamar `cloud_run_job_name(nome)`.

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

    pipeline, env, _ = load_project(Path(config_path), ambiente, {})
    specs = plan_tasks(pipeline, config_path=config_path, ambiente=ambiente, job_config_path=job_config_path)

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
