from __future__ import annotations

import subprocess
import sys
import textwrap
from typing import Any

import pytest
from tests.steps.helpers import ingest_step

from dataeng_core.airflow.planner import RUN_ID_TEMPLATE, TaskSpec, plan_tasks
from dataeng_core.base.errors import ConfigError
from dataeng_core.base.pipeline import Pipeline
from dataeng_core.steps.context import validate_run_id

CONFIG = "config/pipeline.yaml"
WIN_CONFIG = "C:" + chr(92) + "proj" + chr(92) + "config" + chr(92) + "pipeline.yaml"


def _pipeline(etapas: list[dict[str, Any]], **extra: Any) -> Pipeline:
    return Pipeline.model_validate(
        {"schema_version": 1, "nome": "vendas", "agenda": "0 6 * * 1", "etapas": etapas, **extra}
    )


def _command(
    step_id: str, ambiente: str = "prod", config: str = CONFIG, module: str = "dataeng_core.jobs.run_step"
) -> list[str]:
    return [
        "python",
        "-m",
        module,
        "--config",
        config,
        "--etapa",
        step_id,
        "--ambiente",
        ambiente,
        "--run-id",
        "{{ ts_nodash }}",
    ]


def test_ingestion_cloud_run_maps_to_cloud_run_job_command() -> None:
    pipeline = _pipeline([{**ingest_step("ingere"), "motor": "cloud_run"}])
    assert plan_tasks(pipeline, config_path=CONFIG) == [
        TaskSpec(task_id="ingere", kind="cloud_run_job", command=_command("ingere"), depends_on=[], resources={})
    ]


def test_command_carries_config_path_and_environment_verbatim() -> None:
    pipeline = _pipeline([ingest_step("ingere")])  # sem `motor`: padrão cloud_run
    (task,) = plan_tasks(pipeline, config_path=WIN_CONFIG, ambiente="dev")
    assert task.kind == "cloud_run_job"
    assert task.command == _command("ingere", "dev", "C:/proj/config/pipeline.yaml")  # barras normalizadas


def test_run_id_template_renders_to_an_id_accepted_by_the_jobs() -> None:
    # `{{ ts_nodash }}` é renderizado pelo Airflow como AAAAMMDDTHHMMSS; validate_run_id (usado por build_context
    # e por jobs.run_step --run-id) precisa aceitá-lo e dele extrair o dia da ingestão.
    assert RUN_ID_TEMPLATE == "{{ ts_nodash }}"
    assert validate_run_id("20261019T060000") == "20261019T060000"
    # o id nativo do Airflow continua recusado: por isso não é usado
    with pytest.raises(ConfigError):
        validate_run_id("scheduled__2026-10-19T06:00:00+00:00")


def test_ingestion_dataproc_maps_to_dataproc_batch() -> None:
    pipeline = _pipeline(
        [{**ingest_step("ingere"), "motor": "dataproc"}], execucao_padrao={"dataproc": {"container_image": "img:1"}}
    )
    (task,) = plan_tasks(pipeline, config_path=CONFIG)
    assert task.kind == "dataproc_batch"
    # ingestão no Dataproc roda o job com a transformação em Spark (Tarefa 22)
    assert task.command == _command("ingere", module="dataeng_core.jobs.ingest_spark")
    assert task.resources == {"container_image": "img:1"}


def test_resources_come_from_the_default_execution_settings_of_the_motor() -> None:
    execucao = {
        "cloud_run": {"cpu": 1, "memoria": "2Gi", "timeout": 3600},
        "dataproc": {"runtime": "2.2", "executores": 2, "container_image": "img:1"},
    }
    pipeline = _pipeline(
        [
            {**ingest_step("a"), "motor": "cloud_run"},
            {**ingest_step("b"), "motor": "dataproc"},
        ],
        execucao_padrao=execucao,
    )
    a, b = plan_tasks(pipeline, config_path=CONFIG)
    assert a.resources == {"cpu": 1, "memoria": "2Gi", "timeout": 3600}
    assert b.resources == {"runtime": "2.2", "executores": 2, "container_image": "img:1"}
    a.resources["cpu"] = 99  # cópia: não altera o pipeline
    assert pipeline.execucao_padrao["cloud_run"]["cpu"] == 1


def test_dependencies_follow_depends_on() -> None:
    pipeline = _pipeline(
        [
            {**ingest_step("c"), "depende_de": ["b"]},
            {**ingest_step("a"), "depende_de": []},
            {**ingest_step("b"), "depende_de": ["a"]},
        ]
    )
    tasks = plan_tasks(pipeline, config_path=CONFIG)
    assert [(t.task_id, t.depends_on) for t in tasks] == [("a", []), ("b", ["a"]), ("c", ["b"])]


def test_default_dependency_is_the_previous_step_and_empty_list_means_none() -> None:
    pipeline = _pipeline([ingest_step("um"), ingest_step("dois"), {**ingest_step("tres"), "depende_de": []}])
    tasks = plan_tasks(pipeline, config_path=CONFIG)
    assert [(t.task_id, t.depends_on) for t in tasks] == [("um", []), ("dois", ["um"]), ("tres", [])]


def test_single_step_pipeline() -> None:
    tasks = plan_tasks(_pipeline([ingest_step("unica")]), config_path=CONFIG)
    assert [(t.task_id, t.depends_on) for t in tasks] == [("unica", [])]


def test_diamond_dependencies_in_stable_topological_order() -> None:
    pipeline = _pipeline(
        [
            {**ingest_step("topo"), "depende_de": ["esq", "dir"]},
            {**ingest_step("dir"), "depende_de": ["raiz"]},
            {**ingest_step("esq"), "depende_de": ["raiz"]},
            {**ingest_step("raiz"), "depende_de": []},
        ]
    )
    tasks = plan_tasks(pipeline, config_path=CONFIG)
    assert [(t.task_id, t.depends_on) for t in tasks] == [
        ("raiz", []),
        ("dir", ["raiz"]),
        ("esq", ["raiz"]),
        ("topo", ["esq", "dir"]),
    ]


def test_step_ids_are_valid_airflow_task_ids() -> None:
    # Airflow aceita letras, dígitos, '-', '_' e '.'; o modelo só aceita letras, dígitos, '-' e '_'.
    pipeline = _pipeline([ingest_step("ingere-1_A")])
    assert plan_tasks(pipeline, config_path=CONFIG)[0].task_id == "ingere-1_A"
    for invalid in ("com espaco", "a.b", "ação", "a/b"):
        with pytest.raises(ValueError):
            _pipeline([ingest_step(invalid)])


_BLOCKER = textwrap.dedent(
    """
    import importlib.abc, sys

    class Block(importlib.abc.MetaPathFinder):
        def find_spec(self, name, path=None, target=None):
            if name.split(".")[0] in ("airflow", "google"):
                raise ImportError("importação proibida no planejador: " + name)
            return None

    sys.meta_path.insert(0, Block())
    import dataeng_core.airflow.planner  # noqa: F401
    leaked = sorted(m for m in sys.modules if m.split(".")[0] in ("airflow", "google"))
    assert not leaked, leaked
    print("ok")
    """
)


def test_planner_does_not_import_airflow() -> None:
    # Subprocesso limpo: não depende do estado de sys.modules da suíte; o bloqueador faz o import falhar de verdade.
    out = subprocess.run([sys.executable, "-c", _BLOCKER], capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "ok"


def test_command_uses_job_config_path_when_given_and_config_path_otherwise() -> None:
    pipeline = _pipeline([ingest_step("ingere")])
    (own,) = plan_tasks(pipeline, config_path="/dags/proj/config/pipeline.yaml", job_config_path="config/pipeline.yaml")
    assert own.command == _command("ingere", config="config/pipeline.yaml")
    (fallback,) = plan_tasks(pipeline, config_path="/dags/proj/config/pipeline.yaml")
    assert fallback.command == _command("ingere", config="/dags/proj/config/pipeline.yaml")


def test_job_config_path_is_normalized_to_forward_slashes() -> None:
    pipeline = _pipeline([ingest_step("ingere")])
    (task,) = plan_tasks(pipeline, config_path=WIN_CONFIG, job_config_path="config" + chr(92) + "pipeline.yaml")
    assert task.command == _command("ingere", config="config/pipeline.yaml")
    assert chr(92) not in "".join(task.command)
