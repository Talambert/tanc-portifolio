from __future__ import annotations

import ast
import importlib.util
import sys
from pathlib import Path

import pytest
from tests.cli.project import ENV_LOCAL, make_project

from dataeng_core.airflow import dag as dag_module
from dataeng_core.airflow.dag import (
    DEFAULT_DATAPROC_MAIN,
    build_dag,
    cloud_run_job_name,
    cloud_run_overrides,
    dataproc_batch,
    dataproc_batch_id,
)
from dataeng_core.airflow.planner import TaskSpec
from dataeng_core.base.errors import ConfigError
from dataeng_core.base.params import Environment

COMMAND = [
    "python", "-m", "dataeng_core.jobs.run_step", "--config", "config/pipeline.yaml", "--etapa", "ingere",
    "--ambiente", "prod", "--run-id", "{{ ts_nodash }}",
]  # fmt: skip
ARGS = COMMAND[3:]


def _env(**extra: str) -> Environment:
    return Environment.model_validate(
        {
            "modo": "gcp", "projeto_gcp": "p", "regiao": "r", "bucket_bronze": "b",
            "dataset_silver": "s", "dataset_gold": "g", **extra,
        }
    )  # fmt: skip


def test_dag_module_has_no_airflow_import_at_module_level() -> None:
    tree = ast.parse(Path(dag_module.__file__ or "").read_text(encoding="utf-8"))
    top: list[str] = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            top += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            top.append(node.module or "")
    assert top, "o módulo deveria importar algo"
    assert not [m for m in top if m.split(".")[0] in ("airflow", "google")]
    # e o import real existe, mas dentro de build_dag
    build = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "build_dag")
    inner = [n.module for n in ast.walk(build) if isinstance(n, ast.ImportFrom)]
    assert any((m or "").startswith("airflow") for m in inner)


def test_build_dag_without_airflow_gives_actionable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "airflow", None)
    with pytest.raises(ConfigError) as exc:
        build_dag("config/pipeline.yaml")
    message = str(exc.value)
    assert "dataeng-core[airflow]" in message and "não está instalado" in message


def test_cloud_run_overrides_pass_step_args_and_timeout() -> None:
    spec = TaskSpec("ingere", "cloud_run_job", COMMAND, [], {"cpu": 1, "memoria": "2Gi", "timeout": 3600})
    assert cloud_run_overrides(spec) == {"container_overrides": [{"args": ARGS}], "timeout": "3600s"}
    bare = TaskSpec("ingere", "cloud_run_job", COMMAND, [], {})
    assert cloud_run_overrides(bare) == {"container_overrides": [{"args": ARGS}]}


def test_cloud_run_job_name_is_valid() -> None:
    assert cloud_run_job_name("Vendas_Semanais") == "vendas-semanais"


def test_dataproc_batch_body() -> None:
    spec = TaskSpec("ingere", "dataproc_batch", COMMAND, [], {"runtime": "2.2", "executores": 2})
    assert dataproc_batch(spec, _env(service_account="sa@p.iam")) == {
        "pyspark_batch": {"main_python_file_uri": DEFAULT_DATAPROC_MAIN, "args": ARGS},
        "runtime_config": {"version": "2.2", "properties": {"spark.executor.instances": "2"}},
        "environment_config": {"execution_config": {"service_account": "sa@p.iam"}},
    }
    bare = TaskSpec("ingere", "dataproc_batch", COMMAND, [], {})
    assert dataproc_batch(bare, _env()) == {
        "pyspark_batch": {"main_python_file_uri": DEFAULT_DATAPROC_MAIN, "args": ARGS},
        "runtime_config": {"version": "2.2"},
    }


def test_dataproc_batch_id_is_stable_and_valid() -> None:
    assert dataproc_batch_id("Vendas_Semanais", "ingere_1") == "vendas-semanais-ingere-1-{{ ts_nodash | lower }}"
    assert len(dataproc_batch_id("x" * 100, "y" * 100)) <= 40 + len("-{{ ts_nodash | lower }}")


@pytest.mark.airflow
@pytest.mark.skipif(importlib.util.find_spec("airflow") is None, reason="Airflow não instalado (roda no CI em Linux)")
def test_build_dag_imports_a_real_dag(tmp_path: Path) -> None:
    config = make_project(tmp_path)
    (tmp_path / "config" / "ambientes" / "prod.yaml").write_text(
        ENV_LOCAL.replace("modo: local", "modo: gcp"), encoding="utf-8"
    )
    called: list[object] = []
    dag = build_dag(str(config), extra_tasks=called.append)
    assert dag.dag_id == "vendas"
    assert sorted(dag.task_ids) == ["ingere", "ingere2"]
    assert dag.get_task("ingere2").upstream_task_ids == {"ingere"}
    assert dag.get_task("ingere").upstream_task_ids == set()
    assert called == [dag]


class _FakeOp:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.task_id = str(kwargs["task_id"])
        self.upstream: list[str] = []

    def __rshift__(self, other: _FakeOp) -> _FakeOp:
        other.upstream.append(self.task_id)
        return other


class _FakeDag:
    current: _FakeDag | None = None

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs

    def __enter__(self) -> _FakeDag:
        _FakeDag.current = self
        return self

    def __exit__(self, *exc: object) -> None:
        _FakeDag.current = None


def test_build_dag_wiring_with_fake_airflow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Não prova o Airflow real (só o CI em Linux): prova que build_dag usa os operadores e a ordem corretos.
    import types

    def module(name: str, **attrs: object) -> None:
        mod = types.ModuleType(name)
        mod.__dict__.update(attrs)
        monkeypatch.setitem(sys.modules, name, mod)

    created: list[_FakeOp] = []

    class CloudRun(_FakeOp):
        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)
            assert _FakeDag.current is not None  # criado dentro do contexto da DAG
            created.append(self)

    module("airflow", DAG=_FakeDag)
    module("airflow.providers")
    module("airflow.providers.google")
    module("airflow.providers.google.cloud")
    module("airflow.providers.google.cloud.operators")
    module("airflow.providers.google.cloud.operators.cloud_run", CloudRunExecuteJobOperator=CloudRun)
    module("airflow.providers.google.cloud.operators.dataproc", DataprocCreateBatchOperator=_FakeOp)
    monkeypatch.setitem(sys.modules, "airflow.sdk", None)  # força o caminho do Airflow 2.x

    config = make_project(tmp_path)
    (tmp_path / "config" / "ambientes" / "prod.yaml").write_text(
        ENV_LOCAL.replace("modo: local", "modo: gcp"), encoding="utf-8"
    )
    seen: list[object] = []

    def extra(dag: object) -> None:
        assert _FakeDag.current is dag  # dentro do contexto
        seen.append(dag)

    dag = build_dag(str(config), extra_tasks=extra, catchup=True)
    assert isinstance(dag, _FakeDag) and seen == [dag]
    assert dag.kwargs["dag_id"] == "vendas" and dag.kwargs["schedule"] == "0 6 * * 1"
    assert dag.kwargs["catchup"] is True  # dag_kwargs prevalece
    assert [op.task_id for op in created] == ["ingere", "ingere2"]
    assert [op.upstream for op in created] == [[], ["ingere"]]
    first = created[0].kwargs
    assert (first["project_id"], first["region"], first["job_name"]) == ("projeto-teste", "regiao-teste", "vendas")
    expected_args = ["--config", str(config), "--etapa", "ingere", "--ambiente", "prod", "--run-id", "{{ ts_nodash }}"]
    assert first["overrides"] == {"container_overrides": [{"args": expected_args}]}
