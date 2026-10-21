from __future__ import annotations

import ast
import hashlib
import importlib.util
import re
import sys
from pathlib import Path

import pytest
import yaml
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
from dataeng_core.airflow.planner import DATAPROC_INGEST_ENTRYPOINT, TaskSpec
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


FORBIDDEN = ("airflow", "google")


def module_level_forbidden_imports(source: str) -> list[str]:
    """Imports de airflow/google fora de corpos de função (inclusive em try/if); `if TYPE_CHECKING:` é permitido."""
    found: list[str] = []

    def is_type_checking(test: ast.expr) -> bool:
        return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
            isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
        )

    def visit(node: ast.AST) -> None:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            return
        if isinstance(node, ast.If) and is_type_checking(node.test):
            for child in node.orelse:
                visit(child)
            return
        if isinstance(node, ast.Import):
            found.extend(a.name for a in node.names if a.name.split(".")[0] in FORBIDDEN)
        elif isinstance(node, ast.ImportFrom) and (node.module or "").split(".")[0] in FORBIDDEN:
            found.append(node.module or "")
        for child in ast.iter_child_nodes(node):
            visit(child)

    visit(ast.parse(source))
    return found


def test_the_import_check_itself_rejects_nested_and_allows_type_checking() -> None:
    assert module_level_forbidden_imports("try:\n    import airflow\nexcept ImportError:\n    pass\n") == ["airflow"]
    assert module_level_forbidden_imports("if x:\n    from google.cloud import storage\n") == ["google.cloud"]
    assert module_level_forbidden_imports("def f():\n    import airflow\n") == []
    assert module_level_forbidden_imports("if TYPE_CHECKING:\n    import airflow\n") == []
    assert module_level_forbidden_imports("if TYPE_CHECKING:\n    pass\nelse:\n    import airflow\n") == ["airflow"]


def test_dag_module_has_no_airflow_import_at_module_level() -> None:
    source = Path(dag_module.__file__ or "").read_text(encoding="utf-8")
    assert module_level_forbidden_imports(source) == []
    # e o import real existe, mas dentro de build_dag
    build = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == "build_dag")
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


def render_batch_id(template: str, ts_nodash: str = "20261019T060000", try_number: int = 1) -> str:
    return template.replace("{{ ts_nodash | lower }}", ts_nodash.lower()).replace(
        "{{ ti.try_number }}", str(try_number)
    )


def _valid_batch_id(batch_id: str) -> bool:
    return re.fullmatch(r"[a-z][a-z0-9-]{2,61}[a-z0-9]", batch_id) is not None


def test_dataproc_batch_id_exact_value_and_retry_attempt() -> None:
    template = dataproc_batch_id("Vendas_Semanais", "ingere_1")
    digest = hashlib.sha1(b"Vendas_Semanais:ingere_1").hexdigest()[:6]
    assert render_batch_id(template) == f"vendas-semanais-ingere-1-{digest}-20261019t060000-1"
    assert render_batch_id(template, try_number=2) == f"vendas-semanais-ingere-1-{digest}-20261019t060000-2"


def test_dataproc_batch_id_always_fits_the_dataproc_rules() -> None:
    for name, task in [("x" * 100, "y" * 100), ("9nome", "1"), ("___", "---"), ("a", "b")]:
        rendered = render_batch_id(dataproc_batch_id(name, task), try_number=123)
        assert _valid_batch_id(rendered), rendered
        assert len(rendered) <= 63


def test_dataproc_batch_ids_of_long_tasks_with_same_prefix_differ() -> None:
    a = render_batch_id(dataproc_batch_id("pipeline", "ingestao_" + "x" * 60 + "_a"))
    b = render_batch_id(dataproc_batch_id("pipeline", "ingestao_" + "x" * 60 + "_b"))
    assert a != b and len(a) <= 63 and len(b) <= 63


def test_invalid_timeout_is_an_actionable_error() -> None:
    spec = TaskSpec("ingere", "cloud_run_job", COMMAND, [], {"timeout": "1h"})
    with pytest.raises(ConfigError) as exc:
        cloud_run_overrides(spec)
    assert "timeout" in str(exc.value) and "1h" in str(exc.value)


def test_empty_cloud_run_job_name_is_an_error() -> None:
    with pytest.raises(ConfigError):
        cloud_run_job_name("___")


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


def _install_fake_airflow(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[_FakeOp], list[_FakeOp], list[_FakeOp]]:
    import types

    def module(name: str, **attrs: object) -> None:
        mod = types.ModuleType(name)
        mod.__dict__.update(attrs)
        monkeypatch.setitem(sys.modules, name, mod)

    cloud_run: list[_FakeOp] = []
    dataproc: list[_FakeOp] = []
    bigquery: list[_FakeOp] = []

    def recording(target: list[_FakeOp]) -> type[_FakeOp]:
        class Recorder(_FakeOp):
            def __init__(self, **kwargs: object) -> None:
                super().__init__(**kwargs)
                assert _FakeDag.current is not None  # criado dentro do contexto da DAG
                target.append(self)

        return Recorder

    module("airflow", DAG=_FakeDag)
    module("airflow.providers")
    module("airflow.providers.google")
    module("airflow.providers.google.cloud")
    module("airflow.providers.google.cloud.operators")
    module("airflow.providers.google.cloud.operators.bigquery", BigQueryInsertJobOperator=recording(bigquery))
    module("airflow.providers.google.cloud.operators.cloud_run", CloudRunExecuteJobOperator=recording(cloud_run))
    module("airflow.providers.google.cloud.operators.dataproc", DataprocCreateBatchOperator=recording(dataproc))
    monkeypatch.setitem(sys.modules, "airflow.sdk", None)  # força o caminho do Airflow 2.x
    return cloud_run, dataproc, bigquery


def _gcp_project(tmp_path: Path, *, dataproc_step: str | None = None) -> Path:
    config = make_project(tmp_path)
    (tmp_path / "config" / "ambientes" / "prod.yaml").write_text(
        ENV_LOCAL.replace("modo: local", "modo: gcp"), encoding="utf-8"
    )
    if dataproc_step:
        raw = yaml.safe_load(config.read_text(encoding="utf-8"))
        for etapa in raw["etapas"]:
            if etapa["id"] == dataproc_step:
                etapa["motor"] = "dataproc"
        config.write_text(yaml.safe_dump(raw, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return config


def test_build_dag_wiring_with_fake_airflow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Não prova o Airflow real (só o CI em Linux): prova que build_dag usa os operadores e a ordem corretos.
    created, _, _ = _install_fake_airflow(monkeypatch)
    config = _gcp_project(tmp_path)
    seen: list[object] = []

    def extra(dag: object) -> None:
        assert _FakeDag.current is dag  # dentro do contexto
        seen.append(dag)

    dag = build_dag(str(config), extra_tasks=extra, job_config_path="config/pipeline.yaml", catchup=True)
    assert isinstance(dag, _FakeDag) and seen == [dag]
    assert dag.kwargs["dag_id"] == "vendas" and dag.kwargs["schedule"] == "0 6 * * 1"
    assert dag.kwargs["catchup"] is True  # dag_kwargs prevalece
    assert [op.task_id for op in created] == ["ingere", "ingere2"]
    assert [op.upstream for op in created] == [[], ["ingere"]]
    first = created[0].kwargs
    assert (first["project_id"], first["region"], first["job_name"]) == ("projeto-teste", "regiao-teste", "vendas")
    expected = ["--config", "config/pipeline.yaml", "--etapa", "ingere", "--ambiente", "prod"]
    assert first["overrides"] == {"container_overrides": [{"args": [*expected, "--run-id", "{{ ts_nodash }}"]}]}


def test_build_dag_dataproc_branch_with_fake_airflow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cloud_run, dataproc, _ = _install_fake_airflow(monkeypatch)
    config = _gcp_project(tmp_path, dataproc_step="ingere2")
    build_dag(str(config))
    assert [op.task_id for op in cloud_run] == ["ingere"]
    (op,) = dataproc
    assert op.task_id == "ingere2" and op.upstream == ["ingere"]
    assert (op.kwargs["project_id"], op.kwargs["region"]) == ("projeto-teste", "regiao-teste")
    assert op.kwargs["batch_id"] == dataproc_batch_id("vendas", "ingere2")
    batch = op.kwargs["batch"]
    assert isinstance(batch, dict)
    assert batch["pyspark_batch"]["args"][:2] == ["--config", str(config).replace(chr(92), "/")]
    assert batch["runtime_config"] == {"version": "2.2"}
    assert batch["pyspark_batch"]["main_python_file_uri"] == DATAPROC_INGEST_ENTRYPOINT
