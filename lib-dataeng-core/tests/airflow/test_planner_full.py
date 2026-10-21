"""Planejador completo (Tarefa 22): todos os tipos de etapa, SQL renderizado, entrypoints do Dataproc e `build_dag`."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import jinja2
import jinja2.sandbox
import pytest
import yaml
from tests.airflow.test_dag_static import _FakeDag, _gcp_project, _install_fake_airflow
from tests.steps.helpers import ingest_step

from dataeng_core.airflow.dag import (
    DEFAULT_DATAPROC_MAIN,
    bigquery_configuration,
    build_dag,
    dataproc_batch,
)
from dataeng_core.airflow.planner import (
    DATAPROC_INGEST_ENTRYPOINT,
    DATAPROC_RUN_STEP_ENTRYPOINT,
    TaskSpec,
    escape_jinja,
    plan_tasks,
    task_args,
)
from dataeng_core.base.errors import ConfigError, SqlError
from dataeng_core.base.params import Environment
from dataeng_core.base.pipeline import Pipeline

RUN = "{{ ts_nodash }}"
DAY = "{{ ds }}"
PARAMS = {"dataset_gold": "gold", "dataset_silver": "silver", "rotulo": "padrao"}

MARCA = (
    "-- marca; com ponto e vírgula em texto\n"
    "CREATE OR REPLACE TABLE ${dataset_gold}.marca AS\n"
    "SELECT '${run_id}' AS run_id, DATE '${ingestion_date}' AS dia, '${rotulo}' AS rotulo, ';' AS sep "
    "FROM ${dataset_silver}.vendas;\n"
    "DELETE FROM ${dataset_gold}.marca WHERE rotulo = 'velho' -- fim\n"
)


def _config(root: Path) -> str:
    return str(root / "config" / "pipeline.yaml")


def _write(root: Path, name: str, text: str) -> None:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _pipeline(etapas: list[dict[str, Any]], **extra: Any) -> Pipeline:
    return Pipeline.model_validate(
        {
            "schema_version": 1, "nome": "vendas", "agenda": "0 6 * * 1",
            "parametros": PARAMS, "etapas": etapas, **extra,
        }
    )  # fmt: skip


def _job(module: str, step_id: str, config: str = "config/pipeline.yaml") -> list[str]:
    return ["python", "-m", module, "--config", config, "--etapa", step_id, "--ambiente", "prod", "--run-id", RUN]


def _one(root: Path, step: dict[str, Any], **extra: Any) -> TaskSpec:
    (task,) = plan_tasks(_pipeline([step], **extra), config_path=_config(root))
    return task


def _render(statement: str) -> str:
    """O que o Airflow faz com o campo: Jinja com o contexto da execução (aqui, valores fixos)."""
    env = jinja2.sandbox.SandboxedEnvironment(undefined=jinja2.StrictUndefined, keep_trailing_newline=True)
    return env.from_string(statement).render(ts_nodash="20261019T060000", ds="2026-10-19")


# ------------------------------------------------------------------ mapa de tipos


def test_planner_maps_every_step_type_to_a_task_kind(tmp_path: Path) -> None:
    _write(tmp_path, "sql/marca.sql", "SELECT 1 AS n;")
    _write(tmp_path, "sql/v.sql", "SELECT 1 AS n")
    etapas = [
        {**ingest_step("ing_cr"), "motor": "cloud_run", "depende_de": []},
        {**ingest_step("ing_dp"), "motor": "dataproc", "depende_de": ["ing_cr"]},
        {"id": "agrega", "tipo": "sql", "sql": ["sql/marca.sql"], "depende_de": ["ing_cr"]},
        {"id": "visao", "tipo": "view", "sql": "sql/v.sql", "nome_view": "gold.v", "depende_de": ["agrega"]},
        {"id": "py_cr", "tipo": "python", "funcao": "pkg.mod:fn", "depende_de": ["visao"]},
        {"id": "py_dp", "tipo": "python", "funcao": "pkg.mod:fn", "motor": "dataproc", "depende_de": ["py_cr"]},
        {"id": "spk", "tipo": "spark", "script": "spark/job.py", "depende_de": ["py_dp"]},
        {
            "id": "qual", "tipo": "qualidade", "depende_de": ["spk"],
            "verificacoes": [{"nao_nulo": "id", "tabela": "silver.vendas"}],
        },
    ]  # fmt: skip
    _write(tmp_path, "spark/job.py", "print('x')")
    pipeline = _pipeline(etapas, execucao_padrao={"dataproc": {"script_base_uri": "gs://b/proj"}})
    tasks = plan_tasks(pipeline, config_path=_config(tmp_path))
    assert [(t.task_id, t.kind, t.depends_on) for t in tasks] == [
        ("ing_cr", "cloud_run_job", []),
        ("ing_dp", "dataproc_batch", ["ing_cr"]),
        ("agrega", "bigquery_job", ["ing_cr"]),
        ("visao", "bigquery_job", ["agrega"]),
        ("py_cr", "cloud_run_job", ["visao"]),
        ("py_dp", "dataproc_batch", ["py_cr"]),
        ("spk", "dataproc_batch", ["py_dp"]),
        ("qual", "cloud_run_job", ["spk"]),
    ]
    by_id = {t.task_id: t for t in tasks}
    config = _config(tmp_path).replace("\\", "/")
    assert by_id["ing_cr"].command == _job("dataeng_core.jobs.run_step", "ing_cr", config)
    assert by_id["ing_dp"].command == _job("dataeng_core.jobs.ingest_spark", "ing_dp", config)
    assert by_id["py_cr"].command == _job("dataeng_core.jobs.run_step", "py_cr", config)
    assert by_id["py_dp"].command == _job("dataeng_core.jobs.run_step", "py_dp", config)
    assert by_id["qual"].command == _job("dataeng_core.jobs.run_step", "qual", config)


def test_quality_step_runs_in_a_container_with_cloud_run_resources(tmp_path: Path) -> None:
    # `qualidade` NÃO é bigquery_job: run_checks opera sobre DataFrame e grava o controle (veja o docstring).
    step = {"id": "qual", "tipo": "qualidade", "verificacoes": [{"nao_nulo": "id", "tabela": "silver.vendas"}]}
    task = _one(tmp_path, step, execucao_padrao={"cloud_run": {"cpu": 2, "timeout": 600}})
    assert task.kind == "cloud_run_job"
    assert task.resources == {"cpu": 2, "timeout": 600}
    assert task.command[:3] == ["python", "-m", "dataeng_core.jobs.run_step"]
    assert "silver.vendas" not in " ".join(task.command)  # a tarefa só aponta a etapa; as consultas são da etapa


# ------------------------------------------------------------------ SQL


def test_planner_sql_task_carries_rendered_sql_without_container(tmp_path: Path) -> None:
    _write(tmp_path, "sql/marca.sql", MARCA)
    task = _one(tmp_path, {"id": "agrega", "tipo": "sql", "sql": ["sql/marca.sql"]})
    assert task.kind == "bigquery_job"
    assert task.resources == {}
    assert task.command == [
        "CREATE OR REPLACE TABLE gold.marca AS\n"
        f"SELECT '{RUN}' AS run_id, DATE '{DAY}' AS dia, 'padrao' AS rotulo, ';' AS sep FROM silver.vendas",
        "DELETE FROM gold.marca WHERE rotulo = 'velho'",
    ]
    assert not any(part in ("python", "-m") for part in task.command)  # sem contêiner


def test_sql_statements_of_several_files_keep_the_declared_order(tmp_path: Path) -> None:
    _write(tmp_path, "sql/a.sql", "SELECT 1 AS a;\nSELECT 2 AS b;")
    _write(tmp_path, "sql/b.sql", "SELECT 3 AS c")
    task = _one(tmp_path, {"id": "s", "tipo": "sql", "sql": ["sql/b.sql", "sql/a.sql"]})
    assert task.command == ["SELECT 3 AS c", "SELECT 1 AS a", "SELECT 2 AS b"]


def test_undefined_parameter_fails_when_planning(tmp_path: Path) -> None:
    _write(tmp_path, "sql/x.sql", "SELECT '${nao_definido}' AS x")
    with pytest.raises(SqlError) as exc:
        _one(tmp_path, {"id": "s", "tipo": "sql", "sql": ["sql/x.sql"]})
    assert "nao_definido" in str(exc.value) and "sql/x.sql" in str(exc.value)


def test_invalid_syntax_and_empty_file_fail_when_planning(tmp_path: Path) -> None:
    _write(tmp_path, "sql/ruim.sql", "SELEC 1 FROM")
    _write(tmp_path, "sql/vazio.sql", "-- só comentário\n")
    with pytest.raises(SqlError):
        _one(tmp_path, {"id": "s", "tipo": "sql", "sql": ["sql/ruim.sql"]})
    with pytest.raises(ConfigError) as exc:
        _one(tmp_path, {"id": "s", "tipo": "sql", "sql": ["sql/vazio.sql"]})
    assert "não contém instruções" in str(exc.value)


def test_user_jinja_in_sql_and_in_parameters_is_literal_for_airflow(tmp_path: Path) -> None:
    sql = (
        "SELECT '{{ 7*7 }}' AS a, '{% if x %}' AS b, '{# nota #}' AS c, '${rotulo}' AS d, '${run_id}' AS e, "
        "'{{ ts_nodash }}' AS forjado FROM silver.t"
    )
    _write(tmp_path, "sql/j.sql", sql)
    injected = "{{ var.value.segredo }}{% raw %}"
    task = _one(
        tmp_path,
        {"id": "s", "tipo": "sql", "sql": ["sql/j.sql"]},
        parametros={**PARAMS, "rotulo": injected},
    )
    (statement,) = task.command
    assert _render(statement) == (
        f"SELECT '{{{{ 7*7 }}}}' AS a, '{{% if x %}}' AS b, '{{# nota #}}' AS c, '{injected}' AS d, "
        "'20261019T060000' AS e, '{{ ts_nodash }}' AS forjado FROM silver.t"
    )


def test_escape_jinja_round_trips_adversarial_text() -> None:
    for text in [
        "{{",
        "{%",
        "{#",
        "{{{",
        "{{{{",
        "{%{",
        "{ {{ }}",
        "a{{b}}c{%d%}{#e#}",
        "}}{{",
        "{{ '{{' }}",
        "ação {{ é }}",
    ]:
        escaped = escape_jinja(text)
        env = jinja2.Environment(undefined=jinja2.StrictUndefined, keep_trailing_newline=True)
        assert env.from_string(escaped).render() == text, text
    assert escape_jinja("SELECT 1 FROM t WHERE a = '${x}' AND {b}") == "SELECT 1 FROM t WHERE a = '${x}' AND {b}"


def test_view_with_create_statement_and_with_select_plus_name(tmp_path: Path) -> None:
    _write(tmp_path, "sql/v1.sql", "CREATE OR REPLACE VIEW gold.v1 AS SELECT '${run_id}' AS r FROM silver.t")
    _write(tmp_path, "sql/v2.sql", "SELECT 1 AS n FROM silver.t;")
    first = _one(tmp_path, {"id": "v1", "tipo": "view", "sql": "sql/v1.sql"})
    second = _one(tmp_path, {"id": "v2", "tipo": "view", "sql": "sql/v2.sql", "nome_view": "gold.v2"})
    assert first.kind == second.kind == "bigquery_job"
    assert first.command == [f"CREATE OR REPLACE VIEW gold.v1 AS SELECT '{RUN}' AS r FROM silver.t"]
    assert second.command == ["CREATE OR REPLACE VIEW gold.v2 AS SELECT 1 AS n FROM silver.t"]


def test_view_errors_fail_when_planning(tmp_path: Path) -> None:
    _write(tmp_path, "sql/v.sql", "SELECT 1 AS n")
    with pytest.raises(ConfigError):  # SELECT sem nome_view
        _one(tmp_path, {"id": "v", "tipo": "view", "sql": "sql/v.sql"})
    with pytest.raises(ConfigError):  # nome_view inseguro
        _one(tmp_path, {"id": "v", "tipo": "view", "sql": "sql/v.sql", "nome_view": "gold.v; DROP TABLE x"})


# ------------------------------------------------------------------ Spark e Dataproc


def test_spark_step_runs_its_own_script_with_etapa_run_id_and_params(tmp_path: Path) -> None:
    _write(tmp_path, "spark/job.py", "print('x')")
    step = {"id": "spk", "tipo": "spark", "script": "spark/job.py", "dataproc": {"executores": 4}}
    task = _one(
        tmp_path,
        step,
        parametros={"a": "1", "b": "{{ x }}"},
        execucao_padrao={"dataproc": {"runtime": "2.2", "script_base_uri": "gs://bucket/proj/"}},
    )
    assert task.kind == "dataproc_batch"
    assert task.command == [
        "python", "spark/job.py", "--etapa", "spk", "--run-id", RUN,
        "--param", "a=1", "--param", "b={{ '{{' }} x }}",
        "--param", f"ingestion_date={DAY}", "--param", f"run_id={RUN}",
    ]  # fmt: skip
    assert task.resources == {
        "runtime": "2.2", "script_base_uri": "gs://bucket/proj/", "executores": 4,
        "main_python_file_uri": "gs://bucket/proj/spark/job.py",
    }  # fmt: skip
    assert task_args(task) == task.command[2:]
    batch = dataproc_batch(task, Environment.model_validate(_env()))
    assert batch["pyspark_batch"] == {"main_python_file_uri": "gs://bucket/proj/spark/job.py", "args": task.command[2:]}
    assert batch["runtime_config"] == {"version": "2.2", "properties": {"spark.executor.instances": "4"}}


def _env() -> dict[str, str]:
    return {
        "modo": "gcp", "projeto_gcp": "p", "regiao": "r", "bucket_bronze": "b", "dataset_silver": "s",
        "dataset_gold": "g",
    }  # fmt: skip


def test_spark_script_location_is_never_invented(tmp_path: Path) -> None:
    _write(tmp_path, "spark/job.py", "print('x')")
    with pytest.raises(ConfigError) as exc:
        _one(tmp_path, {"id": "spk", "tipo": "spark", "script": "spark/job.py"})
    message = str(exc.value)
    assert "script_base_uri" in message and "main_python_file_uri" in message and "'spk'" in message


def test_spark_step_may_set_its_own_script_uri(tmp_path: Path) -> None:
    _write(tmp_path, "spark/job.py", "print('x')")
    step = {"id": "spk", "tipo": "spark", "script": "spark/job.py", "dataproc": {"main_python_file_uri": "gs://x/y.py"}}
    assert _one(tmp_path, step).resources["main_python_file_uri"] == "gs://x/y.py"


def test_dataproc_entrypoints_per_step_type(tmp_path: Path) -> None:
    env = Environment.model_validate(_env())
    ingest = _one(tmp_path, {**ingest_step("i"), "motor": "dataproc"})
    python = _one(tmp_path, {"id": "p", "tipo": "python", "funcao": "m:f", "motor": "dataproc"})
    batch_i, batch_p = dataproc_batch(ingest, env), dataproc_batch(python, env)
    assert batch_i["pyspark_batch"]["main_python_file_uri"] == DATAPROC_INGEST_ENTRYPOINT
    assert batch_p["pyspark_batch"]["main_python_file_uri"] == DATAPROC_RUN_STEP_ENTRYPOINT == DEFAULT_DATAPROC_MAIN
    assert DATAPROC_INGEST_ENTRYPOINT != DATAPROC_RUN_STEP_ENTRYPOINT
    assert batch_i["pyspark_batch"]["args"] == task_args(ingest) == ingest.command[3:]
    # configuráveis, sem caminho fixo
    cfg = {"dataproc": {"ingest_entrypoint_uri": "gs://c/i.py", "run_step_entrypoint_uri": "gs://c/r.py"}}
    ingest2 = _one(tmp_path, {**ingest_step("i"), "motor": "dataproc"}, execucao_padrao=cfg)
    python2 = _one(tmp_path, {"id": "p", "tipo": "python", "funcao": "m:f", "motor": "dataproc"}, execucao_padrao=cfg)
    assert dataproc_batch(ingest2, env)["pyspark_batch"]["main_python_file_uri"] == "gs://c/i.py"
    assert dataproc_batch(python2, env)["pyspark_batch"]["main_python_file_uri"] == "gs://c/r.py"


def test_dataproc_task_without_script_is_an_actionable_error() -> None:
    spec = TaskSpec("x", "dataproc_batch", ["python", "job.py", "--etapa", "x"], [], {})
    with pytest.raises(ConfigError) as exc:
        dataproc_batch(spec, Environment.model_validate(_env()))
    assert "main_python_file_uri" in str(exc.value)


# ------------------------------------------------------------------ ambos (dois YAMLs, duas DAGs)


def test_planner_with_ambos_creates_two_task_sets_named_cloud_run_and_dataproc(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # `ambos` é variável do template, não valor do modelo: gera pipeline.yaml (cloud_run) e pipeline_dataproc.yaml
    # (dataproc), cada um planejado à parte, e duas DAGs com os nomes abaixo (dag_id).
    created_cr, created_dp, _ = _install_fake_airflow(monkeypatch)
    cloud_run_yaml = _gcp_project(tmp_path)
    raw = yaml.safe_load(cloud_run_yaml.read_text(encoding="utf-8"))
    for etapa in raw["etapas"]:
        etapa["motor"] = "dataproc"
    dataproc_yaml = cloud_run_yaml.with_name("pipeline_dataproc.yaml")
    dataproc_yaml.write_text(yaml.safe_dump(raw, allow_unicode=True, sort_keys=False), encoding="utf-8")

    dag_cr = build_dag(str(cloud_run_yaml), dag_id="vendas_cloud_run")
    dag_dp = build_dag(str(dataproc_yaml), dag_id="vendas_dataproc")

    assert isinstance(dag_cr, _FakeDag) and isinstance(dag_dp, _FakeDag)
    assert (dag_cr.kwargs["dag_id"], dag_dp.kwargs["dag_id"]) == ("vendas_cloud_run", "vendas_dataproc")
    assert [op.task_id for op in created_cr] == [op.task_id for op in created_dp] == ["ingere", "ingere2"]
    assert [op.upstream for op in created_cr] == [op.upstream for op in created_dp] == [[], ["ingere"]]
    assert all("overrides" in op.kwargs and "batch" not in op.kwargs for op in created_cr)
    assert all("batch" in op.kwargs and "overrides" not in op.kwargs for op in created_dp)
    kinds_cr = {t.kind for t in plan_tasks(_planned(cloud_run_yaml), config_path=str(cloud_run_yaml))}
    kinds_dp = {t.kind for t in plan_tasks(_planned(dataproc_yaml), config_path=str(dataproc_yaml))}
    assert (kinds_cr, kinds_dp) == ({"cloud_run_job"}, {"dataproc_batch"})


def _planned(path: Path) -> Pipeline:
    from dataeng_core.base.pipeline import load_pipeline

    return load_pipeline(path, ambiente="prod")


# ------------------------------------------------------------------ build_dag


def _sql_project(tmp_path: Path) -> Path:
    config = _gcp_project(tmp_path)
    _write(tmp_path, "sql/resumo.sql", "SELECT '${run_id}' AS r, '{{ literal }}' AS l;\nSELECT 2 AS dois;")
    raw = yaml.safe_load(config.read_text(encoding="utf-8"))
    raw["etapas"].append({"id": "resumo", "tipo": "sql", "sql": ["sql/resumo.sql"], "depende_de": ["ingere2"]})
    raw["etapas"].append(
        {
            "id": "qual", "tipo": "qualidade", "depende_de": ["resumo"],
            "verificacoes": [{"nao_nulo": "id", "tabela": "silver.vendas"}],
        }
    )  # fmt: skip
    config.write_text(yaml.safe_dump(raw, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return config


def test_build_dag_creates_bigquery_container_and_quality_tasks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cloud_run, dataproc, bigquery = _install_fake_airflow(monkeypatch)
    dag = build_dag(str(_sql_project(tmp_path)), job_config_path="config/pipeline.yaml")
    assert isinstance(dag, _FakeDag)
    assert [op.task_id for op in cloud_run] == ["ingere", "ingere2", "qual"] and dataproc == []
    (op,) = bigquery
    assert op.task_id == "resumo" and op.upstream == ["ingere2"]
    assert set(op.kwargs) == {"task_id", "configuration", "project_id", "location"}
    assert (op.kwargs["project_id"], op.kwargs["location"]) == ("projeto-teste", "regiao-teste")
    assert op.kwargs["configuration"] == {
        "query": {
            "query": f"SELECT '{RUN}' AS r, '{{{{ '{{{{' }}}} literal }}}}' AS l;\nSELECT 2 AS dois;\n",
            "useLegacySql": False,
        }
    }
    qual = next(o for o in cloud_run if o.task_id == "qual")
    assert qual.upstream == ["resumo"]
    assert qual.kwargs["overrides"]["container_overrides"][0]["args"][:4] == [
        "--config", "config/pipeline.yaml", "--etapa", "qual",
    ]  # fmt: skip


def test_bigquery_configuration_joins_statements_and_never_ends_with_dot_sql() -> None:
    spec = TaskSpec("s", "bigquery_job", ["SELECT 1", "SELECT 'a.sql'"], [], {})
    query = bigquery_configuration(spec)["query"]
    assert query == {"query": "SELECT 1;\nSELECT 'a.sql';\n", "useLegacySql": False}
    assert not query["query"].endswith(".sql")


def test_extra_tasks_hook_is_called(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_airflow(monkeypatch)
    seen: list[object] = []

    def extra(dag: object) -> None:
        assert _FakeDag.current is dag  # dentro do contexto da DAG, depois das tarefas da biblioteca
        seen.append(dag)

    dag = build_dag(str(_sql_project(tmp_path)), extra_tasks=extra)
    assert seen == [dag]


@pytest.mark.airflow
@pytest.mark.skipif(importlib.util.find_spec("airflow") is None, reason="Airflow não instalado (roda no CI em Linux)")
def test_extra_tasks_hook_is_called_with_real_airflow(tmp_path: Path) -> None:
    config = _sql_project(tmp_path)
    called: list[object] = []
    dag = build_dag(str(config), extra_tasks=called.append)
    assert sorted(dag.task_ids) == ["ingere", "ingere2", "qual", "resumo"]
    assert dag.get_task("resumo").upstream_task_ids == {"ingere2"}
    assert called == [dag]
    # o SQL é renderizado pelo Airflow: ts_nodash e o literal escapado
    rendered = dag.get_task("resumo").render_template(
        dag.get_task("resumo").configuration, {"ts_nodash": "20261019T060000", "ds": "2026-10-19"}
    )
    assert "SELECT '20261019T060000' AS r, '{{ literal }}' AS l;" in rendered["query"]["query"]
