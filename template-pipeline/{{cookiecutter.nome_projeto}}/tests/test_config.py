"""Valida a configuração do pipeline: YAML, arquivos referenciados, SQL e a montagem estática das DAGs."""

import ast
from pathlib import Path

import pytest

from dataeng_core import cli

ROOT = Path(__file__).resolve().parent.parent
CONFIGS = sorted((ROOT / "config").glob("pipeline*.yaml"))
DAGS = sorted((ROOT / "dags").glob("*.py"))


def test_there_are_configs_and_dags() -> None:
    assert CONFIGS and DAGS


@pytest.mark.parametrize("config", CONFIGS, ids=lambda p: p.name)
@pytest.mark.parametrize("comando", ["validate-config", "plan"])
def test_config_validates_and_plans(config: Path, comando: str) -> None:
    assert cli.main([comando, "--config", str(config), "--ambiente", "local"]) == 0


@pytest.mark.parametrize("dag", DAGS, ids=lambda p: p.name)
def test_dag_file_is_wired_to_the_config(dag: Path) -> None:
    """Checagem estática (o Airflow não roda no Windows): chama build_dag com os dois caminhos."""
    tree = ast.parse(dag.read_text(encoding="utf-8"))
    imports = [n for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom))]
    assert not any((getattr(n, "module", None) or "").startswith("airflow") for n in imports)
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "build_dag"]
    assert len(calls) == 1
    keywords = {kw.arg: kw.value for kw in calls[0].keywords}
    job_config = ast.literal_eval(keywords["job_config_path"])
    assert (ROOT / job_config).is_file(), f"job_config_path aponta para um arquivo inexistente: {job_config}"
    assert ast.unparse(calls[0].args[0]) == "str(CONFIG_PATH)"
