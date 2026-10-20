"""Executa o pipeline de exemplo no ambiente local (DuckDB) e confere que repetir a execução não duplica."""

import shutil
from pathlib import Path

import duckdb
import pytest

from dataeng_core import cli

ROOT = Path(__file__).resolve().parent.parent
RUN_ID = "20260301T060000-aaaaaa"


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """Cópia do projeto em `tmp_path`, para o `.local` (DuckDB) não sujar o repositório."""
    copy = tmp_path / "projeto"
    shutil.copytree(ROOT, copy, ignore=shutil.ignore_patterns(".local", "__pycache__", ".pytest_cache"))
    return copy


def _count(project: Path, tabela: str) -> int:
    con = duckdb.connect(str(next((project / ".local").rglob("*.duckdb"))), read_only=True)
    try:
        return int(con.execute(f"SELECT count(*) FROM {tabela}").fetchone()[0])  # type: ignore[index]
    finally:
        con.close()


def test_run_local_loads_the_sample_and_is_idempotent(project: Path) -> None:
    argv = ["run", "local", "--config", str(project / "config/pipeline.yaml"), "--run-id", RUN_ID]
    assert cli.main(argv) == 0
    assert _count(project, "silver.registros_arquivo") == 5
    assert cli.main(argv) == 0  # mesma tentativa: não duplica
    assert _count(project, "silver.registros_arquivo") == 5
