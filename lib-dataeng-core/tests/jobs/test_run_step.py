from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from tests.cli.project import make_project

from dataeng_core.jobs import ingest, run_step

RID = "20260101T060000-aaaaaa"


@pytest.fixture
def config(tmp_path: Path) -> Path:
    return make_project(tmp_path)


def _args(config: Path, etapa: str, *extra: str) -> list[str]:
    return ["--config", str(config), "--ambiente", "local", "--etapa", etapa, "--run-id", RID, *extra]


def _query(tmp_path: Path, sql: str) -> list[tuple[object, ...]]:
    db = next((tmp_path / ".local").rglob("*.duckdb"))
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def _tables(tmp_path: Path) -> set[str]:
    rows = _query(tmp_path, "SELECT table_schema || '.' || table_name FROM information_schema.tables")
    return {str(r[0]) for r in rows}


def test_run_step_runs_only_the_chosen_step(config: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert run_step.main(_args(config, "ingere")) == 0
    out = capsys.readouterr().out
    assert "ingere" in out and "ok" in out
    tables = _tables(tmp_path)
    assert "silver.vendas" in tables and "silver._controle_execucoes" in tables
    assert "silver.vendas2" not in tables  # a etapa dependente não rodou


def test_run_step_param_flag_with_equals_in_value(config: Path, tmp_path: Path) -> None:
    assert run_step.main(_args(config, "ingere", "--param", "rotulo=a=b")) == 0
    assert _query(tmp_path, "SELECT rotulo FROM gold.marca") == [("a=b",)]


def test_run_step_failure_prints_and_returns_one(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    config = make_project(tmp_path, sql="INSERT INTO silver.nada SELECT 1;")
    assert run_step.main(_args(config, "ingere")) == 1
    captured = capsys.readouterr()
    assert "ingere" in captured.err and "falha" in captured.err and "Como corrigir" in captured.err
    assert "Traceback" not in captured.err


def test_run_step_unknown_step_is_actionable(config: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert run_step.main(_args(config, "fantasma")) == 1
    err = capsys.readouterr().err
    assert "fantasma" in err and "ingere" in err and "Como corrigir" in err and "Referência:" in err


def test_run_step_requires_arguments(capsys: pytest.CaptureFixture[str]) -> None:
    assert run_step.main([]) == 2
    assert "--config" in capsys.readouterr().err


def test_run_step_bad_param_is_usage_error(config: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert run_step.main(_args(config, "ingere", "--param", "semigual")) == 2
    assert "chave=valor" in capsys.readouterr().err


def test_run_step_missing_environment_is_actionable(config: Path, capsys: pytest.CaptureFixture[str]) -> None:
    argv = ["--config", str(config), "--ambiente", "prod", "--etapa", "ingere", "--run-id", RID]
    assert run_step.main(argv) == 1
    err = capsys.readouterr().err
    assert "prod" in err and "local" in err


def test_run_step_without_run_id_generates_one(config: Path, tmp_path: Path) -> None:
    assert run_step.main(["--config", str(config), "--ambiente", "local", "--etapa", "ingere"]) == 0
    assert "silver.vendas" in _tables(tmp_path)


def test_run_step_invalid_run_id_is_actionable(config: Path, capsys: pytest.CaptureFixture[str]) -> None:
    argv = ["--config", str(config), "--ambiente", "local", "--etapa", "ingere", "--run-id", "scheduled__2026:01"]
    assert run_step.main(argv) == 1
    err = capsys.readouterr().err
    assert "run_id" in err and "Como corrigir" in err


def test_ingest_runs_ingestion_step(config: Path, tmp_path: Path) -> None:
    assert ingest.main(_args(config, "ingere")) == 0
    assert "silver.vendas" in _tables(tmp_path)


def test_ingest_refuses_non_ingestion_step(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = make_project(tmp_path, with_sql_step=True)
    assert ingest.main(_args(config, "resumo")) == 1
    err = capsys.readouterr().err
    assert "resumo" in err and "ingestao" in err and "sql" in err and "Como corrigir" in err
    assert not (tmp_path / ".local").exists()  # nada foi executado
