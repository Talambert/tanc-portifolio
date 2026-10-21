from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest
from tests.cli.project import make_project

from dataeng_core.cli import main

RID = "20260101T060000-aaaaaa"


@pytest.fixture
def config(tmp_path: Path) -> Path:
    return make_project(tmp_path)


def _gold(tmp_path: Path) -> list[tuple[object, ...]]:
    db = next((tmp_path / ".local").rglob("*.duckdb"))
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute("SELECT n, rotulo FROM gold.marca").fetchall()
    finally:
        con.close()


def _count(tmp_path: Path, table: str) -> int:
    db = next((tmp_path / ".local").rglob("*.duckdb"))
    con = duckdb.connect(str(db), read_only=True)
    try:
        return int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])  # type: ignore[index]
    finally:
        con.close()


def test_validate_config_ok_and_error_exit_codes(config: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["validate-config", "--config", str(config)]) == 0
    out = capsys.readouterr().out
    assert "válida" in out and "vendas" in out and "2 etapas" in out

    config.write_text("schema_version: 1\nnome: x\n", encoding="utf-8")
    assert main(["validate-config", "--config", str(config)]) == 1
    # uso incorreto do argparse é código 2, distinto de falha de validação (1)
    assert main(["validate-config"]) == 2


def test_validate_config_prints_actionable_error(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = make_project(tmp_path, with_sql_step=True)
    config.write_text(config.read_text(encoding="utf-8").replace("tipo: sql", "tipo: inexistente"), encoding="utf-8")
    assert main(["validate-config", "--config", str(config)]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    err = captured.err
    for label in ("Configuração inválida", "O que aconteceu", "Como corrigir", "Referência:"):
        assert label in err
    assert "inexistente" in err
    assert "Traceback" not in err


def test_validate_config_rejects_bad_sql_and_undefined_param(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = make_project(tmp_path, sql="CREATE TABLE ${dataset_gold}.t AS SELECT FROM WHERE;")
    assert main(["validate-config", "--config", str(config)]) == 1
    err = capsys.readouterr().err
    assert "Erro de SQL" in err and "marca.sql" in err

    (tmp_path / "sql" / "marca.sql").write_text("SELECT '${nao_definido}';\n", encoding="utf-8")
    assert main(["validate-config", "--config", str(config)]) == 1
    err = capsys.readouterr().err
    assert "nao_definido" in err and "Como corrigir" in err


@pytest.mark.parametrize(
    "argv, fragment",
    [
        (["--param", "semigual"], "chave=valor"),
        (["--param", "=valor"], "chave"),
    ],
)
def test_invalid_param_is_usage_error(
    config: Path, capsys: pytest.CaptureFixture[str], argv: list[str], fragment: str
) -> None:
    assert main(["plan", "--config", str(config), *argv]) == 2
    err = capsys.readouterr().err
    assert fragment in err and "Traceback" not in err


def test_missing_config_file_is_actionable(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["validate-config", "--config", str(tmp_path / "nao_existe.yaml")]) == 1
    err = capsys.readouterr().err
    assert "nao_existe.yaml" in err and "Como corrigir" in err and "Traceback" not in err


def test_plan_lists_steps_in_topological_order(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = make_project(tmp_path, with_sql_step=True)
    assert main(["plan", "--config", str(config), "--run-id", RID]) == 0
    out = capsys.readouterr().out
    heads = [ln for ln in out.splitlines() if ln[:1].isdigit()]
    # ingere2 foi declarada antes, mas depende de ingere; resumo depende de ingere2
    assert [h.split()[1] for h in heads] == ["ingere", "ingere2", "resumo"]
    assert "motor=cloud_run" in heads[0] and "motor=bigquery" in heads[2]
    assert "tipo=sql" in heads[2] and "depende_de=ingere2" in heads[2]
    # SQL já renderizado: parâmetros do ambiente e do pipeline substituídos, sem sobras
    assert "CREATE OR REPLACE TABLE gold.marca" in out
    assert "FROM silver.vendas" in out and "'padrao' AS rotulo" in out and "';' AS sep" in out
    assert "${" not in out
    assert "sql/marca.sql" in out


def test_plan_param_flag_overrides_and_undefined_param_errors(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = make_project(tmp_path)
    assert main(["plan", "--config", str(config), "--param", "rotulo=a=b", "--param", "rotulo=x=y=z"]) == 0
    assert "'x=y=z' AS rotulo" in capsys.readouterr().out  # valor com '=' e a última ocorrência vence

    (tmp_path / "sql" / "marca.sql").write_text("SELECT '${nao_definido}' AS x;\n", encoding="utf-8")
    assert main(["plan", "--config", str(config)]) == 1
    captured = capsys.readouterr()
    assert "nao_definido" in captured.err and "${nao_definido}" not in captured.out


def test_param_flag_overrides_parameter(config: Path, tmp_path: Path) -> None:
    assert main(["run", "local", "--config", str(config), "--run-id", RID, "--param", "rotulo=via=flag"]) == 0
    assert _gold(tmp_path) == [(2, "via=flag")]


def test_run_local_executes_pipeline_and_returns_zero(
    config: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["run", "local", "--config", str(config)]) == 0  # sem --run-id: gera um com data
    out = capsys.readouterr().out
    assert "[ok] ingere:" in out and "[ok] ingere2:" in out
    assert out.index("[ok] ingere:") < out.index("[ok] ingere2:")
    assert _gold(tmp_path) == [(2, "padrao")]  # CSV sintético: 2 linhas válidas após deduplicar
    assert _count(tmp_path, "silver.vendas2") == 2


def test_run_local_failure_returns_one_with_message(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = make_project(tmp_path, sql="INSERT INTO silver.tabela_que_nao_existe SELECT 1;")
    assert main(["run", "local", "--config", str(config), "--run-id", RID]) == 1
    captured = capsys.readouterr()
    assert "[falha] ingere:" in captured.err and "[nao_executada] ingere2:" in captured.err
    assert "Como corrigir" in captured.err
    assert "Traceback" not in captured.err and "Traceback" not in captured.out


def test_run_local_accepts_only_local(config: Path) -> None:
    assert main(["run", "gcp", "--config", str(config)]) == 2


def test_unexpected_exception_is_short_message(
    config: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import dataeng_core.jobs.run_step as rs

    def boom(*_a: object, **_k: object) -> None:
        raise RuntimeError("valor-sensivel-xyz")

    monkeypatch.setattr(rs, "run_pipeline", boom)
    assert main(["run", "local", "--config", str(config), "--run-id", RID]) == 1
    err = capsys.readouterr().err
    human = [line for line in err.splitlines() if not line.startswith("{")]  # linhas JSON = log
    text = chr(10).join(human)
    assert "Erro inesperado (RuntimeError)" in text and "Referência:" in text
    assert "Traceback" not in text and "valor-sensivel-xyz" not in text


def test_accents_survive_pipe_encoding(config: Path) -> None:
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONIOENCODING", "PYTHONUTF8")}
    proc = subprocess.run(
        [sys.executable, "-m", "dataeng_core.cli", "validate-config", "--config", str(config)],
        capture_output=True,
        env=env,
        cwd=str(config.parent.parent),
        check=False,
    )
    assert proc.returncode == 0
    assert "válida".encode() in proc.stdout


def test_usage_errors_show_their_own_fix(config: Path, capsys: pytest.CaptureFixture[str]) -> None:
    generic = "Corrija o campo indicado no YAML"
    assert main(["validate-config", "--config", str(config.parent / "nao_existe.yaml")]) == 1
    err = capsys.readouterr().err
    assert "--config" in err.split("Como corrigir:")[1] and generic not in err

    assert main(["validate-config", "--config", str(config), "--ambiente", "nenhum"]) == 1
    err = capsys.readouterr().err
    assert "nenhum" in err and "config/ambientes" in err.split("Como corrigir:")[1] and generic not in err

    assert main(["run", "local", "--config", str(config), "--run-id", "!!"]) == 1
    err = capsys.readouterr().err
    assert "--run-id" in err.split("Como corrigir:")[1] and generic not in err
