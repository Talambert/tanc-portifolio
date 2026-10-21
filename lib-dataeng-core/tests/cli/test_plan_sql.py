from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
import yaml
from tests.cli.project import make_project

from dataeng_core.cli import main

RID = "20260101T060000-aaaaaa"

INSERT_SQL = (
    "CREATE TABLE IF NOT EXISTS ${dataset_gold}.log (run_id VARCHAR);\n\n"
    "-- grava o run\n"
    "INSERT INTO ${dataset_gold}.log VALUES ('${run_id}');\n"
)


def test_plan_prints_rendered_sql_and_idempotency_warning(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = make_project(tmp_path, sql=INSERT_SQL, with_sql_step=True)
    assert main(["plan", "--config", str(config), "--run-id", RID]) == 0
    out = capsys.readouterr().out
    assert f"INSERT INTO gold.log VALUES ('{RID}');" in out
    assert "CREATE TABLE IF NOT EXISTS gold.log (run_id VARCHAR);" in out
    assert "${" not in out
    warning = (
        "aviso: arquivo sql/marca.sql: linha 4: INSERT sem MERGE, OVERWRITE nem WHERE NOT EXISTS; "
        "a reexecução pode duplicar linhas"
    )
    # o aviso aparece abaixo do SQL de cada etapa que usa o arquivo (ingere e resumo), uma vez por etapa
    assert out.count(warning) == 2
    block = out.split("3. resumo")[1]
    assert block.index("INSERT INTO gold.log") < block.index(warning)
    assert not (tmp_path / ".local").exists()  # o plano não executa nada


def test_plan_without_warnings_prints_none(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = make_project(tmp_path, with_sql_step=True)
    assert main(["plan", "--config", str(config), "--run-id", RID]) == 0
    assert "aviso" not in capsys.readouterr().out


def test_plan_syntax_error_reports_file_and_line_without_executing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = make_project(tmp_path, sql="SELECT 1;\n\n-- ; comentário\nSELECT FROM WHERE;\n", with_sql_step=True)
    assert main(["plan", "--config", str(config), "--run-id", RID]) == 1
    captured = capsys.readouterr()
    assert "arquivo sql/marca.sql: linha 4" in captured.err and "sintaxe inválida" in captured.err
    assert "Como corrigir" in captured.err and "Traceback" not in captured.err
    assert captured.out == ""
    assert not (tmp_path / ".local").exists()


def test_run_local_executes_sql_step(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = make_project(tmp_path, with_sql_step=True)
    assert main(["run", "local", "--config", str(config), "--run-id", RID]) == 0
    out = capsys.readouterr().out
    assert [ln.split(":")[0] for ln in out.splitlines() if ln.startswith("[ok]")] == [
        "[ok] ingere",
        "[ok] ingere2",
        "[ok] resumo",
    ]
    db = next((tmp_path / ".local").rglob("*.duckdb"))
    con = duckdb.connect(str(db), read_only=True)
    try:
        assert con.execute(
            "SELECT etapa, status FROM silver._controle_execucoes ORDER BY registrado_em"
        ).fetchall() == [
            ("ingere", "ok"),
            ("ingere2", "ok"),
            ("resumo", "ok"),
        ]
    finally:
        con.close()


def test_run_local_sql_step_failure_exit_code_and_message(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = make_project(tmp_path, with_sql_step=True)
    text = config.read_text(encoding="utf-8")
    (tmp_path / "sql" / "quebra.sql").write_text("SELECT * FROM gold.nao_existe;\n", encoding="utf-8")
    config.write_text(
        text.replace("- sql/marca.sql\n  depende_de:\n  - ingere2", "- sql/quebra.sql\n  depende_de:\n  - ingere2"),
        encoding="utf-8",
    )
    assert main(["run", "local", "--config", str(config), "--run-id", RID]) == 1
    err = capsys.readouterr().err
    assert "[falha] resumo: arquivo sql/quebra.sql: instrução 1 de 1 (linha 1) falhou" in err
    assert "Traceback" not in err


@pytest.mark.parametrize("command", ["plan", "validate-config"])
def test_empty_sql_file_is_an_error_in_plan_and_validate(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], command: str
) -> None:
    config = make_project(tmp_path, sql="-- só comentário\n", with_sql_step=True)
    assert main([command, "--config", str(config)]) == 1
    err = capsys.readouterr().err
    assert "etapa 'resumo': o arquivo sql/marca.sql não contém instruções" in err and "Traceback" not in err


@pytest.mark.parametrize("command", ["plan", "validate-config"])
def test_view_with_two_statements_is_an_error_in_plan_and_validate(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], command: str
) -> None:
    config = make_project(tmp_path, with_sql_step=True)
    config.write_text(
        config.read_text(encoding="utf-8")
        .replace("tipo: sql", "tipo: view")
        .replace("- sql/marca.sql\n  depende_de", "sql: sql/marca.sql\n  depende_de"),
        encoding="utf-8",
    )
    (tmp_path / "sql" / "marca.sql").write_text("SELECT 1 AS n;\nSELECT 2 AS n;\n", encoding="utf-8")
    assert main([command, "--config", str(config)]) == 1
    assert "tem 2 instruções" in capsys.readouterr().err


def test_plan_shows_the_real_engine_of_quality_steps(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """`qualidade` roda como Cloud Run Job (não BigQuery); sql continua `bigquery`."""
    config = make_project(tmp_path, with_sql_step=True)
    data = yaml.safe_load(config.read_text(encoding="utf-8"))
    data["etapas"].append(
        {
            "id": "checa",
            "tipo": "qualidade",
            "depende_de": ["resumo"],
            "verificacoes": [{"nao_nulo": {"tabela": "gold.marca", "colunas": ["n"]}}],
        }
    )
    config.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")
    assert main(["plan", "--config", str(config), "--run-id", RID]) == 0
    heads = [ln for ln in capsys.readouterr().out.splitlines() if ln[:1].isdigit()]
    assert any(ln.startswith("3. resumo") and "motor=bigquery" in ln for ln in heads)
    assert any(ln.startswith("4. checa") and "motor=cloud_run" in ln for ln in heads)
