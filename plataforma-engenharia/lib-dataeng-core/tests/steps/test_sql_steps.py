from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from tests.steps.conftest import MakeCtx
from tests.steps.helpers import CONTROL, RID1, ingest_step, pipeline, rows

import dataeng_core.steps.registry as registry
from dataeng_core.base.errors import SqlError
from dataeng_core.steps import run_pipeline, run_step


def _write(project: Path, name: str, text: str) -> str:
    path = project / "sql" / name
    path.parent.mkdir(exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return f"sql/{name}"


def _sql(step_id: str, *files: str, **kw: Any) -> dict[str, Any]:
    return {"id": step_id, "tipo": "sql", "sql": list(files), **kw}


def _view(step_id: str, file: str, **kw: Any) -> dict[str, Any]:
    return {"id": step_id, "tipo": "view", "sql": file, **kw}


def _control(ctx: Any) -> list[tuple[Any, ...]]:
    return rows(ctx, f"SELECT etapa, status FROM {CONTROL} ORDER BY registrado_em")


def test_sql_and_view_are_registered() -> None:
    assert {"sql", "view"} <= set(registry.registered_types())


SETUP = "CREATE TABLE IF NOT EXISTS gold.resumo (chave VARCHAR, valor INTEGER);\n"
MERGE = """\
-- upsert; o texto ';' não divide
MERGE INTO gold.resumo AS t
USING (SELECT 'a' AS chave, ${valor} AS valor UNION ALL SELECT 'b', 20) AS s
ON t.chave = s.chave
WHEN MATCHED THEN UPDATE SET valor = s.valor
WHEN NOT MATCHED THEN INSERT (chave, valor) VALUES (s.chave, s.valor);
-- comentário no fim
"""


def test_sql_step_runs_merge_and_is_idempotent(project: Path, make_ctx: MakeCtx) -> None:
    setup, merge = _write(project, "setup.sql", SETUP), _write(project, "merge.sql", MERGE)
    pipe = pipeline(_sql("carga", setup, merge))
    ctx = make_ctx(pipe)
    ctx.params["valor"] = "10"

    first = run_step(pipe.get("carga"), ctx)
    assert (first.status, first.detail) == ("ok", "2 instruções executadas (sql/setup.sql, sql/merge.sql)")
    expected = [("a", 10), ("b", 20)]
    assert rows(ctx, "SELECT chave, valor FROM gold.resumo ORDER BY chave") == expected

    second = run_step(pipe.get("carga"), ctx)  # reexecução: mesmo conteúdo, sem duplicar
    assert second.status == "ok"
    assert rows(ctx, "SELECT chave, valor FROM gold.resumo ORDER BY chave") == expected

    ctx.params["valor"] = "11"  # o MERGE atualiza em vez de inserir de novo
    assert run_step(pipe.get("carga"), ctx).status == "ok"
    assert rows(ctx, "SELECT chave, valor FROM gold.resumo ORDER BY chave") == [("a", 11), ("b", 20)]
    assert _control(ctx) == [("carga", "ok")] * 3


def test_sql_step_renders_params(project: Path, make_ctx: MakeCtx) -> None:
    f = _write(
        project,
        "p.sql",
        "CREATE OR REPLACE TABLE ${dataset_gold}.p AS SELECT '${run_id}' AS r, '${ingestion_date}' AS d, "
        "'${meu}' AS m, ';' AS sep;\n",
    )
    pipe = pipeline(_sql("p", f))
    ctx = make_ctx(pipe)
    ctx.params["meu"] = "valor"
    assert run_step(pipe.get("p"), ctx).status == "ok"
    assert rows(ctx, "SELECT r, d, m, sep FROM gold.p") == [(RID1, "2026-01-01", "valor", ";")]


def test_view_step_creates_and_replaces(project: Path, make_ctx: MakeCtx) -> None:
    src = _write(project, "src.sql", "CREATE OR REPLACE TABLE gold.base AS SELECT 1 AS n UNION ALL SELECT 2;\n")
    v1 = _write(project, "v1.sql", "CREATE OR REPLACE VIEW gold.v_total AS SELECT SUM(n) AS total FROM gold.base;\n")
    v2 = _write(project, "v2.sql", "SELECT MAX(n) AS maximo FROM gold.base;\n")
    pipe = pipeline(
        _sql("base", src),
        _view("v_create", v1, depende_de=["base"]),
        _view("v_select", v2, nome_view="gold.v_total", depende_de=["v_create"]),
    )
    ctx = make_ctx(pipe)
    results = run_pipeline(pipe, ctx, only=["base", "v_create"])
    assert [(r.step_id, r.status) for r in results] == [("base", "ok"), ("v_create", "ok")]
    assert rows(ctx, "SELECT total FROM gold.v_total") == [(3,)]

    # substitui a view existente por um SELECT embrulhado em CREATE OR REPLACE VIEW <nome_view>
    result = run_step(pipe.get("v_select"), ctx)
    assert result.status == "ok", result.detail
    assert list(ctx.warehouse.query("SELECT * FROM gold.v_total").columns) == ["maximo"]
    assert rows(ctx, "SELECT * FROM gold.v_total") == [(2,)]
    assert run_step(pipe.get("v_select"), ctx).status == "ok"  # reexecutar é inofensivo


@pytest.mark.parametrize(
    ("text", "kw", "expected"),
    [
        ("SELECT 1 AS n;\n", {}, "informe `nome_view`"),
        ("SELECT 1 AS n;\n", {"nome_view": "gold.v-ruim"}, "nome de view inválido: 'gold.v-ruim'"),
        ("SELECT 1 AS n;\n", {"nome_view": "a.b.c"}, "nome de view inválido: 'a.b.c'"),
        ("SELECT 1 AS n; SELECT 2 AS n;\n", {"nome_view": "gold.v"}, "tem 2 instruções"),
        ("-- só comentário\n", {"nome_view": "gold.v"}, "sql/v.sql não contém instruções"),
        ("CREATE TABLE gold.t (n INTEGER);\n", {"nome_view": "gold.v"}, "não é CREATE OR REPLACE VIEW nem SELECT"),
        ("CREATE VIEW gold.v AS SELECT 1 AS n;\n", {}, "use CREATE OR REPLACE VIEW"),
        ("DROP TABLE gold.t;\n", {}, "não é CREATE OR REPLACE VIEW nem SELECT"),
    ],
)
def test_view_step_config_errors_are_actionable_and_run_nothing(
    project: Path, make_ctx: MakeCtx, text: str, kw: dict[str, Any], expected: str
) -> None:
    f = _write(project, "v.sql", text)
    pipe = pipeline(_view("v", f, **kw))
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("v"), ctx)
    assert result.status == "falha"
    assert expected in result.detail
    assert not ctx.warehouse.table_exists("gold.v") and not ctx.warehouse.table_exists("gold.t")


def test_missing_param_fails_before_executing_any_statement(project: Path, make_ctx: MakeCtx) -> None:
    ok = _write(project, "ok.sql", "CREATE TABLE gold.nao_pode_existir (n INTEGER);\n")
    bad = _write(project, "bad.sql", "SELECT 1;\n\n-- ok\nSELECT '${nao_definido}' AS x;\n")
    pipe = pipeline(_sql("s", ok, bad))
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("s"), ctx)
    assert result.status == "falha"
    assert "arquivo sql/bad.sql" in result.detail and "'${nao_definido}'" in result.detail
    assert ctx.warehouse.table_exists("gold.nao_pode_existir") is False  # a 1ª instrução NÃO rodou

    # marcador malformado no 2º arquivo também impede tudo
    _write(project, "bad.sql", "SELECT 1;\nSELECT '${ a }';\n")
    result = run_step(pipe.get("s"), ctx)
    assert result.status == "falha" and "arquivo sql/bad.sql: linha 2" in result.detail
    assert ctx.warehouse.table_exists("gold.nao_pode_existir") is False


def test_syntax_error_reports_file_and_line(project: Path, make_ctx: MakeCtx) -> None:
    ok = _write(project, "ok.sql", "CREATE TABLE gold.nao_pode_existir (n INTEGER);\n")
    bad = _write(project, "bad.sql", "SELECT 1;\n\n-- comentário; com ponto e vírgula\nSELECT FROM WHERE;\n")
    pipe = pipeline(_sql("s", ok, bad))
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("s"), ctx)
    assert result.status == "falha"
    assert "arquivo sql/bad.sql: linha 4" in result.detail and "sintaxe inválida" in result.detail
    assert ctx.warehouse.table_exists("gold.nao_pode_existir") is False


def test_empty_and_comment_only_files_are_config_errors(project: Path, make_ctx: MakeCtx) -> None:
    ok = _write(project, "ok.sql", "CREATE TABLE gold.nao_pode_existir (n INTEGER);\n")
    empty = _write(project, "vazio.sql", "-- nada aqui\n")
    pipe = pipeline(_sql("s", ok, empty))
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("s"), ctx)
    assert result.status == "falha"
    assert "o arquivo sql/vazio.sql não contém instruções" in result.detail
    assert ctx.warehouse.table_exists("gold.nao_pode_existir") is False


def test_missing_sql_file_is_actionable(project: Path, make_ctx: MakeCtx) -> None:
    pipe = pipeline(_sql("s", "sql/nao_existe.sql"))
    result = run_step(pipe.get("s"), make_ctx(pipe))
    assert result.status == "falha" and "sql/nao_existe.sql" in result.detail and "FileNotFoundError" in result.detail


def test_failing_statement_stops_and_reports_position(project: Path, make_ctx: MakeCtx) -> None:
    a = _write(project, "a.sql", "CREATE TABLE gold.t1 (n INTEGER);\nINSERT INTO gold.t1 VALUES (1);\n")
    b = _write(
        project,
        "b.sql",
        "CREATE TABLE gold.t2 (n INTEGER);\n\nSELECT * FROM gold.tabela_inexistente;\n"
        "CREATE TABLE gold.t3 (n INTEGER);\n",
    )
    pipe = pipeline(_sql("s", a, b))
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("s"), ctx)
    assert result.status == "falha"
    assert "arquivo sql/b.sql: instrução 2 de 3 (linha 3) falhou" in result.detail
    assert "3 de 5 instruções já executadas" in result.detail and "não garante atomicidade" in result.detail
    assert ctx.warehouse.table_exists("gold.t2") is True  # já rodou
    assert ctx.warehouse.table_exists("gold.t3") is False  # a seguinte NÃO rodou
    assert _control(ctx) == [("s", "falha")]


def test_warehouse_message_never_reaches_detail(
    project: Path, make_ctx: MakeCtx, monkeypatch: pytest.MonkeyPatch
) -> None:
    f = _write(project, "a.sql", "SELECT 1;\n")
    pipe = pipeline(_sql("s", f))
    ctx = make_ctx(pipe)
    secret = "senha-super-secreta"
    monkeypatch.setenv("TOKEN_X", secret)
    step = pipe.get("s")
    monkeypatch.setattr(type(step), "model_dump", lambda self, **_: {"x": {"segredo_env": "TOKEN_X"}})
    raised: list[SqlError] = []

    def boom(_statements: Any) -> None:
        raised.append(SqlError(f"mensagem do warehouse com {secret} e valor-de-dado", operation="warehouse.run_sql"))
        raise raised[0]

    monkeypatch.setattr(ctx.warehouse, "run_sql", boom)
    result = run_step(step, ctx)
    assert result.status == "falha"
    assert (
        secret not in result.detail
        and "valor-de-dado" not in result.detail
        and "mensagem do warehouse" not in result.detail
    )
    assert f"(SqlError, referência {raised[0].reference})" in result.detail
    assert "arquivo sql/a.sql: instrução 1 de 1 (linha 1) falhou" in result.detail


SENTINEL = "dado-sensivel-777"


def test_data_value_in_warehouse_error_does_not_leak(
    project: Path, make_ctx: MakeCtx, caplog: pytest.LogCaptureFixture, capfd: pytest.CaptureFixture[str]
) -> None:
    f = _write(
        project,
        "a.sql",
        f"CREATE TABLE gold.p AS SELECT '{SENTINEL}' AS v;\nSELECT CAST(v AS INT) FROM gold.p;\n",
    )
    pipe = pipeline(_sql("s", f))
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("s"), ctx)
    assert result.status == "falha"
    assert "instrução 2 de 2 (linha 2) falhou (SqlError, referência " in result.detail
    assert "1 de 2 instruções já executadas" in result.detail
    everything = result.detail + caplog.text + capfd.readouterr().err + repr(rows(ctx, f"SELECT * FROM {CONTROL}"))
    assert SENTINEL not in everything


def test_duckdb_syntax_error_does_not_leak_sql_text(project: Path, make_ctx: MakeCtx) -> None:
    # válido como BigQuery (passa na validação), inválido no DuckDB: o erro vem do warehouse
    f = _write(
        project,
        "a.sql",
        "SELECT 'texto-sentinela-abc' AS v FROM (SELECT 1) AS t WHERE FUNCAO_INEXISTENTE_XYZ(1) = 1;\n",
    )
    pipe = pipeline(_sql("s", f))
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("s"), ctx)
    assert result.status == "falha"
    assert "falhou (SqlError, referência " in result.detail
    assert "texto-sentinela-abc" not in result.detail and "funcao_inexistente_xyz" not in result.detail.lower()
    assert "texto-sentinela-abc" not in repr(rows(ctx, f"SELECT * FROM {CONTROL}"))


def test_sql_file_with_utf8_bom_is_read(project: Path, make_ctx: MakeCtx) -> None:
    path = project / "sql" / "bom.sql"
    path.parent.mkdir(exist_ok=True)
    path.write_bytes(b"\xef\xbb\xbf" + "CREATE TABLE gold.bom AS SELECT 'ação' AS v;\n".encode())
    pipe = pipeline(_sql("s", "sql/bom.sql"))
    ctx = make_ctx(pipe)
    assert run_step(pipe.get("s"), ctx).status == "ok"
    assert rows(ctx, "SELECT v FROM gold.bom") == [("ação",)]


@pytest.mark.parametrize("nome", ["gold.v_certa", "GOLD.V_CERTA", None])
def test_view_name_matching_file_is_accepted(project: Path, make_ctx: MakeCtx, nome: str | None) -> None:
    f = _write(project, "v.sql", "CREATE OR REPLACE VIEW gold.v_certa AS SELECT 1 AS n;\n")
    kw = {"nome_view": nome} if nome else {}
    pipe = pipeline(_view("v", f, **kw))
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("v"), ctx)
    assert result.status == "ok", result.detail
    assert rows(ctx, "SELECT n FROM gold.v_certa") == [(1,)]


def test_view_name_diverging_from_file_is_config_error(project: Path, make_ctx: MakeCtx) -> None:
    f = _write(project, "v.sql", "CREATE OR REPLACE VIEW gold.v_arquivo AS SELECT 1 AS n;\n")
    pipe = pipeline(_view("v", f, nome_view="gold.v_outra"))
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("v"), ctx)
    assert result.status == "falha"
    assert "cria a view 'gold.v_arquivo', mas `nome_view` é 'gold.v_outra'" in result.detail
    assert not ctx.warehouse.table_exists("gold.v_arquivo")


def test_idempotency_warning_goes_to_detail_without_failing(project: Path, make_ctx: MakeCtx) -> None:
    f = _write(project, "i.sql", "CREATE TABLE gold.t (n INTEGER);\n\nINSERT INTO gold.t VALUES (1);\n")
    pipe = pipeline(_sql("s", f))
    result = run_step(pipe.get("s"), make_ctx(pipe))
    assert result.status == "ok"
    assert "aviso: arquivo sql/i.sql: linha 3: INSERT sem MERGE" in result.detail


def test_step_order_respects_depends_on(project: Path, make_ctx: MakeCtx) -> None:
    a = _write(project, "a.sql", "CREATE OR REPLACE TABLE gold.ordem AS SELECT 'a' AS passo;\n")
    b = _write(
        project,
        "b.sql",
        "INSERT INTO gold.ordem SELECT 'b' WHERE NOT EXISTS (SELECT 1 FROM gold.ordem WHERE passo = 'b');\n",
    )
    v = _write(project, "v.sql", "SELECT STRING_AGG(passo, ',' ORDER BY passo) AS passos FROM gold.ordem;\n")
    # declaradas fora de ordem: a execução segue depende_de
    pipe = pipeline(
        _view("v", v, nome_view="gold.v_ordem", depende_de=["b"]),
        _sql("b", b, depende_de=["a"]),
        _sql("a", a, depende_de=[]),
    )
    ctx = make_ctx(pipe)
    results = run_pipeline(pipe, ctx)
    assert [(r.step_id, r.status) for r in results] == [("a", "ok"), ("b", "ok"), ("v", "ok")]
    assert rows(ctx, "SELECT passos FROM gold.v_ordem") == [("a,b",)]


def test_failed_step_leaves_dependents_not_executed(project: Path, make_ctx: MakeCtx) -> None:
    a = _write(project, "a.sql", "SELECT * FROM gold.nao_existe;\n")
    b = _write(project, "b.sql", "CREATE TABLE gold.nao_pode_existir (n INTEGER);\n")
    v = _write(project, "v.sql", "SELECT 1 AS n;\n")
    pipe = pipeline(
        _sql("a", a, depende_de=[]),
        _sql("b", b, depende_de=["a"]),
        _view("v", v, nome_view="gold.v_nao", depende_de=["b"]),
    )
    ctx = make_ctx(pipe)
    results = run_pipeline(pipe, ctx)
    assert [(r.step_id, r.status) for r in results] == [("a", "falha"), ("b", "nao_executada"), ("v", "nao_executada")]
    assert not ctx.warehouse.table_exists("gold.nao_pode_existir")
    assert _control(ctx) == [("a", "falha"), ("b", "nao_executada"), ("v", "nao_executada")]


def test_sql_step_after_ingestion_reads_silver(project: Path, make_ctx: MakeCtx) -> None:
    f = _write(
        project,
        "r.sql",
        "CREATE OR REPLACE TABLE ${dataset_gold}.total AS SELECT COUNT(*) AS n FROM ${dataset_silver}.vendas;\n",
    )
    pipe = pipeline(ingest_step("ingere"), {**_sql("resumo", f), "depende_de": ["ingere"]})
    ctx = make_ctx(pipe)
    results = run_pipeline(pipe, ctx)
    assert [(r.step_id, r.status) for r in results] == [("ingere", "ok"), ("resumo", "ok")]
    assert rows(ctx, "SELECT n FROM gold.total") == [(2,)]
