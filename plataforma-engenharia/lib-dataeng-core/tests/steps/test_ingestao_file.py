from __future__ import annotations

import datetime as dt
import hashlib
import io
import json
import zipfile
from pathlib import Path

import pandas as pd
import pytest
import requests
import responses
from tests.steps.conftest import MakeCtx
from tests.steps.helpers import (
    BRONZE,
    CSV,
    QUARANTINE,
    RID1,
    RID2,
    RID3,
    TABLE,
    FlakyWarehouse,
    control_rows,
    environment,
    ingest_step,
    list_bronze,
    names,
    pipeline,
    rows,
    with_run,
    write_source,
)

import dataeng_core.steps.ingestao as ingestao
from dataeng_core.base.errors import ConfigError
from dataeng_core.steps import build_context, run_step

DAY1 = dt.date(2026, 1, 1)
BRONZE_DIR1 = f"{BRONZE}/ingestion_date=2026-01-01/run_id={RID1}"
URI1 = f"{BRONZE_DIR1}/vendas.csv"


def _silver(ctx: object) -> list[tuple[object, ...]]:
    return rows(ctx, f"SELECT id, produto, valor, _run_id FROM {TABLE} ORDER BY id")  # type: ignore[arg-type]


# ------------------------------------------------------------------- ponta a ponta


def test_ingest_file_end_to_end(project: Path, make_ctx: MakeCtx) -> None:
    (project / "sql").mkdir()
    (project / "sql" / "resumo.sql").write_text(
        "-- roda depois da carga\n"
        "CREATE OR REPLACE TABLE ${dataset_silver}.resumo AS\n"
        "SELECT count(*) AS n, max(_run_id) AS run FROM ${dataset_silver}.vendas;\n",
        encoding="utf-8",
    )
    pipe = pipeline(ingest_step(dml_pos_carga=["sql/resumo.sql"], qualidade=[{"nao_nulo": ["id"]}]))
    ctx = make_ctx(pipe)
    before = pd.Timestamp.now(tz="UTC")

    result = run_step(pipe.get("ingere"), ctx)

    assert result.status == "ok", result.detail
    assert (result.rows_read, result.rows_written, result.rows_rejected) == (6, 2, 3)
    assert "1 duplicada(s) removida(s)" in result.detail

    # bronze: arquivo original + manifesto com tamanho e SHA-256
    assert list_bronze(ctx) == [f"{BRONZE_DIR1}/_manifesto.json", URI1]
    data = CSV.encode("utf-8")
    assert ctx.storage.read(URI1) == data
    manifest = json.loads(ctx.storage.read(f"{BRONZE_DIR1}/_manifesto.json"))
    assert manifest["arquivo"] == "vendas.csv"
    assert manifest["tamanho_bytes"] == len(data)
    assert manifest["sha256"] == hashlib.sha256(data).hexdigest()
    assert (manifest["run_id"], manifest["etapa"]) == (RID1, "ingere")

    # silver: colunas do schema de destino + controle
    silver = ctx.warehouse.query(f"SELECT * FROM {TABLE} ORDER BY id")
    assert list(silver.columns) == [
        "id",
        "produto",
        "valor",
        "_run_id",
        "_source_file",
        "_loaded_at",
        "_ingestion_date",
    ]
    got = [
        (
            r["id"],
            r["produto"],
            float(r["valor"]),
            r["_run_id"],
            r["_source_file"],
            pd.Timestamp(r["_ingestion_date"]).date(),
        )
        for r in silver.to_dict("records")
    ]
    assert got == [
        ("1", "gasolina", 5.5, RID1, URI1, DAY1),
        ("2", "etanol", 3.99, RID1, URI1, DAY1),
    ]
    loaded = pd.Timestamp(silver["_loaded_at"].iloc[0])
    assert loaded.tzinfo is not None and before - pd.Timedelta(seconds=1) <= loaded <= pd.Timestamp.now(tz="UTC")

    # quarentena: linha original e motivo
    quarantine = rows(ctx, f"SELECT _linha_original, _motivo, _run_id, _source_file FROM {QUARANTINE} ORDER BY _motivo")
    assert quarantine == [
        (
            json.dumps({"id": None, "produto": "gnv", "valor": "4,00"}),
            "coluna 'id': valor nulo (regra nao_nulo)",
            RID1,
            URI1,
        ),
        (
            json.dumps({"id": "3", "produto": "diesel", "valor": "abc"}),
            "coluna 'valor': valor não convertível para decimal",
            RID1,
            URI1,
        ),
        ("4;extra;1;2", "numero_de_colunas_inesperado", RID1, URI1),
    ]

    # controle com as contagens e a qualidade
    assert control_rows(ctx) == [(RID1, "ingere", "ok", 6, 2, 3)]
    (quality_json,) = rows(ctx, "SELECT qualidade FROM silver._controle_execucoes")[0]
    assert json.loads(str(quality_json)) == [
        {"nome": "nao_nulo", "coluna": "id", "aprovado": True, "taxa": 1.0, "limiar": 1.0, "bloqueante": False}
    ]
    assert result.quality[0].aprovado is True

    # dml_pos_carga rodou depois da carga, com parâmetros
    assert rows(ctx, "SELECT n, run FROM silver.resumo") == [(2, RID1)]


def test_ddl_runs_before_load_with_params(project: Path, make_ctx: MakeCtx) -> None:
    (project / "sql").mkdir()
    (project / "sql" / "ddl.sql").write_text(
        "CREATE TABLE IF NOT EXISTS ${dataset_silver}.vendas (\n"
        "  id VARCHAR, produto VARCHAR, valor DECIMAL(10, 2), extra_ddl VARCHAR,\n"
        "  _run_id VARCHAR, _source_file VARCHAR, _loaded_at TIMESTAMPTZ, _ingestion_date DATE\n);\n",
        encoding="utf-8",
    )
    pipe = pipeline(ingest_step(ddl=["sql/ddl.sql"]))
    ctx = make_ctx(pipe)
    assert run_step(pipe.get("ingere"), ctx).status == "ok"
    types = dict(rows(ctx, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'vendas'"))
    assert types["valor"] == "DECIMAL(10,2)" and "extra_ddl" in types
    assert rows(ctx, f"SELECT id, extra_ddl FROM {TABLE} ORDER BY id") == [("1", None), ("2", None)]


# --------------------------------------------------------------------- idempotência


def test_rerun_does_not_duplicate_overwrite_partition(project: Path, make_ctx: MakeCtx) -> None:
    pipe = pipeline(ingest_step())
    ctx = make_ctx(pipe)
    step = pipe.get("ingere")
    assert run_step(step, ctx).status == "ok"
    assert run_step(step, with_run(ctx, RID2)).status == "ok"  # outro run, mesmo dia
    assert run_step(step, with_run(ctx, RID2)).status == "ok"  # o mesmo run de novo

    assert _silver(ctx) == [("1", "gasolina", 5.5, RID2), ("2", "etanol", 3.99, RID2)]
    assert rows(ctx, f"SELECT count(*), min(_run_id) FROM {QUARANTINE}") == [(3, RID2)]
    assert [r[:3] for r in control_rows(ctx)] == [
        (RID1, "ingere", "ok"),
        (RID2, "ingere", "ok"),
        (RID2, "ingere", "ok"),
    ]


def test_rerun_does_not_duplicate_merge(project: Path, make_ctx: MakeCtx) -> None:
    pipe = pipeline(ingest_step(modo_carga="merge", chaves_merge=["id"]))
    ctx = make_ctx(pipe)
    step = pipe.get("ingere")
    assert run_step(step, ctx).status == "ok"
    assert run_step(step, ctx).status == "ok"
    assert _silver(ctx) == [("1", "gasolina", 5.5, RID1), ("2", "etanol", 3.99, RID1)]

    write_source(project, "id;produto;valor\n1;gasolina;6,00\n5;gnv;4,10\n")
    assert run_step(step, with_run(ctx, RID3)).status == "ok"  # outro dia: merge por chave
    assert _silver(ctx) == [
        ("1", "gasolina", 6.0, RID3),
        ("2", "etanol", 3.99, RID1),
        ("5", "gnv", 4.1, RID3),
    ]


def test_rerun_same_run_does_not_duplicate_append(project: Path, make_ctx: MakeCtx) -> None:
    pipe = pipeline(ingest_step(modo_carga="append"))
    ctx = make_ctx(pipe)
    step = pipe.get("ingere")
    assert run_step(step, ctx).status == "ok"
    assert run_step(step, ctx).status == "ok"  # reexecução do mesmo run
    assert _silver(ctx) == [("1", "gasolina", 5.5, RID1), ("2", "etanol", 3.99, RID1)]
    assert rows(ctx, f"SELECT count(*) FROM {QUARANTINE}") == [(3,)]

    assert run_step(step, with_run(ctx, RID2)).status == "ok"  # run novo: append acrescenta
    assert rows(ctx, f"SELECT _run_id, count(*) FROM {TABLE} GROUP BY 1 ORDER BY 1") == [(RID1, 2), (RID2, 2)]


# ------------------------------------------------------- falha entre bronze e silver


def test_failure_between_bronze_and_silver_then_rerun_completes_without_duplicates(
    project: Path, make_ctx: MakeCtx
) -> None:
    pipe = pipeline(ingest_step())
    ctx = make_ctx(pipe)
    ctx.warehouse = FlakyWarehouse(ctx.warehouse, TABLE, times=1)  # type: ignore[assignment]
    step = pipe.get("ingere")

    failed = run_step(step, ctx)
    assert failed.status == "falha"
    assert "fase 'carga_silver'" in failed.detail and "falha simulada" in failed.detail
    assert (failed.rows_read, failed.rows_written) == (6, 0)
    assert names(list_bronze(ctx)) == ["_manifesto.json", "vendas.csv"]  # o bronze existe
    assert ctx.warehouse.table_exists(TABLE) is False
    assert control_rows(ctx) == [(RID1, "ingere", "falha", 6, 0, 3)]

    done = run_step(step, ctx)  # reexecução do mesmo run
    assert done.status == "ok", done.detail
    assert _silver(ctx) == [("1", "gasolina", 5.5, RID1), ("2", "etanol", 3.99, RID1)]
    assert names(list_bronze(ctx)) == ["_manifesto.json", "vendas.csv"]  # sobrescrito, não duplicado
    assert rows(ctx, f"SELECT count(*) FROM {QUARANTINE}") == [(3,)]
    assert control_rows(ctx) == [(RID1, "ingere", "falha", 6, 0, 3), (RID1, "ingere", "ok", 6, 2, 3)]


@pytest.mark.parametrize("modo", ["append", "overwrite_particao"])
def test_failure_after_silver_load_then_rerun_does_not_duplicate(project: Path, make_ctx: MakeCtx, modo: str) -> None:
    pipe = pipeline(ingest_step(modo_carga=modo))
    ctx = make_ctx(pipe)
    ctx.warehouse = FlakyWarehouse(ctx.warehouse, QUARANTINE, times=1)  # type: ignore[assignment]
    step = pipe.get("ingere")

    failed = run_step(step, ctx)
    assert failed.status == "falha" and "fase 'quarentena'" in failed.detail
    assert len(_silver(ctx)) == 2  # a silver já foi gravada nesta tentativa

    assert run_step(step, ctx).status == "ok"
    assert _silver(ctx) == [("1", "gasolina", 5.5, RID1), ("2", "etanol", 3.99, RID1)]
    assert [r[2] for r in control_rows(ctx)] == ["falha", "ok"]


# ---------------------------------------------------------------------- entrada vazia


@pytest.mark.parametrize("content", ["", "id;produto;valor\n", "﻿id;produto;valor\r\n\r\n"])
def test_empty_input_succeeds_with_zero_rows_and_control_row(project: Path, make_ctx: MakeCtx, content: str) -> None:
    pipe = pipeline(ingest_step(qualidade=[{"nao_nulo": ["id"], "bloqueante": True}]))
    ctx = make_ctx(pipe)
    step = pipe.get("ingere")
    assert run_step(step, ctx).status == "ok"  # carga anterior na mesma partição

    write_source(project, content)
    result = run_step(step, with_run(ctx, RID2))

    assert result.status == "ok", result.detail
    assert (result.rows_read, result.rows_written, result.rows_rejected) == (0, 0, 0)
    assert "entrada vazia" in result.detail
    assert [q.aprovado for q in result.quality] == [True]
    assert _silver(ctx) == [("1", "gasolina", 5.5, RID1), ("2", "etanol", 3.99, RID1)]  # nada foi apagado
    assert control_rows(ctx)[-1] == (RID2, "ingere", "ok", 0, 0, 0)
    bronze2 = f"{BRONZE}/ingestion_date=2026-01-01/run_id={RID2}"
    assert json.loads(ctx.storage.read(f"{bronze2}/_manifesto.json"))["tamanho_bytes"] == len(content.encode())


def test_empty_first_run_then_normal_run_loads(project: Path, make_ctx: MakeCtx) -> None:
    write_source(project, "id;produto;valor\n")
    pipe = pipeline(ingest_step())
    ctx = make_ctx(pipe)
    step = pipe.get("ingere")
    assert run_step(step, ctx).status == "ok"
    assert ctx.warehouse.table_exists(TABLE) is False

    write_source(project, CSV)
    assert run_step(step, with_run(ctx, RID2)).status == "ok"
    assert _silver(ctx) == [("1", "gasolina", 5.5, RID2), ("2", "etanol", 3.99, RID2)]


def test_all_null_column_gets_destination_type_on_first_load(project: Path, make_ctx: MakeCtx) -> None:
    write_source(project, "id;produto;valor\n1;;\n")  # produto e valor sempre nulos na 1ª carga
    pipe = pipeline(ingest_step())
    ctx = make_ctx(pipe)
    step = pipe.get("ingere")
    assert run_step(step, ctx).status == "ok"

    write_source(project, CSV)
    result = run_step(step, with_run(ctx, RID3))
    assert result.status == "ok", result.detail
    assert rows(ctx, f"SELECT id, produto, valor FROM {TABLE} ORDER BY _ingestion_date, id") == [
        ("1", None, None),
        ("1", "gasolina", 5.5),
        ("2", "etanol", 3.99),
    ]


# ------------------------------------------------------------------------- qualidade


def test_blocking_quality_failure_marks_step_failed(project: Path, make_ctx: MakeCtx) -> None:
    (project / "sql").mkdir()
    (project / "sql" / "pos.sql").write_text("CREATE TABLE silver.nao_deveria AS SELECT 1 AS x", encoding="utf-8")
    checks = [{"faixa": {"coluna": "valor", "minimo": 4}, "bloqueante": True}]
    pipe = pipeline(ingest_step(qualidade=checks, dml_pos_carga=["sql/pos.sql"]))
    ctx = make_ctx(pipe)

    result = run_step(pipe.get("ingere"), ctx)

    assert result.status == "falha"
    assert "fase 'qualidade'" in result.detail and "faixa em 'valor'" in result.detail
    assert [(q.nome, q.coluna, q.aprovado, q.taxa) for q in result.quality] == [("faixa", "valor", False, 0.5)]
    assert ctx.warehouse.table_exists("silver.nao_deveria") is False  # dml_pos_carga não rodou
    assert control_rows(ctx) == [(RID1, "ingere", "falha", 6, 2, 3)]
    (quality_json,) = rows(ctx, "SELECT qualidade FROM silver._controle_execucoes")[0]
    assert json.loads(str(quality_json))[0]["aprovado"] is False


def test_non_blocking_quality_failure_keeps_step_ok(project: Path, make_ctx: MakeCtx) -> None:
    pipe = pipeline(ingest_step(qualidade=[{"faixa": {"coluna": "valor", "minimo": 4}}]))
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("ingere"), ctx)
    assert result.status == "ok"
    assert [q.aprovado for q in result.quality] == [False]


# ------------------------------------------------------------- outras formas de origem


def test_zip_source_reads_members_of_the_format(project: Path, make_ctx: MakeCtx) -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("parte_a.csv", "id;produto;valor\n1;gasolina;5,50\n")
        zf.writestr("parte_b.csv", "id;produto;valor\n2;etanol;3,99\n")
        zf.writestr("leia-me.txt", "ignorar")
    write_source(project, buf.getvalue(), name="vendas.zip")
    step_cfg = ingest_step()
    step_cfg["origem"]["localizacao"] = "dados/vendas.zip"
    pipe = pipeline(step_cfg)
    ctx = make_ctx(pipe)

    assert run_step(pipe.get("ingere"), ctx).status == "ok"
    uri = f"{BRONZE_DIR1}/vendas.zip"
    assert rows(ctx, f"SELECT id, _source_file FROM {TABLE} ORDER BY id") == [
        ("1", f"{uri}!parte_a.csv"),
        ("2", f"{uri}!parte_b.csv"),
    ]


@responses.activate
def test_http_source_downloads_and_hides_query_in_errors(project: Path, make_ctx: MakeCtx) -> None:
    url = "https://dados.example.test/arquivos/vendas.csv"
    responses.add(responses.GET, url, body=CSV.encode("utf-8"))
    step_cfg = ingest_step()
    step_cfg["origem"]["localizacao"] = url + "?token=abc123"
    pipe = pipeline(step_cfg)
    ctx = make_ctx(pipe)
    assert run_step(pipe.get("ingere"), ctx).status == "ok"
    assert names(list_bronze(ctx)) == ["_manifesto.json", "vendas.csv"]

    responses.replace(responses.GET, url, status=404)
    failed = run_step(pipe.get("ingere"), with_run(ctx, RID2))
    assert failed.status == "falha" and "HTTP 404" in failed.detail
    assert "abc123" not in failed.detail


def test_missing_source_file_fails_with_actionable_detail(project: Path, make_ctx: MakeCtx) -> None:
    (project / "dados" / "vendas.csv").unlink()
    pipe = pipeline(ingest_step())
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("ingere"), ctx)
    assert result.status == "falha"
    assert "fase 'leitura'" in result.detail and "arquivo de origem inexistente" in result.detail
    assert list_bronze(ctx) == []
    assert control_rows(ctx) == [(RID1, "ingere", "falha", 0, 0, 0)]


def test_invalid_table_name_fails_before_writing_anything(project: Path, make_ctx: MakeCtx) -> None:
    pipe = pipeline(ingest_step(tabela="silver.vendas; DROP TABLE x"))
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("ingere"), ctx)
    assert result.status == "falha" and "nome de tabela inválido" in result.detail
    assert list_bronze(ctx) == []


def test_missing_required_destination_column_fails(project: Path, make_ctx: MakeCtx) -> None:
    pipe = pipeline(ingest_step(transformacao=[{"renomear": {"id": "codigo"}}]))
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("ingere"), ctx)
    assert result.status == "falha"
    assert "fase 'transformacao'" in result.detail and "coluna obrigatória do schema de destino" in result.detail
    assert ctx.warehouse.table_exists(TABLE) is False


# ---------------------------------------------------------------------------- origem API

API_URL = "https://api.example.test/vendas"


def _api_step(**kw: object) -> dict[str, object]:
    cfg = ingest_step(
        "ingere_api",
        origem={"tipo": "api", "endpoint": API_URL, "schema": "schemas/origem.yaml", **kw},
        transformacao=[{"tipar": {"valor": {"tipo": "decimal"}}}],
    )
    return cfg


@responses.activate
def test_ingest_api_end_to_end(project: Path, make_ctx: MakeCtx) -> None:
    records = [
        {"id": 1, "produto": "gasolina", "valor": 5.5, "extra": {"a": 1}},
        {"id": 2, "produto": None, "valor": "3.99"},
        {"id": 3, "produto": "", "valor": "x"},
    ]
    responses.add(responses.GET, API_URL, json=records)
    pipe = pipeline(_api_step())
    ctx = make_ctx(pipe)

    result = run_step(pipe.get("ingere_api"), ctx)

    assert result.status == "ok", result.detail
    assert (result.rows_read, result.rows_written, result.rows_rejected) == (3, 2, 1)
    uri = f"{BRONZE_DIR1}/ingere_api.json"
    assert json.loads(ctx.storage.read(uri)) == records
    assert rows(ctx, f"SELECT id, produto, valor, _source_file FROM {TABLE} ORDER BY id") == [
        ("1", "gasolina", 5.5, uri),
        ("2", None, 3.99, uri),
    ]
    assert rows(ctx, f"SELECT _linha_original, _motivo FROM {QUARANTINE}") == [
        (
            json.dumps({"id": "3", "produto": None, "valor": "x", "extra": None}),
            "coluna 'valor': valor não convertível para decimal",
        )
    ]


@responses.activate
def test_api_without_records_succeeds_with_zero_rows(project: Path, make_ctx: MakeCtx) -> None:
    responses.add(responses.GET, API_URL, json=[])
    pipe = pipeline(_api_step())
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("ingere_api"), ctx)
    assert result.status == "ok" and result.rows_read == 0
    assert control_rows(ctx) == [(RID1, "ingere_api", "ok", 0, 0, 0)]


# ------------------------------------------------- revisão 1: dml_pos_carga sem tabela

RESUMO_SQL = "CREATE OR REPLACE TABLE silver.resumo AS SELECT count(*) AS n FROM silver.vendas"


def _with_resumo(project: Path) -> dict[str, object]:
    (project / "sql").mkdir(exist_ok=True)
    (project / "sql" / "pos.sql").write_text(RESUMO_SQL, encoding="utf-8")
    return ingest_step(dml_pos_carga=["sql/pos.sql"])


@pytest.mark.parametrize(
    "content",
    ["id;produto;valor\n", "id;produto;valor\n;sem_id;1,00\n3;diesel;abc\n"],
    ids=["vazio", "tudo_rejeitado"],
)
def test_first_run_without_rows_skips_post_load_dml(project: Path, make_ctx: MakeCtx, content: str) -> None:
    write_source(project, content)
    pipe = pipeline(_with_resumo(project))
    ctx = make_ctx(pipe)

    result = run_step(pipe.get("ingere"), ctx)

    assert result.status == "ok", result.detail
    assert result.rows_written == 0
    assert "dml_pos_carga não executado" in result.detail and TABLE in result.detail
    assert ctx.warehouse.table_exists(TABLE) is False
    assert ctx.warehouse.table_exists("silver.resumo") is False
    assert control_rows(ctx)[-1][2] == "ok"


def test_empty_run_with_existing_table_runs_post_load_dml(project: Path, make_ctx: MakeCtx) -> None:
    pipe = pipeline(_with_resumo(project))
    ctx = make_ctx(pipe)
    step = pipe.get("ingere")
    assert run_step(step, ctx).status == "ok"
    ctx.warehouse.run_sql(["DROP TABLE silver.resumo"])

    write_source(project, "id;produto;valor\n")
    result = run_step(step, with_run(ctx, RID2))

    assert result.status == "ok", result.detail
    assert "dml_pos_carga não executado" not in result.detail
    assert rows(ctx, "SELECT n FROM silver.resumo") == [(2,)]


# ------------------------------------------- revisão 1: ingestion_date explícita / run_id sem data


def test_run_id_without_date_requires_explicit_ingestion_date(project: Path) -> None:
    pipe = pipeline(ingest_step())
    with pytest.raises(ConfigError, match="não contém data"):
        build_context(pipe, environment(), project_root=project, run_id="manual-run")


def test_explicit_ingestion_date_makes_rerun_on_another_day_idempotent(project: Path) -> None:
    pipe = pipeline(ingest_step())
    ctx = build_context(
        pipe, environment(), project_root=project, run_id="manual-run", ingestion_date=dt.date(2025, 12, 31)
    )
    try:
        ctx.warehouse = FlakyWarehouse(ctx.warehouse, QUARANTINE, times=1)  # type: ignore[assignment]
        step = pipe.get("ingere")
        assert run_step(step, ctx).status == "falha"  # silver gravada, quarentena falhou
        assert run_step(step, ctx).status == "ok"  # reexecução (em qualquer dia) com a mesma data explícita
        assert rows(ctx, f"SELECT CAST(_ingestion_date AS VARCHAR), count(*) FROM {TABLE} GROUP BY 1") == [
            ("2025-12-31", 2)
        ]
        assert names(list_bronze(ctx)) == ["_manifesto.json", "vendas.csv"]
        assert list_bronze(ctx)[0].startswith(f"{BRONZE}/ingestion_date=2025-12-31/run_id=manual-run/")
    finally:
        ctx.warehouse.close()  # type: ignore[attr-defined]


def test_explicit_ingestion_date_overrides_run_id_date(project: Path) -> None:
    ctx = build_context(
        pipeline(ingest_step()), environment(), project_root=project, run_id=RID1, ingestion_date=dt.date(2025, 5, 6)
    )
    try:
        assert ctx.params["ingestion_date"] == "2025-05-06"
        assert run_step(ctx.pipeline.get("ingere"), ctx).status == "ok"
        assert rows(ctx, f"SELECT DISTINCT CAST(_ingestion_date AS VARCHAR) FROM {TABLE}") == [("2025-05-06",)]
    finally:
        ctx.warehouse.close()  # type: ignore[attr-defined]


# ------------------------------------------------------------- revisão 1: origem http(s)


@pytest.mark.parametrize("loc", ["file:///etc/passwd", "ftp://host/vendas.csv", "FILE://x/y.csv"])
def test_unsupported_url_scheme_is_config_error(project: Path, make_ctx: MakeCtx, loc: str) -> None:
    step_cfg = ingest_step()
    step_cfg["origem"]["localizacao"] = loc
    pipe = pipeline(step_cfg)
    result = run_step(pipe.get("ingere"), make_ctx(pipe))
    assert result.status == "falha"
    assert "Configuração inválida" in result.detail and "esquema" in result.detail
    assert "inexistente" not in result.detail


@responses.activate
def test_uppercase_https_scheme_is_accepted(project: Path, make_ctx: MakeCtx) -> None:
    responses.add(responses.GET, "https://dados.example.test/vendas.csv", body=CSV.encode())
    step_cfg = ingest_step()
    step_cfg["origem"]["localizacao"] = "HTTPS://dados.example.test/vendas.csv"
    pipe = pipeline(step_cfg)
    assert run_step(pipe.get("ingere"), make_ctx(pipe)).status == "ok"


@responses.activate
def test_http_redirect_is_not_followed(project: Path, make_ctx: MakeCtx) -> None:
    url = "https://dados.example.test/vendas.csv"
    target = "http://169.254.169.254/computeMetadata/v1/"
    responses.add(responses.GET, url, status=302, headers={"Location": target})
    responses.add(responses.GET, target, body="segredo-da-metadata")
    step_cfg = ingest_step()
    step_cfg["origem"]["localizacao"] = url
    pipe = pipeline(step_cfg)

    result = run_step(pipe.get("ingere"), make_ctx(pipe))

    assert result.status == "falha" and "redirecionamento (HTTP 302)" in result.detail
    assert [c.request.url for c in responses.calls] == [url]


@responses.activate
def test_http_download_over_limit_fails(project: Path, make_ctx: MakeCtx, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ingestao, "MAX_DOWNLOAD_BYTES", 10)
    url = "https://dados.example.test/vendas.csv"
    responses.add(responses.GET, url, body=CSV.encode())
    step_cfg = ingest_step()
    step_cfg["origem"]["localizacao"] = url
    pipe = pipeline(step_cfg)
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("ingere"), ctx)
    assert result.status == "falha" and "excede o limite de 10 bytes" in result.detail
    assert list_bronze(ctx) == []


def test_http_timeout_is_connect_and_read_tuple(
    project: Path, make_ctx: MakeCtx, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: dict[str, object] = {}

    def fake_get(url: str, **kwargs: object) -> object:
        seen.update(kwargs)
        raise requests.ConnectionError("sem rede")

    monkeypatch.setattr(ingestao.requests, "get", fake_get)
    step_cfg = ingest_step()
    step_cfg["origem"]["localizacao"] = "https://dados.example.test/vendas.csv"
    pipe = pipeline(step_cfg)
    result = run_step(pipe.get("ingere"), make_ctx(pipe))
    assert result.status == "falha" and "ConnectionError" in result.detail
    timeout = seen["timeout"]
    assert isinstance(timeout, tuple) and len(timeout) == 2
    assert seen["allow_redirects"] is False and seen["stream"] is True


# ------------------------------------------------- revisão 1: colunas reservadas e log


@pytest.mark.parametrize(
    "col", ["_run_id", "_source_file", "_loaded_at", "_ingestion_date", "_motivo", "_linha_original"]
)
def test_reserved_column_in_source_fails_with_its_name(project: Path, make_ctx: MakeCtx, col: str) -> None:
    write_source(project, f"id;produto;valor;{col}\n1;gasolina;5,50;x\n")
    pipe = pipeline(ingest_step())
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("ingere"), ctx)
    assert result.status == "falha"
    assert f"coluna reservada '{col}'" in result.detail
    assert ctx.warehouse.table_exists(TABLE) is False


@responses.activate
def test_reserved_column_in_api_response_fails(project: Path, make_ctx: MakeCtx) -> None:
    responses.add(responses.GET, API_URL, json=[{"id": 1, "_run_id": "forjado"}])
    pipe = pipeline(_api_step())
    result = run_step(pipe.get("ingere_api"), make_ctx(pipe))
    assert result.status == "falha" and "coluna reservada '_run_id'" in result.detail


def test_unexpected_error_is_logged_with_redacted_traceback(
    project: Path,
    make_ctx: MakeCtx,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret = "S3GR3D0-log"
    monkeypatch.setenv("API_TOKEN_LOG", secret)

    def boom(*args: object, **kwargs: object) -> object:
        raise RuntimeError(f"explodiu com {secret}")

    monkeypatch.setattr(ingestao, "fetch_records", boom)
    auth = {"tipo": "bearer", "segredo_env": "API_TOKEN_LOG"}
    pipe = pipeline(ingest_step("ingere_api", origem={"tipo": "api", "endpoint": API_URL, "autenticacao": auth}))
    result = run_step(pipe.get("ingere_api"), make_ctx(pipe))

    err = capsys.readouterr().err
    assert result.status == "falha" and "RuntimeError" in result.detail
    assert "Traceback" in err and "RuntimeError" in err and "in boom" in err
    assert secret not in err and secret not in result.detail


def test_hook_import_failure_does_not_leak_the_exception_message(
    project: Path, make_ctx: MakeCtx, capsys: pytest.CaptureFixture[str]
) -> None:
    (project / "custom").mkdir()
    (project / "custom" / "__init__.py").write_text("", encoding="utf-8")
    (project / "custom" / "limpa.py").write_text("raise ValueError('CNPJ SENTINELA-654 inválido')\n", encoding="utf-8")
    pipe = pipeline(ingest_step(transformacao=[{"hook": "custom.limpa:f"}]))
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("ingere"), ctx)
    assert result.status == "falha"
    assert "(ValueError)" in result.detail and "custom.limpa" in result.detail
    err = capsys.readouterr().err
    assert "Traceback" in err and "ValueError" in err  # o diagnóstico vai para o log
    for text in (result.detail, str(rows(ctx, "SELECT * FROM silver._controle_execucoes"))):
        assert "SENTINELA-654" not in text


# ------------------------------------------------- revisão final: o texto do warehouse não vaza (ddl/dml_pos_carga)

SENTINEL = "POSTO-SENTINELA-02"


@pytest.mark.parametrize("phase", ["ddl", "dml_pos_carga"])
def test_sql_failure_in_ingestion_never_leaks_warehouse_text(
    project: Path, make_ctx: MakeCtx, caplog: pytest.LogCaptureFixture, phase: str
) -> None:
    write_source(project, f"id;produto;valor\n1;{SENTINEL};5,50\n")
    (project / "sql").mkdir()
    if phase == "ddl":
        sql = f"CREATE TABLE silver.aux AS SELECT CAST('{SENTINEL}' AS INTEGER) AS x;\n"
    else:
        sql = "UPDATE silver.vendas SET id = CAST(produto AS INTEGER);\n"
    (project / "sql" / "falha.sql").write_text(sql, encoding="utf-8")
    pipe = pipeline(ingest_step(**{phase: ["sql/falha.sql"]}))
    ctx = make_ctx(pipe)

    with caplog.at_level("DEBUG"):
        result = run_step(pipe.get("ingere"), ctx)

    assert result.status == "falha"
    assert "sql/falha.sql" in result.detail and "instrução 1 de 1" in result.detail
    assert "SqlError" in result.detail and f"fase '{phase}'" in result.detail
    stored = " ".join(str(r) for r in rows(ctx, "SELECT * FROM silver._controle_execucoes"))
    for text in (result.detail, caplog.text, stored):
        assert SENTINEL not in text and "Could not convert" not in text
