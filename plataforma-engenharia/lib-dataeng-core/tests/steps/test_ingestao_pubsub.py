"""Etapa `ingestao` com origem `pubsub`: puxar → bronze → silver/quarentena → ack (nessa ordem)."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from tests.steps.helpers import RID1, RID2, RID3, FlakyWarehouse, environment, pipeline, rows

import dataeng_core.steps.context as context_module
from dataeng_core.base.errors import LoadError, PubSubError
from dataeng_core.base.pipeline import Pipeline
from dataeng_core.pubsub import LocalPubSub, PubSubMessage
from dataeng_core.steps import StepContext, build_context, run_step

SUB = "eventos-sub"
BRONZE = "gs://bronze-teste/bronze/eventos"
TABLE = "silver.eventos"
QUARANTINE = "silver.eventos_rejeitados"
CONTROL = "silver._controle_execucoes"
T0 = datetime(2026, 1, 1, 6, 0, tzinfo=UTC)
PT = "2026-01-01T06:00:00+00:00"

ORIGEM_SCHEMA = """\
colunas:
  - {nome: id, tipo: texto, obrigatoria: true}
  - {nome: produto__nome, tipo: texto}
  - {nome: valor, tipo: texto}
"""

DESTINO_SCHEMA = """\
colunas:
  - {nome: message_id, tipo: texto, obrigatoria: true}
  - {nome: publish_time, tipo: texto}
  - {nome: id, tipo: texto, obrigatoria: true}
  - {nome: produto__nome, tipo: texto}
  - {nome: valor, tipo: decimal}
"""

TRANSFORM: list[dict[str, Any]] = [
    {"tipar": {"valor": {"tipo": "decimal"}}},
    {"rejeitar_se": [{"coluna": "id", "regra": "nao_nulo"}]},
]


def step(step_id: str = "ingere_eventos", origem: dict[str, Any] | None = None, **destino: Any) -> dict[str, Any]:
    extra = {k: destino.pop(k) for k in ("transformacao", "qualidade") if k in destino}
    return {
        "id": step_id,
        "tipo": "ingestao",
        "origem": {"tipo": "pubsub", "assinatura": SUB, "schema": "schemas/origem.yaml", **(origem or {})},
        "destino": {
            "bronze": BRONZE + "/",
            "tabela": TABLE,
            "schema": "schemas/destino.yaml",
            "modo_carga": "append",
            **destino,
        },
        "transformacao": extra.get("transformacao", TRANSFORM),
        "qualidade": extra.get("qualidade", []),
    }


class Recorder:
    """Ordem dos efeitos colaterais (storage.write, warehouse.load, pubsub.ack/nack)."""

    def __init__(self) -> None:
        self.events: list[tuple[str, ...]] = []


class SpyPubSub:
    def __init__(self, inner: LocalPubSub, rec: Recorder, *, fail_ack: int = 0) -> None:
        self.inner, self.rec, self.fail_ack = inner, rec, fail_ack

    def pull(self, subscription: str, max_messages: int, timeout_s: float) -> list[PubSubMessage]:
        self.rec.events.append(("pull", subscription, str(max_messages)))
        return self.inner.pull(subscription, max_messages, timeout_s)

    def ack(self, subscription: str, ack_ids: Sequence[str]) -> None:
        self.rec.events.append(("ack", *ack_ids))
        if self.fail_ack > 0:
            self.fail_ack -= 1
            raise PubSubError("falha simulada no ack", operation="pubsub.ack")
        self.inner.ack(subscription, ack_ids)

    def nack(self, subscription: str, ack_ids: Sequence[str]) -> None:
        self.rec.events.append(("nack", *ack_ids))
        self.inner.nack(subscription, ack_ids)

    def backlog(self, subscription: str) -> int | None:
        return self.inner.backlog(subscription)


class SpyStorage:
    def __init__(self, inner: Any, rec: Recorder, *, fail: int = 0) -> None:
        self.inner, self.rec, self.fail = inner, rec, fail

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def write(self, path: str, data: bytes) -> None:
        self.rec.events.append(("write", path.rsplit("/", 1)[-1]))
        if self.fail > 0:
            self.fail -= 1
            raise LoadError("falha simulada no bronze", operation="storage.write")
        self.inner.write(path, data)


class SpyWarehouse:
    def __init__(self, inner: Any, rec: Recorder) -> None:
        self.inner, self.rec = inner, rec

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def load(self, table: str, data: pd.DataFrame, **kwargs: Any) -> int:
        self.rec.events.append(("load", table))
        return int(self.inner.load(table, data, **kwargs))


@pytest.fixture
def project(tmp_path: Path) -> Path:
    (tmp_path / "schemas").mkdir()
    (tmp_path / "schemas" / "origem.yaml").write_text(ORIGEM_SCHEMA, encoding="utf-8")
    (tmp_path / "schemas" / "destino.yaml").write_text(DESTINO_SCHEMA, encoding="utf-8")
    return tmp_path


@pytest.fixture
def ps() -> LocalPubSub:
    return LocalPubSub(clock=lambda: T0)


MakeCtx = Callable[..., StepContext]


@pytest.fixture
def make_ctx(project: Path) -> Iterator[MakeCtx]:
    created: list[StepContext] = []

    def factory(pipe: Pipeline, client: Any, run_id: str = RID1) -> StepContext:
        ctx = build_context(pipe, environment(), project_root=project, run_id=run_id, pubsub=client)
        created.append(ctx)
        return ctx

    yield factory
    for ctx in created:
        close = getattr(ctx.warehouse, "close", None)
        if close is not None:
            close()


def publish(ps: LocalPubSub, payload: Any, message_id: str, **attrs: str) -> None:
    data = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")
    ps.publish(SUB, data, attributes=attrs, message_id=message_id)


def silver(ctx: StepContext) -> list[tuple[Any, ...]]:
    return rows(ctx, f"SELECT message_id, id, produto__nome, valor, _run_id FROM {TABLE} ORDER BY message_id")


def quarantine(ctx: StepContext) -> list[tuple[Any, ...]]:
    return rows(ctx, f"SELECT message_id, _motivo, _run_id FROM {QUARANTINE} ORDER BY message_id")


def control(ctx: StepContext) -> list[tuple[Any, ...]]:
    return rows(
        ctx,
        "SELECT run_id, status, linhas_lidas, linhas_gravadas, linhas_rejeitadas, backlog "
        f"FROM {CONTROL} ORDER BY registrado_em",
    )


def bronze_dir(run_id: str = RID1) -> str:
    return f"{BRONZE}/ingestion_date={run_id[:4]}-{run_id[4:6]}-{run_id[6:8]}/run_id={run_id}"


def publish_sample(ps: LocalPubSub) -> None:
    publish(ps, {"id": "1", "produto": {"nome": "gasolina"}, "valor": "5.5"}, "m1", origem="teste")
    publish(ps, {"id": "2", "produto": {"nome": "etanol"}, "valor": 3.99}, "m2")
    publish(ps, b'{"id": "3", "segredo": "SEGREDO-123"', "m3")  # JSON inválido
    publish(ps, {"id": None, "produto": {"nome": "gnv"}}, "m4")  # rejeitada na transformação


def test_pubsub_ingestion_uses_the_context_transform_engine(make_ctx: MakeCtx, ps: LocalPubSub) -> None:
    """`StepContext.transform` (usado por `jobs.ingest_spark`) substitui o motor pandas também no Pub/Sub."""
    from dataclasses import replace

    from dataeng_core.transform.engine_pandas import apply_transform

    seen: list[list[str]] = []

    def engine(df: pd.DataFrame, ops: Any, *, project_root: Path) -> Any:
        seen.append(sorted(map(str, df.columns)))
        return apply_transform(df, ops, project_root=project_root)

    publish_sample(ps)
    pipe = pipeline(step())
    ctx = replace(make_ctx(pipe, ps), transform=engine)
    result = run_step(pipe.get("ingere_eventos"), ctx)
    assert result.status == "ok", result.detail
    assert len(seen) == 1 and "message_id" in seen[0]
    assert silver(ctx) == [("m1", "1", "gasolina", 5.5, RID1), ("m2", "2", "etanol", 3.99, RID1)]


# ------------------------------------------------------------------ ponta a ponta


def test_pubsub_ingestion_end_to_end(make_ctx: MakeCtx, ps: LocalPubSub) -> None:
    publish_sample(ps)
    pipe = pipeline(step())
    ctx = make_ctx(pipe, ps)

    result = run_step(pipe.get("ingere_eventos"), ctx)

    assert result.status == "ok", result.detail
    assert (result.rows_read, result.rows_written, result.rows_rejected, result.backlog) == (4, 2, 2, 0)
    assert silver(ctx) == [("m1", "1", "gasolina", 5.5, RID1), ("m2", "2", "etanol", 3.99, RID1)]
    assert rows(ctx, f"SELECT publish_time, _source_file FROM {TABLE} ORDER BY message_id")[0][0] == PT
    assert quarantine(ctx) == [
        ("m3", "json_invalido", RID1),
        ("m4", "coluna 'id': valor nulo (regra nao_nulo)", RID1),
    ]
    original = json.loads(rows(ctx, f"SELECT _linha_original FROM {QUARANTINE} WHERE message_id = 'm3'")[0][0])
    assert base64.b64decode(original["data_base64"]) == b'{"id": "3", "segredo": "SEGREDO-123"'

    # bronze: as 4 mensagens cruas (com metadados) + manifesto com tamanho e SHA-256
    files = ctx.storage.list(BRONZE + "/")
    assert len(files) == 2 and all(f.startswith(bronze_dir() + "/") for f in files)
    data_uri = next(f for f in files if f.endswith(".jsonl"))
    manifest_uri = next(f for f in files if f.endswith(".json"))
    data = ctx.storage.read(data_uri)
    lines = [json.loads(line) for line in data.decode("utf-8").splitlines()]
    assert [m["message_id"] for m in lines] == ["m1", "m2", "m3", "m4"]
    assert lines[0]["attributes"] == {"origem": "teste"} and lines[0]["publish_time"] == PT
    assert base64.b64decode(lines[2]["data_base64"]) == b'{"id": "3", "segredo": "SEGREDO-123"'
    manifest = json.loads(ctx.storage.read(manifest_uri))
    assert manifest["tamanho_bytes"] == len(data) and manifest["sha256"] == hashlib.sha256(data).hexdigest()
    assert manifest["arquivo"] == data_uri.rsplit("/", 1)[-1]
    assert rows(ctx, f"SELECT DISTINCT _source_file FROM {TABLE}") == [(data_uri,)]

    # todas confirmadas, inclusive as inválidas já na quarentena
    assert ps.backlog(SUB) == 0 and ps.pull(SUB, 10, 0) == []
    assert control(ctx) == [(RID1, "ok", 4, 2, 2, 0)]
    assert "SEGREDO" not in result.detail


def test_ack_is_the_last_effect_after_bronze_silver_and_quarantine(make_ctx: MakeCtx, ps: LocalPubSub) -> None:
    publish_sample(ps)
    rec = Recorder()
    pipe = pipeline(step())
    ctx = make_ctx(pipe, SpyPubSub(ps, rec))
    ctx.storage = SpyStorage(ctx.storage, rec)  # type: ignore[assignment]
    ctx.warehouse = SpyWarehouse(ctx.warehouse, rec)  # type: ignore[assignment]

    assert run_step(pipe.get("ingere_eventos"), ctx).status == "ok"

    kinds = [e[0] for e in rec.events]
    assert kinds[0] == "pull"
    assert kinds[1:3] == ["write", "write"]  # dados e manifesto
    loads = [e for e in rec.events if e[0] == "load"]
    assert loads[:2] == [("load", TABLE), ("load", QUARANTINE)]
    ack_at = kinds.index("ack")
    assert ack_at > max(i for i, e in enumerate(rec.events) if e in loads[:2])
    assert ack_at > max(i for i, k in enumerate(kinds) if k == "write")
    assert rec.events[ack_at] == ("ack", "ack-1", "ack-2", "ack-3", "ack-4")  # inclusive as inválidas
    assert "nack" not in kinds
    assert kinds.count("ack") == 1


# ------------------------------------------------------------------- foco 3: fila vazia e JSON inválido


def test_invalid_json_goes_to_quarantine_and_is_acked(make_ctx: MakeCtx, ps: LocalPubSub) -> None:
    publish(ps, b"nao e json SEGREDO-123", "ruim-1")
    publish(ps, b"\xff\xfe", "ruim-2")
    publish(ps, b"[1, 2]", "ruim-3")
    pipe = pipeline(step())
    ctx = make_ctx(pipe, ps)

    result = run_step(pipe.get("ingere_eventos"), ctx)

    assert result.status == "ok", result.detail
    assert (result.rows_read, result.rows_written, result.rows_rejected) == (3, 0, 3)
    assert quarantine(ctx) == [
        ("ruim-1", "json_invalido", RID1),
        ("ruim-2", "utf8_invalido", RID1),
        ("ruim-3", "registro_json_invalido", RID1),
    ]
    assert not ctx.warehouse.table_exists(TABLE)
    assert ps.backlog(SUB) == 0  # confirmadas: o lote não trava
    second = run_step(pipe.get("ingere_eventos"), ctx)
    assert second.status == "ok" and second.rows_read == 0
    assert "SEGREDO" not in result.detail + second.detail


def test_empty_queue_succeeds_with_zero_rows(make_ctx: MakeCtx, ps: LocalPubSub, project: Path) -> None:
    (project / "sql").mkdir()
    (project / "sql" / "ddl.sql").write_text(
        "CREATE SCHEMA IF NOT EXISTS ${dataset_silver};\n"
        "CREATE TABLE IF NOT EXISTS ${dataset_silver}.eventos (message_id VARCHAR);\n",
        encoding="utf-8",
    )
    (project / "sql" / "dml.sql").write_text(
        "CREATE SCHEMA IF NOT EXISTS ${dataset_silver};\n"
        "CREATE OR REPLACE TABLE ${dataset_silver}.marca_dml AS SELECT 1 AS x;\n",
        encoding="utf-8",
    )
    rec = Recorder()
    pipe = pipeline(step(ddl=["sql/ddl.sql"], dml_pos_carga=["sql/dml.sql"], qualidade=[{"nao_nulo": ["id"]}]))
    ctx = make_ctx(pipe, SpyPubSub(ps, rec))
    ctx.warehouse = SpyWarehouse(ctx.warehouse, rec)  # type: ignore[assignment]

    result = run_step(pipe.get("ingere_eventos"), ctx)

    assert result.status == "ok", result.detail
    assert (result.rows_read, result.rows_written, result.rows_rejected, result.backlog) == (0, 0, 0, 0)
    assert "fila vazia" in result.detail
    assert result.quality == []  # a qualidade nem rodou
    # só o pull e a linha do controle: nem ack/nack, nem DDL, carga, quarentena ou DML
    assert rec.events == [("pull", SUB, "1000"), ("load", CONTROL)]
    assert ctx.storage.list(BRONZE + "/") == []
    assert not ctx.warehouse.table_exists(TABLE) and not ctx.warehouse.table_exists(QUARANTINE)
    assert not ctx.warehouse.table_exists("silver.marca_dml")
    assert control(ctx) == [(RID1, "ok", 0, 0, 0, 0)]


# ------------------------------------------------------------------- confirmação só depois de gravar


def test_ack_happens_only_after_bronze_write(make_ctx: MakeCtx, ps: LocalPubSub) -> None:
    publish_sample(ps)
    rec = Recorder()
    pipe = pipeline(step())
    ctx = make_ctx(pipe, SpyPubSub(ps, rec))
    healthy = ctx.storage
    ctx.storage = SpyStorage(healthy, rec, fail=1)  # type: ignore[assignment]

    result = run_step(pipe.get("ingere_eventos"), ctx)

    assert result.status == "falha"
    assert "fase 'bronze'" in result.detail and "falha simulada no bronze" in result.detail
    assert "4 mensagem(ns)" in result.detail and "reentregue" in result.detail
    assert result.backlog == 4
    kinds = [e[0] for e in rec.events]
    assert "ack" not in kinds and "load" not in kinds
    assert rec.events[-1] == ("nack", "ack-1", "ack-2", "ack-3", "ack-4")
    assert not ctx.warehouse.table_exists(TABLE)
    # as mensagens voltam no pull seguinte
    assert [m.message_id for m in ps.pull(SUB, 10, 0)] == ["m1", "m2", "m3", "m4"]
    ps.nack(SUB, [f"ack-{i}" for i in range(5, 9)])

    ctx.storage = healthy
    again = run_step(pipe.get("ingere_eventos"), ctx)
    assert again.status == "ok", again.detail
    assert silver(ctx) == [("m1", "1", "gasolina", 5.5, RID1), ("m2", "2", "etanol", 3.99, RID1)]
    assert ps.backlog(SUB) == 0
    assert control(ctx) == [(RID1, "falha", 4, 0, 0, 4), (RID1, "ok", 4, 2, 2, 0)]


@pytest.mark.parametrize("fail_table", [TABLE, QUARANTINE])
def test_failure_between_bronze_and_ack_nacks_and_rerun_does_not_duplicate(
    make_ctx: MakeCtx, ps: LocalPubSub, fail_table: str
) -> None:
    publish_sample(ps)
    rec = Recorder()
    pipe = pipeline(step())
    ctx = make_ctx(pipe, SpyPubSub(ps, rec))
    real = ctx.warehouse
    ctx.warehouse = FlakyWarehouse(real, fail_table)  # type: ignore[assignment]

    result = run_step(pipe.get("ingere_eventos"), ctx)

    assert result.status == "falha"
    phase = "carga_silver" if fail_table == TABLE else "quarentena"
    assert f"fase '{phase}'" in result.detail
    assert "ack" not in [e[0] for e in rec.events]
    assert rec.events[-1][0] == "nack" and len(rec.events[-1]) == 5
    assert ps.backlog(SUB) == 4

    again = run_step(pipe.get("ingere_eventos"), ctx)  # mesmo run (retry)
    assert again.status == "ok", again.detail
    assert silver(ctx) == [("m1", "1", "gasolina", 5.5, RID1), ("m2", "2", "etanol", 3.99, RID1)]
    assert [q[0] for q in quarantine(ctx)] == ["m3", "m4"]
    assert ps.backlog(SUB) == 0


def test_blocking_quality_failure_does_not_ack(make_ctx: MakeCtx, ps: LocalPubSub) -> None:
    publish(ps, {"id": "1", "produto": {"nome": None}, "valor": "1"}, "m1")
    rec = Recorder()
    pipe = pipeline(step(qualidade=[{"nao_nulo": ["produto__nome"], "bloqueante": True}]))
    ctx = make_ctx(pipe, SpyPubSub(ps, rec))

    result = run_step(pipe.get("ingere_eventos"), ctx)

    assert result.status == "falha" and "fase 'qualidade'" in result.detail
    assert [e[0] for e in rec.events] == ["pull", "nack"]
    assert ps.backlog(SUB) == 1


def test_ack_failure_is_reported_and_rerun_with_another_run_id_does_not_duplicate(
    make_ctx: MakeCtx, ps: LocalPubSub
) -> None:
    publish_sample(ps)
    rec = Recorder()
    pipe = pipeline(step())
    ctx = make_ctx(pipe, SpyPubSub(ps, rec, fail_ack=1))

    result = run_step(pipe.get("ingere_eventos"), ctx)

    assert result.status == "falha"
    assert "fase 'ack'" in result.detail
    assert "reentregue" in result.detail and "message_id" in result.detail and "não duplica" in result.detail
    assert silver(ctx) == [("m1", "1", "gasolina", 5.5, RID1), ("m2", "2", "etanol", 3.99, RID1)]
    assert ps.backlog(SUB) == 4  # nada foi confirmado

    # pior caso: uma NOVA execução (outro run_id, outro dia) recebe as mesmas mensagens e mais uma
    publish(ps, {"id": "5", "produto": {"nome": "diesel"}, "valor": "6"}, "m5")
    ctx3 = make_ctx(pipe, SpyPubSub(ps, rec), run_id=RID3)
    again = run_step(pipe.get("ingere_eventos"), ctx3)
    assert again.status == "ok", again.detail
    assert silver(ctx3) == [
        ("m1", "1", "gasolina", 5.5, RID3),
        ("m2", "2", "etanol", 3.99, RID3),
        ("m5", "5", "diesel", 6.0, RID3),
    ]
    assert [q[:2] for q in quarantine(ctx3)] == [
        ("m3", "json_invalido"),
        ("m4", "coluna 'id': valor nulo (regra nao_nulo)"),
    ]
    assert ps.backlog(SUB) == 0


def test_failed_nack_keeps_the_original_failure(make_ctx: MakeCtx, ps: LocalPubSub) -> None:
    class BadNack(SpyPubSub):
        def nack(self, subscription: str, ack_ids: Sequence[str]) -> None:
            self.rec.events.append(("nack", *ack_ids))
            raise RuntimeError("SEGREDO-123 nack")

    publish(ps, {"id": "1"}, "m1")
    rec = Recorder()
    pipe = pipeline(step())
    ctx = make_ctx(pipe, BadNack(ps, rec))
    ctx.warehouse = FlakyWarehouse(ctx.warehouse, TABLE)  # type: ignore[assignment]

    result = run_step(pipe.get("ingere_eventos"), ctx)

    assert result.status == "falha"
    assert "fase 'carga_silver'" in result.detail and "falha simulada na carga" in result.detail
    assert "voltam após o prazo de ack" in result.detail
    assert "SEGREDO" not in result.detail
    assert [e[0] for e in rec.events] == ["pull", "nack"]
    assert ps.backlog(SUB) == 1  # nada confirmado


def test_blocking_quality_failure_detail_explains_silver_and_dead_letter(make_ctx: MakeCtx, ps: LocalPubSub) -> None:
    publish(ps, {"id": "1", "produto": {"nome": None}, "valor": "1"}, "m1")
    pipe = pipeline(step(qualidade=[{"nao_nulo": ["produto__nome"], "bloqueante": True}]))
    ctx = make_ctx(pipe, ps)

    result = run_step(pipe.get("ingere_eventos"), ctx)

    assert result.status == "falha" and "fase 'qualidade'" in result.detail
    assert "a silver já foi alterada por este lote" in result.detail
    assert "idempotente" in result.detail
    assert "não foram confirmadas" in result.detail
    assert "dead-letter" in result.detail and "max_delivery_attempts" in result.detail
    assert silver(ctx) == [("m1", "1", None, 1.0, RID1)]  # a carga já aconteceu antes da qualidade
    assert ps.backlog(SUB) == 1


# ------------------------------------------------------------------- deduplicação por message_id


def test_duplicate_message_id_is_deduplicated_in_silver(make_ctx: MakeCtx, ps: LocalPubSub) -> None:
    publish(ps, {"id": "1", "produto": {"nome": "gasolina"}, "valor": "5.5"}, "m1")
    publish(ps, {"id": "1", "produto": {"nome": "gasolina"}, "valor": "5.5"}, "m1")  # entrega duplicada no lote
    publish(ps, {"id": "2", "produto": {"nome": "etanol"}, "valor": "4"}, "m2")
    pipe = pipeline(step())
    ctx = make_ctx(pipe, ps)

    result = run_step(pipe.get("ingere_eventos"), ctx)

    assert result.status == "ok", result.detail
    assert silver(ctx) == [("m1", "1", "gasolina", 5.5, RID1), ("m2", "2", "etanol", 4.0, RID1)]
    assert result.rows_written == 2 and "1 duplicada(s)" in result.detail
    assert ps.backlog(SUB) == 0  # as duas entregas de m1 foram confirmadas

    # reentrega da mesma mensagem em outro run (mesmo dia e outro dia): continua uma linha por message_id
    for rid in (RID2, RID3):
        publish(ps, {"id": "1", "produto": {"nome": "gasolina"}, "valor": "5.5"}, "m1")
        assert run_step(pipe.get("ingere_eventos"), make_ctx(pipe, ps, run_id=rid)).status == "ok"
    assert rows(ctx, f"SELECT message_id, count(*) FROM {TABLE} GROUP BY message_id ORDER BY 1") == [
        ("m1", 1),
        ("m2", 1),
    ]


def test_merge_mode_uses_chaves_merge(make_ctx: MakeCtx, ps: LocalPubSub) -> None:
    publish(ps, {"id": "1", "produto": {"nome": "gasolina"}, "valor": "5.5"}, "m1")
    pipe = pipeline(step(modo_carga="merge", chaves_merge=["id"]))
    ctx = make_ctx(pipe, ps)
    assert run_step(pipe.get("ingere_eventos"), ctx).status == "ok"
    publish(ps, {"id": "1", "produto": {"nome": "gasolina"}, "valor": "6"}, "m9")  # mesma chave de negócio
    assert run_step(pipe.get("ingere_eventos"), make_ctx(pipe, ps, run_id=RID2)).status == "ok"
    assert silver(ctx) == [("m9", "1", "gasolina", 6.0, RID2)]


class StepClock:
    """Relógio de publicação controlado pelo teste (`publish_time` de cada mensagem)."""

    def __init__(self) -> None:
        self.now = T0

    def __call__(self) -> datetime:
        return self.now


def test_merge_by_business_key_keeps_latest_event_and_acks_all(make_ctx: MakeCtx) -> None:
    clock = StepClock()
    ps = LocalPubSub(clock=clock)
    events = [({"id": "1", "valor": "1"}, "e1"), ({"id": "1", "valor": "2"}, "e2"), ({"id": "9", "valor": "2"}, "e3")]
    for minute, (payload, mid) in enumerate(events):
        clock.now = T0 + timedelta(minutes=minute)
        publish(ps, payload, mid)
    rec = Recorder()
    pipe = pipeline(step(modo_carga="merge", chaves_merge=["id"]))
    ctx = make_ctx(pipe, SpyPubSub(ps, rec))

    result = run_step(pipe.get("ingere_eventos"), ctx)

    assert result.status == "ok", result.detail
    assert silver(ctx) == [("e2", "1", None, 2.0, RID1), ("e3", "9", None, 2.0, RID1)]
    assert "1 superada(s)" in result.detail
    assert (result.rows_read, result.rows_written, result.rows_rejected) == (3, 2, 0)
    assert not ctx.warehouse.table_exists(QUARANTINE)  # superadas não vão para a quarentena
    assert ("ack", "ack-1", "ack-2", "ack-3") in rec.events  # inclusive a superada
    assert ps.backlog(SUB) == 0

    # reentrega (entrega duplicada) numa nova execução: upsert, sem duplicar
    clock.now = T0 + timedelta(minutes=1)
    publish(ps, {"id": "1", "valor": "2"}, "e2")
    again = run_step(pipe.get("ingere_eventos"), make_ctx(pipe, ps, run_id=RID2))
    assert again.status == "ok", again.detail
    assert silver(ctx) == [("e2", "1", None, 2.0, RID2), ("e3", "9", None, 2.0, RID1)]


def test_merge_by_business_key_uses_publish_time_then_arrival_order(make_ctx: MakeCtx) -> None:
    clock = StepClock()
    ps = LocalPubSub(clock=clock)
    clock.now = T0 + timedelta(minutes=5)
    publish(ps, {"id": "1", "valor": "5"}, "novo")  # chega primeiro, mas é o mais recente
    clock.now = T0
    publish(ps, {"id": "1", "valor": "1"}, "antigo")
    publish(ps, {"id": "2", "valor": "1"}, "a")
    publish(ps, {"id": "2", "valor": "2"}, "b")  # mesmo publish_time de "a": vence o último a chegar
    publish(ps, b"nao json", "ruim")
    publish(ps, b"nao json", "ruim")  # entrega duplicada de inválida: uma linha na quarentena
    pipe = pipeline(step(modo_carga="merge", chaves_merge=["id"]))
    ctx = make_ctx(pipe, ps)

    result = run_step(pipe.get("ingere_eventos"), ctx)

    assert result.status == "ok", result.detail
    assert silver(ctx) == [("b", "2", None, 2.0, RID1), ("novo", "1", None, 5.0, RID1)]
    assert "2 superada(s)" in result.detail
    assert quarantine(ctx) == [("ruim", "json_invalido", RID1)]
    assert ps.backlog(SUB) == 0


def test_merge_key_missing_after_transform_fails_and_nacks(make_ctx: MakeCtx, ps: LocalPubSub) -> None:
    publish(ps, {"id": "1"}, "m1")
    rec = Recorder()
    pipe = pipeline(step(modo_carga="merge", chaves_merge=["codigo"]))
    result = run_step(pipe.get("ingere_eventos"), make_ctx(pipe, SpyPubSub(ps, rec)))
    assert result.status == "falha" and "chaves_merge" in result.detail and "codigo" in result.detail
    assert [e[0] for e in rec.events] == ["pull", "nack"]


# ------------------------------------------------------------------- backlog e max_mensagens


def test_backlog_is_recorded_in_control_table(make_ctx: MakeCtx, ps: LocalPubSub) -> None:
    for i in range(5):
        publish(ps, {"id": str(i), "produto": {"nome": "p"}, "valor": "1"}, f"m{i}")
    pipe = pipeline(step(origem={"max_mensagens": 2}))
    ctx = make_ctx(pipe, ps)

    first = run_step(pipe.get("ingere_eventos"), ctx)
    second = run_step(pipe.get("ingere_eventos"), make_ctx(pipe, ps, run_id=RID2))

    assert (first.backlog, second.backlog) == (3, 1)
    assert "backlog restante: 3" in first.detail
    assert control(ctx) == [(RID1, "ok", 2, 2, 0, 3), (RID2, "ok", 2, 2, 0, 1)]


def test_unknown_backlog_is_recorded_as_null(make_ctx: MakeCtx, ps: LocalPubSub) -> None:
    class NoBacklog(SpyPubSub):
        def backlog(self, subscription: str) -> int | None:
            return None

    publish(ps, {"id": "1"}, "m1")
    pipe = pipeline(step())
    ctx = make_ctx(pipe, NoBacklog(ps, Recorder()))
    result = run_step(pipe.get("ingere_eventos"), ctx)
    assert result.status == "ok" and result.backlog is None
    assert "backlog restante: desconhecido" in result.detail
    assert control(ctx) == [(RID1, "ok", 1, 1, 0, None)]


def test_respects_max_messages(make_ctx: MakeCtx, ps: LocalPubSub) -> None:
    for i in range(5):
        publish(ps, {"id": str(i)}, f"m{i}")
    rec = Recorder()
    pipe = pipeline(step(origem={"max_mensagens": 3}))
    ctx = make_ctx(pipe, SpyPubSub(ps, rec))

    result = run_step(pipe.get("ingere_eventos"), ctx)

    assert rec.events[0] == ("pull", SUB, "3")
    assert [e[0] for e in rec.events].count("pull") == 1  # um lote por execução, sem laço
    assert result.rows_read == 3
    assert [r[0] for r in silver(ctx)] == ["m0", "m1", "m2"]
    assert ps.backlog(SUB) == 2


# ------------------------------------------------------------------- configuração e contexto


def test_overwrite_particao_is_refused_for_pubsub() -> None:
    with pytest.raises(ValueError, match="overwrite_particao"):
        pipeline(step(modo_carga="overwrite_particao"))


def test_destination_schema_without_message_id_fails_before_pulling(
    make_ctx: MakeCtx, ps: LocalPubSub, project: Path
) -> None:
    (project / "schemas" / "destino.yaml").write_text(
        "colunas:\n  - {nome: id, tipo: texto, obrigatoria: true}\n", encoding="utf-8"
    )
    publish(ps, {"id": "1"}, "m1")
    rec = Recorder()
    pipe = pipeline(step())
    result = run_step(pipe.get("ingere_eventos"), make_ctx(pipe, SpyPubSub(ps, rec)))
    assert result.status == "falha"
    assert "message_id" in result.detail and "schema de destino" in result.detail
    assert rec.events == []  # nada foi puxado


def test_transform_that_drops_message_id_fails_and_nacks(make_ctx: MakeCtx, ps: LocalPubSub) -> None:
    publish(ps, {"id": "1"}, "m1")
    rec = Recorder()
    pipe = pipeline(step(transformacao=[{"renomear": {"message_id": "mid"}}]))
    result = run_step(pipe.get("ingere_eventos"), make_ctx(pipe, SpyPubSub(ps, rec)))
    assert result.status == "falha" and "message_id" in result.detail
    assert [e[0] for e in rec.events] == ["pull", "nack"]


def test_missing_pubsub_client_is_an_actionable_failure(project: Path) -> None:
    pipe = pipeline(step())
    ctx = build_context(pipe, environment(), project_root=project, run_id=RID1)
    ctx.pubsub = None
    try:
        result = run_step(pipe.get("ingere_eventos"), ctx)
    finally:
        ctx.warehouse.close()  # type: ignore[attr-defined]
    assert result.status == "falha"
    assert "Pub/Sub" in result.detail and "pubsub" in result.detail


def test_build_context_creates_pubsub_client_only_when_needed(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    file_pipe = pipeline(
        {
            "id": "arquivo",
            "tipo": "ingestao",
            "origem": {"tipo": "arquivo", "localizacao": "x.csv", "schema": "schemas/origem.yaml"},
            "destino": {"bronze": BRONZE, "tabela": TABLE, "schema": "schemas/destino.yaml", "modo_carga": "append"},
        }
    )

    def boom(env: Any) -> Any:
        raise AssertionError("make_pubsub não deveria ser chamado")

    monkeypatch.setattr(context_module, "make_pubsub", boom)
    ctx = build_context(file_pipe, environment(), project_root=project, run_id=RID1)
    assert ctx.pubsub is None
    ctx.warehouse.close()  # type: ignore[attr-defined]

    monkeypatch.undo()
    ctx = build_context(pipeline(step()), environment(), project_root=project, run_id=RID1)
    assert isinstance(ctx.pubsub, LocalPubSub)
    ctx.warehouse.close()  # type: ignore[attr-defined]

    mine = LocalPubSub()
    ctx = build_context(pipeline(step()), environment(), project_root=project, run_id=RID1, pubsub=mine)
    assert ctx.pubsub is mine
    ctx.warehouse.close()  # type: ignore[attr-defined]


def test_errors_and_logs_never_show_message_content(
    make_ctx: MakeCtx, ps: LocalPubSub, caplog: pytest.LogCaptureFixture
) -> None:
    publish(ps, b'{"id": "SEGREDO-123", "valor": "1", "obs": "SEGREDO-123x"}', "m1")
    publish(ps, b"SEGREDO-123 nao json", "m2")
    pipe = pipeline(step(qualidade=[{"nao_nulo": ["produto__nome"], "bloqueante": True}]))
    ctx = make_ctx(pipe, ps)
    with caplog.at_level(logging.DEBUG):
        result = run_step(pipe.get("ingere_eventos"), ctx)
    assert result.status == "falha"
    assert "SEGREDO" not in result.detail
    assert "SEGREDO" not in caplog.text
    assert all("SEGREDO" not in str(r[0]) for r in rows(ctx, f"SELECT detalhe FROM {CONTROL}"))


def test_unexpected_error_in_ack_is_wrapped(make_ctx: MakeCtx, ps: LocalPubSub) -> None:
    class RawAck(SpyPubSub):
        def ack(self, subscription: str, ack_ids: Sequence[str]) -> None:
            raise RuntimeError("SEGREDO-123")

    publish(ps, {"id": "1"}, "m1")
    pipe = pipeline(step())
    result = run_step(pipe.get("ingere_eventos"), make_ctx(pipe, RawAck(ps, Recorder())))
    assert result.status == "falha" and "fase 'ack'" in result.detail
    assert "RuntimeError" in result.detail and "SEGREDO" not in result.detail


@pytest.mark.parametrize("phase", ["ddl", "dml_pos_carga"])
def test_pubsub_sql_failure_never_leaks_warehouse_text(
    make_ctx: MakeCtx, ps: LocalPubSub, project: Path, caplog: pytest.LogCaptureFixture, phase: str
) -> None:
    sentinel = "POSTO-SENTINELA-02"
    publish(ps, {"id": "1", "produto": {"nome": sentinel}, "valor": "5.5"}, "m1")
    (project / "sql").mkdir()
    if phase == "ddl":
        sql = f"CREATE TABLE silver.aux AS SELECT CAST('{sentinel}' AS INTEGER) AS x;\n"
    else:
        sql = f"UPDATE {TABLE} SET id = CAST(produto__nome AS INTEGER);\n"
    (project / "sql" / "falha.sql").write_text(sql, encoding="utf-8")
    pipe = pipeline(step(**{phase: ["sql/falha.sql"]}))
    ctx = make_ctx(pipe, ps)

    with caplog.at_level("DEBUG"):
        result = run_step(pipe.get("ingere_eventos"), ctx)

    assert result.status == "falha"
    assert "instrução 1 de 1" in result.detail and "SqlError" in result.detail
    stored = " ".join(str(r) for r in rows(ctx, f"SELECT * FROM {CONTROL}"))
    for text in (result.detail, caplog.text, stored):
        assert sentinel not in text and "Could not convert" not in text
