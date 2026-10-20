"""Etapa `ingestao` com origem `pubsub`: puxar → gravar bronze → carregar silver e quarentena → `ack`.

Ordem e garantias:
- UM `pull` por execução (`sources.pubsub.pull_batch`); o agendamento da DAG repete o lote.
- Bronze: as mensagens cruas (metadados + `data_base64`), uma por linha, em
  `<bronze>/ingestion_date=…/run_id=…/<etapa>_<hash>.jsonl` com o manifesto `_manifesto_<hash>.json`
  (tamanho e SHA-256). `<hash>` vem dos `message_id` do lote: reexecutar o mesmo `run_id` com o mesmo
  lote sobrescreve os mesmos objetos, e um lote diferente no mesmo `run_id` não apaga o anterior.
- Silver: uma linha por `message_id`. Com `modo_carga: append` a carga é `merge` por `message_id`
  (o schema de destino precisa da coluna `message_id`); com `merge`, por `chaves_merge`.
  `overwrite_particao` é recusado na validação do YAML (cada lote apagaria os anteriores). Entregas
  repetidas no mesmo lote são descartadas (fica a primeira) e, entre execuções, o merge não duplica.
- Quarentena (`<tabela>_rejeitados`): `message_id`, `_linha_original`, `_motivo` e as colunas de
  controle, também por `merge` em `message_id` (reentrega não duplica). JSON inválido vai para lá.
- SÓ ENTÃO `ack` de todos os `ack_ids` do lote, inclusive os das mensagens quarentenadas.
- Qualquer falha antes do `ack` (bronze, transformação, DDL, silver, quarentena, qualidade bloqueante,
  `dml_pos_carga`): nenhum `ack`; `nack` de todo o lote (reentrega imediata) e a etapa termina em falha
  com a fase. Falha no próprio `ack`: etapa em falha na fase `ack`; as mensagens voltam e a próxima
  execução as regrava sem duplicar.
- Fila vazia: `ok` com 0 linhas; bronze, silver e quarentena não são tocados.
- Backlog: medido depois do `ack` (o que resta na assinatura) e registrado no controle (`backlog`);
  em falha, o medido logo depois do pull. `None` quando o cliente não sabe medir (ex.: `GcpPubSub`).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from typing import Any

import pandas as pd

from dataeng_core.base.errors import ConfigError, DataEngError, PubSubError
from dataeng_core.base.pipeline import IngestaoStep, PubSubOrigem
from dataeng_core.base.runtime import get_logger
from dataeng_core.base.schema import load_schema
from dataeng_core.pubsub.base import PubSubClient
from dataeng_core.sources.pubsub import PulledBatch, pull_batch, raw_message, read_backlog
from dataeng_core.steps.context import StepContext, StepResult
from dataeng_core.steps.ingestao import (
    LINHA_ORIGINAL,
    RESERVED_COLUMNS,
    SOURCE_FILE,
    _add_control,
    _conform,
    _load_outputs,
    _original_line,
    _plain,
    _Progress,
    _safe_name,
    _table_names,
    _with_source,
    _write_bronze,
    ingestion_date_of,
)
from dataeng_core.transform.engine_pandas import MOTIVO, apply_transform

log = get_logger(__name__)

DEDUPE_KEY = "message_id"


def _silver_load_args(step: IngestaoStep) -> dict[str, Any]:
    destino = step.destino
    if destino.modo_carga == "append":
        return {"mode": "merge", "merge_keys": [DEDUPE_KEY]}
    if destino.modo_carga == "merge":
        return {"mode": "merge", "merge_keys": list(destino.chaves_merge)}
    raise ConfigError(  # o modelo já recusa; defesa para pipelines montados sem validação
        "a origem 'pubsub' não aceita modo_carga 'overwrite_particao'; use 'append' ou 'merge'",
        operation="ingestao.destino",
    )


def _bronze_payload(batch: PulledBatch) -> tuple[str, bytes]:
    lines = [json.dumps(raw_message(m), ensure_ascii=False, sort_keys=True) for m in batch.messages]
    ids = "\n".join(str(m.message_id) for m in batch.messages)
    return hashlib.sha256(ids.encode("utf-8")).hexdigest()[:12], ("\n".join(lines) + "\n").encode("utf-8")


def _nack_quietly(client: PubSubClient, subscription: str, ack_ids: list[str], step_id: str) -> bool:
    """Devolve o lote para reentrega imediata; se o `nack` falhar, o prazo de ack do Pub/Sub faz o mesmo."""
    try:
        client.nack(subscription, ack_ids)
    except DataEngError as exc:
        log.warning(f"etapa '{step_id}': nack do lote falhou ({exc}); as mensagens voltam após o prazo de ack")
        return False
    except Exception as exc:
        log.warning(
            f"etapa '{step_id}': nack do lote falhou ({type(exc).__name__}); as mensagens voltam após o prazo de ack"
        )
        return False
    return True


def _ack(client: PubSubClient, origem: PubSubOrigem, ack_ids: list[str], keys: list[str]) -> None:
    problem: str | None = None
    try:
        client.ack(origem.assinatura, ack_ids)
    except DataEngError as exc:
        problem = exc.detail or exc.title
    except Exception as exc:  # a mensagem de um erro desconhecido pode trazer dados: só o tipo
        problem = f"erro inesperado ({type(exc).__name__})"
    if problem is not None:
        raise PubSubError(
            f"o lote foi gravado no bronze e na silver, mas a confirmação (ack) de {len(ack_ids)} mensagem(ns) "
            f"falhou: {problem}. As mensagens serão reentregues pelo Pub/Sub; reexecute a etapa: a silver não "
            f"duplica (merge por {', '.join(keys)}; a deduplicação é por message_id)",
            operation="ingestao.ack",
        )


def _dedupe(frame: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if frame.empty:
        return frame, 0
    kept = frame.drop_duplicates(subset=[DEDUPE_KEY], keep="first").reset_index(drop=True)
    return kept, len(frame) - len(kept)


def _quarantine_frame(batch: PulledBatch, transform_rejected: pd.DataFrame, uri: str) -> pd.DataFrame:
    ids: list[str | None] = []
    lines: list[str] = []
    reasons: list[str] = []
    for rec in batch.rejected.to_dict("records"):
        ids.append(rec[DEDUPE_KEY])
        lines.append(str(rec[LINHA_ORIGINAL]))
        reasons.append(str(rec[MOTIVO]))
    for rec in transform_rejected.to_dict("records"):
        reasons.append(str(rec.pop(MOTIVO)))
        rec.pop(SOURCE_FILE, None)
        ids.append(_plain(rec.get(DEDUPE_KEY)))
        lines.append(_original_line(rec))
    frame = pd.DataFrame(
        {
            DEDUPE_KEY: pd.Series(ids, dtype="string"),
            LINHA_ORIGINAL: pd.Series(lines, dtype="string"),
            MOTIVO: pd.Series(reasons, dtype="string"),
        }
    )
    return _dedupe(frame)[0]


def _process(
    step: IngestaoStep,
    ctx: StepContext,
    prog: _Progress,
    batch: PulledBatch,
    *,
    table: str,
    quarantine_table: str,
    load_args: dict[str, Any],
    dest_schema: Any,
    day: dt.date,
) -> tuple[int, list[str]]:
    """Bronze → transformação → silver/quarentena/qualidade/dml. Devolve (duplicadas no lote, notas)."""
    loaded_at = pd.Timestamp.now(tz="UTC").floor("us")
    params = {**ctx.params, "ingestion_date": day.isoformat()}
    uri = ""
    if batch.messages:
        prog.phase = "bronze"
        digest, data = _bronze_payload(batch)
        name = _safe_name(f"{step.id}_{digest}.jsonl", f"mensagens_{digest}.jsonl")
        uri = _write_bronze(ctx, step, day, name, data, manifest_name=f"_manifesto_{digest}.json")

    prog.phase = "transformacao"
    tr = apply_transform(_with_source(batch.records, uri), step.transformacao, project_root=ctx.project_root)
    for frame in (tr.rows, tr.rejected):
        if len(frame) > 0 and DEDUPE_KEY not in frame.columns:
            raise ConfigError(
                "a transformação removeu ou renomeou a coluna message_id, usada para deduplicar as mensagens; "
                "mantenha-a em `transformacao`",
                operation="ingestao.transformacao",
            )
    rows, duplicates = _dedupe(tr.rows)
    rejected = _quarantine_frame(batch, tr.rejected, uri)
    prog.rejected = len(rejected)

    sources = pd.Series([uri] * len(rows), dtype="string")
    silver = _add_control(_conform(rows, dest_schema), sources, ctx, day, loaded_at)
    quarantine = _add_control(rejected, pd.Series([uri] * len(rejected), dtype="string"), ctx, day, loaded_at)
    notes = _load_outputs(
        step,
        ctx,
        prog,
        table=table,
        silver=silver,
        load_args=load_args,
        quarantine_table=quarantine_table,
        quarantine=quarantine,
        quarantine_args={"mode": "merge", "merge_keys": [DEDUPE_KEY]},
        params=params,
    )
    return duplicates, notes


def ingest_pubsub(step: IngestaoStep, origem: PubSubOrigem, ctx: StepContext, prog: _Progress) -> StepResult:
    client = ctx.pubsub
    if client is None:
        raise ConfigError(
            "a etapa usa a origem pubsub, mas o contexto não tem cliente Pub/Sub; monte o contexto com "
            "build_context (que cria o cliente com make_pubsub) ou passe `pubsub=` com um cliente pronto",
            operation="ingestao.pubsub",
        )
    table, quarantine_table = _table_names(step.destino)
    load_args = _silver_load_args(step)
    day = ingestion_date_of(ctx)
    dest_schema = load_schema(ctx.project_root / step.destino.schema)
    if load_args["merge_keys"] == [DEDUPE_KEY] and DEDUPE_KEY not in {c.nome for c in dest_schema.colunas}:
        raise ConfigError(
            f"o schema de destino {step.destino.schema.as_posix()} não tem a coluna message_id, chave de "
            "deduplicação da silver com origem pubsub; acrescente `{nome: message_id, tipo: texto, obrigatoria: true}`",
            operation="ingestao.destino",
        )
    src_schema = load_schema(ctx.project_root / origem.schema)

    prog.phase = "leitura"
    batch = pull_batch(client, origem, schema=src_schema, reserved=RESERVED_COLUMNS)
    prog.read = len(batch.records) + len(batch.rejected)
    prog.backlog = batch.backlog
    try:
        duplicates, notes = _process(
            step,
            ctx,
            prog,
            batch,
            table=table,
            quarantine_table=quarantine_table,
            load_args=load_args,
            dest_schema=dest_schema,
            day=day,
        )
    except BaseException:
        if batch.ack_ids:
            returned = _nack_quietly(client, origem.assinatura, batch.ack_ids, step.id)
            how = "devolvidas (nack) para reentrega" if returned else "voltam após o prazo de ack"
            prog.note = (
                f"as {len(batch.ack_ids)} mensagem(ns) do lote não foram confirmadas e serão reentregues ({how})"
            )
        raise

    if batch.ack_ids:
        prog.phase = "ack"
        _ack(client, origem, batch.ack_ids, load_args["merge_keys"])
        prog.backlog = read_backlog(client, origem.assinatura)

    backlog = "desconhecido" if prog.backlog is None else str(prog.backlog)
    if not batch.ack_ids:
        detail = f"fila vazia: nenhuma mensagem na assinatura; nada foi carregado em {table}"
    else:
        detail = (
            f"{len(batch.ack_ids)} mensagem(ns) recebida(s) e confirmada(s); {prog.written} linha(s) gravada(s) "
            f"em {table}; {prog.rejected} na quarentena {quarantine_table}"
        )
        if duplicates:
            detail += f"; {duplicates} duplicada(s) no lote (mesmo message_id) descartada(s)"
    detail = "; ".join([detail, f"backlog restante: {backlog}", *notes])
    return StepResult(
        step.id,
        "ok",
        rows_read=prog.read,
        rows_written=prog.written,
        rows_rejected=prog.rejected,
        backlog=prog.backlog,
        quality=prog.quality,
        detail=detail,
    )
