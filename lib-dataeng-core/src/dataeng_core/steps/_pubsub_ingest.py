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
- `merge` por chaves de negócio (`chaves_merge` diferente de `[message_id]`): depois do dedupe por
  `message_id`, o lote fica com UMA mensagem por chave, a de maior `publish_time` (empate: a última a
  chegar). As superadas são eventos antigos da mesma entidade: não vão para a quarentena, mas são
  confirmadas com as demais, e o detalhe traz a contagem. Limitação: o merge do `Warehouse` é
  incondicional, então um evento ATRASADO (de `publish_time` mais antigo) que chegue numa execução
  posterior sobrescreve o mais novo já gravado; quem precisa disso deve comparar `publish_time` num
  `dml_pos_carga` ou usar `append` (uma linha por mensagem).
- Quarentena (`<tabela>_rejeitados`): `message_id`, `_linha_original`, `_motivo` e as colunas de
  controle, também por `merge` em `message_id` (reentrega não duplica). JSON inválido vai para lá.
- SÓ ENTÃO `ack` de todos os `ack_ids` do lote, inclusive os das mensagens quarentenadas.
- Qualquer falha antes do `ack` (bronze, transformação, DDL, silver, quarentena, qualidade bloqueante,
  `dml_pos_carga`): nenhum `ack`; `nack` de todo o lote (reentrega imediata) e a etapa termina em falha
  com a fase. Falha no próprio `ack`: etapa em falha na fase `ack`; as mensagens voltam e a próxima
  execução as regrava sem duplicar.
- Qualidade bloqueante que falha: a silver e a quarentena JÁ foram gravadas (de forma idempotente), mas
  nada é confirmado; se a falha vier do conteúdo, o lote volta sempre. Configure na assinatura uma
  dead-letter policy (`max_delivery_attempts` + tópico dead-letter) e prefira rejeitar por mensagem
  (`rejeitar_se` → quarentena) a usar verificação bloqueante sobre o conteúdo das mensagens.
- Prazo de ack: não é estendido durante o processamento (o `PubSubClient` não tem essa operação).
  Configure o `ackDeadlineSeconds` da assinatura (até 600 s) para cobrir um lote completo (pull +
  bronze + silver + quarentena + dml) e reduza `max_mensagens` se preciso. Um ack depois do prazo pode
  ser ignorado pelo serviço: as mensagens voltam (a carga é idempotente, mas o backlog não drena).
- Fila vazia: retorna logo depois do pull, `ok` com 0 linhas e o backlog registrado; nada de bronze,
  DDL, transformação, carga, qualidade, `dml_pos_carga`, ack ou nack (o polling frequente não roda DML).
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
    RESERVED_COLUMNS,
    _add_control,
    _conform,
    _load_outputs,
    _plain,
    _Progress,
    _quarantine,
    _safe_name,
    _table_names,
    _with_source,
    _write_bronze,
    ingestion_date_of,
)
from dataeng_core.transform.engine_pandas import apply_transform

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


def _keep_latest(frame: pd.DataFrame, keys: list[str]) -> tuple[pd.DataFrame, int]:
    """Uma linha por chave de negócio: a de maior `publish_time` (empate: a última a chegar)."""
    if frame.empty or keys == [DEDUPE_KEY]:
        return frame, 0
    missing = [k for k in keys if k not in frame.columns]
    if missing:
        raise ConfigError(
            f"coluna(s) de `destino.chaves_merge` ausente(s) nos dados depois da transformação: "
            f"{', '.join(missing)}; ajuste `chaves_merge` ou `transformacao`",
            operation="ingestao.carga",
        )
    ordered = frame.assign(_ordem=range(len(frame)))
    if "publish_time" in ordered.columns:
        ordered = ordered.sort_values(["publish_time", "_ordem"], kind="stable", na_position="first")
    kept = ordered.drop_duplicates(subset=keys, keep="last").sort_values("_ordem").drop(columns="_ordem")
    return kept.reset_index(drop=True), len(frame) - len(kept)


def _quarantine_frame(batch: PulledBatch, transform_rejected: pd.DataFrame) -> pd.DataFrame:
    """Quarentena no formato comum (`_linha_original`, `_motivo`) mais `message_id`, sem repetidas."""
    ids = [*batch.rejected[DEDUPE_KEY].tolist()]
    if DEDUPE_KEY in transform_rejected.columns:
        ids += [_plain(v) for v in transform_rejected[DEDUPE_KEY].tolist()]
    common, _ = _quarantine(batch.rejected, transform_rejected, "")
    frame = common.copy()
    frame.insert(0, DEDUPE_KEY, pd.Series(ids, dtype="string", index=frame.index))
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
) -> tuple[int, int, list[str]]:
    """Bronze → transformação → silver/quarentena/qualidade/dml. Devolve (duplicadas, superadas, notas)."""
    loaded_at = pd.Timestamp.now(tz="UTC").floor("us")
    params = {**ctx.params, "ingestion_date": day.isoformat()}
    prog.phase = "bronze"
    digest, data = _bronze_payload(batch)
    name = _safe_name(f"{step.id}_{digest}.jsonl", f"mensagens_{digest}.jsonl")
    uri = _write_bronze(ctx, step, day, name, data, manifest_name=f"_manifesto_{digest}.json")

    prog.phase = "transformacao"
    engine = ctx.transform or apply_transform
    tr = engine(_with_source(batch.records, uri), step.transformacao, project_root=ctx.project_root)
    for frame in (tr.rows, tr.rejected):
        if len(frame) > 0 and DEDUPE_KEY not in frame.columns:
            raise ConfigError(
                "a transformação removeu ou renomeou a coluna message_id, usada para deduplicar as mensagens; "
                "mantenha-a em `transformacao`",
                operation="ingestao.transformacao",
            )
    rows, duplicates = _dedupe(tr.rows)
    rows, superseded = _keep_latest(rows, load_args["merge_keys"])
    rejected = _quarantine_frame(batch, tr.rejected)
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
    return duplicates, superseded, notes


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
    if not batch.messages:
        return StepResult(
            step.id,
            "ok",
            backlog=prog.backlog,
            detail=f"fila vazia: nenhuma mensagem na assinatura; nada foi carregado em {table}; "
            f"backlog restante: {'desconhecido' if prog.backlog is None else prog.backlog}",
        )
    try:
        duplicates, superseded, notes = _process(
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
            if prog.phase == "qualidade":
                prog.note = (
                    "a silver já foi alterada por este lote (a carga é idempotente: reprocessar não duplica); "
                    f"{prog.note}. Se a falha vem do conteúdo das mensagens, o lote voltará sempre: configure "
                    "na assinatura uma dead-letter policy (max_delivery_attempts + tópico dead-letter) e prefira "
                    "rejeitar por mensagem com `rejeitar_se` (quarentena) a verificação bloqueante"
                )
        raise

    prog.phase = "ack"
    _ack(client, origem, batch.ack_ids, load_args["merge_keys"])
    prog.backlog = read_backlog(client, origem.assinatura)

    backlog = "desconhecido" if prog.backlog is None else str(prog.backlog)
    detail = (
        f"{len(batch.ack_ids)} mensagem(ns) recebida(s) e confirmada(s); {prog.written} linha(s) gravada(s) "
        f"em {table}; {prog.rejected} na quarentena {quarantine_table}"
    )
    if duplicates:
        detail += f"; {duplicates} duplicada(s) no lote (mesmo message_id) descartada(s)"
    if superseded:
        detail += (
            f"; {superseded} superada(s) por evento mais recente da mesma chave "
            f"({', '.join(load_args['merge_keys'])}), confirmada(s) sem gravar"
        )
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
