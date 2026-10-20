"""Etapa `ingestao` (origens `arquivo`, `api` e `pubsub`).

Fluxo: lê a origem → grava o bronze (arquivo + manifesto) → interpreta → transforma → carrega a silver
com as colunas de controle → grava a quarentena (`<tabela>_rejeitados`) → roda a qualidade →
roda `dml_pos_carga`. O executor (`run_step`) registra o resultado no controle.

Idempotência (reexecutar não duplica nem perde linhas):
- bronze: caminho determinado por `ingestion_date` e `run_id`; reexecutar sobrescreve os mesmos objetos;
- `overwrite_particao`: substitui as partições (`destino.particao`) presentes nos dados;
- `merge`: atualiza/insere por `destino.chaves_merge`;
- `append`: acrescenta, mas substitui as linhas do mesmo `_run_id` (reexecutar o mesmo run não duplica);
- quarentena: substitui pela mesma chave da silver (`_ingestion_date` em `overwrite_particao` por
  `_ingestion_date`; `_run_id` nos demais casos).
`_ingestion_date` vem de `ctx.params["ingestion_date"]` (preenchido por `build_context`: data explícita
ou a data do `run_id`), para que a reexecução de um run em outro dia caia na mesma partição.

Entrada vazia (arquivo vazio, só cabeçalho, API sem registros) ou com todas as linhas rejeitadas: sucesso
com zero linhas; a silver e a quarentena (se vazia) não são tocadas. Se nada foi carregado e a tabela de
destino ainda não existe, o `dml_pos_carga` é pulado (ele leria uma tabela inexistente) e o detalhe diz isso.

Colunas reservadas (`_run_id`, `_source_file`, `_loaded_at`, `_ingestion_date`, `_motivo`,
`_linha_original`) não podem vir da origem: a etapa falha com o nome da coluna.

Limitações conhecidas:
- reexecutar o mesmo `run_id` com a origem ALTERADA não remove linhas antigas que deixaram de existir
  (em `merge`, chaves que sumiram; em `overwrite_particao`, partições que sumiram);
- em `overwrite_particao` por coluna de dados (não `_ingestion_date`), a quarentena é chaveada por
  `_run_id` (as rejeitadas não têm a coluna de partição confiável);
- se a reexecução não tiver rejeitadas, as rejeitadas antigas com a mesma chave continuam na quarentena.

Mensagens de falha nunca trazem valores dos dados; o segredo da API passa por `redact`.

A origem `pubsub` (puxar → bronze → silver/quarentena → `ack`, dedupe por `message_id`) fica em
`steps/_pubsub_ingest.py`, que reaproveita o bronze e a carga daqui.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlsplit

import pandas as pd
import requests

from dataeng_core.base.errors import ConfigError, DataEngError, SourceError
from dataeng_core.base.pipeline import ApiOrigem, ArquivoOrigem, Destino, IngestaoStep, PubSubOrigem, Step
from dataeng_core.base.runtime import get_logger, redact
from dataeng_core.base.schema import SchemaSpec, load_schema
from dataeng_core.quality.checks import CheckResult, raise_if_blocking, run_checks
from dataeng_core.sources.api import fetch_records
from dataeng_core.sources.files import extract_zip_members, read_file
from dataeng_core.sql.render import render_sql, split_with_lines
from dataeng_core.steps.context import StepContext, StepResult, resolve_ingestion_date
from dataeng_core.steps.registry import log_unexpected, register_step_type, step_secrets
from dataeng_core.transform.engine_pandas import MOTIVO, apply_transform
from dataeng_core.warehouse.base import LoadMode, split_table, validate_identifier

log = get_logger(__name__)

MANIFEST_NAME = "_manifesto.json"
QUARANTINE_SUFFIX = "_rejeitados"
SOURCE_FILE = "_source_file"
CONTROL_COLUMNS = ("_run_id", SOURCE_FILE, "_loaded_at", "_ingestion_date")
LINHA_ORIGINAL = "_linha_original"
RESERVED_COLUMNS = (*CONTROL_COLUMNS, MOTIVO, LINHA_ORIGINAL)
HTTP_TIMEOUT_S = (10.0, 120.0)  # (conexão, leitura)
MAX_DOWNLOAD_BYTES = 512 * 1024**2
_CHUNK = 1024 * 1024
_UNSAFE_NAME = re.compile(r"[^\w.-]")
# dtype para coluna toda nula, para o warehouse não inferir um tipo errado (o DuckDB infere INTEGER)
_NULL_DTYPES = {
    "texto": "string",
    "inteiro": "Int64",
    "decimal": "Float64",
    "booleano": "boolean",
    "timestamp": "datetime64[us, UTC]",
}


@dataclass
class _Progress:
    """O que já se sabe da execução; vai para o `StepResult` também em caso de falha."""

    phase: str = "preparacao"
    read: int = 0
    written: int = 0
    rejected: int = 0
    quality: list[CheckResult] = field(default_factory=list)
    backlog: int | None = None
    note: str = ""  # acrescentado ao detalhe em caso de falha (ex.: mensagens devolvidas com nack)


# --------------------------------------------------------------------------- utilitários


def ingestion_date_of(ctx: StepContext) -> dt.date:
    """Data de ingestão do contexto (`params["ingestion_date"]`, ou a data do `run_id`)."""
    text = ctx.params.get("ingestion_date")
    if text is None:
        return resolve_ingestion_date(ctx.run_id, None)
    try:
        return dt.date.fromisoformat(text)
    except ValueError as exc:
        raise ConfigError(
            f"parâmetro ingestion_date inválido: '{text[:40]}'; use AAAA-MM-DD", operation="ingestao"
        ) from exc


def _safe_name(name: str, fallback: str) -> str:
    name = _UNSAFE_NAME.sub("_", name).strip(".")
    if not name:
        return fallback
    return f"dados_{name}" if name == MANIFEST_NAME else name


def _safe_url(url: str) -> str:
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.hostname or ''}{parts.path}"


def _plain(value: Any) -> Any:
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _table_names(destino: Destino) -> tuple[str, str]:
    dataset, name = split_table(destino.tabela, operation="ingestao.destino")
    if dataset is None:
        raise ConfigError(
            f"destino.tabela '{destino.tabela}' deve ter o formato 'dataset.tabela'", operation="ingestao.destino"
        )
    validate_identifier(name + QUARANTINE_SUFFIX, what="nome da tabela de quarentena", operation="ingestao.destino")
    return f"{dataset}.{name}", f"{dataset}.{name}{QUARANTINE_SUFFIX}"


def _load_args(destino: Destino) -> dict[str, Any]:
    """Modo de carga do YAML → argumentos de `Warehouse.load` (todos idempotentes, ver docstring do módulo)."""
    mode: LoadMode
    if destino.modo_carga == "overwrite_particao":
        mode = "overwrite_partition"
        part = validate_identifier(destino.particao, what="destino.particao", operation="ingestao.destino")
        return {"mode": mode, "partition_column": part}
    if destino.modo_carga == "merge":
        mode = "merge"
        keys = [
            validate_identifier(k, what="destino.chaves_merge", operation="ingestao.destino")
            for k in destino.chaves_merge
        ]
        return {"mode": mode, "merge_keys": keys}
    mode = "overwrite_partition"
    return {"mode": mode, "partition_column": "_run_id"}


def _quarantine_key(destino: Destino) -> str:
    if destino.modo_carga == "overwrite_particao" and destino.particao == "_ingestion_date":
        return "_ingestion_date"
    return "_run_id"


# ------------------------------------------------------------------------------ leitura


def _download(url: str) -> bytes:
    """GET sem seguir redirecionamentos, em pedaços e com teto de `MAX_DOWNLOAD_BYTES`."""
    safe = _safe_url(url)
    limit = MAX_DOWNLOAD_BYTES
    problem: str | None = None
    try:
        response = requests.get(url, timeout=HTTP_TIMEOUT_S, allow_redirects=False, stream=True)
    except requests.RequestException as exc:
        problem = f"falha de rede ({type(exc).__name__}) ao baixar {safe}; verifique a URL e a conectividade"
    if problem is not None:
        raise SourceError(problem, operation="ingestao.download")
    with response:
        status = response.status_code
        if 300 <= status < 400:
            raise SourceError(
                f"o servidor respondeu com redirecionamento (HTTP {status}) em {safe}; redirecionamentos não são "
                "seguidos: use a URL final em `origem.localizacao`",
                operation="ingestao.download",
            )
        if status >= 400:
            raise SourceError(
                f"HTTP {status} ao baixar {safe}; verifique a URL em `origem.localizacao`",
                operation="ingestao.download",
            )
        out = bytearray()
        try:
            for chunk in response.iter_content(chunk_size=_CHUNK):
                out += chunk
                if len(out) > limit:
                    problem = f"o download de {safe} excede o limite de {limit} bytes"
                    break
        except requests.RequestException as exc:
            problem = f"falha de rede ({type(exc).__name__}) durante o download de {safe}"
    if problem is not None:
        raise SourceError(problem, operation="ingestao.download")
    return bytes(out)


def _read_location(origem: ArquivoOrigem, ctx: StepContext) -> tuple[str, bytes]:
    """Devolve (nome do arquivo, bytes). Aceita `gs://`, `http(s)://` ou caminho local (relativo à raiz)."""
    loc = origem.localizacao
    scheme = urlsplit(loc).scheme.lower()
    if scheme == "gs":
        return loc.rstrip("/").rsplit("/", 1)[-1], ctx.storage.read(loc)
    if scheme in ("http", "https"):
        return PurePosixPath(urlsplit(loc).path).name, _download(loc)
    if len(scheme) > 1:  # esquema de uma letra é a unidade do Windows (ex.: C:)
        raise ConfigError(
            f"esquema '{scheme}://' não suportado em `origem.localizacao`; use gs://, http://, https:// "
            "ou um caminho local",
            operation="ingestao.leitura",
        )
    path = Path(loc)
    if not path.is_absolute():
        path = ctx.project_root / path
    if not path.is_file():
        raise SourceError(
            f"arquivo de origem inexistente: {path}; confira `origem.localizacao`", operation="ingestao.leitura"
        )
    return path.name, path.read_bytes()


def _check_reserved(columns: Any, where: str) -> None:
    reserved = [str(c) for c in columns if c in RESERVED_COLUMNS]
    if reserved:
        nomes = ", ".join(f"coluna reservada '{c}'" for c in reserved)
        raise SourceError(
            f"{where} tem {nomes}; renomeie-a na origem (nomes reservados pela biblioteca: "
            f"{', '.join(RESERVED_COLUMNS)})",
            operation="ingestao.leitura",
        )


def _with_source(frame: pd.DataFrame, source: str) -> pd.DataFrame:
    frame = frame.copy()
    frame[SOURCE_FILE] = pd.Series([source] * len(frame), dtype=object, index=frame.index)
    return frame


def _concat(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if len(frames) == 1:
        return frames[0]
    out = pd.concat(frames, ignore_index=True).astype(object)
    return out.where(out.notna(), None)


def _parse_file(
    data: bytes, name: str, uri: str, origem: ArquivoOrigem, schema: SchemaSpec
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(linhas, rejeitadas na leitura), ambas com `_source_file`. Zip: lê os membros do formato da origem."""
    if not name.lower().endswith(".zip"):
        res = read_file(data, origem, schema)
        _check_reserved(res.rows.columns, "o arquivo")
        return _with_source(res.rows, uri), _with_source(res.rejected, uri)
    members = extract_zip_members(data)
    suffix = f".{origem.formato}"
    chosen = sorted(m for m in members if m.lower().endswith(suffix))
    if not chosen:
        raise SourceError(
            f"o zip não contém arquivo {suffix}; membros: {', '.join(sorted(members)) or 'nenhum'}",
            operation="ingestao.leitura",
        )
    rows, rejected = [], []
    for member in chosen:
        res = read_file(members[member], origem, schema)
        _check_reserved(res.rows.columns, f"o membro '{member}' do zip")
        rows.append(_with_source(res.rows, f"{uri}!{member}"))
        rejected.append(_with_source(res.rejected, f"{uri}!{member}"))
    return _concat(rows), _concat(rejected)


def _api_cell(value: Any) -> str | None:
    """Mesmo contrato dos arquivos: nulo/vazio vira `None`, o resto vira texto."""
    value = _plain(value)
    if value is None or value == "":
        return None
    if isinstance(value, bool | dict | list):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _api_frame(records: list[dict[str, Any]], schema: SchemaSpec | None, uri: str) -> pd.DataFrame:
    columns: list[str] = []
    for rec in records:
        columns.extend(k for k in rec if k not in columns)
    if not records and schema is not None:
        columns = [c.nome for c in schema.colunas]
    _check_reserved(columns, "a resposta da API")
    if records and schema is not None:
        faltando = [c.nome for c in schema.colunas if c.obrigatoria and c.nome not in columns]
        if faltando:
            raise SourceError(
                f"coluna obrigatória ausente na resposta da API: {', '.join(faltando)}", operation="ingestao.api"
            )
    frame = pd.DataFrame(
        {c: pd.Series([_api_cell(r.get(c)) for r in records], dtype=object) for c in columns},
        index=pd.RangeIndex(len(records)),
    )
    return _with_source(frame, uri)


# ------------------------------------------------------------------------------- bronze


def _write_bronze(
    ctx: StepContext, step: IngestaoStep, day: dt.date, name: str, data: bytes, manifest_name: str = MANIFEST_NAME
) -> str:
    """Grava o arquivo e depois o manifesto (tamanho e SHA-256); devolve a URI do arquivo."""
    folder = f"{step.destino.bronze.rstrip('/')}/ingestion_date={day.isoformat()}/run_id={ctx.run_id}"
    uri = f"{folder}/{name}"
    ctx.storage.write(uri, data)
    manifest = {
        "arquivo": name,
        "tamanho_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "run_id": ctx.run_id,
        "etapa": step.id,
        "gravado_em": dt.datetime.now(dt.UTC).isoformat(),
    }
    ctx.storage.write(f"{folder}/{manifest_name}", json.dumps(manifest, ensure_ascii=False, indent=2).encode())
    return uri


# ------------------------------------------------------------------------------- carga


def _conform(rows: pd.DataFrame, schema: SchemaSpec) -> pd.DataFrame:
    """Colunas do schema de destino, na ordem dele; colunas a mais ficam fora da silver."""
    present = set(rows.columns)
    wanted = [c for c in schema.colunas if c.nome not in CONTROL_COLUMNS]
    faltando = [c.nome for c in wanted if c.obrigatoria and c.nome not in present]
    if faltando and len(rows) > 0:
        raise ConfigError(
            f"coluna obrigatória do schema de destino ausente depois da transformação: {', '.join(faltando)}; "
            f"colunas disponíveis: {', '.join(map(str, rows.columns))}. Ajuste `transformacao` ou o schema de destino",
            operation="ingestao.carga",
        )
    out: dict[str, pd.Series] = {}
    for col in wanted:
        series = rows[col.nome] if col.nome in present else pd.Series([None] * len(rows), dtype=object)
        series = series.reset_index(drop=True)
        if col.tipo in _NULL_DTYPES and series.isna().all():
            series = pd.Series([None] * len(rows), dtype=_NULL_DTYPES[col.tipo])
        out[col.nome] = series
    return pd.DataFrame(out, index=pd.RangeIndex(len(rows)))


def _add_control(
    frame: pd.DataFrame, sources: pd.Series, ctx: StepContext, day: dt.date, loaded_at: pd.Timestamp
) -> pd.DataFrame:
    n = len(frame)
    frame = frame.copy()
    frame["_run_id"] = pd.Series([ctx.run_id] * n, dtype="string", index=frame.index)
    frame[SOURCE_FILE] = pd.Series(list(sources), dtype="string", index=frame.index)
    frame["_loaded_at"] = pd.Series([loaded_at] * n, dtype="datetime64[us, UTC]", index=frame.index)
    frame["_ingestion_date"] = pd.Series([day] * n, dtype=object, index=frame.index)
    return frame


def _original_line(row: dict[Any, Any]) -> str:
    return json.dumps({str(k): _plain(v) for k, v in row.items()}, ensure_ascii=False, default=str)


def _quarantine(
    read_rejected: pd.DataFrame, transform_rejected: pd.DataFrame, uri: str
) -> tuple[pd.DataFrame, pd.Series]:
    """Quarentena no formato único (`_linha_original`, `_motivo`) e a série de `_source_file`."""
    lines: list[str] = []
    reasons: list[str] = []
    sources: list[str] = []
    for rec in read_rejected.to_dict("records"):
        lines.append(str(rec[LINHA_ORIGINAL]))
        reasons.append(str(rec[MOTIVO]))
        sources.append(str(rec.get(SOURCE_FILE) or uri))
    for rec in transform_rejected.to_dict("records"):
        reasons.append(str(rec.pop(MOTIVO)))
        sources.append(str(_plain(rec.pop(SOURCE_FILE, None)) or uri))
        lines.append(_original_line(rec))
    frame = pd.DataFrame({LINHA_ORIGINAL: pd.Series(lines, dtype="string"), MOTIVO: pd.Series(reasons, dtype="string")})
    return frame, pd.Series(sources, dtype="string")


def _run_sql_files(ctx: StepContext, paths: list[Path], params: dict[str, str], *, ddl: bool) -> None:
    for rel in paths:
        source = rel.as_posix()
        try:
            text = (ctx.project_root / rel).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            raise ConfigError(
                f"não foi possível ler o arquivo SQL {source} ({type(exc).__name__})", operation="ingestao.sql"
            ) from exc
        statements = [s.text for s in split_with_lines(render_sql(text, params, source=source), source=source)]
        if ddl:
            for statement in statements:
                ctx.warehouse.execute_ddl(statement)
        else:
            ctx.warehouse.run_sql(statements)


def _load_outputs(
    step: IngestaoStep,
    ctx: StepContext,
    prog: _Progress,
    *,
    table: str,
    silver: pd.DataFrame,
    load_args: dict[str, Any],
    quarantine_table: str,
    quarantine: pd.DataFrame,
    quarantine_args: dict[str, Any],
    params: dict[str, str],
) -> list[str]:
    """DDL → silver → quarentena → qualidade → `dml_pos_carga` (comum a todas as origens); devolve notas."""
    destino = step.destino
    prog.phase = "ddl"
    _run_sql_files(ctx, destino.ddl, params, ddl=True)

    prog.phase = "carga_silver"
    if len(silver) > 0:
        prog.written = ctx.warehouse.load(table, silver, **load_args)
    else:
        log.warning(f"etapa '{step.id}': entrada sem linhas válidas; a tabela {table} não foi alterada")

    prog.phase = "quarentena"
    if len(quarantine) > 0:
        ctx.warehouse.load(quarantine_table, quarantine, **quarantine_args)

    prog.phase = "qualidade"
    if step.qualidade:
        prog.quality = run_checks(silver, step.qualidade)
        raise_if_blocking(prog.quality)

    prog.phase = "dml_pos_carga"
    notes: list[str] = []
    if destino.dml_pos_carga and prog.written == 0 and not ctx.warehouse.table_exists(table):
        notes.append(f"dml_pos_carga não executado: nada foi carregado e a tabela {table} ainda não existe")
        log.warning(f"etapa '{step.id}': {notes[-1]}")
    else:
        _run_sql_files(ctx, destino.dml_pos_carga, params, ddl=False)
    return notes


# ------------------------------------------------------------------------------ etapa


def _ingest(step: IngestaoStep, ctx: StepContext, prog: _Progress) -> StepResult:
    origem, destino = step.origem, step.destino
    if isinstance(origem, PubSubOrigem):
        # importação tardia: o módulo do Pub/Sub importa os utilitários deste módulo
        from dataeng_core.steps._pubsub_ingest import ingest_pubsub

        return ingest_pubsub(step, origem, ctx, prog)
    if not isinstance(origem, ArquivoOrigem | ApiOrigem):
        raise ConfigError(f"a origem '{origem.tipo}' não é suportada pela etapa ingestao", operation="ingestao")
    table, quarantine_table = _table_names(destino)
    load_args = _load_args(destino)
    day = ingestion_date_of(ctx)
    loaded_at = pd.Timestamp.now(tz="UTC").floor("us")
    params = {**ctx.params, "ingestion_date": day.isoformat()}
    dest_schema = load_schema(ctx.project_root / destino.schema)

    prog.phase = "leitura"
    records: list[dict[str, Any]] = []
    if isinstance(origem, ArquivoOrigem):
        src_schema: SchemaSpec | None = load_schema(ctx.project_root / origem.schema)
        raw_name, data = _read_location(origem, ctx)
        name = _safe_name(raw_name, f"{step.id}.{origem.formato}")
    else:
        src_schema = load_schema(ctx.project_root / origem.schema) if origem.schema else None
        records = fetch_records(origem)
        name = f"{step.id}.json"
        data = json.dumps(records, ensure_ascii=False, default=str).encode("utf-8")

    prog.phase = "bronze"
    uri = _write_bronze(ctx, step, day, name, data)

    prog.phase = "interpretacao"
    if isinstance(origem, ArquivoOrigem):
        assert src_schema is not None
        rows, read_rejected = _parse_file(data, name, uri, origem, src_schema)
    else:
        rows, read_rejected = _api_frame(records, src_schema, uri), pd.DataFrame(columns=[LINHA_ORIGINAL, MOTIVO])
    prog.read = len(rows) + len(read_rejected)

    prog.phase = "transformacao"
    tr = (ctx.transform or apply_transform)(rows, step.transformacao, project_root=ctx.project_root)
    rejected, rejected_sources = _quarantine(read_rejected, tr.rejected, uri)
    prog.rejected = len(rejected)
    sources = tr.rows[SOURCE_FILE] if SOURCE_FILE in tr.rows.columns else pd.Series([uri] * len(tr.rows))
    sources = sources.map(lambda v: _plain(v) or uri)
    silver = _add_control(_conform(tr.rows, dest_schema), sources, ctx, day, loaded_at)
    notes = _load_outputs(
        step,
        ctx,
        prog,
        table=table,
        silver=silver,
        load_args=load_args,
        quarantine_table=quarantine_table,
        quarantine=_add_control(rejected, rejected_sources, ctx, day, loaded_at),
        quarantine_args={"mode": "overwrite_partition", "partition_column": _quarantine_key(destino)},
        params=params,
    )

    if prog.read == 0:
        detail = f"entrada vazia (0 linhas): nada foi carregado em {table}"
    else:
        detail = f"{prog.written} linha(s) gravada(s) em {table}; {prog.rejected} na quarentena {quarantine_table}"
        if tr.duplicates_removed:
            detail += f"; {tr.duplicates_removed} duplicada(s) removida(s)"
    detail = "; ".join([detail, *notes])
    return StepResult(
        step.id,
        "ok",
        rows_read=prog.read,
        rows_written=prog.written,
        rows_rejected=prog.rejected,
        quality=prog.quality,
        detail=detail,
    )


def run_ingestao(step: Step, ctx: StepContext) -> StepResult:
    """Runner da etapa `ingestao`. Falhas viram `status="falha"` com a fase e a contagem até ali."""
    if not isinstance(step, IngestaoStep):
        raise ConfigError(f"a etapa '{step.id}' não é do tipo ingestao", operation="ingestao")
    secrets = step_secrets(step)
    prog = _Progress()
    try:
        return _ingest(step, ctx, prog)
    except DataEngError as exc:
        detail = f"falha na fase '{prog.phase}': {exc}"
    except Exception as exc:  # nunca deixar exceção crua; a mensagem dela pode conter valores dos dados
        log_unexpected(step.id, exc, secrets, where=f" na fase '{prog.phase}'")
        detail = f"falha na fase '{prog.phase}': erro inesperado ({type(exc).__name__}); veja o log"
    if prog.note:
        detail = f"{detail}; {prog.note}"
    detail = redact(detail, secrets)
    return StepResult(
        step.id,
        "falha",
        rows_read=prog.read,
        rows_written=prog.written,
        rows_rejected=prog.rejected,
        backlog=prog.backlog,
        quality=prog.quality,
        detail=detail,
    )


register_step_type("ingestao", run_ingestao)
