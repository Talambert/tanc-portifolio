"""Pull em lote de uma assinatura Pub/Sub: uma linha por mensagem, com os metadados e o JSON achatado.

`pull_batch` faz UMA chamada de `pull(assinatura, max_mensagens, tempo_maximo_s)` e devolve o lote;
não há laço: o agendamento da DAG repete o lote (pull em lote agendado). Quem consome (a etapa
`ingestao`) grava o bronze e a silver e SÓ ENTÃO confirma (`ack`) todos os `ack_ids` do lote.

Linhas (`records`), todas em texto, como nas outras origens (a tipagem é feita em `transformacao`):
- `message_id`, `publish_time` (ISO 8601 em UTC) e `attributes` (objeto JSON, chaves ordenadas);
- os campos do JSON da mensagem, achatados: objetos aninhados viram colunas `pai__filho`
  (separador `FLATTEN_SEPARATOR` = `__`, que mantém o nome válido como coluna SQL); listas e
  booleanos viram texto JSON; nulo, `NaN` e texto vazio viram `None` (contrato de nulos das fontes).

Mensagens inválidas vão para `rejected` (`message_id`, `_linha_original`, `_motivo`) e seu `ack_id`
TAMBÉM entra em `ack_ids`: depois de quarentenadas são confirmadas, para não voltarem para sempre.
`_linha_original` é um JSON com os metadados e o conteúdo em `data_base64` (sem perda, mesmo com
UTF-8 inválido). `_motivo` nunca traz conteúdo da mensagem, só a categoria (e o nome do campo, quando
for o caso). Motivos: `mensagem_vazia`, `utf8_invalido`, `json_invalido`, `registro_json_invalido`
(JSON que não é objeto), `aninhamento_excessivo`, `campo_reservado (<nome>)`,
`campo_repetido_apos_achatamento (<nome>)`, `campo_obrigatorio_ausente (<nome>)` e
`erro_inesperado (<tipo>)`. Rejeitar por mensagem (e não o lote inteiro) evita que uma mensagem ruim
trave a assinatura.

Fila vazia: `records` e `rejected` vazios, `ack_ids=[]`, sem erro. `backlog` vem do cliente logo
depois do pull (inclui as mensagens deste lote, ainda não confirmadas; `None` se o cliente não sabe
medir, como o `GcpPubSub`, ou se a medição falhar).
"""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from dataeng_core.base.errors import DataEngError
from dataeng_core.base.pipeline import PubSubOrigem
from dataeng_core.base.runtime import get_logger
from dataeng_core.base.schema import SchemaSpec
from dataeng_core.pubsub.base import PubSubClient, PubSubMessage

log = get_logger(__name__)

FLATTEN_SEPARATOR = "__"
METADATA_COLUMNS = ("message_id", "publish_time", "attributes")
LINHA_ORIGINAL = "_linha_original"
MOTIVO = "_motivo"
REJECTED_COLUMNS = ["message_id", LINHA_ORIGINAL, MOTIVO]
_MAX_NAME_SHOWN = 64


@dataclass
class PulledBatch:
    records: pd.DataFrame
    rejected: pd.DataFrame
    ack_ids: list[str]
    backlog: int | None
    messages: list[PubSubMessage] = field(default_factory=list)  # o lote cru, para o bronze


class _Rejected(ValueError):
    """Mensagem inválida; `motivo` é a categoria, sem conteúdo da mensagem."""

    def __init__(self, motivo: str) -> None:
        super().__init__(motivo)
        self.motivo = motivo


def _name(name: str) -> str:
    return name if len(name) <= _MAX_NAME_SHOWN else name[:_MAX_NAME_SHOWN] + "..."


def _json_constant(_name: str) -> None:
    """`NaN`, `Infinity` e `-Infinity` (aceitos pelo Python) são tratados como nulo, como nos arquivos."""
    return None


def _cell(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, float) and value != value:  # NaN
        return None
    if isinstance(value, bool | list | dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _flatten(obj: dict[str, Any], prefix: str, out: dict[str, str | None]) -> None:
    for key, value in obj.items():
        name = f"{prefix}{FLATTEN_SEPARATOR}{key}" if prefix else str(key)
        if isinstance(value, dict) and value:
            _flatten(value, name, out)
            continue
        if name in out:
            raise _Rejected(f"campo_repetido_apos_achatamento ({_name(name)})")
        out[name] = None if isinstance(value, dict) else _cell(value)


def publish_time_text(value: datetime) -> str:
    """ISO 8601 em UTC (horário sem fuso é tomado como UTC)."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _metadata(msg: PubSubMessage) -> dict[str, str | None]:
    return {
        "message_id": str(msg.message_id),
        "publish_time": publish_time_text(msg.publish_time),
        "attributes": json.dumps(dict(msg.attributes), ensure_ascii=False, sort_keys=True),
    }


def raw_message(msg: PubSubMessage) -> dict[str, Any]:
    """A mensagem sem perda (conteúdo em base64), como vai para o bronze e para `_linha_original`."""
    return {
        "message_id": str(msg.message_id),
        "publish_time": publish_time_text(msg.publish_time),
        "attributes": dict(msg.attributes),
        "data_base64": base64.b64encode(bytes(msg.data)).decode("ascii"),
    }


def _parse(msg: PubSubMessage, required: Sequence[str], reserved: set[str]) -> dict[str, str | None]:
    data = bytes(msg.data)
    if not data.strip():
        raise _Rejected("mensagem_vazia")
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise _Rejected("utf8_invalido") from None
    try:
        value = json.loads(text, parse_constant=_json_constant)
    except RecursionError:
        raise _Rejected("aninhamento_excessivo") from None
    except ValueError:
        raise _Rejected("json_invalido") from None
    if not isinstance(value, dict):
        raise _Rejected("registro_json_invalido")
    fields: dict[str, str | None] = {}
    try:
        _flatten(value, "", fields)
    except RecursionError:
        raise _Rejected("aninhamento_excessivo") from None
    for name in fields:
        if name in reserved:
            raise _Rejected(f"campo_reservado ({_name(name)})")
    for name in required:
        if name not in fields:
            raise _Rejected(f"campo_obrigatorio_ausente ({_name(name)})")
    return {**_metadata(msg), **fields}


def _frame(rows: list[dict[str, str | None]], columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {c: pd.Series([r.get(c) for r in rows], dtype=object) for c in columns},
        index=pd.RangeIndex(len(rows)),
    )


def read_backlog(client: PubSubClient, subscription: str) -> int | None:
    """Backlog da assinatura, sem nunca falhar: `None` se o cliente não souber medir ou a medição falhar."""
    try:
        value = client.backlog(subscription)
    except DataEngError as exc:
        log.warning(f"não foi possível medir o backlog da assinatura: {exc}")
        return None
    except Exception as exc:  # medir o backlog é informativo; não pode derrubar o lote
        log.warning(f"não foi possível medir o backlog da assinatura ({type(exc).__name__})")
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def pull_batch(
    client: PubSubClient,
    origem: PubSubOrigem,
    *,
    schema: SchemaSpec | None = None,
    reserved: Sequence[str] = (),
) -> PulledBatch:
    """Puxa UM lote (até `origem.max_mensagens`, esperando até `origem.tempo_maximo_s`).

    `schema` (opcional) é o schema da origem: suas colunas `obrigatoria` precisam existir em cada
    mensagem (senão a mensagem é rejeitada) e todas as suas colunas existem no resultado (com `None`
    quando nenhuma mensagem do lote as traz, inclusive no lote vazio). `reserved` são nomes
    que a mensagem não pode trazer (além de `message_id`, `publish_time` e `attributes`).
    Não confirma nada: o chamador faz `ack` (ou `nack`) de `ack_ids` depois de gravar.
    """
    messages = list(client.pull(origem.assinatura, origem.max_mensagens, origem.tempo_maximo_s))
    required = [c.nome for c in schema.colunas if c.obrigatoria] if schema is not None else []
    blocked = {*METADATA_COLUMNS, *reserved}

    rows: list[dict[str, str | None]] = []
    rejected: list[dict[str, str | None]] = []
    ack_ids: list[str] = []
    for msg in messages:
        ack_ids.append(msg.ack_id)
        try:
            rows.append(_parse(msg, required, blocked))
            continue
        except _Rejected as exc:
            motivo = exc.motivo
        except Exception as exc:  # nunca deixar uma mensagem travar o lote
            motivo = f"erro_inesperado ({type(exc).__name__})"
        try:
            original = json.dumps(raw_message(msg), ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError, binascii.Error):
            original = json.dumps({"message_id": str(msg.message_id)})
        rejected.append({"message_id": str(msg.message_id), LINHA_ORIGINAL: original, MOTIVO: motivo})

    columns = list(METADATA_COLUMNS)
    for row in rows:
        columns.extend(k for k in row if k not in columns)
    if schema is not None:  # colunas do schema existem mesmo que nenhuma mensagem do lote as traga
        columns.extend(c.nome for c in schema.colunas if c.nome not in columns)
    return PulledBatch(
        records=_frame(rows, columns),
        rejected=_frame(rejected, REJECTED_COLUMNS),
        ack_ids=ack_ids,
        backlog=read_backlog(client, origem.assinatura),
        messages=messages,
    )
