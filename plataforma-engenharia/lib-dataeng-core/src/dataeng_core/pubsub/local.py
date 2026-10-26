"""Pub/Sub em memória, determinístico, sem threads nem espera real (para testes e execução local).

Modelo simples e documentado:
- `publish` enfileira a mensagem na assinatura (a assinatura nasce na primeira publicação).
- `pull` devolve até `max_messages` mensagens pendentes, na ordem de publicação. Pendente = ainda não
  entregue OU entregue e não confirmada: "mensagem sem `ack` volta no próximo `pull`" (não há prazo de ack).
  Cada entrega recebe um `ack_id` novo; só o `ack_id` da entrega mais recente vale.
- `ack` remove definitivamente. `nack` devolve já (a mensagem fica pendente e o `ack_id` antigo deixa de valer).
- `ack`/`nack` com `ack_id` desconhecido, repetido ou de entrega antiga são ignorados em silêncio
  (idempotente, como o Pub/Sub real para `ack` repetido).
- Assinatura inexistente: se comporta como fila vazia (`pull` -> `[]`, `backlog` -> 0), sem erro.
- `pull` em fila vazia devolve `[]` na hora: sem threads, ninguém poderia publicar durante a espera,
  então esperar até `timeout_s` não mudaria o resultado (o tempo gasto é sempre <= `timeout_s`).
- `pull(max_messages=N)` sem ack repete sempre as N primeiras da fila (a cabeça): mensagens reentregues vêm
  antes das novas. Quem consome (T19) deve fazer `ack`/`nack` de cada lote antes do próximo `pull`.
- `backlog` = mensagens pendentes (não entregues + entregues sem ack).
- Publicar o mesmo `message_id` duas vezes simula entrega duplicada: mesmo `message_id`, `ack_id` diferentes.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

from dataeng_core.base.errors import PubSubError
from dataeng_core.pubsub.base import PubSubMessage


@dataclass
class _Stored:
    message_id: str
    data: bytes
    attributes: dict[str, str]
    publish_time: datetime
    current_ack_id: str | None = None  # ack_id da última entrega ainda não confirmada nem devolvida


@dataclass
class _Subscription:
    pending: list[_Stored] = field(default_factory=list)


def _check_ids(ack_ids: Sequence[str], operation: str) -> None:
    if isinstance(ack_ids, str | bytes):
        raise PubSubError("ack_ids deve ser uma lista, não um texto: passe uma lista.", operation=operation)


def _check_subscription(subscription: str, operation: str) -> None:
    if not isinstance(subscription, str) or not subscription.strip():
        raise PubSubError(
            "o nome da assinatura está vazio ou não é texto. Informe o nome da assinatura.", operation=operation
        )


class LocalPubSub:
    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        """`clock` devolve o instante de publicação (UTC); injete um relógio fixo para testes determinísticos."""
        self._clock = clock or (lambda: datetime.now(UTC))
        self._subs: dict[str, _Subscription] = {}
        self._message_seq = 0
        self._used_ids: set[str] = set()
        self._ack_seq = 0

    def publish(
        self,
        subscription: str,
        data: bytes,
        attributes: dict[str, str] | None = None,
        message_id: str | None = None,
    ) -> str:
        _check_subscription(subscription, "pubsub.publish")
        if not isinstance(data, bytes):
            raise PubSubError(
                f"o conteúdo da mensagem deve ser bytes (recebido {type(data).__name__}). "
                "Codifique o texto, por exemplo com .encode('utf-8').",
                operation="pubsub.publish",
            )
        if message_id is None:
            while True:
                self._message_seq += 1
                message_id = f"local-{self._message_seq}"
                if message_id not in self._used_ids:  # não colide com id explícito já publicado
                    break
        elif not isinstance(message_id, str) or not message_id:
            raise PubSubError("o message_id deve ser um texto não vazio.", operation="pubsub.publish")
        self._used_ids.add(message_id)
        published = self._clock()
        sub = self._subs.setdefault(subscription, _Subscription())
        sub.pending.append(_Stored(message_id, data, dict(attributes or {}), published))
        return message_id

    def pull(self, subscription: str, max_messages: int, timeout_s: float) -> list[PubSubMessage]:
        _check_subscription(subscription, "pubsub.pull")
        if isinstance(max_messages, bool) or not isinstance(max_messages, int) or max_messages <= 0:
            raise PubSubError(
                f"max_messages deve ser um inteiro maior que zero (recebido {max_messages!r}).",
                operation="pubsub.pull",
            )
        if isinstance(timeout_s, bool) or not isinstance(timeout_s, int | float) or timeout_s < 0:
            raise PubSubError(
                f"timeout_s não pode ser negativo (recebido {timeout_s!r}). Use 0 para não esperar.",
                operation="pubsub.pull",
            )
        sub = self._subs.get(subscription)
        if sub is None:
            return []
        out: list[PubSubMessage] = []
        for stored in sub.pending[:max_messages]:
            self._ack_seq += 1
            stored.current_ack_id = f"ack-{self._ack_seq}"
            out.append(
                PubSubMessage(
                    message_id=stored.message_id,
                    ack_id=stored.current_ack_id,
                    data=stored.data,
                    attributes=dict(stored.attributes),
                    publish_time=stored.publish_time,
                )
            )
        return out

    def ack(self, subscription: str, ack_ids: Sequence[str]) -> None:
        _check_subscription(subscription, "pubsub.ack")
        _check_ids(ack_ids, "pubsub.ack")
        sub = self._subs.get(subscription)
        if sub is None:
            return
        wanted = set(ack_ids)
        sub.pending = [s for s in sub.pending if s.current_ack_id not in wanted]

    def nack(self, subscription: str, ack_ids: Sequence[str]) -> None:
        _check_subscription(subscription, "pubsub.nack")
        _check_ids(ack_ids, "pubsub.nack")
        sub = self._subs.get(subscription)
        if sub is None:
            return
        wanted = set(ack_ids)
        for stored in sub.pending:
            if stored.current_ack_id in wanted:
                stored.current_ack_id = None

    def backlog(self, subscription: str) -> int | None:
        _check_subscription(subscription, "pubsub.backlog")
        sub = self._subs.get(subscription)
        return 0 if sub is None else len(sub.pending)
