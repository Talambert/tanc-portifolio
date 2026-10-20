"""Interface do cliente Pub/Sub (pull em lote, confirmação explícita)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from dataeng_core.base.params import Environment


@dataclass
class PubSubMessage:
    message_id: str
    ack_id: str
    data: bytes
    attributes: dict[str, str]
    publish_time: datetime


class PubSubClient(Protocol):
    """Cliente de assinatura: puxa, confirma (`ack`), devolve (`nack`) e mede o backlog."""

    def pull(self, subscription: str, max_messages: int, timeout_s: float) -> list[PubSubMessage]: ...

    def ack(self, subscription: str, ack_ids: Sequence[str]) -> None: ...

    def nack(self, subscription: str, ack_ids: Sequence[str]) -> None: ...

    def backlog(self, subscription: str) -> int | None: ...


def make_pubsub(env: Environment) -> PubSubClient:
    """`LocalPubSub` (em memória, novo a cada chamada) em modo local; `GcpPubSub` em `gcp` (exige o extra `[gcp]`)."""
    if env.modo == "local":
        from dataeng_core.pubsub.local import LocalPubSub

        return LocalPubSub()
    from dataeng_core.pubsub.gcp import GcpPubSub

    return GcpPubSub(env.projeto_gcp)
