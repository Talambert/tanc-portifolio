"""Bateria de contrato reutilizável para qualquer implementação de PubSubClient."""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable

from dataeng_core.pubsub.base import PubSubClient, PubSubMessage

Publish = Callable[..., object]  # publish(data: bytes, message_id: str | None = None)


def _drain(client: PubSubClient, subscription: str, attempts: int = 20) -> None:
    for _ in range(attempts):
        got = client.pull(subscription, 100, 0)
        if not got:
            return
        client.ack(subscription, [m.ack_id for m in got])


def _pull_until(
    client: PubSubClient, subscription: str, payload: bytes, wait_s: float, max_messages: int = 10
) -> PubSubMessage | None:
    """Puxa até aparecer a mensagem com `payload` (ou esgotar `wait_s`); ignora as demais."""
    deadline = time.monotonic() + wait_s
    while True:
        for m in client.pull(subscription, max_messages, 1):
            if m.data == payload:
                return m
        if time.monotonic() >= deadline:
            return None
        time.sleep(0.5)


def run_pubsub_contract(
    client: PubSubClient,
    publish: Publish,
    subscription: str,
    *,
    redelivery_wait_s: float = 0.0,
    can_duplicate: bool = True,
    slack_s: float = 5.0,
) -> None:
    """Executa o contrato numa assinatura que o teste controla (e que esvazia no início e no fim).

    `publish(data, message_id=None)` publica na assinatura. `redelivery_wait_s`: quanto esperar pela volta de
    uma mensagem sem ack/nack (0 no local; no GCP real use mais que o prazo de ack da assinatura).
    `can_duplicate=False` pula a duplicata (o GCP real atribui o `message_id`). `slack_s`: folga de rede.
    """
    run = uuid.uuid4().hex[:8].encode()
    try:
        _drain(client, subscription)

        # fila vazia: devolve [] sem esperar além do timeout (mais a folga de rede)
        started = time.monotonic()
        assert client.pull(subscription, 10, 1) == []
        assert time.monotonic() - started <= 1 + slack_s
        backlog = client.backlog(subscription)
        assert backlog is None or backlog == 0

        # pull respeita max_messages
        for i in range(3):
            publish(run + b"-max-%d" % i)
        first = client.pull(subscription, 2, 1)
        assert 1 <= len(first) <= 2
        client.ack(subscription, [m.ack_id for m in first])
        _drain(client, subscription)

        # sem ack volta
        payload = run + b"-sem-ack"
        publish(payload)
        m1 = _pull_until(client, subscription, payload, 5)
        assert m1 is not None
        m2 = _pull_until(client, subscription, payload, redelivery_wait_s)
        assert m2 is not None, "mensagem sem ack deveria voltar"
        assert m2.message_id == m1.message_id
        assert m2.data == payload

        # ack remove
        client.ack(subscription, [m2.ack_id])
        client.ack(subscription, [m2.ack_id])  # repetido: idempotente
        assert _pull_until(client, subscription, payload, redelivery_wait_s) is None

        # nack devolve já
        payload = run + b"-nack"
        publish(payload)
        m = _pull_until(client, subscription, payload, 5)
        assert m is not None
        client.nack(subscription, [m.ack_id])
        again = _pull_until(client, subscription, payload, max(redelivery_wait_s, 5.0))
        assert again is not None, "mensagem com nack deveria voltar"
        client.ack(subscription, [again.ack_id])
        _drain(client, subscription)

        # backlog conta as pendentes (None = a implementação não sabe medir)
        backlog = client.backlog(subscription)
        assert backlog is None or backlog == 0
        if backlog is not None:
            publish(run + b"-bl-1")
            publish(run + b"-bl-2")
            assert client.backlog(subscription) == 2
            got = client.pull(subscription, 1, 1)
            assert client.backlog(subscription) == 2  # entregue e não confirmada continua pendente
            client.ack(subscription, [got[0].ack_id])
            assert client.backlog(subscription) == 1
            _drain(client, subscription)
            assert client.backlog(subscription) == 0

        # duplicata: mesmo message_id, ack_id diferentes
        if can_duplicate:
            dup = run + b"-dup"
            publish(dup, message_id="dup-" + run.decode())
            publish(dup, message_id="dup-" + run.decode())
            got = client.pull(subscription, 10, 1)
            same = [m for m in got if m.data == dup]
            assert len(same) == 2
            assert same[0].message_id == same[1].message_id == "dup-" + run.decode()
            assert same[0].ack_id != same[1].ack_id
            client.ack(subscription, [m.ack_id for m in got])
    finally:
        _drain(client, subscription)
