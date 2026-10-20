"""Bateria de contrato reutilizável para qualquer implementação de PubSubClient."""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable

from dataeng_core.pubsub.base import PubSubClient, PubSubMessage

Publish = Callable[..., object]  # publish(data: bytes, message_id: str | None = None)


def _drain(
    client: PubSubClient,
    subscription: str,
    *,
    timeout_s: float = 2.0,
    max_empty: int = 2,
    max_iterations: int = 50,
) -> None:
    """Esvazia a assinatura: para só após `max_empty` pulls vazios seguidos (o serviço real pode devolver
    vazio havendo mensagens) e nunca passa de `max_iterations` pulls."""
    empty = 0
    for _ in range(max_iterations):
        got = client.pull(subscription, 100, timeout_s)
        if got:
            empty = 0
            client.ack(subscription, [m.ack_id for m in got])
        else:
            empty += 1
            if empty >= max_empty:
                return


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
    poll_timeout_s: float = 2.0,
) -> None:
    """Executa o contrato numa assinatura que o teste controla (e que esvazia no início e no fim).

    `publish(data, message_id=None)` publica na assinatura. `redelivery_wait_s`: quanto esperar pela volta de
    uma mensagem sem ack/nack (0 no local; no GCP real use mais que o prazo de ack da assinatura).
    `can_duplicate=False` pula a duplicata (o GCP real atribui o `message_id`). `slack_s`: folga de rede.
    `poll_timeout_s`: espera dos pulls de drenagem e de fila vazia (>0 no GCP real).
    """
    run = uuid.uuid4().hex[:8].encode()
    try:
        _drain(client, subscription, timeout_s=poll_timeout_s)

        # fila vazia: devolve [] sem esperar além do timeout (mais a folga de rede)
        started = time.monotonic()
        assert client.pull(subscription, 10, poll_timeout_s) == []
        assert time.monotonic() - started <= poll_timeout_s + slack_s
        backlog = client.backlog(subscription)
        assert backlog is None or backlog == 0

        # pull respeita max_messages
        for i in range(3):
            publish(run + b"-max-%d" % i)
        first = client.pull(subscription, 2, 1)
        assert 1 <= len(first) <= 2
        client.ack(subscription, [m.ack_id for m in first])
        _drain(client, subscription, timeout_s=poll_timeout_s)

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
        _drain(client, subscription, timeout_s=poll_timeout_s)

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
            _drain(client, subscription, timeout_s=poll_timeout_s)
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
        _drain(client, subscription, timeout_s=poll_timeout_s)
