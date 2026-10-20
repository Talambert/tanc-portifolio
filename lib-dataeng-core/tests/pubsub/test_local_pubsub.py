"""LocalPubSub: contrato compartilhado e comportamentos específicos do modelo em memória."""

from __future__ import annotations

import time
from datetime import UTC, datetime

import pytest
from tests.pubsub.contract import run_pubsub_contract

from dataeng_core.base.errors import PubSubError
from dataeng_core.base.params import Environment
from dataeng_core.pubsub import LocalPubSub, PubSubMessage, make_pubsub

SUB = "assinatura-teste"
FIXED = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)


def _client() -> LocalPubSub:
    return LocalPubSub(clock=lambda: FIXED)


def test_local_pubsub_passes_contract() -> None:
    client = _client()
    run_pubsub_contract(client, lambda data, message_id=None: client.publish(SUB, data, None, message_id), SUB)


def test_pull_respects_max_messages_in_publish_order() -> None:
    c = _client()
    for i in range(5):
        c.publish(SUB, b"m%d" % i)
    got = c.pull(SUB, 3, 0)
    assert [m.data for m in got] == [b"m0", b"m1", b"m2"]


def test_pull_returns_message_with_all_fields() -> None:
    c = _client()
    mid = c.publish(SUB, b"abc", {"k": "v"}, message_id="id-1")
    assert mid == "id-1"
    [m] = c.pull(SUB, 10, 0)
    assert m == PubSubMessage(
        message_id="id-1", ack_id=m.ack_id, data=b"abc", attributes={"k": "v"}, publish_time=FIXED
    )
    assert m.ack_id


def test_publish_generates_distinct_ids_and_copies_attributes() -> None:
    c = _client()
    attrs = {"a": "1"}
    id1 = c.publish(SUB, b"x", attrs)
    id2 = c.publish(SUB, b"y")
    assert id1 != id2
    attrs["a"] = "mudou"
    got = c.pull(SUB, 10, 0)
    assert got[0].attributes == {"a": "1"}
    got[0].attributes["z"] = "1"  # mutar o devolvido não afeta o armazenado
    c.nack(SUB, [got[0].ack_id])
    assert c.pull(SUB, 10, 0)[0].attributes == {"a": "1"}


def test_unacked_message_returns_with_new_ack_id_and_old_ack_id_is_stale() -> None:
    c = _client()
    c.publish(SUB, b"x", message_id="m")
    [first] = c.pull(SUB, 10, 0)
    [second] = c.pull(SUB, 10, 0)
    assert second.message_id == first.message_id == "m"
    assert second.ack_id != first.ack_id
    c.ack(SUB, [first.ack_id])  # ack_id de entrega antiga: ignorado
    assert c.backlog(SUB) == 1
    c.ack(SUB, [second.ack_id])
    assert c.backlog(SUB) == 0
    assert c.pull(SUB, 10, 0) == []


def test_ack_unknown_and_repeated_are_silent() -> None:
    c = _client()
    c.ack(SUB, ["nunca-existiu"])  # assinatura inexistente
    c.publish(SUB, b"x")
    c.ack(SUB, ["nunca-existiu"])
    [m] = c.pull(SUB, 10, 0)
    c.ack(SUB, [m.ack_id])
    c.ack(SUB, [m.ack_id])
    c.nack(SUB, [m.ack_id])
    c.nack("outra", ["x"])
    assert c.backlog(SUB) == 0


def test_nack_makes_message_available_and_invalidates_ack_id() -> None:
    c = _client()
    c.publish(SUB, b"x")
    [m] = c.pull(SUB, 10, 0)
    c.nack(SUB, [m.ack_id])
    c.ack(SUB, [m.ack_id])  # ack_id já devolvido: não confirma
    assert c.backlog(SUB) == 1
    [again] = c.pull(SUB, 10, 0)
    assert again.data == b"x"


def test_backlog_counts_undelivered_plus_unacked() -> None:
    c = _client()
    assert c.backlog(SUB) == 0
    for _ in range(3):
        c.publish(SUB, b"x")
    assert c.backlog(SUB) == 3
    got = c.pull(SUB, 2, 0)
    assert c.backlog(SUB) == 3
    c.ack(SUB, [got[0].ack_id])
    assert c.backlog(SUB) == 2


def test_duplicate_message_id_keeps_id_and_gets_distinct_ack_ids() -> None:
    c = _client()
    c.publish(SUB, b"x", message_id="dup")
    c.publish(SUB, b"x", message_id="dup")
    a, b = c.pull(SUB, 10, 0)
    assert a.message_id == b.message_id == "dup"
    assert a.ack_id != b.ack_id
    c.ack(SUB, [a.ack_id])
    assert c.backlog(SUB) == 1


def test_subscriptions_are_isolated_and_unknown_subscription_is_empty() -> None:
    c = _client()
    c.publish("a-sub", b"x")
    assert c.pull("b-sub", 10, 0) == []
    assert c.backlog("b-sub") == 0
    assert c.backlog("a-sub") == 1


def test_empty_pull_never_waits_beyond_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []
    monkeypatch.setattr(time, "sleep", slept.append)
    c = _client()
    started = time.monotonic()
    assert c.pull(SUB, 10, 30.0) == []
    elapsed = time.monotonic() - started
    assert elapsed <= 30.0
    assert elapsed < 1.0  # sem threads, não há o que esperar
    assert slept == []


@pytest.mark.parametrize("bad", [0, -1, 1.5, True, "3"])
def test_invalid_max_messages_is_actionable_error(bad: object) -> None:
    with pytest.raises(PubSubError, match=r"max_messages deve ser um inteiro maior que zero"):
        _client().pull(SUB, bad, 0)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad", [-0.1, -1, True, "1"])
def test_invalid_timeout_is_actionable_error(bad: object) -> None:
    with pytest.raises(PubSubError, match=r"timeout_s não pode ser negativo"):
        _client().pull(SUB, 1, bad)  # type: ignore[arg-type]


def test_invalid_arguments_in_other_operations() -> None:
    c = _client()
    for call in (
        lambda: c.publish("", b"x"),
        lambda: c.publish(SUB, "texto"),  # type: ignore[arg-type]
        lambda: c.publish(SUB, b"x", message_id=""),
        lambda: c.pull(" ", 1, 0),
        lambda: c.ack("", []),
        lambda: c.nack("", []),
        lambda: c.backlog(""),
    ):
        with pytest.raises(PubSubError):
            call()


def test_publish_time_defaults_to_utc_now() -> None:
    c = LocalPubSub()
    c.publish(SUB, b"x")
    [m] = c.pull(SUB, 1, 0)
    assert m.publish_time.tzinfo is not None
    assert m.publish_time.utcoffset() == UTC.utcoffset(None)
    assert abs((datetime.now(UTC) - m.publish_time).total_seconds()) < 60


def _env(modo: str) -> Environment:
    return Environment(
        modo=modo,  # type: ignore[arg-type]
        projeto_gcp="meu-projeto",
        regiao="southamerica-east1",
        bucket_bronze="b",
        dataset_silver="s",
        dataset_gold="g",
    )


def test_make_pubsub_local_returns_independent_local_pubsub() -> None:
    a = make_pubsub(_env("local"))
    assert isinstance(a, LocalPubSub)
    assert a is not make_pubsub(_env("local"))
