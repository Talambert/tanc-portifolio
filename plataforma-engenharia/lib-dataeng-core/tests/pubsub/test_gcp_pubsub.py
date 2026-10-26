"""GcpPubSub: testes unitários com cliente falso (sempre) e contrato real (só com credenciais GCP)."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from tests.gcp_support import hide_google, needs_gcp_credentials, needs_pubsub
from tests.pubsub.contract import run_pubsub_contract

from dataeng_core.base.errors import ConfigError, PubSubError
from dataeng_core.base.params import Environment
from dataeng_core.pubsub import GcpPubSub, PubSubMessage, make_pubsub

CANARY = "SEGREDO-NO-CORPO-DA-RESPOSTA"
PROJECT = "meu-projeto"
FULL = f"projects/{PROJECT}/subscriptions/sub-1"


def _exc() -> Any:
    from google.api_core import exceptions

    return exceptions


def _received(
    ack_id: str = "ack-1",
    message_id: str = "m-1",
    data: bytes = b"dados",
    attributes: dict[str, str] | None = None,
    publish_time: Any = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC),
) -> Any:
    msg = SimpleNamespace(message_id=message_id, data=data, attributes=attributes or {}, publish_time=publish_time)
    return SimpleNamespace(ack_id=ack_id, message=msg)


class FakeSubscriber:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.received: list[Any] = []
        self.fail_with: Exception | None = None

    def pull(self, **kwargs: Any) -> Any:
        self.calls.append(("pull", kwargs))
        if self.fail_with is not None:
            raise self.fail_with
        return SimpleNamespace(received_messages=self.received)

    def acknowledge(self, **kwargs: Any) -> None:
        self.calls.append(("acknowledge", kwargs))
        if self.fail_with is not None:
            raise self.fail_with

    def modify_ack_deadline(self, **kwargs: Any) -> None:
        self.calls.append(("modify_ack_deadline", kwargs))
        if self.fail_with is not None:
            raise self.fail_with


def _make() -> tuple[GcpPubSub, FakeSubscriber]:
    fake = FakeSubscriber()
    return GcpPubSub(PROJECT, client=fake, rpc_timeout_s=45), fake


def test_pull_calls_client_with_full_path_max_messages_and_timeout() -> None:
    c, fake = _make()
    c.pull("sub-1", 7, 12.5)
    assert fake.calls == [
        ("pull", {"subscription": FULL, "max_messages": 7, "return_immediately": False, "timeout": 17.5})
    ]


def test_pull_with_zero_timeout_returns_immediately_with_explicit_rpc_deadline() -> None:
    c, fake = _make()
    c.pull(FULL, 3, 0)
    assert fake.calls == [
        ("pull", {"subscription": FULL, "max_messages": 3, "return_immediately": True, "timeout": 30.0})
    ]


def test_pull_converts_received_messages() -> None:
    c, fake = _make()
    brt = timezone(timedelta(hours=-3))
    fake.received = [
        _received("a1", "m1", b"x", {"k": "v"}, datetime(2026, 5, 6, 7, 8, 9, 10, tzinfo=brt)),
        _received("a2", "m2", b"", {}, datetime(2026, 5, 6, 7, 8, 9)),  # sem fuso: assume UTC
    ]
    got = c.pull("sub-1", 10, 1)
    assert got == [
        PubSubMessage("m1", "a1", b"x", {"k": "v"}, datetime(2026, 5, 6, 10, 8, 9, 10, tzinfo=UTC)),
        PubSubMessage("m2", "a2", b"", {}, datetime(2026, 5, 6, 7, 8, 9, tzinfo=UTC)),
    ]
    assert all(m.publish_time.tzinfo is UTC for m in got)
    assert isinstance(got[0].data, bytes)
    assert type(got[0].attributes) is dict


def test_pull_empty_returns_empty_list() -> None:
    c, _ = _make()
    assert c.pull("sub-1", 10, 1) == []


@needs_pubsub
def test_pull_deadline_exceeded_is_empty_not_error() -> None:
    c, fake = _make()
    fake.fail_with = _exc().DeadlineExceeded("504 Deadline Exceeded " + CANARY)
    assert c.pull("sub-1", 10, 1) == []


@needs_pubsub
def test_pull_api_errors_are_typed_actionable_and_do_not_leak() -> None:
    c, fake = _make()
    fake.fail_with = _exc().PermissionDenied("403 " + CANARY)
    with pytest.raises(PubSubError, match=r"PermissionDenied \(HTTP 403\).*roles/pubsub\.subscriber") as perm:
        c.pull("sub-1", 10, 1)
    fake.fail_with = _exc().NotFound("404 " + CANARY)
    with pytest.raises(PubSubError, match=r"NotFound \(HTTP 404\).*assinatura não encontrada") as nf:
        c.pull("sub-1", 10, 1)
    fake.fail_with = _exc().ServiceUnavailable("503 " + CANARY)
    with pytest.raises(PubSubError, match=r"ServiceUnavailable") as other:
        c.pull("sub-1", 10, 1)
    for err in (perm, nf, other):
        assert CANARY not in str(err.value)
        assert err.value.__cause__ is not None


@needs_pubsub
def test_ack_and_nack_errors_are_typed_and_do_not_leak() -> None:
    c, fake = _make()
    fake.fail_with = _exc().PermissionDenied("403 " + CANARY)
    with pytest.raises(PubSubError, match=r"confirmar.*PermissionDenied") as a:
        c.ack("sub-1", ["x"])
    with pytest.raises(PubSubError, match=r"devolver.*PermissionDenied") as n:
        c.nack("sub-1", ["x"])
    fake.fail_with = _exc().DeadlineExceeded("504 " + CANARY)  # em ack/nack o prazo estourado é erro
    with pytest.raises(PubSubError, match=r"excedeu o prazo") as d:
        c.ack("sub-1", ["x"])
    for err in (a, n, d):
        assert CANARY not in str(err.value)


def test_ack_calls_acknowledge_with_ids_and_timeout() -> None:
    c, fake = _make()
    c.ack("sub-1", ("a", "b"))
    assert fake.calls == [("acknowledge", {"subscription": FULL, "ack_ids": ["a", "b"], "timeout": 45.0})]


def test_nack_sets_ack_deadline_to_zero() -> None:
    c, fake = _make()
    c.nack("sub-1", ["a"])
    assert fake.calls == [
        ("modify_ack_deadline", {"subscription": FULL, "ack_ids": ["a"], "ack_deadline_seconds": 0, "timeout": 45.0})
    ]


def test_ack_nack_with_no_ids_make_no_calls_and_large_lists_are_chunked() -> None:
    c, fake = _make()
    c.ack("sub-1", [])
    c.nack("sub-1", [])
    assert fake.calls == []
    c.ack("sub-1", [f"a{i}" for i in range(1200)])
    assert [len(kw["ack_ids"]) for _, kw in fake.calls] == [500, 500, 200]


def test_backlog_is_none() -> None:
    c, fake = _make()
    assert c.backlog("sub-1") is None
    assert fake.calls == []


@pytest.mark.parametrize(
    "name",
    [
        "",
        "ab",
        "1abc",
        "a/b",
        "../x",
        "sub 1",
        "a" * 256,
        "projects/outro/subscriptions/sub-1",
        "projects/meu-projeto/subscriptions/../x",
        "projects/meu-projeto/subscriptions/a/b",
        "projects/meu-projeto/topics/sub-1",
        "sub-1\n/../x",
    ],
)
def test_invalid_subscription_names_are_rejected_before_any_call(name: str) -> None:
    c, fake = _make()
    for call in (
        lambda: c.pull(name, 1, 0),
        lambda: c.ack(name, ["a"]),
        lambda: c.nack(name, ["a"]),
        lambda: c.backlog(name),
    ):
        with pytest.raises(ConfigError, match=r"assinatura inválida"):
            call()
    assert fake.calls == []


def test_invalid_pull_arguments() -> None:
    c, fake = _make()
    with pytest.raises(PubSubError, match=r"max_messages deve ser um inteiro maior que zero"):
        c.pull("sub-1", 0, 1)
    with pytest.raises(PubSubError, match=r"timeout_s não pode ser negativo"):
        c.pull("sub-1", 1, -1)
    assert fake.calls == []


def test_constructor_validates_project_and_timeout() -> None:
    for bad in ("", " ", "a/b", "../x"):
        with pytest.raises(ConfigError, match=r"projeto GCP"):
            GcpPubSub(bad, client=FakeSubscriber())
    with pytest.raises(ConfigError, match=r"rpc_timeout_s"):
        GcpPubSub(PROJECT, client=FakeSubscriber(), rpc_timeout_s=0)


def test_missing_publish_time_is_typed_error() -> None:
    c, fake = _make()
    fake.received = [_received(publish_time=None)]
    with pytest.raises(PubSubError, match=r"publish_time"):
        c.pull("sub-1", 1, 1)


def _env(modo: str = "gcp") -> Environment:
    return Environment(
        modo=modo,  # type: ignore[arg-type]
        projeto_gcp=PROJECT,
        regiao="southamerica-east1",
        bucket_bronze="b",
        dataset_silver="s",
        dataset_gold="g",
    )


def test_make_pubsub_gcp_without_extra_gives_actionable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    hide_google(monkeypatch)
    monkeypatch.setitem(sys.modules, "google.cloud.pubsub_v1", None)
    with pytest.raises(ConfigError, match=r"dataeng-core\[gcp\]"):
        make_pubsub(_env())


@needs_pubsub
def test_make_pubsub_gcp_returns_gcp_pubsub(monkeypatch: pytest.MonkeyPatch) -> None:
    from google.cloud import pubsub_v1

    monkeypatch.setattr(pubsub_v1, "SubscriberClient", FakeSubscriber)
    client = make_pubsub(_env())
    assert isinstance(client, GcpPubSub)
    assert client.pull("sub-1", 1, 0) == []


@needs_pubsub
def test_client_creation_failure_does_not_leak_original_message(monkeypatch: pytest.MonkeyPatch) -> None:
    from google.cloud import pubsub_v1

    def boom() -> None:
        raise RuntimeError("arquivo C:\\segredo\\chave.json " + CANARY)

    monkeypatch.setattr(pubsub_v1, "SubscriberClient", boom)
    with pytest.raises(ConfigError, match=r"RuntimeError.*GOOGLE_APPLICATION_CREDENTIALS") as err:
        GcpPubSub(PROJECT)
    assert CANARY not in str(err.value)


def test_importing_package_without_gcp_extra_works() -> None:
    code = (
        "import sys\n"
        "for n in ('google', 'google.cloud', 'google.cloud.pubsub_v1', 'google.api_core'):\n"
        "    sys.modules[n] = None\n"
        "import dataeng_core, dataeng_core.pubsub, dataeng_core.pubsub.gcp, dataeng_core.steps\n"
        "print('ok')\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "ok"


@needs_gcp_credentials
@pytest.mark.gcp
def test_gcp_pubsub_passes_contract_on_real_gcp() -> None:
    """Exige DATAENG_GCP_TOPIC e DATAENG_GCP_SUBSCRIPTION: recursos de teste DEDICADOS (a bateria esvazia a fila)."""
    topic = os.environ.get("DATAENG_GCP_TOPIC")
    subscription = os.environ.get("DATAENG_GCP_SUBSCRIPTION")
    if not topic or not subscription:
        pytest.skip(
            "defina DATAENG_GCP_TOPIC e DATAENG_GCP_SUBSCRIPTION (recursos de teste) para rodar o contrato real"
        )
    from google.cloud import pubsub_v1

    project = os.environ["DATAENG_GCP_PROJECT"]
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic)

    def publish(data: bytes, message_id: str | None = None) -> None:
        publisher.publish(topic_path, data).result(timeout=30)

    run_pubsub_contract(GcpPubSub(project), publish, subscription, redelivery_wait_s=60.0, can_duplicate=False)


def test_pull_rpc_deadline_includes_configurable_slack() -> None:
    fake = FakeSubscriber()
    GcpPubSub(PROJECT, client=fake, rpc_slack_s=2).pull("sub-1", 1, 10)
    GcpPubSub(PROJECT, client=fake).pull("sub-1", 1, 10)  # padrão: 5 s
    assert [kw["timeout"] for _, kw in fake.calls] == [12.0, 15.0]
    with pytest.raises(ConfigError, match=r"rpc_slack_s"):
        GcpPubSub(PROJECT, client=fake, rpc_slack_s=-1)


@needs_pubsub
def test_retry_error_names_cause_type_only_and_invalid_argument_is_typed() -> None:
    c, fake = _make()
    fake.fail_with = _exc().RetryError("esgotou " + CANARY, cause=_exc().ServiceUnavailable("503 " + CANARY))
    with pytest.raises(PubSubError, match=r"RetryError.*causa: ServiceUnavailable") as r:
        c.pull("sub-1", 1, 1)
    fake.fail_with = _exc().InvalidArgument("400 " + CANARY)
    with pytest.raises(PubSubError, match=r"InvalidArgument \(HTTP 400\)") as i:
        c.ack("sub-1", ["x"])
    assert CANARY not in str(r.value) + str(i.value)


def test_bare_string_ack_ids_are_rejected() -> None:
    c, fake = _make()
    with pytest.raises(PubSubError, match=r"passe uma lista"):
        c.ack("sub-1", "ack-123")
    with pytest.raises(PubSubError, match=r"passe uma lista"):
        c.nack("sub-1", "ack-123")
    assert fake.calls == []
