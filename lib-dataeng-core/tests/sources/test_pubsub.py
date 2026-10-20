from __future__ import annotations

import base64
import json
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import pytest

from dataeng_core.base.errors import PubSubError
from dataeng_core.base.pipeline import PubSubOrigem
from dataeng_core.base.schema import Coluna, SchemaSpec
from dataeng_core.pubsub import LocalPubSub, PubSubMessage
from dataeng_core.sources.pubsub import FLATTEN_SEPARATOR, REJECTED_COLUMNS, PulledBatch, pull_batch

SUB = "eventos-sub"
T0 = datetime(2026, 1, 1, 6, 0, tzinfo=UTC)


def origem(**kw: Any) -> PubSubOrigem:
    return PubSubOrigem.model_validate({"tipo": "pubsub", "assinatura": SUB, "schema": "s.yaml", **kw})


def local() -> LocalPubSub:
    return LocalPubSub(clock=lambda: T0)


def publish(ps: LocalPubSub, payload: Any, message_id: str, **attrs: str) -> None:
    data = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")
    ps.publish(SUB, data, attributes=attrs, message_id=message_id)


def records(batch: PulledBatch) -> list[dict[Any, Any]]:
    return batch.records.to_dict("records")


class SpyClient:
    """Delegação para o LocalPubSub, guardando as chamadas."""

    def __init__(self, inner: LocalPubSub) -> None:
        self.inner = inner
        self.calls: list[tuple[Any, ...]] = []

    def pull(self, subscription: str, max_messages: int, timeout_s: float) -> list[PubSubMessage]:
        self.calls.append(("pull", subscription, max_messages, timeout_s))
        return self.inner.pull(subscription, max_messages, timeout_s)

    def ack(self, subscription: str, ack_ids: Sequence[str]) -> None:
        self.calls.append(("ack", subscription, list(ack_ids)))
        self.inner.ack(subscription, ack_ids)

    def nack(self, subscription: str, ack_ids: Sequence[str]) -> None:
        self.calls.append(("nack", subscription, list(ack_ids)))
        self.inner.nack(subscription, ack_ids)

    def backlog(self, subscription: str) -> int | None:
        return self.inner.backlog(subscription)


def test_pull_batch_builds_rows_with_message_metadata() -> None:
    ps = local()
    publish(
        ps,
        {
            "id": "1",
            "produto": {"nome": "gasolina", "info": {"tipo": "comum"}},
            "valor": 5.5,
            "ativo": True,
            "tags": ["a", "b"],
            "vazio": "",
            "nulo": None,
            "qtd": 3,
            "sem_campos": {},
        },
        "m1",
        origem="teste",
        b="2",
    )
    publish(ps, {"id": "2", "valor": None, "extra": "x"}, "m2")

    batch = pull_batch(ps, origem())

    assert FLATTEN_SEPARATOR == "__"
    assert list(batch.records.columns) == [
        "message_id", "publish_time", "attributes",
        "id", "produto__nome", "produto__info__tipo", "valor", "ativo", "tags", "vazio", "nulo", "qtd", "sem_campos",
        "extra",
    ]  # fmt: skip
    assert records(batch) == [
        {
            "message_id": "m1",
            "publish_time": "2026-01-01T06:00:00+00:00",
            "attributes": '{"b": "2", "origem": "teste"}',
            "id": "1",
            "produto__nome": "gasolina",
            "produto__info__tipo": "comum",
            "valor": "5.5",
            "ativo": "true",
            "tags": '["a", "b"]',
            "vazio": None,
            "nulo": None,
            "qtd": "3",
            "sem_campos": None,
            "extra": None,
        },
        {
            "message_id": "m2",
            "publish_time": "2026-01-01T06:00:00+00:00",
            "attributes": "{}",
            "id": "2",
            "produto__nome": None,
            "produto__info__tipo": None,
            "valor": None,
            "ativo": None,
            "tags": None,
            "vazio": None,
            "nulo": None,
            "qtd": None,
            "sem_campos": None,
            "extra": "x",
        },
    ]
    assert batch.rejected.empty and list(batch.rejected.columns) == REJECTED_COLUMNS
    assert batch.ack_ids == ["ack-1", "ack-2"]  # ainda não confirmados: pull_batch não faz ack
    assert batch.backlog == 2  # medido logo depois do pull: o lote ainda conta
    assert [m.message_id for m in batch.messages] == ["m1", "m2"]
    assert ps.backlog(SUB) == 2


def test_publish_time_is_normalized_to_utc() -> None:
    ps = LocalPubSub(clock=lambda: datetime(2026, 1, 1, 3, 0, tzinfo=timezone(timedelta(hours=-3))))
    publish(ps, {"id": "1"}, "m1")
    assert records(pull_batch(ps, origem()))[0]["publish_time"] == "2026-01-01T06:00:00+00:00"


INVALID: list[tuple[bytes, str]] = [
    (b'{"id": "1", "segredo": "SEGREDO-123"', "json_invalido"),
    (b"\xff\xfe SEGREDO-123", "utf8_invalido"),
    (b"", "mensagem_vazia"),
    (b"   \n", "mensagem_vazia"),
    (b'["SEGREDO-123"]', "registro_json_invalido"),
    (b'"SEGREDO-123"', "registro_json_invalido"),
    (b"null", "registro_json_invalido"),
    (b"[" * 100_000 + b"]" * 100_000, "aninhamento_excessivo"),
]


def test_invalid_json_goes_to_rejected_and_is_acked() -> None:
    ps = local()
    publish(ps, {"id": "1"}, "ok-1")
    for i, (data, _) in enumerate(INVALID):
        publish(ps, data, f"ruim-{i}", chave="v")

    batch = pull_batch(ps, origem())

    assert [r["message_id"] for r in records(batch)] == ["ok-1"]
    got = batch.rejected.to_dict("records")
    assert [(r["message_id"], r["_motivo"]) for r in got] == [(f"ruim-{i}", m) for i, (_, m) in enumerate(INVALID)]
    for (data, _), row in zip(INVALID, got, strict=True):
        original = json.loads(row["_linha_original"])
        assert base64.b64decode(original["data_base64"]) == data  # sem perda, mesmo com UTF-8 inválido
        assert original["attributes"] == {"chave": "v"}
        assert original["publish_time"] == "2026-01-01T06:00:00+00:00"
        assert "SEGREDO" not in row["_motivo"]
    # o ack_id das inválidas TAMBÉM entra: depois de quarentenadas, são confirmadas e não voltam
    assert len(batch.ack_ids) == 1 + len(INVALID)
    ps.ack(SUB, batch.ack_ids)
    assert ps.backlog(SUB) == 0
    assert ps.pull(SUB, 100, 0) == []


def test_reserved_duplicated_and_missing_required_fields_reject_only_that_message() -> None:
    ps = local()
    schema = SchemaSpec(colunas=[Coluna(nome="id", tipo="texto", obrigatoria=True)])
    publish(ps, {"id": "1", "message_id": "forjado"}, "m1")
    publish(ps, {"id": "2", "_run_id": "x"}, "m2")
    publish(ps, {"id": "3", "a": {"b": 1}, "a__b": 2}, "m3")
    publish(ps, {"nome": "sem id"}, "m4")
    publish(ps, {"id": "5"}, "m5")

    batch = pull_batch(ps, origem(), schema=schema, reserved=["_run_id"])

    assert [r["message_id"] for r in records(batch)] == ["m5"]
    assert [(r["message_id"], r["_motivo"]) for r in batch.rejected.to_dict("records")] == [
        ("m1", "campo_reservado (message_id)"),
        ("m2", "campo_reservado (_run_id)"),
        ("m3", "campo_repetido_apos_achatamento (a__b)"),
        ("m4", "campo_obrigatorio_ausente (id)"),
    ]
    assert len(batch.ack_ids) == 5


def test_empty_queue_succeeds_with_zero_rows() -> None:
    schema = SchemaSpec(colunas=[Coluna(nome="id", tipo="texto", obrigatoria=True)])
    for ps in (local(), LocalPubSub()):  # assinatura inexistente e fila esvaziada
        batch = pull_batch(ps, origem(), schema=schema)
        assert batch.records.empty and batch.rejected.empty
        assert list(batch.records.columns) == ["message_id", "publish_time", "attributes", "id"]
        assert list(batch.rejected.columns) == REJECTED_COLUMNS
        assert batch.ack_ids == [] and batch.messages == []
        assert batch.backlog == 0


def test_respects_max_messages() -> None:
    ps = local()
    for i in range(5):
        publish(ps, {"id": str(i)}, f"m{i}")
    spy = SpyClient(ps)

    batch = pull_batch(spy, origem(max_mensagens=2, tempo_maximo_s=7))

    assert spy.calls == [("pull", SUB, 2, 7.0)]  # uma única chamada, com o teto da origem
    assert [r["message_id"] for r in records(batch)] == ["m0", "m1"]
    assert len(batch.ack_ids) == 2
    assert batch.backlog == 5


class _BrokenBacklog(SpyClient):
    def __init__(self, inner: LocalPubSub, exc: Exception) -> None:
        super().__init__(inner)
        self.exc = exc

    def backlog(self, subscription: str) -> int | None:
        raise self.exc


@pytest.mark.parametrize("exc", [PubSubError("indisponível"), RuntimeError("boom")])
def test_backlog_failure_does_not_break_the_batch(exc: Exception) -> None:
    ps = local()
    publish(ps, {"id": "1"}, "m1")
    batch = pull_batch(_BrokenBacklog(ps, exc), origem())
    assert batch.backlog is None
    assert len(batch.ack_ids) == 1


def test_pull_failure_propagates_without_ack_ids() -> None:
    class Failing(SpyClient):
        def pull(self, subscription: str, max_messages: int, timeout_s: float) -> list[PubSubMessage]:
            raise PubSubError("falha simulada", operation="pubsub.pull")

    with pytest.raises(PubSubError, match="falha simulada"):
        pull_batch(Failing(local()), origem())


def test_records_are_text_frames_with_none_for_nulls() -> None:
    ps = local()
    publish(ps, {"id": 1, "x": float("nan")}, "m1")  # json.dumps escreve NaN; o leitor trata como nulo
    batch = pull_batch(ps, origem())
    assert records(batch) == [
        {"message_id": "m1", "publish_time": "2026-01-01T06:00:00+00:00", "attributes": "{}", "id": "1", "x": None}
    ]
    assert all(batch.records[c].dtype == object for c in batch.records.columns)
    assert isinstance(batch.records, pd.DataFrame)
