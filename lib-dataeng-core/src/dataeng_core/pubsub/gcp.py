"""Pub/Sub no Google Cloud: `pull` síncrono do `SubscriberClient`, com confirmação explícita.

As importações de `google.cloud` são tardias (só no construtor, e só sem `client=`), para o pacote base
continuar funcionando sem o extra `[gcp]`. O parâmetro `client` existe para injeção em testes.
Os erros da API são reconhecidos pelo nome/código (sem importar `google.api_core`) e as mensagens nunca
repetem o texto devolvido pelo serviço, que pode conter dados.

`backlog` devolve sempre `None`: o backlog real exige a métrica `num_undelivered_messages` do Cloud
Monitoring, que esta classe não consulta.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from dataeng_core.base.errors import ConfigError, DataEngError, PubSubError
from dataeng_core.pubsub.base import PubSubMessage

_EXTRA_HINT = 'Instale o extra de GCP: pip install "dataeng-core[gcp]".'
_NAME = r"[A-Za-z][A-Za-z0-9\-_.~+%]{2,254}"
_SHORT_NAME = re.compile(rf"^{_NAME}$")
_FULL_NAME = re.compile(rf"^projects/([^/]+)/subscriptions/{_NAME}$")
_PROJECT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9\-_.:]*$")
_ACK_CHUNK = 500  # ack_ids por requisição, para ficar longe do limite de tamanho da API
_NO_WAIT_RPC_TIMEOUT_S = 30.0  # prazo da chamada quando `timeout_s == 0` (retorno imediato)


def _api_failure(exc: Exception, what: str) -> str:
    """Texto seguro de uma falha da API: só o tipo e o código HTTP, nunca o corpo da resposta."""
    name = type(exc).__name__
    code = getattr(exc, "code", None)
    shown = f"{name} (HTTP {code})" if isinstance(code, int) else name
    if code in (401, 403):
        hint = "sem permissão ou credenciais inválidas: confira a conta de serviço e o papel roles/pubsub.subscriber"
    elif code == 404:
        hint = "assinatura não encontrada: confira o nome da assinatura e o projeto"
    elif _is_deadline(exc):
        hint = "a chamada excedeu o prazo: tente novamente (as mensagens não confirmadas voltam)"
    else:
        hint = "verifique a conectividade, a cota e o nome da assinatura, e tente novamente"
    return f"{what}: {shown}; {hint}."


def _is_deadline(exc: Exception) -> bool:
    return type(exc).__name__ == "DeadlineExceeded" or getattr(exc, "code", None) == 504


class GcpPubSub:
    def __init__(self, project: str, *, client: Any = None, rpc_timeout_s: float = 60.0) -> None:
        if not isinstance(project, str) or not _PROJECT.match(project):
            raise ConfigError("o projeto GCP do Pub/Sub está vazio ou inválido.", operation="GcpPubSub")
        if isinstance(rpc_timeout_s, bool) or not isinstance(rpc_timeout_s, int | float) or rpc_timeout_s <= 0:
            raise ConfigError("rpc_timeout_s deve ser maior que zero.", operation="GcpPubSub")
        self._project = project
        self._rpc_timeout_s = float(rpc_timeout_s)
        if client is None:
            try:
                import google.cloud.pubsub_v1 as pubsub_v1
            except ImportError as exc:
                raise ConfigError(
                    f"o pacote google-cloud-pubsub não está instalado. {_EXTRA_HINT}", operation="GcpPubSub"
                ) from exc
            try:
                client = pubsub_v1.SubscriberClient()
            except Exception as exc:  # credenciais ausentes etc.; a mensagem original pode citar caminhos
                raise ConfigError(
                    f"não foi possível criar o cliente do Pub/Sub ({type(exc).__name__}). "
                    "Confira GOOGLE_APPLICATION_CREDENTIALS ou rode `gcloud auth application-default login`.",
                    operation="GcpPubSub",
                ) from exc
        self._client: Any = client

    def _path(self, subscription: str, operation: str) -> str:
        """Aceita o nome curto ou `projects/<projeto>/subscriptions/<nome>` (do projeto configurado)."""
        if isinstance(subscription, str):
            if _SHORT_NAME.match(subscription):
                return f"projects/{self._project}/subscriptions/{subscription}"
            full = _FULL_NAME.match(subscription)
            if full and full.group(1) == self._project:
                return subscription
        shown = subscription[:80] if isinstance(subscription, str) else type(subscription).__name__
        raise ConfigError(
            f"assinatura inválida '{shown}'. Use o nome curto (letras, números e - _ . ~ + %) "
            f"ou projects/{self._project}/subscriptions/<nome>.",
            operation=operation,
        )

    def pull(self, subscription: str, max_messages: int, timeout_s: float) -> list[PubSubMessage]:
        path = self._path(subscription, "pubsub.pull")
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
        immediate = timeout_s == 0
        rpc_timeout = _NO_WAIT_RPC_TIMEOUT_S if immediate else float(timeout_s)
        try:
            response = self._client.pull(
                subscription=path, max_messages=max_messages, return_immediately=immediate, timeout=rpc_timeout
            )
        except DataEngError:
            raise
        except Exception as exc:
            if _is_deadline(exc):  # fila vazia até o prazo: não é erro
                return []
            raise PubSubError(_api_failure(exc, f"falha ao puxar de {subscription}"), operation="pubsub.pull") from exc
        return [self._convert(r) for r in response.received_messages]

    @staticmethod
    def _convert(received: Any) -> PubSubMessage:
        msg = received.message
        when = msg.publish_time
        if not isinstance(when, datetime):
            raise PubSubError("mensagem recebida sem publish_time válido.", operation="pubsub.pull")
        when = when.replace(tzinfo=UTC) if when.tzinfo is None else when.astimezone(UTC)
        return PubSubMessage(
            message_id=str(msg.message_id),
            ack_id=str(received.ack_id),
            data=bytes(msg.data),
            attributes={str(k): str(v) for k, v in dict(msg.attributes).items()},
            publish_time=datetime(
                when.year, when.month, when.day, when.hour, when.minute, when.second, when.microsecond, tzinfo=UTC
            ),
        )

    def ack(self, subscription: str, ack_ids: Sequence[str]) -> None:
        path = self._path(subscription, "pubsub.ack")
        for chunk in self._chunks(ack_ids):
            try:
                self._client.acknowledge(subscription=path, ack_ids=chunk, timeout=self._rpc_timeout_s)
            except DataEngError:
                raise
            except Exception as exc:
                raise PubSubError(
                    _api_failure(exc, f"falha ao confirmar mensagens de {subscription}"), operation="pubsub.ack"
                ) from exc

    def nack(self, subscription: str, ack_ids: Sequence[str]) -> None:
        path = self._path(subscription, "pubsub.nack")
        for chunk in self._chunks(ack_ids):
            try:
                self._client.modify_ack_deadline(
                    subscription=path, ack_ids=chunk, ack_deadline_seconds=0, timeout=self._rpc_timeout_s
                )
            except DataEngError:
                raise
            except Exception as exc:
                raise PubSubError(
                    _api_failure(exc, f"falha ao devolver mensagens de {subscription}"), operation="pubsub.nack"
                ) from exc

    def backlog(self, subscription: str) -> int | None:
        self._path(subscription, "pubsub.backlog")
        return None

    @staticmethod
    def _chunks(ack_ids: Sequence[str]) -> list[list[str]]:
        ids = list(ack_ids)
        return [ids[i : i + _ACK_CHUNK] for i in range(0, len(ids), _ACK_CHUNK)]
