from __future__ import annotations

import base64
import logging
from typing import Any

import pytest
import requests
import responses

import dataeng_core.sources.api as api_mod
from dataeng_core.base.errors import ConfigError, SourceError
from dataeng_core.base.pipeline import ApiOrigem
from dataeng_core.sources.api import fetch_records

URL = "https://api.example.test/items"
SECRET = "S3CR3T"


def _origem(**kw: Any) -> ApiOrigem:
    return ApiOrigem.model_validate({"tipo": "api", "endpoint": URL, **kw})


class Sleeps:
    def __init__(self) -> None:
        self.calls: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


def _params(call: Any) -> dict[str, str]:
    result: dict[str, str] = call.request.params
    return result


def _req_kwargs(call: Any) -> dict[str, Any]:
    result: dict[str, Any] = call.request.req_kwargs
    return result


@pytest.fixture
def sleeps() -> Sleeps:
    return Sleeps()


def _fetch(origem: ApiOrigem, sleeps: Sleeps, env: dict[str, str] | None = None) -> list[dict[str, Any]]:
    return fetch_records(origem, env=env or {}, sleep=sleeps)


@responses.activate
def test_no_pagination_reads_root_list(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, json=[{"id": 1}, {"id": 2}])
    assert _fetch(_origem(), sleeps) == [{"id": 1}, {"id": 2}]
    assert len(responses.calls) == 1


@responses.activate
def test_data_path_nested(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, json={"result": {"rows": [{"id": 1}]}})
    origem = _origem(paginacao={"tipo": "nenhuma", "caminho_dados": "result.rows"})
    assert _fetch(origem, sleeps) == [{"id": 1}]


@responses.activate
def test_page_pagination_until_empty(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, json=[{"id": 1}, {"id": 2}])
    responses.add(responses.GET, URL, json=[{"id": 3}])
    responses.add(responses.GET, URL, json=[])
    origem = _origem(paginacao={"tipo": "pagina", "parametro": "page", "tamanho": 2})
    assert [r["id"] for r in _fetch(origem, sleeps)] == [1, 2, 3]
    queries = [_params(c) for c in responses.calls]
    assert [q["page"] for q in queries] == ["1", "2", "3"]
    assert all(q["page_size"] == "2" for q in queries)


@responses.activate
def test_offset_pagination(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, json=[{"id": 1}, {"id": 2}])
    responses.add(responses.GET, URL, json=[{"id": 3}])
    responses.add(responses.GET, URL, json=[])
    origem = _origem(paginacao={"tipo": "offset", "parametro": "offset", "tamanho": 2})
    assert [r["id"] for r in _fetch(origem, sleeps)] == [1, 2, 3]
    queries = [_params(c) for c in responses.calls]
    assert [q["offset"] for q in queries] == ["0", "2", "3"]
    assert all(q["limit"] == "2" for q in queries)


@responses.activate
def test_cursor_pagination_follows_next_path(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, json={"data": [{"id": 1}], "meta": {"next": "abc"}})
    responses.add(responses.GET, URL, json={"data": [{"id": 2}], "meta": {"next": "def"}})
    responses.add(responses.GET, URL, json={"data": [], "meta": {"next": None}})
    origem = _origem(
        paginacao={"tipo": "cursor", "parametro": "cursor", "caminho_proximo": "meta.next", "caminho_dados": "data"}
    )
    assert [r["id"] for r in _fetch(origem, sleeps)] == [1, 2]
    queries = [_params(c) for c in responses.calls]
    assert "cursor" not in queries[0]
    assert [q["cursor"] for q in queries[1:]] == ["abc", "def"]


@responses.activate
def test_cursor_repeated_raises_instead_of_looping(sleeps: Sleeps) -> None:
    for _ in range(5):
        responses.add(responses.GET, URL, json={"data": [{"id": 1}], "next": "same"})
    origem = _origem(paginacao={"tipo": "cursor", "caminho_proximo": "next", "caminho_dados": "data"})
    with pytest.raises(SourceError, match="cursor repetido"):
        _fetch(origem, sleeps)
    assert len(responses.calls) == 2


@responses.activate
def test_repeated_page_content_raises(sleeps: Sleeps) -> None:
    for _ in range(5):
        responses.add(responses.GET, URL, json=[{"id": 1}])
    origem = _origem(paginacao={"tipo": "pagina"})
    with pytest.raises(SourceError, match="repetid"):
        _fetch(origem, sleeps)
    assert len(responses.calls) == 2


@responses.activate
def test_page_ceiling_raises(sleeps: Sleeps, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api_mod, "MAX_PAGES", 3)
    for i in range(10):
        responses.add(responses.GET, URL, json=[{"id": i}])
    with pytest.raises(SourceError, match="limite de 3 páginas"):
        _fetch(_origem(paginacao={"tipo": "pagina"}), sleeps)
    assert len(responses.calls) == 3


@responses.activate
def test_empty_last_page_and_204(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, json=[{"id": 1}])
    responses.add(responses.GET, URL, status=204)
    assert _fetch(_origem(paginacao={"tipo": "pagina"}), sleeps) == [{"id": 1}]


@responses.activate
def test_first_page_empty_returns_empty_list(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, json=[])
    assert _fetch(_origem(paginacao={"tipo": "pagina"}), sleeps) == []


@responses.activate
def test_bearer_token_read_from_env_var(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, json=[])
    origem = _origem(autenticacao={"tipo": "bearer", "segredo_env": "API_TOKEN"})
    _fetch(origem, sleeps, env={"API_TOKEN": SECRET})
    assert responses.calls[0].request.headers["Authorization"] == f"Bearer {SECRET}"


@responses.activate
def test_custom_header_and_basic_auth(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, json=[])
    origem = _origem(autenticacao={"tipo": "header", "segredo_env": "K", "nome_cabecalho": "X-Api-Key"})
    _fetch(origem, sleeps, env={"K": SECRET})
    assert responses.calls[0].request.headers["X-Api-Key"] == SECRET

    origem = _origem(autenticacao={"tipo": "basic", "segredo_env": "K"})
    _fetch(origem, sleeps, env={"K": "user:pw"})
    expected = "Basic " + base64.b64encode(b"user:pw").decode()
    assert responses.calls[1].request.headers["Authorization"] == expected


def test_missing_secret_env_names_the_variable(sleeps: Sleeps) -> None:
    origem = _origem(autenticacao={"tipo": "bearer", "segredo_env": "API_TOKEN_X"})
    with pytest.raises(ConfigError, match="API_TOKEN_X"):
        _fetch(origem, sleeps, env={"API_TOKEN_X": ""})
    with pytest.raises(ConfigError, match="API_TOKEN_X"):
        _fetch(origem, sleeps, env={})


@responses.activate
def test_rate_limit_sleeps_between_requests(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, json=[{"id": 1}])
    responses.add(responses.GET, URL, json=[{"id": 2}])
    responses.add(responses.GET, URL, json=[])
    origem = _origem(paginacao={"tipo": "pagina"}, limite_taxa=2)
    _fetch(origem, sleeps)
    assert len(sleeps.calls) == 2  # não espera antes da primeira
    assert all(0 < s <= 0.5 for s in sleeps.calls)
    assert max(sleeps.calls) > 0.4


@responses.activate
def test_no_rate_limit_means_no_sleep(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, json=[{"id": 1}])
    responses.add(responses.GET, URL, json=[])
    _fetch(_origem(paginacao={"tipo": "pagina"}), sleeps)
    assert sleeps.calls == []


@responses.activate
def test_retries_on_503_then_succeeds(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, status=503)
    responses.add(responses.GET, URL, status=503)
    responses.add(responses.GET, URL, json=[{"id": 1}])
    assert _fetch(_origem(), sleeps) == [{"id": 1}]
    assert len(responses.calls) == 3
    assert sleeps.calls == [0.5, 1.0]


@responses.activate
def test_503_exhausts_retries_and_raises_source_error(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, status=503)
    with pytest.raises(SourceError, match="503"):
        _fetch(_origem(), sleeps)
    assert len(responses.calls) == 4


@responses.activate
def test_429_honors_retry_after(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, status=429, headers={"Retry-After": "7"})
    responses.add(responses.GET, URL, json=[{"id": 1}])
    assert _fetch(_origem(), sleeps) == [{"id": 1}]
    assert len(responses.calls) == 2
    assert sleeps.calls == [7.0]


@responses.activate
def test_429_with_garbage_retry_after_uses_backoff(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, status=429, headers={"Retry-After": "amanha"})
    responses.add(responses.GET, URL, json=[])
    _fetch(_origem(), sleeps)
    assert sleeps.calls == [0.5]


@responses.activate
@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_4xx_is_not_retried_and_raises_source_error(status: int, sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, status=status, body="nope")
    with pytest.raises(SourceError, match=str(status)):
        _fetch(_origem(), sleeps)
    assert len(responses.calls) == 1
    assert sleeps.calls == []


@responses.activate
def test_network_error_is_retried_then_source_error(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, body=requests.ConnectionError("boom"))
    with pytest.raises(SourceError, match="ConnectionError"):
        _fetch(_origem(), sleeps)
    assert len(responses.calls) == 4


@responses.activate
def test_timeout_is_set_on_every_request(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, json=[{"id": 1}])
    responses.add(responses.GET, URL, json=[])
    _fetch(_origem(paginacao={"tipo": "pagina"}), sleeps)
    assert all(_req_kwargs(c)["timeout"] == api_mod.TIMEOUT_S for c in responses.calls)


@responses.activate
@pytest.mark.parametrize(
    "body,content_type",
    [("", "application/json"), ("<html>", "text/html"), ("{nao json", "application/json")],
)
def test_empty_or_invalid_json_raises_source_error(body: str, content_type: str, sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, body=body, content_type=content_type)
    with pytest.raises(SourceError, match="JSON"):
        _fetch(_origem(), sleeps)


@responses.activate
@pytest.mark.parametrize(
    "payload,paginacao",
    [
        ({"a": 1}, {"tipo": "nenhuma"}),  # raiz objeto sem caminho_dados
        ({"a": 1}, {"tipo": "nenhuma", "caminho_dados": "data"}),  # caminho ausente
        ({"data": {"x": 1}}, {"tipo": "nenhuma", "caminho_dados": "data"}),  # não é lista
        ([1, 2], {"tipo": "nenhuma"}),  # itens não são objetos
    ],
)
def test_unexpected_shape_raises_source_error(payload: Any, paginacao: dict[str, Any], sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, json=payload)
    with pytest.raises(SourceError):
        _fetch(_origem(paginacao=paginacao), sleeps)


@responses.activate
def test_secret_never_in_error_message_or_log(caplog: pytest.LogCaptureFixture, sleeps: Sleeps) -> None:
    logger = logging.getLogger("dataeng_core.sources.api")
    logger.addHandler(caplog.handler)
    caplog.set_level(logging.DEBUG)
    try:
        url = f"https://user:{SECRET}@api.example.test/items?api_key={SECRET}"
        cases: list[tuple[Exception | None, tuple[int, str] | None]] = [
            (requests.ConnectionError(f"falha em {url} token {SECRET}"), None),
            (None, (401, f"invalid token {SECRET}")),
            (None, (500, f"erro interno {SECRET}")),
            (None, (200, "{" + SECRET)),
        ]
        for exc, status_body in cases:
            responses.reset()
            if exc is not None:
                responses.add(responses.GET, URL, body=exc)
            else:
                assert status_body is not None
                responses.add(responses.GET, URL, status=status_body[0], body=status_body[1])
            origem = _origem(
                endpoint=f"{URL}?api_key={SECRET}",
                autenticacao={"tipo": "bearer", "segredo_env": "T"},
            )
            with pytest.raises(SourceError) as info:
                _fetch(origem, sleeps, env={"T": SECRET})
            err = info.value
            assert SECRET not in str(err)
            assert SECRET not in repr(err)
            assert err.__cause__ is None and err.__context__ is None
    finally:
        logger.removeHandler(caplog.handler)
    assert caplog.records, "esperava logs de aviso/retry"
    assert SECRET not in caplog.text
    assert all(SECRET not in r.getMessage() for r in caplog.records)


@responses.activate
def test_basic_auth_password_and_encoded_form_redacted(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, status=401, body="bad pw hunter2 / cred dXNlcjpodW50ZXIy")
    origem = _origem(autenticacao={"tipo": "basic", "segredo_env": "K"})
    with pytest.raises(SourceError) as info:
        _fetch(origem, sleeps, env={"K": "user:hunter2"})
    assert "hunter2" not in str(info.value)
    assert "dXNlcjpodW50ZXIy" not in str(info.value)


@responses.activate
def test_query_string_of_endpoint_not_in_messages(sleeps: Sleeps) -> None:
    responses.add(responses.GET, URL, status=404)
    with pytest.raises(SourceError) as info:
        _fetch(_origem(endpoint=f"{URL}?key=abc123"), sleeps)
    assert "abc123" not in str(info.value)
    assert "api.example.test/items" in str(info.value)
