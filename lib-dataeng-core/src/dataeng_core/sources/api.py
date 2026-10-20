"""Origem API: leitura paginada com autenticação, limite de taxa e retry.

Segredos nunca saem desta camada: todo texto de erro e de log passa por `redact`
e as URLs são registradas sem credenciais nem query string.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import time
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlsplit

import requests

from dataeng_core.base.errors import ConfigError, SourceError
from dataeng_core.base.pipeline import ApiOrigem
from dataeng_core.base.runtime import get_logger, redact, retry

log = get_logger(__name__)

TIMEOUT_S = 30.0
MAX_PAGES = 10_000
MAX_ATTEMPTS = 4
BASE_DELAY_S = 0.5
MAX_RETRY_AFTER_S = 300.0
_BODY_SNIPPET = 200

_PAGE_DEFAULT = "page"
_PAGE_SIZE_PARAM = "page_size"
_OFFSET_DEFAULT = "offset"
_LIMIT_PARAM = "limit"
_CURSOR_DEFAULT = "cursor"


class _Transient(Exception):
    """Falha que vale nova tentativa (rede, 5xx, 429). Texto já sem segredos."""

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


def _safe_url(url: str) -> str:
    """URL sem credenciais embutidas nem query string."""
    parts = urlsplit(url)
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    return f"{parts.scheme}://{host}{parts.path}"


def _resolve_auth(origem: ApiOrigem, env: Mapping[str, str]) -> tuple[dict[str, str], list[str]]:
    """Devolve (cabeçalhos, lista de textos sensíveis para `redact`)."""
    auth = origem.autenticacao
    if auth.tipo == "none":
        return {}, []
    assert auth.segredo_env is not None
    secret = env.get(auth.segredo_env, "")
    if not secret:
        raise ConfigError(
            f"a variável de ambiente '{auth.segredo_env}' não está definida ou está vazia; "
            f"defina-a com o segredo da API (autenticação '{auth.tipo}')",
            operation="api.auth",
        )
    sensitive = [secret]
    if auth.tipo == "bearer":
        return {"Authorization": f"Bearer {secret}"}, sensitive
    if auth.tipo == "header":
        assert auth.nome_cabecalho is not None
        return {auth.nome_cabecalho: secret}, sensitive
    encoded = base64.b64encode(secret.encode("utf-8")).decode("ascii")
    sensitive.append(encoded)
    if ":" in secret:
        sensitive.append(secret.split(":", 1)[1])  # a senha isolada
    return {"Authorization": f"Basic {encoded}"}, sensitive


def _dig(payload: Any, path: str) -> Any:
    """Segue um caminho com pontos (`a.b.0.c`); `KeyError` se não existir."""
    current = payload
    for key in path.split("."):
        if isinstance(current, dict):
            current = current[key]
        elif isinstance(current, list) and key.isdigit():
            current = current[int(key)]
        else:
            raise KeyError(key)
    return current


def _retry_after_seconds(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        return None
    if seconds < 0 or seconds != seconds:
        return None
    return min(seconds, MAX_RETRY_AFTER_S)


def fetch_records(
    origem: ApiOrigem,
    *,
    session: requests.Session | None = None,
    env: Mapping[str, str] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> list[dict[str, Any]]:
    """Lê todos os registros da API, seguindo a paginação configurada."""
    env = os.environ if env is None else env
    headers, sensitive = _resolve_auth(origem, env)
    http = session or requests.Session()
    safe = _safe_url(origem.endpoint)
    pag = origem.paginacao
    min_interval = 1.0 / origem.limite_taxa if origem.limite_taxa else 0.0
    last_request: list[float] = []  # início da última requisição (monotonic)

    def clean(text: str) -> str:
        return redact(text, sensitive)

    def fail(message: str) -> SourceError:
        return SourceError(clean(message), operation=f"api {safe}")

    def throttle() -> None:
        if min_interval and last_request:
            wait = min_interval - (time.monotonic() - last_request[0])
            if wait > 0:
                sleep(wait)
        last_request[:] = [time.monotonic()]

    def request_once(params: dict[str, str]) -> Any:
        """Uma tentativa. Devolve o JSON (ou None em 204); levanta `_Transient`/`SourceError`.

        Os `raise` ficam fora dos blocos `except` para não reter a exceção original
        (cuja mensagem pode conter URL ou cabeçalhos) em `__context__`.
        """
        throttle()
        problem: str | None = None
        transient = False
        response: requests.Response | None = None
        try:
            response = http.get(origem.endpoint, params=params or None, headers=headers, timeout=TIMEOUT_S)
        except (requests.ConnectionError, requests.Timeout) as exc:
            problem, transient = f"erro de rede ({type(exc).__name__}) ao acessar {safe}", True
        except requests.RequestException as exc:
            problem = f"requisição inválida ({type(exc).__name__}) para {safe}"
        if response is None:
            assert problem is not None
            if transient:
                log.warning(clean(problem))
                raise _Transient(clean(problem))
            raise fail(problem)

        status = response.status_code
        snippet = clean(response.text[:_BODY_SNIPPET]) if status >= 400 else ""
        if status == 429 or status >= 500:
            message = f"HTTP {status} em {safe}" + (f": {snippet}" if snippet else "")
            log.warning(clean(message))
            raise _Transient(clean(message), _retry_after_seconds(response.headers.get("Retry-After")))
        if status >= 400:
            raise fail(
                f"HTTP {status} em {safe}"
                + (f": {snippet}" if snippet else "")
                + "; verifique endpoint, parâmetros e credenciais"
            )
        if status == 204:
            return None
        invalid = False
        data: Any = None
        try:
            data = response.json()
        except ValueError:
            invalid = True
        if invalid:
            raise fail(f"a resposta de {safe} não é JSON válido (HTTP {status}, {len(response.content)} bytes)")
        return data

    def get_page(params: dict[str, str]) -> Any:
        pending: list[float] = []

        def attempt() -> Any:
            try:
                return request_once(params)
            except _Transient as exc:
                pending[:] = [exc.retry_after] if exc.retry_after is not None else []
                raise

        def wait(delay: float) -> None:
            sleep(pending[0] if pending else delay)

        failure: str | None = None
        try:
            return retry(attempt, attempts=MAX_ATTEMPTS, base_delay=BASE_DELAY_S, retry_on=(_Transient,), sleep=wait)
        except _Transient as exc:
            failure = str(exc)
        raise fail(f"falha após {MAX_ATTEMPTS} tentativas: {failure}")

    def extract(payload: Any) -> list[dict[str, Any]]:
        if payload is None:
            return []
        data = payload
        if pag.caminho_dados:
            missing = False
            try:
                data = _dig(payload, pag.caminho_dados)
            except (KeyError, IndexError):
                missing = True
            if missing:
                raise fail(f"caminho_dados '{pag.caminho_dados}' não existe na resposta de {safe}")
        if data is None:
            return []
        if not isinstance(data, list):
            hint = "" if pag.caminho_dados else "; informe `paginacao.caminho_dados`"
            raise fail(f"esperava uma lista de registros em {safe}, veio {type(data).__name__}{hint}")
        if not all(isinstance(item, dict) for item in data):
            raise fail(f"os itens da resposta de {safe} devem ser objetos JSON")
        return data

    records: list[dict[str, Any]] = []
    params: dict[str, str] = {}
    page_number = 1
    offset = 0
    cursors_seen: set[str] = set()
    pages_seen: set[str] = set()
    pages = 0

    while True:
        if pages >= MAX_PAGES:
            raise fail(f"limite de {MAX_PAGES} páginas atingido sem fim da paginação; interrompido para evitar loop")
        if pag.tipo == "pagina":
            params = {pag.parametro or _PAGE_DEFAULT: str(page_number)}
            if pag.tamanho:
                params[_PAGE_SIZE_PARAM] = str(pag.tamanho)
        elif pag.tipo == "offset":
            params = {pag.parametro or _OFFSET_DEFAULT: str(offset)}
            if pag.tamanho:
                params[_LIMIT_PARAM] = str(pag.tamanho)

        payload = get_page(params)
        pages += 1
        batch = extract(payload)
        log.info(clean(f"página {pages} de {safe}: {len(batch)} registros"))
        if not batch and pag.tipo != "cursor":
            break
        if pag.tipo in ("pagina", "offset"):
            digest = hashlib.sha256(json.dumps(batch, sort_keys=True, default=str).encode()).hexdigest()
            if digest in pages_seen:
                raise fail(f"página repetida em {safe}: a API ignora o parâmetro de paginação; interrompido")
            pages_seen.add(digest)
        records.extend(batch)

        if pag.tipo == "nenhuma":
            break
        if pag.tipo == "pagina":
            page_number += 1
        elif pag.tipo == "offset":
            offset += len(batch)
        else:
            assert pag.caminho_proximo is not None
            nxt: Any = None
            if payload is not None:
                try:
                    nxt = _dig(payload, pag.caminho_proximo)
                except (KeyError, IndexError):
                    nxt = None
            if nxt is None or nxt == "":
                break
            cursor = str(nxt)
            if cursor in cursors_seen:
                raise fail(f"cursor repetido '{cursor}' em {safe}: a paginação não avança; interrompido")
            cursors_seen.add(cursor)
            params = {pag.parametro or _CURSOR_DEFAULT: cursor}
    return records
