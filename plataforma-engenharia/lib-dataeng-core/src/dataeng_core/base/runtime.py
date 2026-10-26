"""Utilidades de execução: run_id, retry, logging JSON e redação de segredos."""

from __future__ import annotations

import json
import logging
import secrets
import sys
import time
from collections.abc import Callable, Iterable
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import TextIO, TypeVar

T = TypeVar("T")

run_id_var: ContextVar[str | None] = ContextVar("run_id", default=None)


def new_run_id() -> str:
    """Gera `AAAAMMDDTHHMMSS-<6 hex>` (UTC)."""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    return f"{stamp}-{secrets.token_hex(3)}"


def retry(
    fn: Callable[[], T],
    *,
    attempts: int = 3,
    base_delay: float = 0.5,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Executa `fn` até `attempts` vezes, com espera `base_delay * 2**tentativa`."""
    if attempts < 1:
        raise ValueError("attempts deve ser >= 1")
    attempt = 0
    while True:
        try:
            return fn()
        except retry_on:
            if attempt >= attempts - 1:
                raise
            sleep(base_delay * 2**attempt)
            attempt += 1


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, str] = {
            "time": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        run_id = run_id_var.get()
        if run_id:
            payload["run_id"] = run_id
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


class _StderrHandler(logging.StreamHandler):  # type: ignore[type-arg]
    """Handler que resolve `sys.stderr` a cada emissão."""

    _dataeng = True

    def __init__(self) -> None:
        super().__init__(sys.stderr)

    @property
    def stream(self) -> TextIO:  # type: ignore[override]
        return sys.stderr

    @stream.setter
    def stream(self, value: TextIO) -> None:
        pass


def get_logger(name: str) -> logging.Logger:
    """Logger que escreve linhas JSON em stderr, com `run_id` quando definido."""
    logger = logging.getLogger(name)
    if not any(getattr(h, "_dataeng", False) for h in logger.handlers):
        handler = _StderrHandler()
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def redact(text: str, secrets: Iterable[str]) -> str:
    """Troca cada segredo (não vazio) por `***`."""
    for secret in sorted({s for s in secrets if s}, key=len, reverse=True):
        text = text.replace(secret, "***")
    return text
