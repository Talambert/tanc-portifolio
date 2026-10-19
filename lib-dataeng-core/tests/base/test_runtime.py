import json
import logging
import re

import pytest

from dataeng_core.base import runtime


def test_retry_succeeds_after_transient_failures() -> None:
    calls = {"n": 0}
    sleeps: list[float] = []

    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("falha transitória")
        return "ok"

    assert runtime.retry(flaky, sleep=sleeps.append) == "ok"
    assert sleeps == [0.5, 1.0]


def test_retry_raises_after_attempts() -> None:
    calls = {"n": 0}
    sleeps: list[float] = []

    def always() -> None:
        calls["n"] += 1
        raise ValueError("sempre")

    with pytest.raises(ValueError):
        runtime.retry(always, attempts=3, sleep=sleeps.append)
    assert calls["n"] == 3
    assert sleeps == [0.5, 1.0]


def test_retry_only_retries_listed_exceptions() -> None:
    calls = {"n": 0}

    def boom() -> None:
        calls["n"] += 1
        raise KeyError("x")

    with pytest.raises(KeyError):
        runtime.retry(boom, retry_on=(ValueError,), sleep=lambda _s: None)
    assert calls["n"] == 1


def test_run_id_format_and_uniqueness() -> None:
    ids = {runtime.new_run_id() for _ in range(50)}
    assert len(ids) == 50
    assert all(re.fullmatch(r"\d{8}T\d{6}-[0-9a-f]{6}", i) for i in ids)


def test_redact_hides_every_secret_and_ignores_empty() -> None:
    assert runtime.redact("t=abc", ["abc", ""]) == "t=***"
    assert runtime.redact("a b a", ["a", "b"]) == "*** *** ***"


def test_logger_emits_json_with_run_id(capsys: pytest.CaptureFixture[str]) -> None:
    log = runtime.get_logger("dataeng_core.teste")
    token = runtime.run_id_var.set("RID1")
    try:
        log.info("olá")
    finally:
        runtime.run_id_var.reset(token)
    line = capsys.readouterr().err.strip().splitlines()[-1]
    data = json.loads(line)
    assert data["message"] == "olá"
    assert data["run_id"] == "RID1"
    assert data["level"] == "INFO"
    assert isinstance(log, logging.Logger)
