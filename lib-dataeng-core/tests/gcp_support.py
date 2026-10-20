"""Apoio aos testes de GCP: pular sem credenciais e simular a ausência do extra [gcp]."""

from __future__ import annotations

import importlib.util
import os
import sys

import pytest

_REQUIRED_ENV = ("GOOGLE_APPLICATION_CREDENTIALS", "DATAENG_GCP_PROJECT")


def _has_google() -> bool:
    try:
        return importlib.util.find_spec("google.cloud.bigquery") is not None
    except ModuleNotFoundError:
        return False


needs_google = pytest.mark.skipif(not _has_google(), reason="extra [gcp] (google-cloud-*) não instalado")

_missing = [name for name in _REQUIRED_ENV if not os.environ.get(name)]
needs_gcp_credentials = pytest.mark.skipif(
    bool(_missing), reason="sem credenciais GCP: defina " + " e ".join(_REQUIRED_ENV) + " para rodar"
)


def hide_google(monkeypatch: pytest.MonkeyPatch) -> None:
    """Faz `import google...` falhar mesmo com o extra instalado (ruling R13)."""
    for name in ("google", "google.cloud", "google.cloud.storage", "google.cloud.bigquery"):
        monkeypatch.setitem(sys.modules, name, None)
