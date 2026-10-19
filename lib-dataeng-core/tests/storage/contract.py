"""Bateria de contrato reutilizável para qualquer implementação de Storage."""

from __future__ import annotations

import pytest

from dataeng_core.base.errors import SourceError
from dataeng_core.storage.base import Storage


def run_storage_contract(storage: Storage) -> None:
    base = "gs://bucket-teste/bronze/vendas"
    a = f"{base}/a.csv"
    b = f"{base}/b.csv"
    sub = f"{base}/sub/c.csv"

    # escrever e ler
    assert storage.exists(a) is False
    storage.write(a, b"id\n1\n")
    assert storage.exists(a) is True
    assert storage.read(a) == b"id\n1\n"

    # sobrescrever
    storage.write(a, b"novo")
    assert storage.read(a) == b"novo"

    # list por prefixo: em ordem, sem pastas
    storage.write(b, b"b")
    storage.write(sub, b"c")
    storage.write("gs://bucket-teste/outro/x.csv", b"x")
    assert storage.list(f"{base}/") == [a, b, sub]
    assert storage.list("gs://bucket-teste/outro/") == ["gs://bucket-teste/outro/x.csv"]
    assert storage.list("gs://bucket-teste/vazio/") == []

    # delete idempotente
    storage.delete(b)
    assert storage.exists(b) is False
    storage.delete(b)

    # ler inexistente levanta SourceError
    with pytest.raises(SourceError):
        storage.read(b)

    # barra invertida e barra normal são equivalentes
    mixed = "gs://bucket-teste" + chr(92) + "bronze" + chr(92) + "vendas" + chr(92) + "a.csv"
    assert storage.exists(mixed) is True
    assert storage.read(mixed) == b"novo"
