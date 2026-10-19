"""Bateria de contrato reutilizável para qualquer implementação de Storage."""

from __future__ import annotations

import pytest

from dataeng_core.base.errors import ConfigError, SourceError
from dataeng_core.storage.base import Storage


def run_storage_contract(storage: Storage, bucket: str = "bucket-teste", prefix: str = "contrato") -> None:
    """Executa o contrato sob `gs://<bucket>/<prefix>/` e remove o que criou."""
    root = f"gs://{bucket}/{prefix}"
    a = f"{root}/vendas/a.csv"
    b = f"{root}/vendas/b.csv"
    sub = f"{root}/vendas/sub/c.csv"
    sibling_dir = f"{root}/vendas2/x.csv"
    sibling_file = f"{root}/vendas_old.csv"
    outro = f"{root}/outro/x.csv"
    created = [a, b, sub, sibling_dir, sibling_file, outro]
    try:
        # escrever e ler
        assert storage.exists(a) is False
        storage.write(a, b"id\n1\n")
        assert storage.exists(a) is True
        assert storage.read(a) == b"id\n1\n"

        # sobrescrever
        storage.write(a, b"novo")
        assert storage.read(a) == b"novo"

        for obj in (b, sub, sibling_dir, sibling_file, outro):
            storage.write(obj, b"x")

        # list com barra final: só aquele "diretório", em ordem, só arquivos
        assert storage.list(f"{root}/vendas/") == [a, b, sub]
        assert storage.list(f"{root}/outro/") == [outro]
        assert storage.list(f"{root}/vazio/") == []

        # list sem barra: prefixo é texto puro (semântica do GCS)
        assert storage.list(f"{root}/vendas") == [a, b, sub, sibling_dir, sibling_file]
        assert storage.list(f"{root}/vend") == [a, b, sub, sibling_dir, sibling_file]
        assert storage.list(f"{root}/vendas_") == [sibling_file]
        assert storage.list(f"{root}/vendas/s") == [sub]

        # bucket inteiro
        assert set(created) <= set(storage.list(f"gs://{bucket}"))
        assert set(created) <= set(storage.list(f"gs://{bucket}/"))

        # delete idempotente
        storage.delete(b)
        assert storage.exists(b) is False
        storage.delete(b)

        # ler inexistente levanta SourceError
        with pytest.raises(SourceError):
            storage.read(b)

        # barra invertida e barra normal são equivalentes
        mixed = f"gs://{bucket}" + chr(92) + prefix + chr(92) + "vendas" + chr(92) + "a.csv"
        assert storage.exists(mixed) is True
        assert storage.read(mixed) == b"novo"

        # URIs inválidas
        for bad in ("gs:///x", "gs://", "gs://b", "s3://b/x", "bucket/chave", ""):
            with pytest.raises(ConfigError):
                storage.read(bad)
        for bad in ("gs://", "", "bucket/chave", "s3://b/x"):
            with pytest.raises(ConfigError):
                storage.list(bad)
    finally:
        for obj in created:
            storage.delete(obj)
