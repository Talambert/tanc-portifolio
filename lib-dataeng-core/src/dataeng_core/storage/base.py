"""Interface de armazenamento de objetos e fábrica por ambiente."""

from __future__ import annotations

from typing import Protocol

from dataeng_core.base.params import Environment


class Storage(Protocol):
    """Armazenamento de objetos endereçado por `gs://<bucket>/<chave>`."""

    def read(self, path: str) -> bytes: ...

    def write(self, path: str, data: bytes) -> None: ...

    def list(self, prefix: str) -> list[str]: ...

    def exists(self, path: str) -> bool: ...

    def delete(self, path: str) -> None: ...


def make_storage(env: Environment) -> Storage:
    """Local quando `env.modo == "local"`; GCS (Tarefa 14) quando `gcp`."""
    if env.modo == "local":
        from pathlib import Path

        from dataeng_core.storage.local import LocalStorage

        return LocalStorage(Path(env.pasta_local))
    raise NotImplementedError("Storage do modo gcp (GcsStorage) será adicionado na Tarefa 14.")
