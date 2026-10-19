"""Camada de armazenamento de objetos."""

from dataeng_core.storage.base import Storage, make_storage
from dataeng_core.storage.local import LocalStorage

__all__ = ["LocalStorage", "Storage", "make_storage"]
