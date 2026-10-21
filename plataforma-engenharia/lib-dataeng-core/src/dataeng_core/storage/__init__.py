"""Camada de armazenamento de objetos."""

from dataeng_core.storage.base import Storage, make_storage
from dataeng_core.storage.gcs import GcsStorage
from dataeng_core.storage.local import LocalStorage

__all__ = ["GcsStorage", "LocalStorage", "Storage", "make_storage"]
