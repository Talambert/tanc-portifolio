"""Storage em disco: `gs://<bucket>/<chave>` vira `<raiz>/<bucket>/<chave>`."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from dataeng_core.base.errors import ConfigError, SourceError

_SCHEME = "gs://"
_FORMAT = "Use o formato gs://bucket/chave."


class LocalStorage:
    def __init__(self, root: Path) -> None:
        self._root = Path(root).resolve()

    def _invalid(self, path: str, motivo: str, operation: str) -> ConfigError:
        return ConfigError(f"caminho inválido '{path}': {motivo}. {_FORMAT}", operation=operation)

    def _parse(self, path: str, *, need_key: bool, operation: str) -> tuple[str, list[str], str]:
        """Devolve (bucket, segmentos da chave, chave textual) de uma URI `gs://`."""
        text = path.replace("\\", "/")
        if not text.startswith(_SCHEME):
            raise self._invalid(path, "esquema gs:// ausente", operation)
        bucket, _, key = text[len(_SCHEME) :].partition("/")
        if not bucket:
            raise self._invalid(path, "bucket vazio", operation)
        if need_key and not key.strip("/"):
            raise self._invalid(path, "chave vazia", operation)
        segments = [s for s in key.split("/") if s]
        for part in (bucket, *segments):
            if part in (".", "..") or ":" in part:
                raise self._invalid(path, "o caminho sai da raiz do armazenamento", operation)
        return bucket, segments, key

    def _dir(self, bucket: str, segments: list[str], path: str, operation: str) -> Path:
        base = (self._root / bucket).resolve()
        target = base.joinpath(*segments).resolve()
        if base.parent != self._root or (target != base and base not in target.parents):
            raise self._invalid(path, "o caminho sai da raiz do armazenamento", operation)
        return target

    def _resolve(self, path: str, operation: str) -> Path:
        bucket, segments, _ = self._parse(path, need_key=True, operation=operation)
        return self._dir(bucket, segments, path, operation)

    def read(self, path: str) -> bytes:
        target = self._resolve(path, "storage.read")
        if not target.is_file():
            raise SourceError(
                f"objeto inexistente: {path}. Confira o caminho e se a etapa anterior gravou o arquivo.",
                operation="storage.read",
            )
        return target.read_bytes()

    def write(self, path: str, data: bytes) -> None:
        target = self._resolve(path, "storage.write")
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=target.parent, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
            os.replace(tmp_name, target)
        except BaseException:
            Path(tmp_name).unlink(missing_ok=True)
            raise

    def list(self, prefix: str) -> list[str]:
        """Prefixo é texto puro, como no GCS: `a/vend` casa `a/vendas`, `a/vendas2/x`."""
        bucket, segments, key = self._parse(prefix, need_key=False, operation="storage.list")
        # Diretório a varrer: a própria pasta se termina em `/`; senão, a pasta pai.
        scan = segments if (not segments or key.endswith("/")) else segments[:-1]
        start = self._dir(bucket, scan, prefix, "storage.list")
        if not start.is_dir():
            return []
        bucket_dir = self._root / bucket
        wanted = "/".join(segments) + ("/" if segments and key.endswith("/") else "")
        found = []
        for p in start.rglob("*"):
            if p.is_file():
                rel = p.relative_to(bucket_dir).as_posix()
                if rel.startswith(wanted):
                    found.append(f"{_SCHEME}{bucket}/{rel}")
        return sorted(found)

    def exists(self, path: str) -> bool:
        return self._resolve(path, "storage.exists").is_file()

    def delete(self, path: str) -> None:
        self._resolve(path, "storage.delete").unlink(missing_ok=True)
