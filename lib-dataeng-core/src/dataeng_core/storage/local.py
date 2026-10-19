"""Storage em disco: `gs://<bucket>/<chave>` vira `<raiz>/<bucket>/<chave>`."""

from __future__ import annotations

from pathlib import Path

from dataeng_core.base.errors import ConfigError, SourceError

_SCHEME = "gs://"


class LocalStorage:
    def __init__(self, root: Path) -> None:
        self._root = Path(root).resolve()

    def _relative(self, path: str) -> str:
        """Chave relativa à raiz, com `/` como separador."""
        return path.replace("\\", "/").removeprefix(_SCHEME).lstrip("/")

    def _resolve(self, path: str) -> Path:
        resolved = (self._root / self._relative(path)).resolve()
        if resolved != self._root and self._root not in resolved.parents:
            raise ConfigError(f"o caminho '{path}' sai da raiz do armazenamento local", operation="storage")
        return resolved

    def read(self, path: str) -> bytes:
        target = self._resolve(path)
        if not target.is_file():
            raise SourceError(
                f"objeto inexistente: {path}. Confira o caminho e se a etapa anterior gravou o arquivo.",
                operation="storage.read",
            )
        return target.read_bytes()

    def write(self, path: str, data: bytes) -> None:
        target = self._resolve(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)

    def list(self, prefix: str) -> list[str]:
        base = self._resolve(prefix)
        wanted = self._relative(prefix)
        scheme = _SCHEME if prefix.replace("\\", "/").startswith(_SCHEME) else ""
        start = base if base.is_dir() else base.parent
        if not start.is_dir():
            return []
        keys = (p.relative_to(self._root).as_posix() for p in start.rglob("*") if p.is_file())
        return sorted(f"{scheme}{key}" for key in keys if key.startswith(wanted))

    def exists(self, path: str) -> bool:
        return self._resolve(path).is_file()

    def delete(self, path: str) -> None:
        self._resolve(path).unlink(missing_ok=True)
