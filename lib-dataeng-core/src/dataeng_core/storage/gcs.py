"""Storage no Google Cloud Storage: `gs://<bucket>/<chave>` aponta para o objeto real.

As importações de `google.cloud` são tardias (só no construtor, e só sem `client=`), para o pacote base
continuar funcionando sem o extra `[gcp]`. O parâmetro `client` existe para injeção em testes.
"""

from __future__ import annotations

from typing import Any

from dataeng_core.base.errors import ConfigError, DataEngError, LoadError, SourceError

_SCHEME = "gs://"
_FORMAT = "Use o formato gs://bucket/chave."
_EXTRA_HINT = 'Instale o extra de GCP: pip install "dataeng-core[gcp]".'


def _api_failure(exc: Exception, what: str) -> str:
    """Texto seguro de uma falha da API: só o tipo e o código HTTP, nunca o corpo da resposta."""
    name = type(exc).__name__
    code = getattr(exc, "code", None)
    shown = f"{name} (HTTP {code})" if isinstance(code, int) else name
    if code in (401, 403):
        hint = "sem permissão ou credenciais inválidas: confira a conta de serviço e seus papéis no bucket"
    elif code == 404:
        hint = "bucket ou objeto não encontrado: confira o nome do bucket e o projeto"
    else:
        hint = "verifique a conectividade, a cota e o nome do bucket, e tente novamente"
    return f"{what}: {shown}; {hint}."


class GcsStorage:
    def __init__(self, project: str, client: Any = None) -> None:
        if not isinstance(project, str) or not project.strip():
            raise ConfigError("o projeto GCP do storage está vazio.", operation="GcsStorage")
        self._project = project
        if client is None:
            try:
                import google.cloud.storage as storage
            except ImportError as exc:
                raise ConfigError(
                    f"o pacote google-cloud-storage não está instalado. {_EXTRA_HINT}", operation="GcsStorage"
                ) from exc
            try:
                client = storage.Client(project=project)
            except Exception as exc:  # credenciais ausentes etc.; a mensagem original pode citar caminhos
                raise ConfigError(
                    f"não foi possível criar o cliente do GCS ({type(exc).__name__}). "
                    "Confira GOOGLE_APPLICATION_CREDENTIALS ou rode `gcloud auth application-default login`.",
                    operation="GcsStorage",
                ) from exc
        self._client: Any = client

    # ---- URIs ------------------------------------------------------------------------------

    @staticmethod
    def _invalid(path: str, motivo: str, operation: str) -> ConfigError:
        return ConfigError(f"caminho inválido '{path}': {motivo}. {_FORMAT}", operation=operation)

    def _parse(self, path: str, *, need_key: bool, operation: str) -> tuple[str, str, bool]:
        """Devolve (bucket, chave normalizada, termina em barra) de uma URI `gs://` (mesmas regras do LocalStorage)."""
        if not isinstance(path, str):
            raise self._invalid(str(path)[:80], "esperado texto", operation)
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
        return bucket, "/".join(segments), bool(segments) and key.endswith("/")

    def _blob(self, path: str, operation: str) -> Any:
        bucket, key, _ = self._parse(path, need_key=True, operation=operation)
        return self._client.bucket(bucket).blob(key)

    # ---- operações -------------------------------------------------------------------------

    def read(self, path: str) -> bytes:
        blob = self._blob(path, "storage.read")
        try:
            return bytes(blob.download_as_bytes())
        except DataEngError:
            raise
        except Exception as exc:
            if getattr(exc, "code", None) == 404:
                raise SourceError(
                    f"objeto inexistente: {path}. Confira o caminho e se a etapa anterior gravou o arquivo.",
                    operation="storage.read",
                ) from exc
            raise SourceError(_api_failure(exc, f"falha ao ler {path}"), operation="storage.read") from exc

    def write(self, path: str, data: bytes) -> None:
        blob = self._blob(path, "storage.write")
        try:
            blob.upload_from_string(data)  # o upload do GCS é atômico: o objeto aparece inteiro ou não aparece
        except DataEngError:
            raise
        except Exception as exc:
            raise LoadError(_api_failure(exc, f"falha ao gravar {path}"), operation="storage.write") from exc

    def list(self, prefix: str) -> list[str]:
        """Prefixo é texto puro, como no GCS; devolve só objetos (sem marcadores de pasta), em ordem."""
        bucket, key, trailing = self._parse(prefix, need_key=False, operation="storage.list")
        wanted = key + ("/" if trailing else "")
        try:
            names = [str(b.name) for b in self._client.list_blobs(bucket, prefix=wanted or None)]
        except DataEngError:
            raise
        except Exception as exc:
            raise SourceError(_api_failure(exc, f"falha ao listar {prefix}"), operation="storage.list") from exc
        return sorted(f"{_SCHEME}{bucket}/{n}" for n in names if not n.endswith("/"))

    def exists(self, path: str) -> bool:
        blob = self._blob(path, "storage.exists")
        try:
            return bool(blob.exists())
        except DataEngError:
            raise
        except Exception as exc:
            raise SourceError(_api_failure(exc, f"falha ao consultar {path}"), operation="storage.exists") from exc

    def delete(self, path: str) -> None:
        blob = self._blob(path, "storage.delete")
        try:
            blob.delete()
        except DataEngError:
            raise
        except Exception as exc:
            if getattr(exc, "code", None) == 404:
                return  # idempotente
            raise LoadError(_api_failure(exc, f"falha ao apagar {path}"), operation="storage.delete") from exc
