"""GcsStorage: testes unitários com cliente falso (sempre) e contrato real (só com credenciais GCP)."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from typing import Any

import pytest
from tests.gcp_support import hide_google, needs_gcp_credentials, needs_google
from tests.storage.contract import run_storage_contract

from dataeng_core.base.errors import ConfigError, LoadError, SourceError
from dataeng_core.base.params import Environment
from dataeng_core.storage import make_storage

CANARY = "SEGREDO-NO-CORPO-DA-RESPOSTA"


def _exc() -> Any:
    from google.api_core import exceptions

    return exceptions


class FakeBlob:
    def __init__(self, client: FakeClient, bucket: str, name: str) -> None:
        self.client, self.bucket, self.name = client, bucket, name

    def _fail(self) -> None:
        if self.client.fail_with is not None:
            raise self.client.fail_with

    def download_as_bytes(self) -> bytes:
        self._fail()
        if (self.bucket, self.name) not in self.client.objects:
            raise _exc().NotFound("404 GET ...: " + CANARY)
        return self.client.objects[(self.bucket, self.name)]

    def upload_from_string(self, data: bytes) -> None:
        self._fail()
        self.client.objects[(self.bucket, self.name)] = data

    def exists(self) -> bool:
        self._fail()
        return (self.bucket, self.name) in self.client.objects

    def delete(self) -> None:
        self._fail()
        if (self.bucket, self.name) not in self.client.objects:
            raise _exc().NotFound("404 DELETE")
        del self.client.objects[(self.bucket, self.name)]


class FakeBucket:
    def __init__(self, client: FakeClient, name: str) -> None:
        self.client, self.name = client, name

    def blob(self, key: str) -> FakeBlob:
        self.client.blob_calls.append((self.name, key))
        return FakeBlob(self.client, self.name, key)


class FakeListed:
    def __init__(self, name: str) -> None:
        self.name = name


class FakeClient:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.blob_calls: list[tuple[str, str]] = []
        self.list_calls: list[tuple[str, str | None]] = []
        self.fail_with: Exception | None = None

    def bucket(self, name: str) -> FakeBucket:
        return FakeBucket(self, name)

    def list_blobs(self, bucket: str, prefix: str | None = None) -> Iterator[FakeListed]:
        self.list_calls.append((bucket, prefix))
        if self.fail_with is not None:
            raise self.fail_with
        # ordem propositalmente invertida: o GcsStorage precisa ordenar
        for (b, key), _ in reversed(list(self.objects.items())):
            if b == bucket and key.startswith(prefix or ""):
                yield FakeListed(key)


@pytest.fixture
def client() -> FakeClient:
    return FakeClient()


@pytest.fixture
def storage(client: FakeClient) -> Any:
    from dataeng_core.storage.gcs import GcsStorage

    return GcsStorage("meu-projeto", client=client)


@needs_google
def test_gcs_storage_passes_contract_with_fake_client(storage: Any) -> None:
    run_storage_contract(storage)


@needs_google
def test_uri_maps_to_bucket_and_blob(storage: Any, client: FakeClient) -> None:
    storage.write("gs://bkt/bronze/x/a.csv", b"1")
    assert client.objects == {("bkt", "bronze/x/a.csv"): b"1"}
    assert storage.read("gs://bkt/bronze/x/a.csv") == b"1"
    # barra invertida e barras repetidas normalizam para a mesma chave
    assert storage.exists("gs://bkt" + chr(92) + "bronze//x" + chr(92) + "a.csv") is True
    assert ("bkt", "bronze/x/a.csv") in client.blob_calls


@needs_google
def test_list_uses_plain_text_prefix_only_objects_sorted(storage: Any, client: FakeClient) -> None:
    for key in ("p/b.csv", "p/a.csv", "p/dir/", "p/dir/c.csv", "pp/z.csv", "outro/x"):
        client.objects[("bkt", key)] = b"x"
    assert storage.list("gs://bkt/p/") == ["gs://bkt/p/a.csv", "gs://bkt/p/b.csv", "gs://bkt/p/dir/c.csv"]
    assert client.list_calls[-1] == ("bkt", "p/")
    assert storage.list("gs://bkt/p") == [
        "gs://bkt/p/a.csv",
        "gs://bkt/p/b.csv",
        "gs://bkt/p/dir/c.csv",
        "gs://bkt/pp/z.csv",
    ]
    assert client.list_calls[-1] == ("bkt", "p")
    storage.list("gs://bkt")
    assert client.list_calls[-1] == ("bkt", None)
    assert storage.list("gs://bkt/vazio/") == []


@needs_google
def test_read_missing_is_actionable_source_error_without_response_body(storage: Any) -> None:
    with pytest.raises(SourceError, match="gs://bkt/nao_existe.csv") as info:
        storage.read("gs://bkt/nao_existe.csv")
    assert CANARY not in str(info.value)


@needs_google
def test_delete_is_idempotent(storage: Any, client: FakeClient) -> None:
    client.objects[("bkt", "x")] = b"1"
    storage.delete("gs://bkt/x")
    storage.delete("gs://bkt/x")
    assert client.objects == {}


@needs_google
@pytest.mark.parametrize(
    "bad",
    ["gs:///x", "gs://", "gs://b", "s3://b/x", "bucket/chave", "", "gs://b/../x", "gs://../x", "gs://b/./x"],
)
def test_invalid_uri_rejected_before_any_call(storage: Any, client: FakeClient, bad: str) -> None:
    for op in (storage.read, storage.exists, storage.delete):
        with pytest.raises(ConfigError, match=r"gs://bucket/chave"):
            op(bad)
    with pytest.raises(ConfigError):
        storage.write(bad, b"x")
    assert client.blob_calls == []


@needs_google
@pytest.mark.parametrize("bad", ["gs://", "", "bucket/chave", "s3://b/x", "gs://b/../x"])
def test_invalid_list_prefix_rejected(storage: Any, client: FakeClient, bad: str) -> None:
    with pytest.raises(ConfigError):
        storage.list(bad)
    assert client.list_calls == []


@needs_google
def test_api_errors_become_typed_errors_without_leaking_body(storage: Any, client: FakeClient) -> None:
    client.fail_with = _exc().Forbidden("403 " + CANARY)
    with pytest.raises(SourceError, match="Forbidden") as read_err:
        storage.read("gs://bkt/x")
    with pytest.raises(SourceError):
        storage.exists("gs://bkt/x")
    with pytest.raises(SourceError):
        storage.list("gs://bkt/")
    with pytest.raises(LoadError, match="Forbidden") as write_err:
        storage.write("gs://bkt/x", b"1")
    with pytest.raises(LoadError):
        storage.delete("gs://bkt/x")
    for err in (read_err, write_err):
        assert CANARY not in str(err.value)
        assert "permiss" in str(err.value)


def _env(modo: str = "gcp") -> Environment:
    return Environment(
        modo=modo,  # type: ignore[arg-type]
        projeto_gcp="meu-projeto",
        regiao="southamerica-east1",
        bucket_bronze="b",
        dataset_silver="s",
        dataset_gold="g",
    )


def test_make_storage_gcp_without_extra_gives_actionable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    hide_google(monkeypatch)
    with pytest.raises(ConfigError, match=r"dataeng-core\[gcp\]"):
        make_storage(_env())


@needs_google
def test_make_storage_gcp_returns_gcs_storage(monkeypatch: pytest.MonkeyPatch) -> None:
    from google.cloud import storage as gcs

    created: list[str] = []

    def fake_client(project: str) -> FakeClient:
        created.append(project)
        return FakeClient()

    monkeypatch.setattr(gcs, "Client", fake_client)
    from dataeng_core.storage.gcs import GcsStorage

    assert isinstance(make_storage(_env()), GcsStorage)
    assert created == ["meu-projeto"]


def test_importing_package_without_gcp_extra_works() -> None:
    code = (
        "import sys\n"
        "for n in ('google', 'google.cloud', 'google.cloud.storage', 'google.cloud.bigquery'):\n"
        "    sys.modules[n] = None\n"
        "import dataeng_core, dataeng_core.storage, dataeng_core.warehouse, dataeng_core.cli, dataeng_core.steps\n"
        "import dataeng_core.storage.gcs, dataeng_core.warehouse.bigquery\n"
        "print('ok')\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "ok"


@needs_gcp_credentials
@pytest.mark.gcp
def test_gcs_storage_passes_contract_on_real_gcp() -> None:
    from dataeng_core.storage.gcs import GcsStorage

    bucket = os.environ.get("DATAENG_GCP_BUCKET")
    if not bucket:
        pytest.skip("defina DATAENG_GCP_BUCKET (bucket de teste) para rodar o contrato real")
    run_storage_contract(GcsStorage(os.environ["DATAENG_GCP_PROJECT"]), bucket=bucket)
