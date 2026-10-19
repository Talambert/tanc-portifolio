from __future__ import annotations

from pathlib import Path

import pytest
from contract import run_storage_contract

from dataeng_core.base.errors import ConfigError, SourceError
from dataeng_core.base.params import Environment
from dataeng_core.storage import LocalStorage, make_storage


def test_local_storage_passes_contract(tmp_path: Path) -> None:
    run_storage_contract(LocalStorage(tmp_path))


def test_gs_uri_maps_to_root_bucket_key(tmp_path: Path) -> None:
    LocalStorage(tmp_path).write("gs://bkt/a/b.txt", b"x")
    assert (tmp_path / "bkt" / "a" / "b.txt").read_bytes() == b"x"


def test_path_escaping_root_is_rejected(tmp_path: Path) -> None:
    storage = LocalStorage(tmp_path / "raiz")
    with pytest.raises(ConfigError):
        storage.write("gs://bkt/../../fora.txt", b"x")
    with pytest.raises(ConfigError):
        storage.exists("gs://../fora.txt")
    assert not (tmp_path / "fora.txt").exists()


def test_read_missing_error_names_path(tmp_path: Path) -> None:
    with pytest.raises(SourceError, match="gs://bkt/nao_existe.csv"):
        LocalStorage(tmp_path).read("gs://bkt/nao_existe.csv")


def test_make_storage_local(tmp_path: Path) -> None:
    env = Environment(
        modo="local",
        projeto_gcp="p",
        regiao="r",
        bucket_bronze="b",
        dataset_silver="s",
        dataset_gold="g",
        pasta_local=str(tmp_path),
    )
    storage = make_storage(env)
    assert isinstance(storage, LocalStorage)
    storage.write("gs://b/x", b"1")
    assert (tmp_path / "b" / "x").read_bytes() == b"1"
