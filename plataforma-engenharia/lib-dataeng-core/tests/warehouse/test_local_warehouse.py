from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest
from tests.warehouse.contract import run_warehouse_contract

from dataeng_core.base.errors import ConfigError, LoadError, SqlError
from dataeng_core.base.params import Environment
from dataeng_core.warehouse import LocalWarehouse, make_warehouse


def _env(tmp_path: Path, modo: str = "local") -> Environment:
    return Environment(
        modo=modo,  # type: ignore[arg-type]
        projeto_gcp="p",
        regiao="r",
        bucket_bronze="b",
        dataset_silver="s",
        dataset_gold="g",
        pasta_local=str(tmp_path),
    )


def test_local_warehouse_passes_contract_in_memory() -> None:
    run_warehouse_contract(LocalWarehouse())


def test_local_warehouse_passes_contract_on_file(tmp_path: Path) -> None:
    run_warehouse_contract(LocalWarehouse(tmp_path / "w.duckdb"))


def test_file_database_persists(tmp_path: Path) -> None:
    db = tmp_path / "sub" / "w.duckdb"
    first = LocalWarehouse(db)
    first.load("t", pd.DataFrame({"a": [1, 2]}), mode="append")
    first.close()
    assert LocalWarehouse(db).row_count("t") == 2


def test_missing_duckdb_gives_actionable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "duckdb", None)
    with pytest.raises(ConfigError, match=r"dataeng-core\[local\]"):
        LocalWarehouse()


def test_make_warehouse_local(tmp_path: Path) -> None:
    wh = make_warehouse(_env(tmp_path))
    assert isinstance(wh, LocalWarehouse)
    wh.load("s.t", pd.DataFrame({"a": [1]}), mode="append")
    assert (tmp_path / "warehouse.duckdb").exists()


@pytest.mark.parametrize("bad", ["", "a b", "a;b", 'a"b', "a.b.c", "1abc", "a-b", "a..b", ".a", "a."])
def test_invalid_table_names_rejected(bad: str) -> None:
    wh = LocalWarehouse()
    with pytest.raises(ConfigError):
        wh.load(bad, pd.DataFrame({"a": [1]}), mode="append")
    with pytest.raises(ConfigError):
        wh.table_exists(bad)
    with pytest.raises(ConfigError):
        wh.row_count(bad)
    with pytest.raises(ConfigError):
        wh.create_or_replace_view(bad, "SELECT 1")


def test_invalid_column_names_rejected() -> None:
    wh = LocalWarehouse()
    with pytest.raises(ConfigError):
        wh.load("t", pd.DataFrame({"a b": [1]}), mode="append")
    wh.load("t", pd.DataFrame({"a": [1]}), mode="append")
    with pytest.raises(ConfigError):
        wh.load("t", pd.DataFrame({"a": [1]}), mode="merge", merge_keys=["a; DROP TABLE t"])
    with pytest.raises(ConfigError):
        wh.load("t", pd.DataFrame({"a": [1]}), mode="overwrite_partition", partition_column='a"')
    assert wh.row_count("t") == 1


def test_unknown_mode_rejected_before_touching_data() -> None:
    wh = LocalWarehouse()
    with pytest.raises(ConfigError, match="append"):
        wh.load("t", pd.DataFrame({"a": [1]}), mode="upsert")  # type: ignore[arg-type]
    assert wh.table_exists("t") is False


def test_load_requires_dataframe_with_columns() -> None:
    wh = LocalWarehouse()
    with pytest.raises(ConfigError):
        wh.load("t", pd.DataFrame(), mode="append")
    with pytest.raises(ConfigError):
        wh.load("t", [1, 2], mode="append")  # type: ignore[arg-type]


def test_merge_and_partition_columns_must_exist() -> None:
    wh = LocalWarehouse()
    wh.load("t", pd.DataFrame({"a": [1], "b": ["x"]}), mode="append")
    df = pd.DataFrame({"a": [1], "b": ["y"]})
    with pytest.raises(LoadError, match="zzz"):
        wh.load("t", df, mode="merge", merge_keys=["zzz"])
    with pytest.raises(LoadError, match="zzz"):
        wh.load("t", df, mode="overwrite_partition", partition_column="zzz")
    assert wh.row_count("t") == 1


def test_unknown_column_in_rows_is_load_error() -> None:
    wh = LocalWarehouse()
    wh.load("t", pd.DataFrame({"a": [1]}), mode="append")
    with pytest.raises(LoadError, match="extra"):
        wh.load("t", pd.DataFrame({"a": [1], "extra": [2]}), mode="append")


def test_duplicate_merge_keys_in_input_rejected() -> None:
    wh = LocalWarehouse()
    wh.load("t", pd.DataFrame({"id": [1], "v": ["a"]}), mode="append")
    with pytest.raises(LoadError, match="duplicad"):
        wh.load("t", pd.DataFrame({"id": [2, 2], "v": ["x", "y"]}), mode="merge", merge_keys=["id"])
    assert wh.row_count("t") == 1


def _table_with_row() -> LocalWarehouse:
    wh = LocalWarehouse()
    wh.run_sql(["CREATE TABLE t (id INTEGER, v INTEGER)", "INSERT INTO t VALUES (1, 10)"])
    return wh


def _rows(wh: LocalWarehouse) -> list[tuple[int, int]]:
    return [(int(r.id), int(r.v)) for r in wh.query("SELECT id, v FROM t ORDER BY id").itertuples()]


def test_failed_overwrite_partition_rolls_back_delete_and_hides_values() -> None:
    wh = _table_with_row()
    bad = pd.DataFrame({"id": [1, 2], "v": ["10", "segredo"]})
    with pytest.raises(LoadError) as info:
        wh.load("t", bad, mode="overwrite_partition", partition_column="id")
    assert "segredo" not in str(info.value)
    assert _rows(wh) == [(1, 10)]


def test_failed_merge_rolls_back_update() -> None:
    wh = _table_with_row()
    bad = pd.DataFrame({"id": [1, 2], "v": ["11", "segredo"]})
    with pytest.raises(LoadError) as info:
        wh.load("t", bad, mode="merge", merge_keys=["id"])
    assert "segredo" not in str(info.value)
    assert _rows(wh) == [(1, 10)]


def test_rollback_failure_does_not_mask_original_error(monkeypatch: pytest.MonkeyPatch) -> None:
    wh = _table_with_row()

    class _Con:
        def __init__(self, real: object) -> None:
            self._real = real

        def execute(self, sql: str, *args: object) -> object:
            if sql == "ROLLBACK":
                raise RuntimeError("rollback quebrou")
            return self._real.execute(sql, *args)  # type: ignore[attr-defined]

        def __getattr__(self, name: str) -> object:
            return getattr(self._real, name)

    monkeypatch.setattr(wh, "_con", _Con(wh._con))
    with pytest.raises(LoadError, match="falha ao gravar"):
        wh.load("t", pd.DataFrame({"id": [2], "v": ["segredo"]}), mode="append")


@pytest.mark.parametrize("bad", ["abc\n", "\nabc", "caf\u00e9", "a b"])
def test_identifier_with_newline_or_accent_rejected(bad: str) -> None:
    with pytest.raises(ConfigError):
        LocalWarehouse().table_exists(bad)


def test_column_names_are_case_insensitive() -> None:
    wh = _table_with_row()
    assert wh.load("t", pd.DataFrame({"ID": [2], "V": [20]}), mode="append") == 1
    assert wh.load("t", pd.DataFrame({"ID": [2], "V": [21]}), mode="merge", merge_keys=["id"]) == 1
    assert _rows(wh) == [(1, 10), (2, 21)]
    with pytest.raises(ConfigError, match="repetidos"):
        wh.load("t", pd.DataFrame([[1, 2]], columns=["a", "A"]), mode="append")


def test_merge_with_null_keys_does_not_duplicate() -> None:
    wh = LocalWarehouse()
    df = pd.DataFrame({"k": ["a", None], "v": [1, 2]})
    wh.load("t", df, mode="merge", merge_keys=["k"])
    wh.load("t", df, mode="merge", merge_keys=["k"])
    assert wh.row_count("t") == 2


def test_merge_keeps_columns_absent_from_input() -> None:
    wh = LocalWarehouse()
    wh.run_sql(["CREATE TABLE t (id INTEGER, v VARCHAR, extra VARCHAR)", "INSERT INTO t VALUES (1, 'a', 'keep')"])
    wh.load("t", pd.DataFrame({"id": [1], "v": ["b"]}), mode="merge", merge_keys=["id"])
    out = wh.query("SELECT v, extra FROM t")
    assert out.iloc[0]["v"] == "b"
    assert out.iloc[0]["extra"] == "keep"


def test_empty_frame_is_noop_for_existing_table() -> None:
    wh = LocalWarehouse()
    wh.load("t", pd.DataFrame({"a": [1], "p": ["x"]}), mode="append")
    empty = pd.DataFrame({"a": [], "p": []})
    assert wh.load("t", empty, mode="overwrite_partition", partition_column="p") == 0
    assert wh.load("t", empty, mode="merge", merge_keys=["a"]) == 0
    assert wh.row_count("t") == 1


def test_dataset_table_creates_schema_and_ddl_creates_schema() -> None:
    wh = LocalWarehouse()
    wh.load("meu_ds.t", pd.DataFrame({"a": [1]}), mode="append")
    assert wh.table_exists("meu_ds.t") is True
    assert wh.table_exists("outro_ds.t") is False
    wh.execute_ddl("CREATE TABLE ddl_ds.x (a INTEGER)")
    assert wh.table_exists("ddl_ds.x") is True


def test_sql_errors_are_typed_and_run_sql_stops_at_first_failure() -> None:
    wh = LocalWarehouse()
    with pytest.raises(SqlError):
        wh.query("SELECT * FROM nao_existe")
    with pytest.raises(SqlError):
        wh.execute_ddl("CREATE TABLE")
    with pytest.raises(SqlError):
        wh.run_sql(["CREATE TABLE ok (a INTEGER)", "COMANDO INVALIDO", "CREATE TABLE nunca (a INTEGER)"])
    assert wh.table_exists("ok") is True
    assert wh.table_exists("nunca") is False
    with pytest.raises(SqlError):
        wh.create_or_replace_view("v", "")
    with pytest.raises(LoadError):
        wh.row_count("nao_existe")


def test_view_accepts_trailing_semicolon() -> None:
    wh = LocalWarehouse()
    wh.create_or_replace_view("vds.v", "SELECT 1 AS a;")
    assert int(wh.query("SELECT a FROM vds.v").iloc[0]["a"]) == 1


def test_timezone_aware_values_load_into_naive_timestamp_column_as_utc(monkeypatch: pytest.MonkeyPatch) -> None:
    """Coluna TIMESTAMP (portátil com o BigQuery) recebe o instante em UTC, não na hora local da máquina."""
    monkeypatch.setenv("TZ", "America/Sao_Paulo")
    wh = LocalWarehouse()
    wh.execute_ddl("CREATE TABLE s.t (id INT64, carregado_em TIMESTAMP)")
    stamp = pd.Timestamp("2026-03-01 12:00:00", tz="UTC")
    wh.load("s.t", pd.DataFrame({"id": [1], "carregado_em": [stamp]}), mode="append")
    assert wh.query("SELECT strftime(carregado_em, '%Y-%m-%d %H:%M:%S') AS v FROM s.t")["v"].tolist() == [
        "2026-03-01 12:00:00"
    ]
