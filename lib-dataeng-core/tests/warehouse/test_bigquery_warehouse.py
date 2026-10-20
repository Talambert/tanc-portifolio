"""BigQueryWarehouse: testes unitários com cliente falso (sempre) e contrato real (só com credenciais GCP).

O cliente falso captura o SQL enviado e os DataFrames carregados. Isso prova o SQL gerado, a atomicidade
declarada e a ausência de valores dos dados no SQL, mas NÃO prova a semântica do BigQuery: isso só o
contrato `@pytest.mark.gcp` (rodado pelo usuário com credenciais) faz.
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from typing import Any

import pandas as pd
import pytest
from tests.gcp_support import hide_google, needs_gcp_credentials, needs_google
from tests.warehouse.contract import run_warehouse_contract

from dataeng_core.base.errors import ConfigError, LoadError, SqlError
from dataeng_core.base.params import Environment
from dataeng_core.warehouse import make_warehouse

CANARY = "VALOR-SECRETO-DO-DADO"
T = "`meu-proj.ds.t`"


def _exc() -> Any:
    from google.api_core import exceptions

    return exceptions


class FakeTable:
    def __init__(self, columns: list[str]) -> None:
        from google.cloud import bigquery

        self.schema = [bigquery.SchemaField(c, "STRING") for c in columns]


class FakeResult:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None, frame: pd.DataFrame | None = None) -> None:
        self._rows = rows or []
        self._frame = frame if frame is not None else pd.DataFrame()

    def __iter__(self) -> Any:
        return iter(self._rows)

    def to_dataframe(self) -> pd.DataFrame:
        return self._frame


class FakeJob:
    def __init__(self, result: FakeResult) -> None:
        self._result = result

    def result(self) -> FakeResult:
        return self._result


class FakeClient:
    def __init__(self) -> None:
        self.tables: dict[str, FakeTable] = {}
        self.sql: list[str] = []
        self.query_locations: list[str | None] = []
        self.loads: list[tuple[pd.DataFrame, str, Any, str | None]] = []
        self.events: list[str] = []
        self.on_query: Callable[[str], FakeResult | Exception | None] = lambda sql: None
        self.load_error: Exception | None = None

    def get_table(self, ref: str) -> FakeTable:
        self.events.append("get_table")
        if ref not in self.tables:
            raise _exc().NotFound("404 " + ref)
        return self.tables[ref]

    def query(self, sql: str, location: str | None = None) -> FakeJob:
        self.events.append("query")
        self.sql.append(sql)
        self.query_locations.append(location)
        outcome = self.on_query(sql)
        if isinstance(outcome, Exception):
            raise outcome
        return FakeJob(outcome if outcome is not None else FakeResult())

    def load_table_from_dataframe(
        self, df: pd.DataFrame, destination: str, job_config: Any = None, location: str | None = None
    ) -> FakeJob:
        self.events.append("load")
        if self.load_error is not None:
            raise self.load_error
        self.loads.append((df.copy(), destination, job_config, location))
        return FakeJob(FakeResult())


@pytest.fixture
def client() -> FakeClient:
    return FakeClient()


@pytest.fixture
def wh(client: FakeClient) -> Any:
    from dataeng_core.warehouse.bigquery import BigQueryWarehouse

    return BigQueryWarehouse("meu-proj", "southamerica-east1", client=client)


def _frame(rows: list[tuple[int, str, str]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["id", "nome", "dia"])


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def _staging(client: FakeClient) -> str:
    destination = client.loads[-1][1]
    assert re.fullmatch(r"meu-proj\.ds\._stg_[0-9a-f]{32}", destination), destination
    return f"`{destination}`"


def _existing(client: FakeClient, columns: list[str] | None = None) -> None:
    client.tables["meu-proj.ds.t"] = FakeTable(columns or ["id", "nome", "dia"])


# ---- construtor ----------------------------------------------------------------------------


@needs_google
@pytest.mark.parametrize("project", ["", "a b", "p`; DROP", "Maiusculo", "x" * 40])
def test_invalid_project_rejected(project: str, client: FakeClient) -> None:
    from dataeng_core.warehouse.bigquery import BigQueryWarehouse

    with pytest.raises(ConfigError):
        BigQueryWarehouse(project, "US", client=client)


@needs_google
@pytest.mark.parametrize("location", ["", "US east", "US'"])
def test_invalid_location_rejected(location: str, client: FakeClient) -> None:
    from dataeng_core.warehouse.bigquery import BigQueryWarehouse

    with pytest.raises(ConfigError):
        BigQueryWarehouse("meu-proj", location, client=client)


# ---- SQL livre ------------------------------------------------------------------------------


@needs_google
def test_run_sql_sends_each_statement_in_order_with_location(wh: Any, client: FakeClient) -> None:
    wh.run_sql(["INSERT INTO ds.t VALUES (1)", "INSERT INTO ds.t VALUES (2)"])
    wh.execute_ddl("CREATE TABLE ds.x (id INT64)")
    assert client.sql == ["INSERT INTO ds.t VALUES (1)", "INSERT INTO ds.t VALUES (2)", "CREATE TABLE ds.x (id INT64)"]
    assert set(client.query_locations) == {"southamerica-east1"}


@needs_google
def test_empty_sql_rejected_without_calling_api(wh: Any, client: FakeClient) -> None:
    for call in (lambda: wh.execute_ddl("  "), lambda: wh.run_sql([""]), lambda: wh.query("")):
        with pytest.raises(SqlError):
            call()
    assert client.sql == []


@needs_google
def test_query_returns_dataframe(wh: Any, client: FakeClient) -> None:
    frame = pd.DataFrame({"id": [1, 2]})
    client.on_query = lambda sql: FakeResult(frame=frame)
    assert wh.query("SELECT id FROM ds.t").equals(frame)
    assert client.sql == ["SELECT id FROM ds.t"]


@needs_google
def test_create_or_replace_view_quotes_validated_name(wh: Any, client: FakeClient) -> None:
    wh.create_or_replace_view("ds.v", "SELECT 1 AS id;  ")
    assert client.sql == ["CREATE OR REPLACE VIEW `meu-proj.ds.v` AS SELECT 1 AS id"]
    for bad in ("v", "ds.v; DROP", "a.b.c", ""):
        with pytest.raises(ConfigError):
            wh.create_or_replace_view(bad, "SELECT 1")
    with pytest.raises(SqlError):
        wh.create_or_replace_view("ds.v", " ; ")
    assert len(client.sql) == 1


@needs_google
def test_sql_syntax_error_becomes_sql_error_with_first_line(wh: Any, client: FakeClient) -> None:
    client.on_query = lambda sql: _exc().BadRequest(
        "Syntax error: Unexpected identifier at [1:8]\nsegunda linha " + CANARY
    )
    with pytest.raises(SqlError, match="Syntax error") as info:
        wh.run_sql(["SELEC 1"])
    assert CANARY not in str(info.value)


# ---- catálogo ------------------------------------------------------------------------------


@needs_google
def test_table_exists_uses_get_table_and_maps_not_found(wh: Any, client: FakeClient) -> None:
    assert wh.table_exists("ds.t") is False
    _existing(client)
    assert wh.table_exists("ds.t") is True
    for bad in ("", "a.b.c", "t", "ds.t; DROP"):
        with pytest.raises(ConfigError):
            wh.table_exists(bad)


@needs_google
def test_row_count(wh: Any, client: FakeClient) -> None:
    with pytest.raises(LoadError, match="ds.t"):
        wh.row_count("ds.t")
    _existing(client)
    client.on_query = lambda sql: FakeResult(rows=[(7,)])
    assert wh.row_count("ds.t") == 7
    assert _norm(client.sql[-1]) == f"SELECT COUNT(*) FROM {T}"


@needs_google
def test_api_failure_in_catalog_is_typed_without_body(wh: Any, client: FakeClient) -> None:
    client.get_table = lambda ref: (_ for _ in ()).throw(_exc().Forbidden("403 " + CANARY))  # type: ignore[method-assign]
    with pytest.raises(LoadError, match="Forbidden") as info:
        wh.table_exists("ds.t")
    assert CANARY not in str(info.value)
    assert "permiss" in str(info.value)


# ---- load: validação -----------------------------------------------------------------------


@needs_google
@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"mode": "inexistente"}, ConfigError),
        ({"mode": "append", "table": "t"}, ConfigError),
        ({"mode": "append", "table": "ds.t; DROP TABLE x"}, ConfigError),
        ({"mode": "merge"}, LoadError),
        ({"mode": "overwrite_partition"}, LoadError),
        ({"mode": "overwrite_partition", "partition_column": "nao_existe"}, LoadError),
        ({"mode": "overwrite_partition", "partition_column": "dia; DROP"}, ConfigError),
        ({"mode": "merge", "merge_keys": ["id; x"]}, ConfigError),
        ({"mode": "merge", "merge_keys": ["nao_existe"]}, LoadError),
    ],
)
def test_load_validation_happens_before_any_api_call(
    wh: Any, client: FakeClient, kwargs: dict[str, Any], error: type
) -> None:
    table = kwargs.pop("table", "ds.t")
    with pytest.raises(error):
        wh.load(table, _frame([(1, "a", "d1")]), **kwargs)
    assert client.events == []


@needs_google
def test_load_rejects_bad_dataframes(wh: Any, client: FakeClient) -> None:
    with pytest.raises(ConfigError):
        wh.load("ds.t", pd.DataFrame(), mode="append")
    with pytest.raises(ConfigError):
        wh.load("ds.t", pd.DataFrame({"a b": [1]}), mode="append")
    with pytest.raises(ConfigError):
        wh.load("ds.t", pd.DataFrame([[1, 2]], columns=["a", "A"]), mode="append")
    dup = _frame([(1, "a", "d1"), (1, "b", "d1")])
    with pytest.raises(LoadError, match="duplicadas"):
        wh.load("ds.t", dup, mode="merge", merge_keys=["id"])
    assert client.events == []


@needs_google
def test_load_unknown_column_in_existing_table(wh: Any, client: FakeClient) -> None:
    _existing(client, ["id", "nome"])
    with pytest.raises(LoadError, match="dia"):
        wh.load("ds.t", _frame([(1, "a", "d1")]), mode="append")
    assert client.loads == []
    assert client.sql == []


# ---- load: SQL gerado por modo -------------------------------------------------------------


@needs_google
def test_append_on_existing_table_is_one_insert_from_staging_then_drop(wh: Any, client: FakeClient) -> None:
    _existing(client)
    frame = _frame([(1, CANARY, "d1")])
    assert wh.load("ds.t", frame, mode="append") == 1
    staging = _staging(client)
    assert [_norm(s) for s in client.sql] == [
        f"INSERT INTO {T} (`id`, `nome`, `dia`) SELECT `id`, `nome`, `dia` FROM {staging}",
        f"DROP TABLE IF EXISTS {staging}",
    ]
    df, _, config, location = client.loads[0]
    assert df.equals(frame)  # os valores vão pelo carregamento, não pelo SQL
    assert location == "southamerica-east1"
    assert config.write_disposition == "WRITE_TRUNCATE"
    assert [f.name for f in config.schema] == ["id", "nome", "dia"]  # esquema da tabela existente
    assert all(CANARY not in s for s in client.sql)


@needs_google
def test_overwrite_partition_deletes_then_inserts_in_one_transaction(wh: Any, client: FakeClient) -> None:
    _existing(client)
    frame = _frame([(1, CANARY, "d1")])
    assert wh.load("ds.t", frame, mode="overwrite_partition", partition_column="dia") == 1
    staging = _staging(client)
    assert len(client.sql) == 2
    script, drop = client.sql
    assert _norm(script) == _norm(
        f"""
        BEGIN
        BEGIN TRANSACTION;
        DELETE FROM {T} AS t WHERE EXISTS (SELECT 1 FROM {staging} AS d WHERE d.`dia` IS NOT DISTINCT FROM t.`dia`);
        INSERT INTO {T} (`id`, `nome`, `dia`) SELECT `id`, `nome`, `dia` FROM {staging};
        COMMIT TRANSACTION;
        EXCEPTION WHEN ERROR THEN
        ROLLBACK TRANSACTION;
        RAISE;
        END;
        """
    )
    assert _norm(drop) == f"DROP TABLE IF EXISTS {staging}"
    assert CANARY not in script


@needs_google
def test_overwrite_partition_by_run_id_column_that_is_not_the_table_partition(wh: Any, client: FakeClient) -> None:
    """Ruling da ingestão: `append` vira overwrite por `_run_id` (idempotente por execução)."""
    _existing(client, ["id", "_run_id"])
    frame = pd.DataFrame({"id": [1, 2], "_run_id": ["20260101T000000-abc"] * 2})
    wh.load("ds.t", frame, mode="overwrite_partition", partition_column="_run_id")
    script = _norm(client.sql[0])
    assert script.startswith("BEGIN BEGIN TRANSACTION; DELETE FROM " + T)
    assert "d.`_run_id` IS NOT DISTINCT FROM t.`_run_id`" in script
    assert script.index("DELETE") < script.index("INSERT") < script.index("COMMIT TRANSACTION")
    assert "20260101T000000-abc" not in script


@needs_google
def test_merge_is_a_single_atomic_statement_with_null_safe_keys(wh: Any, client: FakeClient) -> None:
    _existing(client)
    wh.load("ds.t", _frame([(1, CANARY, "d1")]), mode="merge", merge_keys=["id", "dia"])
    staging = _staging(client)
    assert _norm(client.sql[0]) == _norm(
        f"""
        MERGE {T} AS t USING {staging} AS d
        ON t.`id` IS NOT DISTINCT FROM d.`id` AND t.`dia` IS NOT DISTINCT FROM d.`dia`
        WHEN MATCHED THEN UPDATE SET t.`nome` = d.`nome`
        WHEN NOT MATCHED THEN INSERT (`id`, `nome`, `dia`) VALUES (d.`id`, d.`nome`, d.`dia`)
        """
    )
    assert CANARY not in client.sql[0]


@needs_google
def test_merge_with_only_key_columns_has_no_update_clause(wh: Any, client: FakeClient) -> None:
    _existing(client, ["id"])
    wh.load("ds.t", pd.DataFrame({"id": [1]}), mode="merge", merge_keys=["id"])
    assert "WHEN MATCHED" not in client.sql[0]
    assert "WHEN NOT MATCHED THEN INSERT (`id`) VALUES (d.`id`)" in _norm(client.sql[0])


@needs_google
def test_new_table_is_created_from_staging_outside_the_transaction(wh: Any, client: FakeClient) -> None:
    assert wh.load("ds.t", _frame([(1, "a", "d1")]), mode="overwrite_partition", partition_column="dia") == 1
    staging = _staging(client)
    assert _norm(client.sql[0]) == f"CREATE TABLE IF NOT EXISTS {T} AS SELECT * FROM {staging} WHERE FALSE"
    assert "BEGIN TRANSACTION" in client.sql[1]
    assert "CREATE" not in client.sql[1]  # DDL permanente não é permitido dentro de transação
    _, _, config, _ = client.loads[0]
    assert config.schema is None or list(config.schema) == []  # sem tabela, o BigQuery infere o esquema


@needs_google
def test_empty_dataframe_is_noop_and_never_touches_the_table(wh: Any, client: FakeClient) -> None:
    _existing(client)
    for mode, extra in (
        ("append", {}),
        ("overwrite_partition", {"partition_column": "dia"}),
        ("merge", {"merge_keys": ["id"]}),
    ):
        assert wh.load("ds.t", _frame([]), mode=mode, **extra) == 0
    assert client.loads == []
    assert client.sql == []


@needs_google
def test_empty_dataframe_for_missing_table_does_not_create_it(wh: Any, client: FakeClient) -> None:
    assert wh.load("ds.t", _frame([]), mode="append") == 0
    assert client.loads == [] and client.sql == []


# ---- load: falhas --------------------------------------------------------------------------


@needs_google
def test_script_failure_is_load_error_without_data_and_staging_is_dropped(wh: Any, client: FakeClient) -> None:
    _existing(client)

    def fail_script(sql: str) -> Exception | None:
        if "BEGIN TRANSACTION" in sql:
            return _exc().BadRequest("Could not cast literal '" + CANARY + "' to type INT64")
        return None

    client.on_query = fail_script
    with pytest.raises(LoadError, match="BadRequest") as info:
        wh.load("ds.t", _frame([(1, CANARY, "d1")]), mode="overwrite_partition", partition_column="dia")
    assert CANARY not in str(info.value)
    assert "ds.t" in str(info.value)
    assert client.sql[-1] == f"DROP TABLE IF EXISTS {_staging(client)}"


@needs_google
def test_staging_load_failure_is_load_error_without_data(wh: Any, client: FakeClient) -> None:
    _existing(client)
    client.load_error = _exc().Forbidden("403 " + CANARY)
    with pytest.raises(LoadError, match="Forbidden") as info:
        wh.load("ds.t", _frame([(1, CANARY, "d1")]), mode="append")
    assert CANARY not in str(info.value)
    assert "permiss" in str(info.value)


@needs_google
def test_failed_cleanup_does_not_mask_the_result(wh: Any, client: FakeClient) -> None:
    _existing(client)
    client.on_query = lambda sql: _exc().ServiceUnavailable("x") if sql.startswith("DROP") else None
    assert wh.load("ds.t", _frame([(1, "a", "d1")]), mode="append") == 1


# ---- fábrica -------------------------------------------------------------------------------


def _env() -> Environment:
    return Environment(
        modo="gcp",
        projeto_gcp="meu-proj",
        regiao="southamerica-east1",
        bucket_bronze="b",
        dataset_silver="s",
        dataset_gold="g",
    )


def test_make_warehouse_gcp_without_extra_gives_actionable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    hide_google(monkeypatch)
    with pytest.raises(ConfigError, match=r"dataeng-core\[gcp\]"):
        make_warehouse(_env())


@needs_google
def test_make_warehouse_gcp_returns_bigquery_warehouse(monkeypatch: pytest.MonkeyPatch) -> None:
    from google.cloud import bigquery

    created: list[tuple[str, str | None]] = []

    def fake_client(project: str, location: str | None = None) -> FakeClient:
        created.append((project, location))
        return FakeClient()

    monkeypatch.setattr(bigquery, "Client", fake_client)
    from dataeng_core.warehouse.bigquery import BigQueryWarehouse

    assert isinstance(make_warehouse(_env()), BigQueryWarehouse)
    assert created == [("meu-proj", "southamerica-east1")]


@needs_gcp_credentials
@pytest.mark.gcp
def test_bigquery_warehouse_passes_contract_on_real_gcp() -> None:
    from dataeng_core.warehouse.bigquery import BigQueryWarehouse

    dataset = os.environ.get("DATAENG_GCP_DATASET")
    if not dataset:
        pytest.skip("defina DATAENG_GCP_DATASET (dataset de teste existente) para rodar o contrato real")
    wh = BigQueryWarehouse(os.environ["DATAENG_GCP_PROJECT"], os.environ.get("DATAENG_GCP_LOCATION", "US"))
    run_warehouse_contract(wh, dataset=dataset, int_type="INT64", text_type="STRING")
