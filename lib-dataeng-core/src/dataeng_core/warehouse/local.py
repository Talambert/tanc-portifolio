"""Warehouse local em DuckDB: `dataset.tabela` vira `schema.tabela`."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd
import sqlglot
from sqlglot import exp

from dataeng_core.base.errors import ConfigError, DataEngError, LoadError, SqlError
from dataeng_core.warehouse.base import (
    LoadMode,
    check_load_request,
    split_table,
    validate_identifier,
)


def _q(name: str) -> str:
    """Cita um identificador já validado."""
    return f'"{name}"'


def _qualified(schema: str | None, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}" if schema else _q(table)


def _short(exc: Exception) -> str:
    """Só o tipo do erro: o DuckDB cita valores de dados em erros de conversão (usado em `load`)."""
    return type(exc).__name__


class LocalWarehouse:
    """Em memória quando `db_path` é `None`; em arquivo caso contrário."""

    def __init__(self, db_path: Path | None = None) -> None:
        try:
            import duckdb
        except ImportError as exc:
            raise ConfigError(
                'o pacote duckdb não está instalado. Instale o extra local: pip install "dataeng-core[local]".',
                operation="LocalWarehouse",
            ) from exc
        self._duckdb: Any = duckdb
        if db_path is None:
            target = ":memory:"
        else:
            path = Path(db_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            target = str(path)
        self._con: Any = duckdb.connect(target)
        # Determinístico: valores com fuso viram TIMESTAMP (sem fuso, como no BigQuery) em UTC, não na hora local.
        self._con.execute("SET TimeZone = 'UTC'")

    def close(self) -> None:
        self._con.close()

    # ---- SQL livre -------------------------------------------------------------------------

    def _ensure_schemas_for(self, sql: str) -> None:
        """Cria os schemas dos objetos criados por um CREATE (equivale a datasets existentes no BigQuery)."""
        try:
            trees = sqlglot.parse(sql, read="duckdb")
        except sqlglot.errors.SqlglotError:
            return  # o DuckDB reportará o erro de sintaxe
        for tree in trees:
            if isinstance(tree, exp.Create) and isinstance(tree.this, (exp.Table, exp.Schema)):
                table = tree.this if isinstance(tree.this, exp.Table) else tree.this.this
                if isinstance(table, exp.Table) and table.db:
                    schema = validate_identifier(table.db, what="nome de dataset", operation="warehouse.sql")
                    self._con.execute(f"CREATE SCHEMA IF NOT EXISTS {_q(schema)}")

    def _exec(self, sql: str, operation: str) -> None:
        """Executa SQL livre. A mensagem de erro pode citar um trecho do erro do DuckDB."""
        if not isinstance(sql, str) or not sql.strip():
            raise SqlError("o SQL está vazio.", operation=operation)
        try:
            self._ensure_schemas_for(sql)
            self._con.execute(sql)
        except DataEngError:
            raise
        except self._duckdb.Error as exc:
            raise SqlError(f"{_short(exc)}: {self._first_line(exc)}", operation=operation) from exc

    @staticmethod
    def _first_line(exc: Exception) -> str:
        return (str(exc).splitlines() or [""])[0][:300]

    def execute_ddl(self, sql: str) -> None:
        self._exec(sql, "warehouse.execute_ddl")

    def run_sql(self, statements: Sequence[str]) -> None:
        for statement in statements:
            self._exec(statement, "warehouse.run_sql")

    def create_or_replace_view(self, name: str, select_sql: str) -> None:
        schema, view = split_table(name, operation="warehouse.create_or_replace_view")
        body = select_sql.strip().rstrip(";").strip() if isinstance(select_sql, str) else ""
        if not body:
            raise SqlError("o SELECT da view está vazio.", operation="warehouse.create_or_replace_view")
        if schema:
            self._con.execute(f"CREATE SCHEMA IF NOT EXISTS {_q(schema)}")
        self._exec(f"CREATE OR REPLACE VIEW {_qualified(schema, view)} AS {body}", "warehouse.create_or_replace_view")

    def query(self, sql: str) -> pd.DataFrame:
        """Consulta livre. A mensagem de erro pode citar um trecho do erro do DuckDB."""
        if not isinstance(sql, str) or not sql.strip():
            raise SqlError("o SQL está vazio.", operation="warehouse.query")
        try:
            return self._con.execute(sql).df()  # type: ignore[no-any-return]
        except self._duckdb.Error as exc:
            raise SqlError(f"{_short(exc)}: {self._first_line(exc)}", operation="warehouse.query") from exc

    # ---- catálogo --------------------------------------------------------------------------

    def _columns(self, schema: str | None, table: str) -> list[str]:
        rows = self._con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [schema or "main", table],
        ).fetchall()
        return [r[0] for r in rows]

    def table_exists(self, table: str) -> bool:
        schema, name = split_table(table, operation="warehouse.table_exists")
        row = self._con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            [schema or "main", name],
        ).fetchone()
        return bool(row[0])

    def row_count(self, table: str) -> int:
        schema, name = split_table(table, operation="warehouse.row_count")
        if not self.table_exists(table):
            raise LoadError(f"tabela inexistente: {table}.", operation="warehouse.row_count")
        row = self._con.execute(f"SELECT count(*) FROM {_qualified(schema, name)}").fetchone()
        return int(row[0])

    # ---- carga -----------------------------------------------------------------------------

    def load(
        self,
        table: str,
        rows: pd.DataFrame,
        *,
        mode: LoadMode,
        partition_column: str | None = None,
        merge_keys: Sequence[str] = (),
    ) -> int:
        op = "warehouse.load"
        request = check_load_request(
            table, rows, mode=mode, partition_column=partition_column, merge_keys=merge_keys, operation=op
        )
        schema, name, columns, keys, part = request

        target = _qualified(schema, name)
        incoming = f"_incoming_{uuid.uuid4().hex}"
        self._con.register(incoming, rows)
        try:
            if schema:
                self._con.execute(f"CREATE SCHEMA IF NOT EXISTS {_q(schema)}")
            existing = self._columns(schema, name)
            if existing:
                known = {c.lower() for c in existing}
                unknown = [c for c in columns if c.lower() not in known]
                if unknown:
                    raise LoadError(f"coluna(s) inexistente(s) em {table}: {', '.join(unknown)}.", operation=op)
            self._con.execute("BEGIN TRANSACTION")
            try:
                if not existing:
                    self._con.execute(f"CREATE TABLE {target} AS SELECT * FROM {_q(incoming)} WHERE false")
                self._apply(mode, target, incoming, columns, part, keys)
                self._con.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(Exception):  # não mascarar o erro original
                    self._con.execute("ROLLBACK")
                raise
        except DataEngError:
            raise
        except self._duckdb.Error as exc:
            raise LoadError(f"falha ao gravar em {table} ({_short(exc)}).", operation=op) from exc
        finally:
            self._con.unregister(incoming)
        return len(rows)

    def _apply(
        self, mode: str, target: str, incoming: str, columns: list[str], part: str | None, keys: list[str]
    ) -> None:
        src = _q(incoming)
        cols = ", ".join(_q(c) for c in columns)
        if mode == "overwrite_partition" and part:
            self._con.execute(
                f"DELETE FROM {target} WHERE EXISTS "
                f"(SELECT 1 FROM {src} d WHERE d.{_q(part)} IS NOT DISTINCT FROM {target}.{_q(part)})"
            )
        elif mode == "merge":
            match = " AND ".join(f"d.{_q(k)} IS NOT DISTINCT FROM {target}.{_q(k)}" for k in keys)
            updates = [c for c in columns if c not in keys]
            if updates:
                sets = ", ".join(f"{_q(c)} = d.{_q(c)}" for c in updates)
                self._con.execute(f"UPDATE {target} SET {sets} FROM {src} d WHERE {match}")
            self._con.execute(
                f"INSERT INTO {target} ({cols}) SELECT {cols} FROM {src} d "
                f"WHERE NOT EXISTS (SELECT 1 FROM {target} WHERE {match})"
            )
            return
        self._con.execute(f"INSERT INTO {target} ({cols}) SELECT {cols} FROM {src}")
