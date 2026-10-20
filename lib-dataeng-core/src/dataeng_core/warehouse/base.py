"""Interface de data warehouse, validação de nomes e fábrica por ambiente."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Literal, Protocol

import pandas as pd

from dataeng_core.base.errors import ConfigError
from dataeng_core.base.params import Environment

LoadMode = Literal["append", "overwrite_partition", "merge"]
LOAD_MODES: tuple[str, ...] = ("append", "overwrite_partition", "merge")

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class Warehouse(Protocol):
    """Data warehouse: DDL/SQL, views, cargas idempotentes e consultas."""

    def execute_ddl(self, sql: str) -> None: ...

    def run_sql(self, statements: Sequence[str]) -> None: ...

    def create_or_replace_view(self, name: str, select_sql: str) -> None: ...

    def load(
        self,
        table: str,
        rows: pd.DataFrame,
        *,
        mode: LoadMode,
        partition_column: str | None = None,
        merge_keys: Sequence[str] = (),
    ) -> int:
        """Grava `rows` em `table` e devolve o número de linhas gravadas."""
        ...

    def query(self, sql: str) -> pd.DataFrame: ...

    def table_exists(self, table: str) -> bool: ...

    def row_count(self, table: str) -> int: ...


def validate_identifier(name: str, *, what: str, operation: str) -> str:
    """Aceita só ASCII: letras, dígitos e `_` (sem começar por dígito).

    Nomes com acento são recusados de propósito, para evitar SQL montado com texto livre e
    divergências de normalização entre motores.
    """
    if not isinstance(name, str) or not _IDENTIFIER.fullmatch(name):
        shown = name if isinstance(name, str) and len(name) <= 80 else "<valor inválido>"
        raise ConfigError(
            f"{what} inválido: '{shown}'. Use apenas letras, dígitos e '_', sem começar por dígito.",
            operation=operation,
        )
    return name


def split_table(table: str, *, operation: str) -> tuple[str | None, str]:
    """Separa `dataset.tabela` (ou só `tabela`) validando cada parte."""
    if not isinstance(table, str):
        raise ConfigError("nome de tabela deve ser texto no formato 'dataset.tabela'.", operation=operation)
    parts = table.split(".")
    if len(parts) > 2:
        raise ConfigError(
            f"nome de tabela inválido: '{table[:80]}'. Use 'dataset.tabela' ou apenas 'tabela'.",
            operation=operation,
        )
    names = [validate_identifier(p, what="nome de tabela", operation=operation) for p in parts]
    return (names[0], names[1]) if len(names) == 2 else (None, names[0])


def make_warehouse(env: Environment) -> Warehouse:
    """Local (DuckDB em arquivo) quando `env.modo == "local"`; BigQuery (Tarefa 14) quando `gcp`."""
    if env.modo == "local":
        from pathlib import Path

        from dataeng_core.warehouse.local import LocalWarehouse

        return LocalWarehouse(Path(env.pasta_local) / "warehouse.duckdb")
    raise NotImplementedError("Warehouse do modo gcp (BigQueryWarehouse) será adicionado na Tarefa 14.")
