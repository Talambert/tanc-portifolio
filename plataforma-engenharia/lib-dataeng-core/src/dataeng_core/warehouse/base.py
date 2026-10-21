"""Interface de data warehouse, validação de nomes e fábrica por ambiente."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Literal, NamedTuple, Protocol

import pandas as pd

from dataeng_core.base.errors import ConfigError, LoadError
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


class LoadRequest(NamedTuple):
    """Argumentos de `Warehouse.load` já validados (identificadores seguros para citar em SQL)."""

    schema: str | None
    table: str
    columns: list[str]
    keys: list[str]
    partition: str | None


def check_load_request(
    table: str,
    rows: pd.DataFrame,
    *,
    mode: str,
    partition_column: str | None,
    merge_keys: Sequence[str],
    operation: str,
) -> LoadRequest:
    """Validação comum às implementações de `load`, antes de qualquer gravação."""
    if mode not in LOAD_MODES:
        raise ConfigError(
            f"modo de carga desconhecido: '{mode}'. Use um de: {', '.join(LOAD_MODES)}.", operation=operation
        )
    schema, name = split_table(table, operation=operation)
    if not isinstance(rows, pd.DataFrame) or len(rows.columns) == 0:
        raise ConfigError("os dados a carregar devem ser um DataFrame com ao menos uma coluna.", operation=operation)
    columns = [validate_identifier(str(c), what="nome de coluna", operation=operation) for c in rows.columns]
    if len({c.lower() for c in columns}) != len(columns):
        raise ConfigError("o DataFrame tem nomes de coluna repetidos.", operation=operation)
    keys = [validate_identifier(k, what="chave de merge", operation=operation) for k in merge_keys]
    part = (
        validate_identifier(partition_column, what="coluna de partição", operation=operation)
        if partition_column
        else None
    )
    if mode == "merge" and not keys:
        raise LoadError("o modo merge exige merge_keys (as colunas que identificam a linha).", operation=operation)
    if mode == "overwrite_partition" and part is None:
        raise LoadError("o modo overwrite_partition exige partition_column.", operation=operation)
    control = keys if mode == "merge" else ([part] if part else [])
    lowered = {c.lower() for c in columns}
    absent = [c for c in control if c.lower() not in lowered]
    if absent:
        raise LoadError(f"coluna(s) ausente(s) nos dados: {', '.join(absent)}.", operation=operation)
    if mode == "merge" and rows.duplicated(subset=[c for c in columns if c.lower() in {k.lower() for k in keys}]).any():
        raise LoadError(
            f"os dados têm chaves de merge duplicadas ({', '.join(keys)}); deduplique antes de carregar.",
            operation=operation,
        )
    return LoadRequest(schema, name, columns, keys, part)


def make_warehouse(env: Environment) -> Warehouse:
    """Local (DuckDB em arquivo) quando `env.modo == "local"`; BigQuery quando `gcp` (exige o extra `[gcp]`)."""
    if env.modo == "local":
        from pathlib import Path

        from dataeng_core.warehouse.local import LocalWarehouse

        return LocalWarehouse(Path(env.pasta_local) / "warehouse.duckdb")
    from dataeng_core.warehouse.bigquery import BigQueryWarehouse

    return BigQueryWarehouse(env.projeto_gcp, env.regiao)
