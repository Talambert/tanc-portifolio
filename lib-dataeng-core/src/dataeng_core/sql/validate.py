"""Validação sintática de SQL e avisos de carga não idempotente."""

from __future__ import annotations

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError, SqlglotError

from dataeng_core.base.errors import SqlError
from dataeng_core.sql.render import split_with_lines


def _is_idempotent_insert(tree: exp.Expression) -> bool:
    if tree.args.get("overwrite"):
        return True
    query = tree.args.get("expression")
    where = query.args.get("where") if isinstance(query, exp.Select) else None
    return where is not None and any(isinstance(n.this, exp.Exists) for n in where.find_all(exp.Not))


def validate_sql(sql: str, dialect: str = "bigquery", *, source: str | None = None) -> list[str]:
    """Valida a sintaxe; devolve avisos. Sintaxe inválida levanta `SqlError` com a linha."""
    prefix = f"arquivo {source}: " if source else ""
    warnings: list[str] = []
    for stmt in split_with_lines(sql, dialect, source=source):
        try:
            tree = sqlglot.parse_one(stmt.text, read=dialect)
        except ParseError as exc:
            first = exc.errors[0] if exc.errors else {}
            line = stmt.line + int(first.get("line") or 1) - 1
            col = first.get("col")
            at = f", coluna {col}" if col else ""
            raise SqlError(f"{prefix}linha {line}{at}: sintaxe inválida", operation="validate_sql") from exc
        except SqlglotError as exc:
            raise SqlError(f"{prefix}linha {stmt.line}: sintaxe inválida", operation="validate_sql") from exc
        if isinstance(tree, exp.Insert) and not _is_idempotent_insert(tree):
            warnings.append(
                f"{prefix}linha {stmt.line}: INSERT sem MERGE, OVERWRITE nem WHERE NOT EXISTS; "
                "a reexecução pode duplicar linhas"
            )
    return warnings
