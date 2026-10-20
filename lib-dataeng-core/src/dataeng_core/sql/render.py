"""Renderização de parâmetros `${nome}` e divisão de scripts SQL em instruções."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

from sqlglot.dialects.dialect import Dialect
from sqlglot.errors import SqlglotError
from sqlglot.tokens import Token, TokenType

from dataeng_core.base.errors import SqlError
from dataeng_core.base.params import substitute

_PLACEHOLDER = re.compile(r"\$\{(\w+)\}")


def _where(source: str | None) -> str:
    return f"arquivo {source}: " if source else ""


def render_sql(sql: str, params: Mapping[str, str], *, source: str | None = None) -> str:
    """Troca `${nome}` por `params[nome]`; nome ausente levanta `SqlError` antes de qualquer execução.

    A troca é de passada única: valores com `;`, aspas ou `${x}` não são reprocessados.
    """
    missing = sorted({m.group(1) for m in _PLACEHOLDER.finditer(sql)} - params.keys())
    if missing:
        names = ", ".join(f"'${{{n}}}'" for n in missing)
        plural = "parâmetros" if len(missing) > 1 else "parâmetro"
        raise SqlError(f"{_where(source)}{plural} não definido(s): {names}", operation="render_sql")
    return substitute(sql, params)


@dataclass(frozen=True)
class Statement:
    """Instrução com o texto original e a linha (base 1) onde começa."""

    text: str
    line: int


def _tokens(sql: str, dialect: str, source: str | None) -> list[Token]:
    try:
        return Dialect.get_or_raise(dialect).tokenize(sql)
    except ValueError as exc:
        raise SqlError(f"{_where(source)}dialeto desconhecido '{dialect}'", operation="split_statements") from exc
    except SqlglotError as exc:
        # A mensagem do sqlglot repete trecho do SQL (pode ter valores); informamos só a linha.
        raise SqlError(
            f"{_where(source)}linha {_failing_line(sql, dialect)}: texto ou identificador sem fechamento",
            operation="split_statements",
        ) from exc


def _failing_line(sql: str, dialect: str) -> int:
    """Menor número de linhas cujo prefixo já não tokeniza (linha onde o erro começa)."""
    lines = sql.splitlines(keepends=True)
    tokenizer = Dialect.get_or_raise(dialect)
    for k in range(1, len(lines) + 1):
        try:
            tokenizer.tokenize("".join(lines[:k]))
        except SqlglotError:
            return k
    return max(len(lines), 1)


def split_with_lines(sql: str, dialect: str = "bigquery", *, source: str | None = None) -> list[Statement]:
    """Divide em instruções pelo `;` fora de strings, identificadores e comentários."""
    statements: list[Statement] = []
    current: list[Token] = []

    def flush() -> None:
        if current:
            first, last = current[0], current[-1]
            statements.append(Statement(sql[first.start : last.end + 1], first.line))
            current.clear()

    for token in _tokens(sql, dialect, source):
        if token.token_type == TokenType.SEMICOLON:
            flush()
        else:
            current.append(token)
    flush()
    return statements


def split_statements(sql: str, dialect: str = "bigquery") -> list[str]:
    """Instruções do script, sem comentários soltos nem instruções vazias."""
    return [s.text for s in split_with_lines(sql, dialect)]
