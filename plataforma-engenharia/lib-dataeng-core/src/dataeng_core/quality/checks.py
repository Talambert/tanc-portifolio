"""Verificações de qualidade sobre um DataFrame, com limiar e bloqueio opcional.

Semântica (a mesma para todas as verificações):

- `taxa` é a fração de linhas que atendem à regra (0.0 a 1.0); a verificação é
  aprovada quando `taxa >= limiar`.
- Reprovar uma verificação é um *resultado* (`aprovado=False`). Não conseguir
  executá-la (coluna inexistente, regex inválida, tipo incompatível) é um
  *erro de execução*: levanta `QualityError` com a causa, sem valores dos dados.
- Tabela vazia: todas as verificações aprovam por vacuidade (`taxa=1.0`) e um
  aviso é registrado no log.
- `nao_nulo` conta os nulos. `faixa`, `conjunto` e `formato` ignoram nulos
  (use `nao_nulo` para exigi-los). `unico` trata chaves com algum nulo como
  distintas, como o `UNIQUE` do SQL. `formato` exige que a string inteira case
  com o padrão (`re.fullmatch`); valores que não são texto reprovam.
- Em `unico`, `taxa` = (linhas - linhas repetidas) / linhas, isto é, a primeira
  ocorrência de cada chave conta como válida.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

import pandas as pd

from dataeng_core.base.errors import QualityError
from dataeng_core.base.pipeline import QualityCheck
from dataeng_core.base.runtime import get_logger

log = get_logger(__name__)


@dataclass
class CheckResult:
    nome: str
    coluna: str | None
    aprovado: bool
    taxa: float
    limiar: float
    bloqueante: bool


def _fail_exec(check: QualityCheck, coluna: str, motivo: str) -> QualityError:
    return QualityError(
        f"a verificação '{check.tipo}' na coluna '{coluna}' não pôde ser executada: {motivo}",
        operation="run_checks",
    )


def _rate(valid: int, total: int) -> float:
    return 1.0 if total == 0 else valid / total


def _not_null(df: pd.DataFrame, check: QualityCheck, col: str) -> float:
    return _rate(int(df[col].notna().sum()), len(df))


def _unique(df: pd.DataFrame, check: QualityCheck, cols: list[str]) -> float:
    key = df[cols]
    repeated = key.duplicated(keep="first") & key.notna().all(axis=1)
    return _rate(len(df) - int(repeated.sum()), len(df))


def _range(df: pd.DataFrame, check: QualityCheck, col: str) -> float:
    s = df[col].dropna()
    if len(s) == 0:
        return 1.0
    if s.dtype == object:
        try:
            s = pd.to_numeric(s, errors="raise")
        except (ValueError, TypeError) as exc:
            raise _fail_exec(check, col, "`faixa` exige coluna numérica") from exc
    if not pd.api.types.is_numeric_dtype(s) or pd.api.types.is_bool_dtype(s):
        raise _fail_exec(check, col, "`faixa` exige coluna numérica")
    ok = pd.Series(True, index=s.index)
    if check.minimo is not None:
        ok &= s >= check.minimo
    if check.maximo is not None:
        ok &= s <= check.maximo
    return _rate(int(ok.sum()), len(s))


def _in_set(df: pd.DataFrame, check: QualityCheck, col: str) -> float:
    s = df[col].dropna()
    ok = s.isin(check.valores or [])
    return _rate(int(ok.sum()), len(s))


def _pattern(df: pd.DataFrame, check: QualityCheck, col: str) -> float:
    try:
        rx = re.compile(check.padrao or "")
    except re.error as exc:
        raise _fail_exec(check, col, f"o padrão é inválido ({exc.msg})") from exc
    s = df[col].dropna()
    ok = s.map(lambda v: isinstance(v, str) and rx.fullmatch(v) is not None)
    return _rate(int(ok.sum()), len(s))


_PER_COLUMN = {"nao_nulo": _not_null, "faixa": _range, "conjunto": _in_set, "formato": _pattern}


def _result(check: QualityCheck, coluna: str, taxa: float) -> CheckResult:
    return CheckResult(
        nome=check.tipo,
        coluna=coluna,
        aprovado=taxa >= check.limiar,
        taxa=taxa,
        limiar=check.limiar,
        bloqueante=check.bloqueante,
    )


def run_checks(df: pd.DataFrame, checks: Sequence[QualityCheck]) -> list[CheckResult]:
    """Executa as verificações, na ordem; `nao_nulo`/`faixa`/... geram um resultado por coluna."""
    missing = sorted({c for ch in checks for c in ch.colunas if c not in df.columns})
    if missing:
        nomes = ", ".join(f"'{c}'" for c in missing)
        raise QualityError(
            f"a verificação não pôde ser executada: a coluna {nomes} não existe nos dados",
            operation="run_checks",
        )
    duplicated = sorted(
        {c for ch in checks for c in ch.colunas if c in df.columns and df.columns.tolist().count(c) > 1}
    )
    if duplicated:
        nomes = ", ".join(f"'{c}'" for c in duplicated)
        raise QualityError(
            f"a verificação não pôde ser executada: o nome de coluna {nomes} está duplicado nos dados",
            operation="run_checks",
        )
    if len(df) == 0 and checks:
        log.warning("tabela vazia: as verificações de qualidade aprovam por vacuidade (0 linhas)")
    results: list[CheckResult] = []
    for check in checks:
        if check.tipo == "unico":
            results.append(_result(check, ", ".join(check.colunas), _unique(df, check, check.colunas)))
            continue
        per_column = _PER_COLUMN.get(check.tipo)
        if per_column is None:
            raise QualityError(
                f"tipo de verificação desconhecido: '{check.tipo}'; use nao_nulo, unico, faixa, conjunto ou formato",
                operation="run_checks",
            )
        for col in check.colunas:
            results.append(_result(check, col, per_column(df, check, col)))
    return results


def raise_if_blocking(results: Sequence[CheckResult]) -> None:
    """Levanta `QualityError` se alguma verificação bloqueante reprovou."""
    failed = [r for r in results if r.bloqueante and not r.aprovado]
    if not failed:
        return
    linhas = "; ".join(f"{r.nome} em '{r.coluna}' (taxa {r.taxa:.4g} < limiar {r.limiar:.4g})" for r in failed)
    raise QualityError(
        f"{len(failed)} verificação(ões) bloqueante(s) reprovada(s): {linhas}", operation="raise_if_blocking"
    )
