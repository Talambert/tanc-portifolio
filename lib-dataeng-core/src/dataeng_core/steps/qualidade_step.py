"""Etapa `qualidade`: verificações sobre tabelas que já estão no warehouse.

Para cada tabela citada em `verificacoes[].tabela`, a etapa faz UMA consulta (`Warehouse.query`) com só as colunas
que as verificações daquela tabela usam, roda `run_checks` sobre o DataFrame e grava os resultados no controle.

Custo e memória: a consulta é `SELECT <colunas> FROM <tabela>` SEM `LIMIT` nem amostragem. No BigQuery o custo
é o volume das colunas lidas (um `LIMIT` não reduziria o que é cobrado) e o resultado inteiro vai para a memória
do processo (pandas). O modelo (`QualityCheck`) não tem campo de amostra, então a etapa não oferece um; em tabelas
muito grandes, verifique uma view/tabela já filtrada por partição (a `tabela` pode ser uma view).

Segurança: nome de tabela e de colunas passam por `split_table`/`validate_identifier` ANTES de qualquer consulta;
nada vindo dos dados entra no SQL. Identificadores são usados sem aspas (só ASCII, `[A-Za-z_][A-Za-z0-9_]*`): uma
coluna com nome reservado do SQL (ex.: `order`) faz a consulta falhar, e o erro é informado.

Resultado: `StepResult.quality` traz uma `CheckResult` por verificação/coluna (nome, coluna, aprovado, taxa, limiar,
bloqueante). O `detail` (log, CLI e tabela de controle) traz só nomes de tabela, verificação e coluna, taxas e
limiares, a fase, a referência do erro e, nas falhas do warehouse, a CLASSE da exceção; nunca valores das linhas nem
o texto de exceção do warehouse (que pode citar dados ou SQL).

- Verificação bloqueante reprovada (`raise_if_blocking`): etapa `falha`, mas todas as tabelas são avaliadas antes e
  todos os resultados ficam no controle.
- Verificação que não pôde executar (coluna inexistente, regex inválida, tipo incompatível: `QualityError`), tabela
  inexistente ou consulta que falhou: etapa `falha` na hora, com os resultados já obtidos.
- Verificação NÃO bloqueante reprovada: etapa `ok`, com aviso no `detail` e no log.
- Tabela vazia aprova por vacuidade (regra de `run_checks`), com aviso no log.
"""

from __future__ import annotations

import secrets as _secrets
from collections.abc import Sequence

import pandas as pd

from dataeng_core.base.errors import ConfigError, DataEngError, QualityError
from dataeng_core.base.pipeline import QualidadeStep, QualityCheck, Step
from dataeng_core.base.runtime import get_logger, redact
from dataeng_core.quality.checks import CheckResult, raise_if_blocking, run_checks
from dataeng_core.steps.context import StepContext, StepResult
from dataeng_core.steps.registry import log_unexpected, register_step_type, step_secrets
from dataeng_core.warehouse.base import split_table, validate_identifier

log = get_logger(__name__)

_MAX_LISTED = 20


def _failure(
    step_id: str, phase: str, message: str, reference: str | None, quality: list[CheckResult], rows_read: int = 0
) -> StepResult:
    reference = reference or _secrets.token_hex(4)  # erros sem exceção tipada também ganham referência
    return StepResult(
        step_id,
        "falha",
        rows_read=rows_read,
        quality=quality,
        detail=f"fase {phase}: {message} (referência {reference}); corrija e reexecute a etapa",
    )


def _group_by_table(step: QualidadeStep) -> dict[str, list[QualityCheck]]:
    """Valida os identificadores (antes de qualquer consulta) e agrupa as verificações por tabela, em ordem."""
    operation = f"qualidade.{step.id}"
    groups: dict[str, list[QualityCheck]] = {}
    for check in step.verificacoes:
        assert check.tabela is not None  # garantido pelo modelo
        schema, name = split_table(check.tabela, operation=operation)
        for column in check.colunas:
            validate_identifier(column, what="nome de coluna", operation=operation)
        groups.setdefault(f"{schema}.{name}" if schema else name, []).append(check)
    return groups


def _columns_of(checks: Sequence[QualityCheck]) -> str:
    return ", ".join(dict.fromkeys(c for check in checks for c in check.colunas))


def _query_text(table: str, checks: Sequence[QualityCheck]) -> str:
    return f"SELECT {_columns_of(checks)} FROM {table}"


def _with_requested_names(df: pd.DataFrame, checks: Sequence[QualityCheck]) -> pd.DataFrame:
    """Nomes de coluna como no YAML (o warehouse pode devolver outra caixa)."""
    wanted = {c.lower(): c for check in checks for c in check.colunas}
    return df.set_axis([wanted.get(str(c).lower(), c) for c in df.columns], axis="columns")


def _describe(result: CheckResult) -> str:
    return f"{result.nome} em '{result.coluna}' (taxa {result.taxa:.4g}, limiar {result.limiar:.4g})"


def _listed(items: list[str]) -> str:
    shown = "; ".join(items[:_MAX_LISTED])
    return shown + (f"; e mais {len(items) - _MAX_LISTED}" if len(items) > _MAX_LISTED else "")


def _run_quality_step(step: Step, ctx: StepContext) -> StepResult:
    if not isinstance(step, QualidadeStep):
        raise ConfigError(f"a etapa '{step.id}' não é do tipo qualidade", operation="qualidade")
    secrets = step_secrets(step)
    try:
        groups = _group_by_table(step)
    except ConfigError as exc:
        return _failure(step.id, "validação", redact(exc.detail, secrets), exc.reference, [])

    all_results: list[CheckResult] = []
    blocking: list[str] = []
    block_ref: str | None = None
    warnings: list[str] = []
    rows_read = 0
    for table, checks in groups.items():
        try:
            if not ctx.warehouse.table_exists(table):
                return _failure(
                    step.id,
                    "consulta",
                    f"a tabela '{table}' não existe; crie-a antes (etapa anterior do pipeline) ou corrija `tabela`",
                    None,
                    all_results,
                )
            frame = ctx.warehouse.query(_query_text(table, checks))
        except DataEngError as exc:  # o texto do warehouse pode citar dados ou SQL: só a classe
            return _failure(
                step.id,
                "consulta",
                f"a consulta à tabela '{table}' falhou ({type(exc).__name__}); confira se as colunas "
                f"({_columns_of(checks)}) existem na tabela e não são palavras reservadas do SQL",
                exc.reference,
                all_results,
            )
        except Exception as exc:
            log_unexpected(step.id, exc, secrets, where=f" ao consultar a tabela {table}")
            return _failure(
                step.id,
                "consulta",
                f"a consulta à tabela '{table}' falhou ({type(exc).__name__}); veja o log",
                None,
                all_results,
            )
        rows_read += len(frame)
        try:
            results = run_checks(_with_requested_names(frame, checks), checks)
        except QualityError as exc:  # texto montado pela biblioteca: nomes e motivo, sem valores dos dados
            return _failure(
                step.id,
                "verificação",
                f"tabela '{table}': {redact(exc.detail, secrets)}",
                exc.reference,
                all_results,
                rows_read,
            )
        all_results += results
        try:
            raise_if_blocking(results)
        except QualityError as exc:
            blocking.append(f"tabela '{table}': {exc.detail}")
            block_ref = block_ref or exc.reference
        warnings += [f"tabela '{table}': {_describe(r)}" for r in results if not r.aprovado and not r.bloqueante]

    if blocking:
        return _failure(step.id, "verificação", _listed(blocking), block_ref, all_results, rows_read)
    for warning in warnings:
        log.warning(f"etapa '{step.id}': verificação não bloqueante reprovada, {warning}")
    detail = f"{len(all_results)} verificações em {len(groups)} tabela(s) executadas"
    if warnings:
        detail += f"; aviso: {len(warnings)} verificação(ões) não bloqueante(s) reprovada(s): {_listed(warnings)}"
    return StepResult(step.id, "ok", rows_read=rows_read, quality=all_results, detail=detail)


register_step_type("qualidade", _run_quality_step)
