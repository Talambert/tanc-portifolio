"""Etapas `sql` e `view`: SQL do projeto, renderizado com `${param}`, validado e executado no warehouse.

Ordem de trabalho (etapa `sql`): lê, renderiza, divide e valida TODOS os arquivos antes de executar qualquer
instrução; um `${x}` indefinido, marcador malformado ou erro de sintaxe em qualquer arquivo/linha impede a
execução inteira. Depois executa uma instrução por vez (`Warehouse.run_sql`), na ordem declarada.

Atomicidade: o warehouse NÃO garante atomicidade entre instruções (no BigQuery cada instrução é um job
independente). Se a instrução N falha, as N-1 anteriores já foram aplicadas e permanecem; as seguintes não rodam.
O detalhe da falha informa quantas rodaram. Por isso o SQL do projeto deve ser idempotente (MERGE,
CREATE OR REPLACE, DELETE+INSERT); `validate_sql` avisa sobre INSERT que pode duplicar na reexecução. Os avisos vão
para o log e para o `detail` do resultado, e não falham a etapa.

Arquivo sem nenhuma instrução (vazio ou só comentários) é `ConfigError`: quase sempre é erro de configuração,
e um no-op silencioso esconderia isso.

Etapa `view`: o arquivo deve ter exatamente uma instrução, `CREATE OR REPLACE VIEW ...` ou um `SELECT` (com
`nome_view`, validado como `[dataset.]nome`). Um `CREATE OR REPLACE VIEW` ignora `nome_view`.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from dataeng_core.base.errors import ConfigError, DataEngError
from dataeng_core.base.pipeline import SqlStep, Step, ViewStep
from dataeng_core.base.runtime import get_logger, redact
from dataeng_core.sql.render import Statement, render_sql, split_with_lines
from dataeng_core.sql.validate import validate_sql
from dataeng_core.steps.context import StepContext, StepResult
from dataeng_core.steps.registry import register_step_type, step_secrets
from dataeng_core.warehouse.base import split_table

log = get_logger(__name__)

_DIALECT = "bigquery"
_MAX_MESSAGE = 300


@dataclass(frozen=True)
class _Script:
    """Arquivo SQL já renderizado e validado."""

    source: str
    statements: list[Statement]
    warnings: list[str]


def _prepare(step_id: str, ctx: StepContext, rel: Path) -> _Script:
    source = Path(rel).as_posix()
    try:
        text = (ctx.project_root / rel).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise ConfigError(
            f"etapa '{step_id}': não foi possível ler o arquivo SQL {source} ({type(exc).__name__})",
            operation=f"sql.{step_id}",
        ) from exc
    sql = render_sql(text, ctx.params, source=source)
    warnings = validate_sql(sql, _DIALECT, source=source)
    statements = split_with_lines(sql, _DIALECT, source=source)
    if not statements:
        raise ConfigError(
            f"etapa '{step_id}': o arquivo {source} não contém instruções (vazio ou só comentários); "
            "escreva ao menos uma instrução ou retire o arquivo da etapa",
            operation=f"sql.{step_id}",
        )
    return _Script(source, statements, warnings)


def _note(warnings: list[str]) -> str:
    for warning in warnings:
        log.warning(warning)
    return "".join(f"; aviso: {w}" for w in warnings)


def _run_sql_step(step: Step, ctx: StepContext) -> StepResult:
    assert isinstance(step, SqlStep)
    scripts = [_prepare(step.id, ctx, rel) for rel in step.sql]  # tudo validado antes de executar qualquer coisa
    total = sum(len(s.statements) for s in scripts)
    secrets = step_secrets(step)
    done = 0
    for script in scripts:
        for number, statement in enumerate(script.statements, start=1):
            try:
                ctx.warehouse.run_sql([statement.text])
            except DataEngError as exc:
                reason = redact(exc.detail or exc.what, secrets)[:_MAX_MESSAGE]
                return StepResult(
                    step.id,
                    "falha",
                    detail=(
                        f"arquivo {script.source}: instrução {number} de {len(script.statements)} "
                        f"(linha {statement.line}) falhou: {reason}. {done} de {total} instruções já executadas "
                        "e não foram desfeitas (o warehouse não garante atomicidade); as seguintes não rodaram. "
                        "Corrija o SQL e reexecute a etapa"
                    ),
                )
            done += 1
    warnings = [w for s in scripts for w in s.warnings]
    files = ", ".join(s.source for s in scripts)
    return StepResult(step.id, "ok", detail=f"{done} instruções executadas ({files}){_note(warnings)}")


def _run_view_step(step: Step, ctx: StepContext) -> StepResult:
    assert isinstance(step, ViewStep)
    op = f"view.{step.id}"
    if step.nome_view is not None:
        split_table(step.nome_view, operation=op)  # identificador validado antes de tudo
    script = _prepare(step.id, ctx, step.sql)
    if len(script.statements) != 1:
        raise ConfigError(
            f"etapa '{step.id}': o arquivo {script.source} tem {len(script.statements)} instruções; "
            "uma etapa view exige exatamente uma (CREATE OR REPLACE VIEW ou SELECT)",
            operation=op,
        )
    statement = script.statements[0]
    try:
        tree = sqlglot.parse_one(statement.text, read=_DIALECT)
    except SqlglotError as exc:  # já validado; defesa
        raise ConfigError(
            f"etapa '{step.id}': arquivo {script.source}: linha {statement.line}: sintaxe inválida", operation=op
        ) from exc
    is_view = isinstance(tree, exp.Create) and str(tree.args.get("kind", "")).upper() == "VIEW"
    if is_view:
        if not tree.args.get("replace"):
            raise ConfigError(
                f"etapa '{step.id}': arquivo {script.source}: use CREATE OR REPLACE VIEW "
                "(CREATE VIEW falharia na reexecução)",
                operation=op,
            )
    elif isinstance(tree, exp.Query):
        if step.nome_view is None:
            raise ConfigError(
                f"etapa '{step.id}': o arquivo {script.source} traz só um SELECT; informe `nome_view` "
                "na etapa ou escreva CREATE OR REPLACE VIEW no arquivo",
                operation=op,
            )
    else:
        raise ConfigError(
            f"etapa '{step.id}': arquivo {script.source}: linha {statement.line}: a instrução não é "
            "CREATE OR REPLACE VIEW nem SELECT; para outras instruções use uma etapa sql",
            operation=op,
        )
    try:
        if is_view:
            ctx.warehouse.run_sql([statement.text])
            what = "CREATE OR REPLACE VIEW executado"
        else:
            assert step.nome_view is not None
            ctx.warehouse.create_or_replace_view(step.nome_view, statement.text)
            what = f"view {step.nome_view} criada ou substituída"
    except DataEngError as exc:
        reason = redact(exc.detail or exc.what, step_secrets(step))[:_MAX_MESSAGE]
        return StepResult(
            step.id,
            "falha",
            detail=f"arquivo {script.source}: a view não pôde ser criada: {reason}. Corrija o SQL e reexecute",
        )
    return StepResult(step.id, "ok", detail=f"{what} ({script.source}){_note(script.warnings)}")


register_step_type("sql", _run_sql_step)
register_step_type("view", _run_view_step)
