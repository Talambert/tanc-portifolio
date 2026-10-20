"""Etapas `sql` e `view`: SQL do projeto, renderizado com `${param}`, validado e executado no warehouse.

Ordem de trabalho (etapa `sql`): lê, renderiza, divide e valida TODOS os arquivos antes de executar qualquer
instrução; um `${x}` indefinido, marcador malformado ou erro de sintaxe em qualquer arquivo/linha impede a
execução inteira. Depois executa uma instrução por vez (`Warehouse.run_sql`), na ordem declarada.

Atomicidade: o warehouse NÃO garante atomicidade entre instruções (no BigQuery cada instrução é um job
independente). Se a instrução N falha, as N-1 anteriores já foram aplicadas e permanecem; as seguintes não rodam.
O detalhe da falha informa quantas rodaram. Por isso o SQL do projeto deve ser idempotente (MERGE,
CREATE OR REPLACE, DELETE+INSERT); `validate_sql` avisa sobre INSERT que pode duplicar na reexecução. Os avisos vão
para o log e para o `detail` do resultado, e não falham a etapa.

Falha de instrução: o `detail` (e portanto o log, o CLI e a tabela de controle) traz só arquivo, número da
instrução, linha, quantas já rodaram, a CLASSE da exceção do warehouse e a referência do erro tipado. NUNCA o texto
da mensagem do warehouse, que pode citar valores de dados ou trechos do SQL. O diagnóstico fino exige reexecutar a
instrução manualmente no warehouse.

Arquivo sem nenhuma instrução (vazio ou só comentários) é `ConfigError`: quase sempre é erro de configuração,
e um no-op silencioso esconderia isso. Arquivos são lidos em UTF-8 (BOM aceito).

Dialeto: o SQL é validado como BigQuery, mas o `LocalWarehouse` o executa em DuckDB; use o subconjunto portátil
(crase e aspas simples escapadas com barra só funcionam no BigQuery).

Etapa `view`: o arquivo deve ter exatamente uma instrução, `CREATE OR REPLACE VIEW ...` ou um `SELECT` (com
`nome_view`, validado como `[dataset.]nome`). Se o arquivo traz `CREATE OR REPLACE VIEW` e `nome_view` também
foi informado, os nomes devem coincidir (sem diferenciar maiúsculas, aspas ou crase).
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


@dataclass(frozen=True)
class Script:
    """Arquivo SQL já renderizado e validado."""

    source: str
    sql: str
    statements: list[Statement]
    warnings: list[str]


def prepare_script(step_id: str, root: Path, rel: Path, params: dict[str, str]) -> Script:
    """Lê, renderiza, valida e divide um arquivo (usado pela execução e pelo `plan`/`validate-config`)."""
    source = Path(rel).as_posix()
    try:
        text = (root / rel).read_text(encoding="utf-8-sig")
    except (OSError, UnicodeDecodeError) as exc:
        raise ConfigError(
            f"etapa '{step_id}': não foi possível ler o arquivo SQL {source} ({type(exc).__name__})",
            operation=f"sql.{step_id}",
        ) from exc
    sql = render_sql(text, params, source=source)
    warnings = validate_sql(sql, _DIALECT, source=source)
    statements = split_with_lines(sql, _DIALECT, source=source)
    if not statements:
        raise ConfigError(
            f"etapa '{step_id}': o arquivo {source} não contém instruções (vazio ou só comentários); "
            "escreva ao menos uma instrução ou retire o arquivo da etapa",
            operation=f"sql.{step_id}",
        )
    return Script(source, sql, statements, warnings)


def _note(warnings: list[str]) -> str:
    for warning in warnings:
        log.warning(warning)
    return "".join(f"; aviso: {w}" for w in warnings)


def _failure(exc: DataEngError, secrets: list[str]) -> str:
    """Só a classe e a referência do erro: a mensagem do warehouse pode citar dados ou SQL."""
    return redact(f"{type(exc).__name__}, referência {exc.reference}", secrets)


def _run_sql_step(step: Step, ctx: StepContext) -> StepResult:
    assert isinstance(step, SqlStep)
    # tudo é lido, renderizado e validado antes de executar qualquer instrução
    scripts = [prepare_script(step.id, ctx.project_root, rel, ctx.params) for rel in step.sql]
    total = sum(len(s.statements) for s in scripts)
    secrets = step_secrets(step)
    done = 0
    for script in scripts:
        for number, statement in enumerate(script.statements, start=1):
            try:
                ctx.warehouse.run_sql([statement.text])
            except DataEngError as exc:
                return StepResult(
                    step.id,
                    "falha",
                    detail=(
                        f"arquivo {script.source}: instrução {number} de {len(script.statements)} "
                        f"(linha {statement.line}) falhou ({_failure(exc, secrets)}); {done} de {total} "
                        "instruções já executadas e não foram desfeitas (o warehouse não garante atomicidade); "
                        "as seguintes não rodaram. Reexecute a instrução manualmente no warehouse para ver o "
                        "erro completo, corrija o SQL e reexecute a etapa"
                    ),
                )
            done += 1
    warnings = [w for s in scripts for w in s.warnings]
    files = ", ".join(s.source for s in scripts)
    return StepResult(step.id, "ok", detail=f"{done} instruções executadas ({files}){_note(warnings)}")


def check_view_name(step: ViewStep) -> None:
    """`nome_view` deve ser `nome` ou `dataset.nome` com identificadores seguros."""
    if step.nome_view is not None:
        try:
            split_table(step.nome_view, operation=f"view.{step.id}")
        except ConfigError as exc:
            raise ConfigError(
                f"etapa '{step.id}': nome de view inválido: '{step.nome_view[:80]}'; use `nome` ou "
                "`dataset.nome` com letras, dígitos e '_', sem começar por dígito",
                operation=f"view.{step.id}",
            ) from exc


def _plain(name: str) -> str:
    return name.replace("`", "").replace('"', "").lower()


def inspect_view(step: ViewStep, script: Script) -> bool:
    """Confere que o arquivo tem uma instrução do tipo certo; devolve True se for CREATE OR REPLACE VIEW."""
    op = f"view.{step.id}"
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
    if isinstance(tree, exp.Create) and str(tree.args.get("kind", "")).upper() == "VIEW":
        if not tree.args.get("replace"):
            raise ConfigError(
                f"etapa '{step.id}': arquivo {script.source}: use CREATE OR REPLACE VIEW "
                "(CREATE VIEW falharia na reexecução)",
                operation=op,
            )
        target = tree.this.this if isinstance(tree.this, exp.Schema) else tree.this
        if step.nome_view is not None and isinstance(target, exp.Table):
            in_file = ".".join(part for part in (target.catalog, target.db, target.name) if part)
            if _plain(in_file) != _plain(step.nome_view):
                raise ConfigError(
                    f"etapa '{step.id}': o arquivo {script.source} cria a view '{in_file}', mas `nome_view` é "
                    f"'{step.nome_view}'; use o mesmo nome ou retire `nome_view`",
                    operation=op,
                )
        return True
    if isinstance(tree, exp.Query):
        if step.nome_view is None:
            raise ConfigError(
                f"etapa '{step.id}': o arquivo {script.source} traz só um SELECT; informe `nome_view` "
                "na etapa ou escreva CREATE OR REPLACE VIEW no arquivo",
                operation=op,
            )
        return False
    raise ConfigError(
        f"etapa '{step.id}': arquivo {script.source}: linha {statement.line}: a instrução não é "
        "CREATE OR REPLACE VIEW nem SELECT; para outras instruções use uma etapa sql",
        operation=op,
    )


def _run_view_step(step: Step, ctx: StepContext) -> StepResult:
    assert isinstance(step, ViewStep)
    check_view_name(step)  # identificador validado antes de tudo
    script = prepare_script(step.id, ctx.project_root, step.sql, ctx.params)
    is_view = inspect_view(step, script)
    statement = script.statements[0]
    try:
        if is_view:
            ctx.warehouse.run_sql([statement.text])
            what = "CREATE OR REPLACE VIEW executado"
        else:
            assert step.nome_view is not None
            ctx.warehouse.create_or_replace_view(step.nome_view, statement.text)
            what = f"view {step.nome_view} criada ou substituída"
    except DataEngError as exc:
        return StepResult(
            step.id,
            "falha",
            detail=(
                f"arquivo {script.source}: a view não pôde ser criada ({_failure(exc, step_secrets(step))}); "
                "reexecute a instrução manualmente no warehouse para ver o erro completo, corrija o SQL e reexecute"
            ),
        )
    return StepResult(step.id, "ok", detail=f"{what} ({script.source}){_note(script.warnings)}")


register_step_type("sql", _run_sql_step)
register_step_type("view", _run_view_step)
