"""CLI `dataeng`: `validate-config`, `plan` e `run local`.

Códigos de saída: 0 sucesso; 1 falha de validação/execução; 2 uso incorreto (argparse). Detalhes em
`dataeng_core.jobs.run_step`.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from collections.abc import Sequence
from pathlib import Path

from dataeng_core.base.errors import ConfigError
from dataeng_core.base.params import Environment
from dataeng_core.base.pipeline import IngestaoStep, Pipeline, SqlStep, Step, ViewStep
from dataeng_core.base.runtime import new_run_id
from dataeng_core.jobs.run_step import (
    PARAM_HELP,
    ensure_utf8,
    execute,
    guarded,
    load_project,
    params_dict,
    parse_args,
    parse_param,
)
from dataeng_core.sql.render import render_sql
from dataeng_core.sql.validate import validate_sql
from dataeng_core.steps.context import resolve_ingestion_date, validate_run_id
from dataeng_core.steps.registry import step_secrets


def _sql_files(step: Step) -> list[tuple[str, Path]]:
    """Arquivos SQL da etapa, com o papel de cada um."""
    if isinstance(step, SqlStep):
        return [("sql", p) for p in step.sql]
    if isinstance(step, ViewStep):
        return [("view", step.sql)]
    if isinstance(step, IngestaoStep):
        return [("ddl", p) for p in step.destino.ddl] + [("dml_pos_carga", p) for p in step.destino.dml_pos_carga]
    return []


def _render_params(pipeline: Pipeline, env: Environment, run_id: str) -> dict[str, str]:
    """Parâmetros de um contexto de execução (como em `build_context`), sem criar storage nem warehouse."""
    day = dt.datetime.strptime(run_id[:8], "%Y%m%d").date().isoformat()
    return {**env.params(), **pipeline.parametros, "run_id": run_id, "ingestion_date": day}


def _rendered(root: Path, rel: Path, params: dict[str, str]) -> tuple[str, list[str]]:
    """SQL renderizado e validado, com os avisos de idempotência (já citam arquivo e linha).

    Parâmetro indefinido ou sintaxe inválida levanta `SqlError` com arquivo e linha. Nada é executado."""
    source = rel.as_posix()
    try:
        text = (root / rel).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise ConfigError(
            f"não foi possível ler o arquivo SQL {source} ({type(exc).__name__})", operation="cli"
        ) from exc
    sql = render_sql(text, params, source=source)
    return sql, validate_sql(sql, source=source)


def _run_id_of(value: str | None) -> str:
    run_id = validate_run_id(value) if value is not None else new_run_id()
    resolve_ingestion_date(run_id, None)  # exige a data no início do run_id, como a execução
    return run_id


def _cmd_validate(args: argparse.Namespace) -> int:
    def body(secrets: list[str]) -> int:
        pipeline, env, root = load_project(args.config, args.ambiente, params_dict(args.param))
        params = _render_params(pipeline, env, new_run_id())
        for step in pipeline.ordered_steps():
            secrets.extend(step_secrets(step))
            for _role, rel in _sql_files(step):
                for warning in _rendered(root, rel, params)[1]:
                    print(f"aviso: {warning}")
        n = len(pipeline.etapas)
        print(f"Configuração válida: pipeline '{pipeline.nome}', {n} etapas, ambiente '{args.ambiente}'.")
        return 0

    return guarded(body, "validate-config")


def _cmd_plan(args: argparse.Namespace) -> int:
    def body(secrets: list[str]) -> int:
        pipeline, env, root = load_project(args.config, args.ambiente, params_dict(args.param))
        params = _render_params(pipeline, env, _run_id_of(args.run_id))
        blocks: list[str] = []
        for number, step in enumerate(pipeline.ordered_steps(), start=1):
            secrets.extend(step_secrets(step))
            deps = f"  depende_de={','.join(step.depende_de)}" if step.depende_de else ""
            lines = [f"{number}. {step.id}  tipo={step.tipo}  motor={step.motor or 'bigquery'}{deps}"]
            for role, rel in _sql_files(step):
                lines.append(f"   SQL ({role}) {rel.as_posix()}:")
                sql, warnings = _rendered(root, rel, params)
                lines += [f"     {line}" for line in sql.rstrip().splitlines()]
                lines += [f"   aviso: {warning}" for warning in warnings]
            blocks.append("\n".join(lines))
        print(f"Plano do pipeline '{pipeline.nome}' (ambiente '{args.ambiente}'), em ordem de execução:")
        print("\n".join(blocks))
        return 0

    return guarded(body, "plan")


def _cmd_run_local(args: argparse.Namespace) -> int:
    def body(secrets: list[str]) -> int:
        pipeline, env, root = load_project(args.config, "local", params_dict(args.param))
        return execute(pipeline, env, root, args.run_id, None, secrets)

    return guarded(body, "run local")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dataeng", description="CLI do dataeng-core (códigos de saída: 0 ok, 1 falha, 2 uso)."
    )
    sub = parser.add_subparsers(dest="command", required=True, metavar="comando")

    def common(p: argparse.ArgumentParser, *, ambiente: bool) -> None:
        p.add_argument("--config", type=Path, required=True, help="caminho do config/pipeline.yaml")
        if ambiente:
            p.add_argument("--ambiente", default="local", help="ambiente (config/ambientes/<nome>.yaml); padrão: local")
        p.add_argument("--param", type=parse_param, action="append", metavar="CHAVE=VALOR", help=PARAM_HELP)

    validate = sub.add_parser("validate-config", help="valida o YAML, os arquivos referenciados e o SQL")
    common(validate, ambiente=True)
    validate.set_defaults(func=_cmd_validate)

    plan = sub.add_parser("plan", help="mostra a ordem das etapas, o motor e o SQL já renderizado")
    common(plan, ambiente=True)
    plan.add_argument("--run-id", help="run_id usado na renderização do SQL (AAAAMMDD...); padrão: um novo")
    plan.set_defaults(func=_cmd_plan)

    run = sub.add_parser("run", help="executa o pipeline")
    run_sub = run.add_subparsers(dest="target", required=True, metavar="destino")
    local = run_sub.add_parser("local", help="executa todas as etapas no ambiente local (DuckDB e pasta local)")
    common(local, ambiente=False)
    local.add_argument("--run-id", help="run_id da execução (AAAAMMDD...); reutilize para repetir a mesma tentativa")
    local.set_defaults(func=_cmd_run_local)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    ensure_utf8()
    args = parse_args(build_parser(), argv)
    if isinstance(args, int):
        return args
    return int(args.func(args))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
