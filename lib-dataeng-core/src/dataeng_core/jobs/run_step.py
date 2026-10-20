"""Ponto de entrada de uma etapa (`python -m dataeng_core.jobs.run_step`) e utilitários de linha de comando.

Códigos de saída (valem também para `jobs.ingest` e para a CLI `dataeng`):

- 0: sucesso;
- 1: falha de execução ou de validação (erro de configuração, SQL inválido, etapa que falhou, erro inesperado);
- 2: uso incorreto da linha de comando (argumento ausente ou inválido, como `--param` sem `=`), como no argparse.

Nenhum erro mostra traceback ao usuário: `DataEngError` imprime título, o que aconteceu, como corrigir e a
referência (em stderr); uma exceção inesperada vira uma mensagem curta com referência, e o traceback, com
os segredos redigidos, vai só para o log.
"""

from __future__ import annotations

import argparse
import secrets as _secrets
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

from dataeng_core.base.errors import ConfigError, DataEngError
from dataeng_core.base.params import Environment, load_environment
from dataeng_core.base.pipeline import Pipeline, Step, load_pipeline
from dataeng_core.base.runtime import redact
from dataeng_core.steps.context import StepResult, build_context
from dataeng_core.steps.registry import log_unexpected, run_pipeline, step_secrets

PARAM_HELP = "parâmetro `chave=valor` (repetível; o valor pode conter '='); vale mais que o YAML e o ambiente"


def ensure_utf8() -> None:
    """Saída em UTF-8 mesmo em console/pipe do Windows com outra codificação (acentos do pt-BR)."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None and str(getattr(stream, "encoding", "")).lower().replace("-", "") != "utf8":
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except (OSError, ValueError):
                pass


def parse_param(text: str) -> tuple[str, str]:
    """Tipo do argparse para `--param chave=valor`: separa só no primeiro '='."""
    key, sep, value = text.partition("=")
    if not sep:
        raise argparse.ArgumentTypeError(
            f"'{text}' não está no formato chave=valor (ex.: --param data_inicio=2026-01-01)"
        )
    if not key.strip():
        raise argparse.ArgumentTypeError(f"'{text}' tem a chave vazia; use chave=valor com um nome de parâmetro")
    return key.strip(), value


def params_dict(pairs: Sequence[tuple[str, str]] | None) -> dict[str, str]:
    return dict(pairs or [])  # a última ocorrência da chave vence


def project_root_of(config: Path) -> Path:
    """Raiz do projeto: a pasta acima de `config/`, ou a do próprio arquivo (igual a `load_pipeline`)."""
    folder = config.resolve().parent
    return folder.parent if folder.name == "config" else folder


def load_project(config: Path, ambiente: str, params: dict[str, str]) -> tuple[Pipeline, Environment, Path]:
    """Carrega pipeline e ambiente; arquivo de configuração inexistente vira erro acionável."""
    if not config.is_file():
        raise ConfigError(
            f"arquivo de configuração não encontrado: {config}; informe o caminho do pipeline.yaml em --config",
            operation="cli",
        )
    pipeline = load_pipeline(config, ambiente=ambiente, extra_params=params)
    root = project_root_of(config)
    return pipeline, load_environment(root / "config" / "ambientes" / f"{ambiente}.yaml"), root


def print_error(exc: DataEngError, secrets: Sequence[str] = ()) -> None:
    """Erro tipado em stderr: título, o que aconteceu, detalhe, como corrigir e referência."""
    detail = exc.detail + (f" (operação: {exc.operation})" if exc.operation else "")
    lines = [f"ERRO: {exc.title}", f"O que aconteceu: {exc.what}"]
    if detail:
        lines.append(f"Detalhe: {detail}")
    lines += [f"Como corrigir: {exc.fix}", f"Referência: {exc.reference}"]
    print(redact("\n".join(lines), secrets), file=sys.stderr)


def print_unexpected(exc: BaseException, secrets: Sequence[str], where: str) -> None:
    """Exceção inesperada: mensagem curta com referência; o traceback (redigido) só vai para o log."""
    reference = _secrets.token_hex(4)
    log_unexpected(where, exc, secrets, where=f" [referência {reference}]")
    print(
        f"ERRO: Erro inesperado ({type(exc).__name__}) em {where}.\n"
        "Como corrigir: veja o log (stderr) com a referência abaixo; se persistir, reporte-a.\n"
        f"Referência: {reference}",
        file=sys.stderr,
    )


def guarded(body: Callable[[list[str]], int], where: str) -> int:
    """Roda `body` (que acrescenta à lista os segredos conhecidos) e converte erros em código de saída 1."""
    secrets: list[str] = []
    try:
        return body(secrets)
    except DataEngError as exc:
        print_error(exc, secrets)
    except Exception as exc:
        print_unexpected(exc, secrets, where)
    return 1


def parse_args(parser: argparse.ArgumentParser, argv: Sequence[str] | None) -> argparse.Namespace | int:
    """`parse_args` que devolve o código de saída (2 em uso incorreto, 0 em `--help`) em vez de `SystemExit`."""
    try:
        return parser.parse_args(argv)
    except SystemExit as exc:
        code = exc.code
        return code if isinstance(code, int) else (0 if code is None else 2)


def print_results(results: Sequence[StepResult], secrets: Sequence[str]) -> int:
    """Uma linha por etapa; falhas e não executadas em stderr, com como corrigir. Devolve 0 ou 1."""
    failed = False
    for r in results:
        if r.status == "ok":
            print(f"[ok] {r.step_id}: lidas={r.rows_read} gravadas={r.rows_written} rejeitadas={r.rows_rejected}")
        else:
            failed = failed or r.status == "falha"
            print(redact(f"[{r.status}] {r.step_id}: {r.detail}", secrets), file=sys.stderr)
    if failed:
        print(
            "Como corrigir: corrija a causa indicada acima e reexecute; a carga é idempotente por run_id "
            "(passe o mesmo --run-id para repetir a mesma tentativa).",
            file=sys.stderr,
        )
    return 1 if failed or any(r.status != "ok" for r in results) else 0


def execute(
    pipeline: Pipeline,
    env: Environment,
    root: Path,
    run_id: str | None,
    only: Sequence[str] | None,
    secrets: list[str],
) -> int:
    """Monta o contexto (storage e warehouse do ambiente), executa as etapas e fecha o warehouse."""
    for step in pipeline.etapas:
        secrets.extend(step_secrets(step))
    ctx = build_context(pipeline, env, project_root=root, run_id=run_id)
    try:
        results = run_pipeline(pipeline, ctx, only=only)
    finally:
        close = getattr(ctx.warehouse, "close", None)
        if callable(close):
            close()
    return print_results(results, secrets)


def run_job(argv: Sequence[str] | None, *, prog: str, description: str, required_type: str | None = None) -> int:
    """Corpo comum de `jobs.run_step` e `jobs.ingest`; `required_type` recusa etapas de outro tipo."""
    ensure_utf8()
    parser = argparse.ArgumentParser(prog=prog, description=description)
    parser.add_argument("--config", type=Path, required=True, help="caminho do config/pipeline.yaml")
    parser.add_argument("--ambiente", required=True, help="nome do ambiente (config/ambientes/<nome>.yaml)")
    parser.add_argument("--etapa", required=True, help="id da etapa a executar")
    parser.add_argument("--param", type=parse_param, action="append", metavar="CHAVE=VALOR", help=PARAM_HELP)
    parser.add_argument(
        "--run-id", help="identificador da execução (AAAAMMDD...; reutilize para repetir a mesma tentativa)"
    )
    args = parse_args(parser, argv)
    if isinstance(args, int):
        return args

    def body(secrets: list[str]) -> int:
        pipeline, env, root = load_project(args.config, args.ambiente, params_dict(args.param))
        step: Step = pipeline.get(args.etapa)
        secrets.extend(step_secrets(step))
        if required_type is not None and step.tipo != required_type:
            raise ConfigError(
                f"a etapa '{step.id}' é do tipo '{step.tipo}', mas este ponto de entrada só executa etapas do "
                f"tipo '{required_type}'; para outros tipos use `python -m dataeng_core.jobs.run_step`",
                operation=prog,
            )
        return execute(pipeline, env, root, args.run_id, [step.id], secrets)

    return guarded(body, args.etapa)


def main(argv: Sequence[str] | None = None) -> int:
    """Executa uma etapa de qualquer tipo. Veja os códigos de saída no módulo."""
    return run_job(argv, prog="run_step", description="Executa uma etapa do pipeline.")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
