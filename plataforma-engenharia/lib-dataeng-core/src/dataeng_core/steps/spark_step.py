"""Etapa `spark`: script PySpark do projeto (`script`), com `spark-submit` local ou lote do Dataproc.

Modo `local` (`env.modo == "local"`): executa `spark-submit --master local[*]` sobre o script, sem shell (lista de
argumentos), com tempo limite (`dataproc.timeout_local`, em segundos; padrão 3600) e diretório de trabalho na raiz
do projeto. No tempo limite a árvore de processos inteira é encerrada (`taskkill /T /F` no Windows, grupo de
processos nos demais). No Windows, argumentos com `& | < > ^ %` são recusados (o `spark-submit.cmd` passa pelo
cmd.exe, que os interpretaria). O script recebe `--etapa <id> --run-id <run_id>` e um `--param chave=valor` por
parâmetro do contexto (os do pipeline e do ambiente, `run_id` e `ingestion_date`; nenhum segredo). `PYSPARK_PYTHON` e
`PYSPARK_DRIVER_PYTHON` apontam para este interpretador, se não estiverem definidos. A saída completa do processo
(stdout e stderr) vai para `<pasta_local>/logs/spark/<etapa>_<run_id>.log`; o resultado, o log da biblioteca e a
tabela de controle trazem só o código de saída, a referência e o caminho desse arquivo (nunca a saída, que pode
conter dados).

Modo `gcp`: a etapa roda como lote do Dataproc Serverless criado pela DAG (planejador de tarefas); este runner não
executa nada e devolve `nao_executada` (registrado no controle), para que nenhuma etapa conste como `ok` sem ter
rodado.

O caminho do script já é restrito à raiz do projeto por `load_pipeline`; o runner confere de novo (etapas montadas
sem passar pelo YAML).
"""

from __future__ import annotations

import os
import secrets as _secrets
import shutil
import signal
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from dataeng_core.base.errors import ConfigError
from dataeng_core.base.pipeline import SparkStep, Step
from dataeng_core.base.runtime import get_logger
from dataeng_core.steps.context import StepContext, StepResult
from dataeng_core.steps.registry import register_step_type

log = get_logger(__name__)

DEFAULT_TIMEOUT_S = 3600


def _script_path(step: SparkStep, root: Path) -> Path:
    rel = step.script.as_posix()
    script = (root / step.script).resolve()
    if not script.is_relative_to(root.resolve()):
        raise ConfigError(
            f"etapa '{step.id}': o script {rel[:120]} fica fora da raiz do projeto ({root}); "
            "use um caminho relativo dentro do projeto (ex.: spark/recalcula.py)",
            operation=f"spark.{step.id}",
        )
    if not script.is_file():
        raise ConfigError(
            f"etapa '{step.id}': script {rel} não encontrado em {root}; crie o arquivo ou corrija `script`",
            operation=f"spark.{step.id}",
        )
    return script


def _timeout(step: SparkStep) -> int:
    value = step.dataproc.get("timeout_local", DEFAULT_TIMEOUT_S)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigError(
            f"etapa '{step.id}': `dataproc.timeout_local` deve ser um inteiro positivo (segundos), "
            f"mas é {str(value)[:40]!r}",
            operation=f"spark.{step.id}",
        )
    return value


def _spark_submit(step: SparkStep) -> str:
    found = shutil.which("spark-submit")
    if found is None:
        raise ConfigError(
            f"etapa '{step.id}': spark-submit não encontrado no PATH; instale o extra [spark] "
            "(pip install 'dataeng-core[spark]', que traz o spark-submit) e um Java compatível "
            "(JAVA_HOME ou java no PATH), ou ponha o bin do Spark no PATH",
            operation=f"spark.{step.id}",
        )
    return found


def spark_submit_command(step: SparkStep, ctx: StepContext, submit: str, script: Path) -> list[str]:
    """Comando do modo local (lista de argumentos, sem shell)."""
    params = [arg for key, value in sorted(ctx.params.items()) for arg in ("--param", f"{key}={value}")]
    return [
        submit,
        "--master",
        "local[*]",
        "--name",
        f"dataeng-{step.id}",
        str(script),
        "--etapa",
        step.id,
        "--run-id",
        ctx.run_id,
        *params,
    ]


# o spark-submit do Windows é um .cmd: roda pelo cmd.exe, e `list2cmdline` não protege estes caracteres
_CMD_META = set("&|<>^%")


def _is_windows() -> bool:
    return sys.platform == "win32"


def _check_windows_args(step: SparkStep, cmd: Sequence[str]) -> None:
    bad = [arg for arg in cmd[1:] if _CMD_META.intersection(arg)]
    if bad:
        names = [a.split("=", 1)[0] if "=" in a else a[:60] for a in bad]
        raise ConfigError(
            f"etapa '{step.id}': no Windows o spark-submit roda pelo cmd.exe, que interpreta os caracteres "
            f"& | < > ^ %; eles aparecem em: {', '.join(names)}. Retire-os desses parâmetros ou caminhos "
            "(ou rode a etapa em Linux)",
            operation=f"spark.{step.id}",
        )


def _kill_tree(proc: subprocess.Popen[bytes]) -> None:
    """Encerra o spark-submit e os filhos (a JVM), não só o processo de entrada."""
    try:
        if _is_windows():
            subprocess.run(
                ["taskkill", "/T", "/F", "/PID", str(proc.pid)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=60,
            )
        else:
            os.killpg(proc.pid, getattr(signal, "SIGKILL", 9))  # type: ignore[attr-defined,unused-ignore]
    except Exception:  # falhar ao encerrar não pode esconder o resultado (tempo limite)
        pass
    try:
        proc.kill()
        proc.wait(timeout=60)
    except Exception:  # falhar ao encerrar não pode esconder o resultado (tempo limite)
        pass


def _run_local(step: SparkStep, ctx: StepContext) -> StepResult:
    rel = step.script.as_posix()
    script = _script_path(step, ctx.project_root)
    timeout = _timeout(step)
    cmd = spark_submit_command(step, ctx, _spark_submit(step), script)
    if _is_windows():
        _check_windows_args(step, cmd)
    env = dict(os.environ)
    env.setdefault("PYSPARK_PYTHON", sys.executable)
    env.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    log_file = Path(ctx.env.pasta_local) / "logs" / "spark" / f"{step.id}_{ctx.run_id}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    reference = _secrets.token_hex(4)
    with open(log_file, "wb") as out:
        # grupo de processos próprio: no tempo limite a árvore inteira (spark-submit e JVM) é encerrada
        group: dict[str, Any] = (
            {"creationflags": getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x200)}
            if _is_windows()
            else {"start_new_session": True}
        )
        try:
            proc = subprocess.Popen(cmd, cwd=ctx.project_root, env=env, stdout=out, stderr=subprocess.STDOUT, **group)
        except OSError as exc:
            log.error(f"etapa '{step.id}': spark-submit não iniciou ({type(exc).__name__}) [referência {reference}]")
            return StepResult(
                step.id,
                "falha",
                detail=(
                    f"não foi possível iniciar o spark-submit ({type(exc).__name__}, referência {reference}); "
                    "verifique a instalação do Spark e do Java"
                ),
            )
        try:
            returncode = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            _kill_tree(proc)
            log.error(
                f"etapa '{step.id}': spark-submit excedeu {timeout} s [referência {reference}]; saída em {log_file}"
            )
            return StepResult(
                step.id,
                "falha",
                detail=(
                    f"o script {rel} excedeu o tempo limite de {timeout} s e foi interrompido "
                    f"(referência {reference}); "
                    f"veja a saída em {log_file}; aumente `dataproc.timeout_local` ou otimize o script"
                ),
            )
    if returncode != 0:
        log.error(
            f"etapa '{step.id}': spark-submit terminou com código {returncode} [referência {reference}]; "
            f"saída em {log_file}"
        )
        return StepResult(
            step.id,
            "falha",
            detail=(
                f"o script {rel} falhou: spark-submit terminou com código {returncode} (referência {reference}); "
                f"veja a saída em {log_file}, corrija o script e reexecute a etapa"
            ),
        )
    return StepResult(
        step.id, "ok", detail=f"script {rel} executado com spark-submit (modo local); saída em {log_file}"
    )


def _run_spark_step(step: Step, ctx: StepContext) -> StepResult:
    if not isinstance(step, SparkStep):
        raise ConfigError(f"a etapa '{step.id}' não é do tipo spark", operation="spark")
    if ctx.env.modo != "local":
        return StepResult(
            step.id,
            "nao_executada",
            detail=(
                "modo gcp: a etapa spark roda como lote do Dataproc Serverless criado pela DAG "
                f"(veja `dataeng plan`); este processo não executa o script {step.script.as_posix()}"
            ),
        )
    return _run_local(step, ctx)


register_step_type("spark", _run_spark_step)
