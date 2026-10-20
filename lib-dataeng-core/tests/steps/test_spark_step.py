"""Runner `spark` sem Spark: `spark-submit` e `subprocess.run` falsos (o real roda só no CI, marca `spark`)."""

from __future__ import annotations

import dataclasses
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from tests.steps.conftest import MakeCtx
from tests.steps.helpers import RID1, pipeline, rows

from dataeng_core.base.pipeline import Pipeline
from dataeng_core.steps import run_step, spark_step
from dataeng_core.steps.registry import registered_types

SUBMIT = "/opt/spark/bin/spark-submit"


class FakeRun:
    """Substitui `subprocess.run`: registra a chamada, escreve a saída no arquivo recebido e devolve o código."""

    def __init__(self, returncode: int = 0, output: str = "", raises: BaseException | None = None) -> None:
        self.returncode = returncode
        self.output = output
        self.raises = raises
        self.calls: list[tuple[list[str], dict[str, Any]]] = []

    def __call__(self, cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        self.calls.append((list(cmd), kwargs))
        out = kwargs.get("stdout")
        if out is not None and self.output:
            out.write(self.output.encode("utf-8"))
            out.flush()
        if self.raises is not None:
            raise self.raises
        return subprocess.CompletedProcess(cmd, self.returncode)


@pytest.fixture
def fake_submit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(spark_step.shutil, "which", lambda name: SUBMIT if name == "spark-submit" else None)


def _spark_pipeline(**extra: Any) -> Pipeline:
    return pipeline({"id": "recalcula", "tipo": "spark", "script": "spark/recalcula.py", **extra})


def _script(project: Path) -> Path:
    (project / "spark").mkdir(exist_ok=True)
    path = project / "spark" / "recalcula.py"
    path.write_text("print('ok')\n", encoding="utf-8")
    return path


def _log_path(project: Path) -> Path:
    return project / ".local" / "logs" / "spark" / f"recalcula_{RID1}.log"


def _status(ctx: Any) -> list[tuple[Any, ...]]:
    return rows(ctx, "SELECT etapa, status FROM silver._controle_execucoes")


def test_spark_type_is_registered() -> None:
    assert "spark" in registered_types()


def test_local_mode_runs_spark_submit_with_exact_command(
    project: Path, make_ctx: MakeCtx, fake_submit: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = _script(project)
    monkeypatch.delenv("PYSPARK_PYTHON", raising=False)
    monkeypatch.delenv("PYSPARK_DRIVER_PYTHON", raising=False)
    fake = FakeRun(output="linha de log do spark\n")
    monkeypatch.setattr(spark_step.subprocess, "run", fake)
    pipe = _spark_pipeline()
    ctx = make_ctx(pipe)

    result = run_step(pipe.get("recalcula"), ctx)

    assert result.status == "ok"
    log = _log_path(project)
    assert result.detail == f"script spark/recalcula.py executado com spark-submit (modo local); saída em {log}"
    assert len(fake.calls) == 1
    cmd, kwargs = fake.calls[0]
    params = [arg for k, v in sorted(ctx.params.items()) for arg in ("--param", f"{k}={v}")]
    assert cmd == [
        SUBMIT,
        "--master",
        "local[*]",
        "--name",
        "dataeng-recalcula",
        str(script.resolve()),
        "--etapa",
        "recalcula",
        "--run-id",
        RID1,
        *params,
    ]
    assert "--param" in cmd and f"run_id={RID1}" in cmd and "ingestion_date=2026-01-01" in cmd
    assert kwargs["cwd"] == project.resolve()
    assert kwargs["timeout"] == spark_step.DEFAULT_TIMEOUT_S == 3600
    assert kwargs.get("shell", False) is False
    assert kwargs["stderr"] == subprocess.STDOUT
    assert kwargs["env"]["PYSPARK_PYTHON"] == sys.executable
    assert kwargs["env"]["PYSPARK_DRIVER_PYTHON"] == sys.executable
    assert log.read_text(encoding="utf-8") == "linha de log do spark\n"
    assert _status(ctx) == [("recalcula", "ok")]


def test_local_mode_keeps_user_pyspark_python(
    project: Path, make_ctx: MakeCtx, fake_submit: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    _script(project)
    monkeypatch.setenv("PYSPARK_PYTHON", "/usr/bin/python3.11")
    fake = FakeRun()
    monkeypatch.setattr(spark_step.subprocess, "run", fake)
    pipe = _spark_pipeline()
    run_step(pipe.get("recalcula"), make_ctx(pipe))
    assert fake.calls[0][1]["env"]["PYSPARK_PYTHON"] == "/usr/bin/python3.11"


def test_process_failure_is_failed_with_reference_and_no_raw_output(
    project: Path, make_ctx: MakeCtx, fake_submit: None, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    _script(project)
    fake = FakeRun(returncode=3, output="Traceback: valor 123.456.789-00 inválido\n")
    monkeypatch.setattr(spark_step.subprocess, "run", fake)
    pipe = _spark_pipeline()
    ctx = make_ctx(pipe)

    result = run_step(pipe.get("recalcula"), ctx)

    assert result.status == "falha"
    log = _log_path(project)
    m = re.fullmatch(
        r"o script spark/recalcula\.py falhou: spark-submit terminou com código 3 \(referência ([0-9a-f]{8})\); "
        + re.escape(f"veja a saída em {log}, corrija o script e reexecute a etapa"),
        result.detail,
    )
    assert m, result.detail
    err = capsys.readouterr().err
    assert m.group(1) in err  # a referência vai para o log
    for text in (result.detail, err, str(_status(ctx))):
        assert "123.456" not in text
    assert "123.456" in log.read_text(encoding="utf-8")  # a saída completa fica só no arquivo local
    assert _status(ctx) == [("recalcula", "falha")]


def test_timeout_is_failed_with_reference(
    project: Path, make_ctx: MakeCtx, fake_submit: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    _script(project)
    fake = FakeRun(raises=subprocess.TimeoutExpired(cmd="spark-submit", timeout=5))
    monkeypatch.setattr(spark_step.subprocess, "run", fake)
    pipe = _spark_pipeline(dataproc={"runtime": "2.2", "timeout_local": 5})

    result = run_step(pipe.get("recalcula"), make_ctx(pipe))

    assert fake.calls[0][1]["timeout"] == 5
    assert result.status == "falha"
    assert re.fullmatch(
        r"o script spark/recalcula\.py excedeu o tempo limite de 5 s e foi interrompido "
        r"\(referência [0-9a-f]{8}\); "
        + re.escape(f"veja a saída em {_log_path(project)}; aumente `dataproc.timeout_local` ou otimize o script"),
        result.detail,
    )


@pytest.mark.parametrize("value", [0, -1, "dez", True, 1.5])
def test_invalid_timeout_is_config_error(
    project: Path, make_ctx: MakeCtx, fake_submit: None, monkeypatch: pytest.MonkeyPatch, value: Any
) -> None:
    _script(project)
    fake = FakeRun()
    monkeypatch.setattr(spark_step.subprocess, "run", fake)
    pipe = _spark_pipeline(dataproc={"timeout_local": value})
    result = run_step(pipe.get("recalcula"), make_ctx(pipe))
    assert result.status == "falha"
    assert "dataproc.timeout_local" in result.detail and "inteiro positivo" in result.detail
    assert fake.calls == []


def test_launch_error_is_failed_without_message(
    project: Path, make_ctx: MakeCtx, fake_submit: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    _script(project)
    monkeypatch.setattr(spark_step.subprocess, "run", FakeRun(raises=PermissionError("acesso negado a /segredo")))
    pipe = _spark_pipeline()
    result = run_step(pipe.get("recalcula"), make_ctx(pipe))
    assert result.status == "falha"
    assert re.fullmatch(
        r"não foi possível iniciar o spark-submit \(PermissionError, referência [0-9a-f]{8}\); "
        r"verifique a instalação do Spark e do Java",
        result.detail,
    )


def test_missing_spark_submit_is_actionable(project: Path, make_ctx: MakeCtx, monkeypatch: pytest.MonkeyPatch) -> None:
    _script(project)
    monkeypatch.setattr(spark_step.shutil, "which", lambda name: None)
    fake = FakeRun()
    monkeypatch.setattr(spark_step.subprocess, "run", fake)
    pipe = _spark_pipeline()
    result = run_step(pipe.get("recalcula"), make_ctx(pipe))
    assert result.status == "falha"
    assert "spark-submit não encontrado" in result.detail and "dataeng-core[spark]" in result.detail
    assert "Java" in result.detail
    assert fake.calls == []


def test_script_outside_root_is_refused_before_running(
    project: Path, make_ctx: MakeCtx, fake_submit: None, monkeypatch: pytest.MonkeyPatch, tmp_path_factory: Any
) -> None:
    fora = tmp_path_factory.mktemp("fora") / "x.py"
    fora.write_text("print(1)\n", encoding="utf-8")
    fake = FakeRun()
    monkeypatch.setattr(spark_step.subprocess, "run", fake)
    pipe = _spark_pipeline()
    ctx = make_ctx(pipe)
    for script in (Path("../fora/x.py"), fora):  # o modelo direto não passa por load_pipeline
        step = pipe.get("recalcula").model_copy(update={"script": script})
        result = run_step(step, ctx)
        assert result.status == "falha"
        assert "fora da raiz do projeto" in result.detail and "recalcula" in result.detail
    assert fake.calls == []


def test_missing_script_is_actionable(
    project: Path, make_ctx: MakeCtx, fake_submit: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeRun()
    monkeypatch.setattr(spark_step.subprocess, "run", fake)
    pipe = _spark_pipeline()
    result = run_step(pipe.get("recalcula"), make_ctx(pipe))
    assert result.status == "falha"
    assert "spark/recalcula.py" in result.detail and "não encontrado" in result.detail
    assert fake.calls == []


def test_gcp_mode_delegates_to_the_dag_and_runs_nothing(
    project: Path, make_ctx: MakeCtx, fake_submit: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    _script(project)
    fake = FakeRun()
    monkeypatch.setattr(spark_step.subprocess, "run", fake)
    pipe = _spark_pipeline()
    local = make_ctx(pipe)
    ctx = dataclasses.replace(local, env=local.env.model_copy(update={"modo": "gcp"}))

    result = run_step(pipe.get("recalcula"), ctx)

    assert fake.calls == []
    assert result.status == "nao_executada"
    assert result.detail == (
        "modo gcp: a etapa spark roda como lote do Dataproc Serverless criado pela DAG "
        "(veja `dataeng plan`); este processo não executa o script spark/recalcula.py"
    )
    assert _status(ctx) == [("recalcula", "nao_executada")]
