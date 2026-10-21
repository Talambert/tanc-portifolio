from __future__ import annotations

import re
from pathlib import Path

import pytest
from tests.steps.conftest import MakeCtx
from tests.steps.helpers import RID1, pipeline, rows

from dataeng_core.steps import run_step
from dataeng_core.steps.registry import registered_types

FUNCS = """\
from dataeng_core.base.errors import LoadError
from dataeng_core.steps.context import StepResult

RECEBIDO = []


def rodar(ctx):
    RECEBIDO.append(ctx)
    (ctx.project_root / "saida.txt").write_text(ctx.run_id + "|" + ctx.params["ingestion_date"], encoding="utf-8")


def contar(ctx):
    return StepResult("outro_id", "ok", rows_read=7, rows_written=5, rows_rejected=2, detail="5 linhas gravadas")


def explodir(ctx):
    raise ValueError("cpf 123.456.789-00 inválido")


def erro_tipado(ctx):
    raise LoadError("linha com cpf 123.456.789-00")


def devolve_numero(ctx):
    return 42
"""


def _project(project: Path) -> None:
    (project / "custom").mkdir()
    (project / "custom" / "__init__.py").write_text("", encoding="utf-8")
    (project / "custom" / "tarefas.py").write_text(FUNCS, encoding="utf-8")


def _python_pipeline(funcao: str):  # type: ignore[no-untyped-def]
    return pipeline({"id": "py", "tipo": "python", "funcao": funcao})


def _control(ctx) -> list[tuple[object, ...]]:  # type: ignore[no-untyped-def]
    sql = "SELECT etapa, status, linhas_lidas, linhas_gravadas, linhas_rejeitadas FROM silver._controle_execucoes"
    return rows(ctx, sql)


def test_python_type_is_registered() -> None:
    assert "python" in registered_types()


def test_python_step_calls_function_with_context(project: Path, make_ctx: MakeCtx) -> None:
    _project(project)
    pipe = _python_pipeline("custom.tarefas:rodar")
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("py"), ctx)
    assert result.status == "ok"
    assert result.detail == "função custom.tarefas:rodar executada"
    assert (project / "saida.txt").read_text(encoding="utf-8") == f"{RID1}|2026-01-01"
    import custom.tarefas as mod  # type: ignore[import-not-found]

    assert mod.RECEBIDO == [ctx]  # o próprio contexto, uma única chamada
    assert _control(ctx) == [("py", "ok", 0, 0, 0)]


def test_python_step_returned_result_keeps_counts_and_step_id(project: Path, make_ctx: MakeCtx) -> None:
    _project(project)
    pipe = _python_pipeline("custom.tarefas:contar")
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("py"), ctx)
    assert (result.step_id, result.status, result.rows_read, result.rows_written, result.rows_rejected) == (
        "py",
        "ok",
        7,
        5,
        2,
    )
    assert result.detail == "5 linhas gravadas"
    assert _control(ctx) == [("py", "ok", 7, 5, 2)]


def test_python_step_failure_is_marked_failed_with_reference(
    project: Path, make_ctx: MakeCtx, capsys: pytest.CaptureFixture[str]
) -> None:
    _project(project)
    pipe = _python_pipeline("custom.tarefas:explodir")
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("py"), ctx)
    assert result.status == "falha"
    m = re.fullmatch(
        r"a função custom\.tarefas:explodir falhou \(ValueError\); referência ([0-9a-f]{8}); "
        r"veja o log com essa referência, corrija a função e reexecute a etapa",
        result.detail,
    )
    assert m, result.detail
    assert "123.456" not in result.detail
    # o log traz a mesma referência e o traceback (para o diagnóstico)
    err = capsys.readouterr().err
    assert f"referência {m.group(1)}" in err and "Traceback" in err and "ValueError" in err
    assert _control(ctx) == [("py", "falha", 0, 0, 0)]


def test_python_step_typed_error_keeps_its_reference_without_message(
    project: Path, make_ctx: MakeCtx, capsys: pytest.CaptureFixture[str]
) -> None:
    _project(project)
    pipe = _python_pipeline("custom.tarefas:erro_tipado")
    result = run_step(pipe.get("py"), make_ctx(pipe))
    assert result.status == "falha"
    m = re.fullmatch(
        r"a função custom\.tarefas:erro_tipado falhou \(LoadError\); referência ([0-9a-f]{8}); .*", result.detail
    )
    assert m, result.detail
    # é a referência do próprio erro tipado (a que aparece na mensagem dele, no traceback do log)
    assert f"LoadError: [{m.group(1)}] Falha na carga" in capsys.readouterr().err
    assert "123.456" not in result.detail and "linha com" not in result.detail


def test_python_step_wrong_return_type_is_actionable(project: Path, make_ctx: MakeCtx) -> None:
    _project(project)
    pipe = _python_pipeline("custom.tarefas:devolve_numero")
    result = run_step(pipe.get("py"), make_ctx(pipe))
    assert result.status == "falha"
    assert result.detail == (
        "a função custom.tarefas:devolve_numero devolveu int; ela deve devolver StepResult ou None"
    )


def test_python_step_missing_function_names_module_and_function(project: Path, make_ctx: MakeCtx) -> None:
    _project(project)
    pipe = _python_pipeline("custom.tarefas:inexistente")
    result = run_step(pipe.get("py"), make_ctx(pipe))
    assert result.status == "falha"
    assert "custom.tarefas" in result.detail and "inexistente" in result.detail
    assert "rodar" in result.detail  # lista as funções encontradas


def test_python_step_missing_module(project: Path, make_ctx: MakeCtx) -> None:
    pipe = _python_pipeline("custom.nada:rodar")
    result = run_step(pipe.get("py"), make_ctx(pipe))
    assert result.status == "falha"
    assert "custom.nada" in result.detail and "não encontrado" in result.detail


SENTINEL = "SENTINELA-987"


def _all_control_text(ctx) -> str:  # type: ignore[no-untyped-def]
    return str(rows(ctx, "SELECT * FROM silver._controle_execucoes"))


def test_python_step_import_error_does_not_leak_the_exception_message(
    project: Path, make_ctx: MakeCtx, capsys: pytest.CaptureFixture[str]
) -> None:
    (project / "custom").mkdir()
    (project / "custom" / "__init__.py").write_text("", encoding="utf-8")
    (project / "custom" / "job.py").write_text(
        f"valor = '{SENTINEL}'\nraise ValueError(f'CNPJ {{valor}} inválido')\n\ndef rodar(ctx):\n    pass\n",
        encoding="utf-8",
    )
    pipe = _python_pipeline("custom.job:rodar")
    ctx = make_ctx(pipe)
    result = run_step(pipe.get("py"), ctx)
    assert result.status == "falha"
    m = re.fullmatch(
        r"a função custom\.job:rodar não pôde ser importada \(ValueError\); referência ([0-9a-f]{8}); "
        r"veja o log com essa referência, corrija o módulo e reexecute a etapa",
        result.detail,
    )
    assert m, result.detail
    err = capsys.readouterr().err
    assert f"referência {m.group(1)}" in err and "Traceback" in err  # o diagnóstico fica no log
    assert SENTINEL not in result.detail and SENTINEL not in _all_control_text(ctx)


def test_python_step_signature_message_talks_about_the_context(project: Path, make_ctx: MakeCtx) -> None:
    (project / "custom").mkdir()
    (project / "custom" / "__init__.py").write_text("", encoding="utf-8")
    (project / "custom" / "dois.py").write_text("def rodar(a, b):\n    pass\n", encoding="utf-8")
    pipe = _python_pipeline("custom.dois:rodar")
    result = run_step(pipe.get("py"), make_ctx(pipe))
    assert result.status == "falha"
    assert "contexto" in result.detail and "DataFrame" not in result.detail


def test_python_step_returned_result_with_none_detail(project: Path, make_ctx: MakeCtx) -> None:
    (project / "custom").mkdir()
    (project / "custom" / "__init__.py").write_text("", encoding="utf-8")
    (project / "custom" / "nada.py").write_text(
        "from dataeng_core.steps.context import StepResult\n\n"
        "def rodar(ctx):\n    return StepResult('py', 'ok', rows_read=1, detail=None)\n",
        encoding="utf-8",
    )
    pipe = _python_pipeline("custom.nada:rodar")
    result = run_step(pipe.get("py"), make_ctx(pipe))
    assert (result.status, result.rows_read, result.detail) == ("ok", 1, "")
