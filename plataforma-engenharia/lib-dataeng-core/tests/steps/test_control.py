from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest
import responses
from tests.steps.conftest import MakeCtx
from tests.steps.helpers import CONTROL, RID1, environment, ingest_step, pipeline, rows

import dataeng_core.steps.ingestao as ingestao
from dataeng_core.base.errors import ConfigError, SourceError
from dataeng_core.quality.checks import CheckResult
from dataeng_core.steps import StepResult, record_result, run_step

SECRET = "S3CR3T-token-xyz"
API_URL = "https://api.example.test/vendas"


def test_record_result_writes_row_with_content(make_ctx: MakeCtx) -> None:
    ctx = make_ctx(pipeline(ingest_step()))
    result = StepResult(
        "ingere",
        "ok",
        rows_read=10,
        rows_written=8,
        rows_rejected=2,
        backlog=5,
        quality=[CheckResult("unico", "id", False, 0.9, 1.0, False)],
        detail="tudo certo",
    )
    record_result(ctx.warehouse, ctx.env, result, RID1)

    got = ctx.warehouse.query(f"SELECT * FROM {CONTROL}")
    assert list(got.columns) == [
        "run_id",
        "etapa",
        "status",
        "linhas_lidas",
        "linhas_gravadas",
        "linhas_rejeitadas",
        "backlog",
        "qualidade",
        "detalhe",
        "registrado_em",
    ]
    row = got.iloc[0]
    assert (row.run_id, row.etapa, row.status, row.detalhe) == (RID1, "ingere", "ok", "tudo certo")
    assert (int(row.linhas_lidas), int(row.linhas_gravadas), int(row.linhas_rejeitadas), int(row.backlog)) == (
        10,
        8,
        2,
        5,
    )
    assert json.loads(row.qualidade) == [
        {"nome": "unico", "coluna": "id", "aprovado": False, "taxa": 0.9, "limiar": 1.0, "bloqueante": False}
    ]
    assert row.registrado_em.tzinfo is not None


def test_record_result_keeps_history_in_order(make_ctx: MakeCtx) -> None:
    ctx = make_ctx(pipeline(ingest_step()))
    for status in ("falha", "ok", "nao_executada"):
        record_result(ctx.warehouse, ctx.env, StepResult("e", status), RID1)  # type: ignore[arg-type]
    assert rows(ctx, f"SELECT status, backlog FROM {CONTROL} ORDER BY registrado_em") == [
        ("falha", None),
        ("ok", None),
        ("nao_executada", None),
    ]


def test_record_result_rejects_invalid_dataset(make_ctx: MakeCtx) -> None:
    ctx = make_ctx(pipeline(ingest_step()))
    env = environment().model_copy(update={"dataset_silver": "silver; DROP TABLE x"})
    with pytest.raises(ConfigError):
        record_result(ctx.warehouse, env, StepResult("e", "ok"), RID1)


# ------------------------------------------------------------------------ foco 5


def _api_pipeline(tipo: str = "bearer") -> object:
    return pipeline(
        ingest_step(
            "ingere_api",
            origem={
                "tipo": "api",
                "endpoint": API_URL,
                "autenticacao": {"tipo": tipo, "segredo_env": "API_TOKEN_TESTE"},
            },
            transformacao=[],
        )
    )


def _all_control_text(ctx: object) -> str:
    df = ctx.warehouse.query(f"SELECT * FROM {CONTROL}")  # type: ignore[attr-defined]
    return json.dumps(df.astype(str).to_dict("records"))


@responses.activate
def test_secret_not_in_control_table_or_detail(
    project: Path, make_ctx: MakeCtx, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("API_TOKEN_TESTE", SECRET)
    responses.add(responses.GET, API_URL, status=401, body=f"token {SECRET} recusado")
    pipe = _api_pipeline()
    ctx = make_ctx(pipe)

    result = run_step(pipe.get("ingere_api"), ctx)  # type: ignore[attr-defined]

    assert result.status == "falha" and "HTTP 401" in result.detail
    assert SECRET not in result.detail
    assert "falha" in _all_control_text(ctx) and SECRET not in _all_control_text(ctx)
    assert SECRET not in capsys.readouterr().err


@pytest.mark.parametrize("tipo", ["bearer", "basic"])
def test_secret_leaked_by_lower_layer_is_redacted(
    project: Path,
    make_ctx: MakeCtx,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tipo: str,
) -> None:
    """Defesa em profundidade: mesmo que uma camada de baixo vaze o segredo, a etapa o remove."""
    secret = "usuario:" + SECRET if tipo == "basic" else SECRET
    encoded = base64.b64encode(secret.encode()).decode()
    monkeypatch.setenv("API_TOKEN_TESTE", secret)

    def leaky(*args: object, **kwargs: object) -> list[dict[str, object]]:
        raise SourceError(f"recusado: {secret} / {encoded} / {SECRET}", operation="api")

    monkeypatch.setattr(ingestao, "fetch_records", leaky)
    pipe = _api_pipeline(tipo)
    ctx = make_ctx(pipe)

    result = run_step(pipe.get("ingere_api"), ctx)  # type: ignore[attr-defined]

    assert result.status == "falha" and "recusado: ***" in result.detail
    for text in (result.detail, _all_control_text(ctx), capsys.readouterr().err):
        assert SECRET not in text and encoded not in text
