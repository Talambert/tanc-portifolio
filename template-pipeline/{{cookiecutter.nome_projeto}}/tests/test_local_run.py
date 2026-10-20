"""Executa o pipeline de exemplo no ambiente local (DuckDB) e confere que repetir a execução não duplica.
{%- if "pubsub" in cookiecutter.origens %}

Origem pubsub: `dataeng run local` cria um Pub/Sub em memória VAZIO (não há como alimentá-lo pela linha
de comando), então a etapa termina ok com 0 linhas. Para exercitar a ingestão de verdade, o teste
`test_pubsub_*` publica mensagens sintéticas num `LocalPubSub` e o injeta em `build_context`.
{%- endif %}
"""

import shutil
from pathlib import Path

import duckdb
import pytest

from dataeng_core import cli
{%- if "pubsub" in cookiecutter.origens %}
from dataeng_core.jobs.run_step import load_project
from dataeng_core.pubsub import LocalPubSub
from dataeng_core.steps import build_context, run_pipeline
{%- endif %}

ROOT = Path(__file__).resolve().parent.parent
RUN_ID = "20260301T060000-aaaaaa"


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """Cópia do projeto em `tmp_path`, para o `.local` (DuckDB) não sujar o repositório."""
    copy = tmp_path / "projeto"
    shutil.copytree(ROOT, copy, ignore=shutil.ignore_patterns(".local", "__pycache__", ".pytest_cache"))
    return copy


def _query(project: Path, sql: str) -> list[tuple]:
    con = duckdb.connect(str(next((project / ".local").rglob("*.duckdb"))), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_run_local_succeeds_and_is_idempotent(project: Path) -> None:
    argv = ["run", "local", "--config", str(project / "config/pipeline.yaml"), "--run-id", RUN_ID]
    assert cli.main(argv) == 0
{%- if "arquivo" in cookiecutter.origens %}
    assert _query(project, "SELECT count(*) FROM silver.registros_arquivo") == [(5,)]
{%- endif %}
    assert cli.main(argv) == 0  # mesma tentativa: não duplica
{%- if "arquivo" in cookiecutter.origens %}
    assert _query(project, "SELECT count(*) FROM silver.registros_arquivo") == [(5,)]
{%- endif %}
{%- if "pubsub" in cookiecutter.origens %}


# Mensagens sintéticas: 3 válidas, 1 entrega repetida (mesmo message_id) e 1 com JSON inválido.
MENSAGENS = [
    ("evt-1", b'{"id": "1", "nome": "Alfa", "valor": "10.5"}'),
    ("evt-2", b'{"id": "2", "nome": "Beta", "valor": "20"}'),
    ("evt-2", b'{"id": "2", "nome": "Beta", "valor": "20"}'),
    ("evt-3", b'{"id": "3", "nome": "Gama"}'),
    ("evt-4", b"isto nao e json"),
]


def _run_pubsub(project: Path, fila: LocalPubSub, run_id: str) -> None:
    pipeline, env, root = load_project(project / "config/pipeline.yaml", "local", {})
    step = next(s for s in pipeline.etapas if s.origem.tipo == "pubsub")
    ctx = build_context(pipeline, env, project_root=root, run_id=run_id, pubsub=fila)
    try:
        results = run_pipeline(pipeline, ctx, only=[step.id])
    finally:
        ctx.warehouse.close()
    assert [r.status for r in results] == ["ok"], [r.detail for r in results]


def test_pubsub_ingests_acks_and_deduplicates_by_message_id(project: Path) -> None:
    pipeline, _, _ = load_project(project / "config/pipeline.yaml", "local", {})
    assinatura = next(s for s in pipeline.etapas if s.origem.tipo == "pubsub").origem.assinatura
    fila = LocalPubSub()
    for message_id, data in MENSAGENS:
        fila.publish(assinatura, data, message_id=message_id)

    _run_pubsub(project, fila, RUN_ID)

    assert fila.backlog(assinatura) == 0  # todas confirmadas, inclusive a inválida (foi para a quarentena)
    assert _query(project, "SELECT message_id, id, nome FROM silver.eventos ORDER BY id") == [
        ("evt-1", 1, "Alfa"),
        ("evt-2", 2, "Beta"),
        ("evt-3", 3, "Gama"),
    ]
    assert _query(project, "SELECT message_id, _motivo FROM silver.eventos_rejeitados") == [("evt-4", "json_invalido")]

    # reentrega da mesma mensagem numa nova execução: continua uma linha por message_id
    fila.publish(assinatura, MENSAGENS[0][1], message_id="evt-1")
    _run_pubsub(project, fila, "20260302T060000-bbbbbb")
    assert _query(project, "SELECT count(*), count(DISTINCT message_id) FROM silver.eventos") == [(3, 3)]
{%- endif %}
