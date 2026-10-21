{%- set tem_pubsub = "pubsub" in cookiecutter.origens and cookiecutter.tipo_projeto != "relatorio" -%}
{%- set sem_spark_cli = cookiecutter.tipo_projeto != "ingestao" and cookiecutter.usa_spark == "sim" -%}
"""Executa o pipeline de exemplo no ambiente local (DuckDB) e confere que repetir a execução não duplica.
{%- if tem_pubsub %}

Origem pubsub: `dataeng run local` cria um Pub/Sub em memória VAZIO (não há como alimentá-lo pela linha
de comando), então a etapa termina ok com 0 linhas. Para exercitar a ingestão de verdade, o teste
`test_pubsub_*` publica mensagens sintéticas num `LocalPubSub` e o injeta em `build_context`.
{%- endif %}
"""

import shutil
{%- if cookiecutter.tipo_projeto != "ingestao" %}
from decimal import Decimal
{%- endif %}
from pathlib import Path

import duckdb
import pytest
{%- if not sem_spark_cli %}

from dataeng_core import cli
{%- endif %}
{%- if sem_spark_cli or tem_pubsub %}
from dataeng_core.jobs.run_step import load_project
{%- endif %}
{%- if tem_pubsub %}
from dataeng_core.pubsub import LocalPubSub
{%- endif %}
{%- if sem_spark_cli or tem_pubsub %}
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
{%- if sem_spark_cli %}


def _run(project: Path, run_id: str = RUN_ID) -> int:
    """Executa todas as etapas MENOS a `spark`, que exige spark-submit + Java (extra [spark]) e fica de fora."""
    pipeline, env, root = load_project(project / "config/pipeline.yaml", "local", {})
    ids = [s.id for s in pipeline.ordered_steps() if s.tipo != "spark"]
    ctx = build_context(pipeline, env, project_root=root, run_id=run_id)
    try:
        results = run_pipeline(pipeline, ctx, only=ids)
    finally:
        ctx.warehouse.close()
    return 0 if all(r.status == "ok" for r in results) else 1
{%- else %}


def _run(project: Path, run_id: str = RUN_ID) -> int:
    return cli.main(["run", "local", "--config", str(project / "config/pipeline.yaml"), "--run-id", run_id])
{%- endif %}
{%- set tem_dados = cookiecutter.tipo_projeto == "relatorio" or "arquivo" in cookiecutter.origens %}


def test_run_local_succeeds_and_is_idempotent(project: Path) -> None:
    assert _run(project) == 0
{%- if cookiecutter.tipo_projeto != "relatorio" and "arquivo" in cookiecutter.origens %}
    assert _query(project, "SELECT count(*) FROM silver.registros_arquivo") == [(5,)]
{%- endif %}
    assert _run(project) == 0  # mesma tentativa: não duplica
{%- if cookiecutter.tipo_projeto != "relatorio" and "arquivo" in cookiecutter.origens %}
    assert _query(project, "SELECT count(*) FROM silver.registros_arquivo") == [(5,)]
{%- endif %}
{%- if cookiecutter.tipo_projeto != "ingestao" %}
{%- if cookiecutter.tipo_projeto == "relatorio" %}
    resumo = [
        ("Alfa", 2, Decimal("15.00")),
        ("Beta", 2, Decimal("25.00")),
        ("Delta", 1, Decimal("3.10")),
        ("Gama", 1, Decimal("7.25")),
    ]
    classes = [("Alfa", "alto"), ("Beta", "alto"), ("Delta", "baixo"), ("Gama", "baixo")]
{%- elif tem_dados %}
    resumo = [
        ("Alfa", 1, Decimal("10.50")),
        ("Beta", 1, Decimal("20.00")),
        ("Delta", 1, Decimal("3.10")),
        ("Epsilon", 1, Decimal("99.99")),
        ("Gama", 1, Decimal("7.25")),
    ]
    classes = [("Alfa", "baixo"), ("Beta", "baixo"), ("Delta", "baixo"), ("Epsilon", "alto"), ("Gama", "baixo")]
{%- else %}
    resumo: list[tuple] = []  # só Pub/Sub: a fila local vazia deixa a silver sem linhas
    classes: list[tuple] = []
{%- endif %}
    # A view e o MERGE dão o mesmo resultado, e a 2ª rodada não duplicou nada.
    assert _query(project, "SELECT nome, qtd, total FROM gold.resumo_por_nome ORDER BY nome") == resumo
    assert _query(project, "SELECT nome, qtd, total FROM gold.agregado_por_nome ORDER BY nome") == resumo
{%- if cookiecutter.usa_python == "sim" %}
    assert _query(project, "SELECT nome, classe FROM gold.agregado_classificado ORDER BY nome") == classes
{%- endif %}
{%- endif %}
    # Uma linha de controle por etapa por execução: 2 rodadas, 2 linhas por etapa.
    contagens = _query(project, "SELECT etapa, count(*) FROM silver._controle_execucoes GROUP BY etapa")
    assert contagens and {n for _, n in contagens} == {2}
    assert _query(project, "SELECT count(*) FROM silver._controle_execucoes WHERE status <> 'ok'") == [(0,)]
{%- if "pubsub" in cookiecutter.origens and cookiecutter.tipo_projeto != "relatorio" %}


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
