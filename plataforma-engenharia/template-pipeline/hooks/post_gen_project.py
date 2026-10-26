"""Remove o que não se aplica às escolhas feitas (roda dentro da pasta do projeto gerado)."""

import shutil
from pathlib import Path

TIPO = {{ cookiecutter.tipo_projeto | tojson }}
# `origens` e `motor_ingestao` só valem para ingestao e hibrido: no relatorio não há origem nem ingestão (o
# valor padrão fica ignorado); a DAG do relatorio usa o Cloud Run (python/qualidade) e as tarefas do BigQuery.
ORIGENS = [] if TIPO == "relatorio" else [o.strip() for o in {{ cookiecutter.origens | tojson }}.split(",")]
MOTOR = "cloud_run" if TIPO == "relatorio" else {{ cookiecutter.motor_ingestao | tojson }}
USA_PYTHON = {{ cookiecutter.usa_python | tojson }} == "sim"
USA_SPARK = {{ cookiecutter.usa_spark | tojson }} == "sim"
ROOT = Path.cwd()


def remove(*caminhos: str) -> None:
    for caminho in caminhos:
        alvo = ROOT / caminho
        if alvo.is_dir():
            shutil.rmtree(alvo)
        elif alvo.exists():
            alvo.unlink()


def main() -> None:
    # Relatório (views, MERGE, python e spark) é dos tipos relatorio/hibrido; `usa_python`/`usa_spark` só valem neles.
    if TIPO == "ingestao":
        remove("sql/views", "sql/dml", "sql/ddl/gold_agregado_por_nome.sql")
    if TIPO == "ingestao" or not USA_PYTHON:
        remove("custom/enriquecimento.py")
    if TIPO == "ingestao" or not USA_SPARK:
        remove("spark")
    # Os dados de exemplo do relatorio só existem nele; no hibrido a tabela de entrada vem da ingestão.
    if TIPO != "relatorio":
        remove("sql/ddl/semeia_vendas_exemplo.sql")
    # Bloco de origem não escolhido (no relatorio ORIGENS é vazio: sem origem, schemas, DDL da silver nem amostra).
    if TIPO == "relatorio":
        remove("schemas")
    if "arquivo" not in ORIGENS:
        remove("data", "schemas/origem.yaml", "sql/ddl/silver_registros_arquivo.sql")
    if "api" not in ORIGENS:
        remove("sql/ddl/silver_registros_api.sql")
    if "pubsub" not in ORIGENS:
        remove("schemas/origem_eventos.yaml", "schemas/destino_eventos.yaml", "sql/ddl/silver_eventos.sql")
    # O teste de execução local roda o pipeline inteiro; com a origem `api` ele faria chamadas de rede.
    # (`arquivo` lê a amostra; `pubsub` usa o LocalPubSub em memória, alimentado pelo próprio teste.)
    if "api" in ORIGENS:
        remove("tests/test_local_run.py")

    # DAG do motor não escolhido. Com um só motor, a DAG se chama dags/pipeline.py.
    if MOTOR == "cloud_run":
        remove("dags/pipeline_dataproc.py")
        (ROOT / "dags/pipeline_cloud_run.py").rename(ROOT / "dags/pipeline.py")
    elif MOTOR == "dataproc":
        remove("dags/pipeline_cloud_run.py")
        (ROOT / "dags/pipeline_dataproc.py").rename(ROOT / "dags/pipeline.py")
    else:  # ambos: o mesmo pipeline com o motor da ingestão trocado
        texto = (ROOT / "config/pipeline.yaml").read_text(encoding="utf-8")
        (ROOT / "config/pipeline_dataproc.yaml").write_text(
            texto.replace("motor: cloud_run", "motor: dataproc"), encoding="utf-8"
        )

    # Dependências: `requirements.txt` só existe com o Artifact Registry (precisa do --extra-index-url).
    if {{ cookiecutter.origem_da_biblioteca | tojson }} != "artifact_registry":
        remove("requirements.txt")


main()
