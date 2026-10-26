"""DAG do pipeline (motor Dataproc Serverless), montada pela biblioteca a partir do YAML.

O Airflow é dependência do ambiente de DAGs (Composer/Linux), não deste projeto: este arquivo só chama
`build_dag`, que importa o Airflow na hora de montar a DAG.
"""

import os
from pathlib import Path

from dataeng_core.airflow.dag import build_dag

# Caminho absoluto, resolvido a partir deste arquivo, usado no parse da DAG.
CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "{{ "pipeline_dataproc.yaml" if cookiecutter.tipo_projeto != "relatorio" and cookiecutter.motor_ingestao == "ambos" else "pipeline.yaml" }}"

dag = build_dag(
    str(CONFIG_PATH),
    ambiente=os.environ.get("DATAENG_AMBIENTE", "prod"),
    # Caminho do mesmo arquivo DENTRO da imagem usada pelo lote (relativo ao WORKDIR do Dockerfile).
    job_config_path="config/{{ "pipeline_dataproc.yaml" if cookiecutter.tipo_projeto != "relatorio" and cookiecutter.motor_ingestao == "ambos" else "pipeline.yaml" }}",
{%- if cookiecutter.tipo_projeto != "relatorio" and cookiecutter.motor_ingestao == "ambos" %}
    dag_id="{{ cookiecutter.nome_projeto }}_dataproc",
{%- endif %}
)
