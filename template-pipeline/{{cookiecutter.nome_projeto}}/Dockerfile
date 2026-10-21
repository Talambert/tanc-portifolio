# Imagem do Cloud Run Job (e do lote Dataproc). A versão da biblioteca é FIXADA; atualizar = trocar a versão
# aqui e no pyproject.toml e rodar os testes. Sem segredos: variáveis como API_TOKEN vêm da execução
# (Secret Manager no Cloud Run), nunca da imagem.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

{% if cookiecutter.origem_da_biblioteca == "git" %}RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
RUN pip install "dataeng-core[gcp] @ git+{{ cookiecutter.repositorio_biblioteca }}@dataeng-core-v{{ cookiecutter.versao_biblioteca }}#subdirectory=lib-dataeng-core"
{% else %}RUN pip install --extra-index-url {{ cookiecutter.indice_artifact_registry }} "dataeng-core[gcp]=={{ cookiecutter.versao_biblioteca.rsplit(".", 1)[0] }}.*"
{% endif %}
RUN useradd --uid 10001 --create-home app
WORKDIR /app
COPY config/ config/
{%- if cookiecutter.tipo_projeto != "relatorio" %}
COPY schemas/ schemas/
{%- endif %}
COPY sql/ sql/
COPY custom/ custom/
{%- if cookiecutter.tipo_projeto != "relatorio" and "arquivo" in cookiecutter.origens %}
COPY data/ data/
{%- endif %}
USER app

# Os argumentos (--config, --ambiente, --etapa, --run-id) vêm da execução do job.
ENTRYPOINT ["python", "-m", "dataeng_core.jobs.run_step"]
