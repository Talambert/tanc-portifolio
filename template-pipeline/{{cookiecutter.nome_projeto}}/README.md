# {{ cookiecutter.nome_projeto }}

Pipeline de ingestão (origens: {{ cookiecutter.origens }}; motor: {{ cookiecutter.motor_ingestao }}; carga: {{ cookiecutter.modo_carga }}), gerado a partir do `template-pipeline`. A lógica vive na biblioteca `dataeng-core`; aqui ficam só configuração, SQL, schemas e ganchos.

## Rodar local (sem nuvem)

```bash
pip install -e ".[dev]"            # instala dataeng-core[local]{% if cookiecutter.origem_da_biblioteca == "artifact_registry" %} (ou: pip install -r requirements.txt){% endif %}

dataeng validate-config --config config/pipeline.yaml
dataeng plan --config config/pipeline.yaml
dataeng run local --config config/pipeline.yaml
pytest
```
{% if "arquivo" in cookiecutter.origens %}
O exemplo funciona sem editar nada: lê `data/amostra.csv` (5 linhas sintéticas) e grava em `.local/` (DuckDB). Repetir a execução com o mesmo `--run-id` não duplica linhas.
{% endif %}
## O que editar

- `config/pipeline.yaml`: origem, schema, transformação, qualidade e modo de carga. Parâmetros nos YAML e no SQL são `${nome}`.
- `schemas/` e `sql/ddl/`: as colunas de exemplo (`id`, `nome`, `valor`) são genéricas; troque pelas suas.
- `config/ambientes/{local,dev,prod}.yaml`: os valores de projeto, bucket e Service Account são FICTÍCIOS (`meu-projeto-dev`); troque pelos reais.
- `custom/`: funções próprias, referenciadas no YAML como `modulo:funcao`.
{%- if "api" in cookiecutter.origens %}

## Segredos da origem API

O YAML guarda só o NOME da variável de ambiente (`segredo_env: API_TOKEN`). O valor nunca entra no repositório, na imagem nem nos logs: no Cloud Run, injete-o pelo Secret Manager; localmente, `export API_TOKEN=...` no seu shell. Troque também o `endpoint` de exemplo. Não há teste local de execução para a origem API (sem rede nos testes); só `validate-config` e `plan`.
{%- endif %}

## Implantação (NÃO validada aqui)

- `Dockerfile`: instala `dataeng-core[gcp]` na versão fixada ({{ cookiecutter.versao_biblioteca }}) e usa `python -m dataeng_core.jobs.run_step` como ENTRYPOINT; `--config`, `--ambiente`, `--etapa` e `--run-id` vêm da execução. O Cloud Run Job precisa ter o mesmo nome que a biblioteca calcula a partir de `nome` (minúsculas, dígitos e `-`: `{{ cookiecutter.nome_projeto.lower() }}`).
- `dags/`: monta a DAG com `build_dag(config_path, job_config_path=...)`. O Airflow é dependência do ambiente de DAGs (Linux), não deste projeto.
{%- if cookiecutter.motor_ingestao == "ambos" %}
- Motor `ambos`: duas DAGs, `{{ cookiecutter.nome_projeto }}_cloud_run` (`config/pipeline.yaml`) e `{{ cookiecutter.nome_projeto }}_dataproc` (`config/pipeline_dataproc.yaml`, igual ao primeiro com `motor: dataproc`). Se editar um, replique no outro.
{%- endif %}
- Nada disso (DAG, Cloud Run, Dataproc, BigQuery reais) foi executado na geração do template; só `validate-config`, `plan`, `run local` e checagem estática da DAG.
