# {{ cookiecutter.nome_projeto }}

{% if cookiecutter.tipo_projeto == "ingestao" %}Pipeline de ingestão (origens: {{ cookiecutter.origens }}; motor: {{ cookiecutter.motor_ingestao }}; carga: {{ cookiecutter.modo_carga }}){% elif cookiecutter.tipo_projeto == "relatorio" %}Pipeline de relatório (sem ingestão: etapas `sql`, `view`{% if cookiecutter.usa_python == "sim" %}, `python`{% endif %}{% if cookiecutter.usa_spark == "sim" %}, `spark`{% endif %} e `qualidade`){% else %}Pipeline híbrido (ingestão + relatório numa só `pipeline.yaml`; origens: {{ cookiecutter.origens }}; motor da ingestão: {{ cookiecutter.motor_ingestao }}; carga: {{ cookiecutter.modo_carga }}){% endif %}, gerado a partir do `template-pipeline`. A lógica vive na biblioteca `dataeng-core`; aqui ficam só configuração, SQL, schemas e ganchos.

## Rodar local (sem nuvem)

```bash
pip install -e ".[dev]"            # instala dataeng-core[local]{% if cookiecutter.origem_da_biblioteca == "artifact_registry" %} (ou: pip install -r requirements.txt){% endif %}

dataeng validate-config --config config/pipeline.yaml
dataeng plan --config config/pipeline.yaml
dataeng run local --config config/pipeline.yaml
pytest
```
{% if cookiecutter.tipo_projeto == "relatorio" %}
O exemplo funciona sem editar nada: a etapa `semeia_dados` cria e popula `silver.vendas_exemplo` (6 linhas sintéticas); a view `gold.resumo_por_nome` e o `MERGE` em `gold.agregado_por_nome` agregam por `nome`{% if cookiecutter.usa_python == "sim" %}; a etapa `python` grava `gold.agregado_classificado`{% endif %}. Tudo vai para `.local/` (DuckDB). Repetir a execução com o mesmo `--run-id` não duplica linhas.
{%- elif "arquivo" in cookiecutter.origens %}
O exemplo funciona sem editar nada: lê `data/amostra.csv` (5 linhas sintéticas) e grava em `.local/` (DuckDB). Repetir a execução com o mesmo `--run-id` não duplica linhas.{% if cookiecutter.tipo_projeto == "hibrido" %} Depois da ingestão, a view `gold.resumo_por_nome` e o `MERGE` em `gold.agregado_por_nome` agregam a silver por `nome`{% if cookiecutter.usa_python == "sim" %}, e a etapa `python` grava `gold.agregado_classificado`{% endif %}.{% endif %}
{%- endif %}
{%- if cookiecutter.tipo_projeto != "ingestao" and cookiecutter.usa_spark == "sim" %}

**Etapa `spark`:** é a última e roda como lote do Dataproc Serverless. Localmente exige `spark-submit` + Java (extra `[spark]`); sem eles `dataeng run local` executa todas as outras etapas e falha só nela, com uma mensagem explicativa. O script `spark/recalcula.py` é um EXEMPLO que usa o conector `spark-bigquery` (não existe no `spark-submit` local): adapte-o. O template nunca executou Spark; os testes só conferem geração, `validate-config` e `plan`. Em `execucao_padrao.dataproc`, `script_base_uri` é um prefixo de EXEMPLO onde o projeto deve ser publicado.
{%- endif %}
{%- if cookiecutter.tipo_projeto != "ingestao" %}

**SQL portátil:** os `.sql` de `sql/` usam só o subconjunto aceito por DuckDB (local) e BigQuery (`CREATE OR REPLACE ... AS SELECT` com `UNION ALL`, `MERGE`, `COUNT`/`SUM`/`GROUP BY`, tipos `STRING`/`BIGINT`/`DECIMAL`); a sintaxe é validada como BigQuery, mas o SQL nunca rodou no BigQuery real. Parâmetros são `${nome}`.
{%- endif %}
{%- if "pubsub" in cookiecutter.origens and cookiecutter.tipo_projeto != "relatorio" %}
## Origem Pub/Sub

- Cada execução faz UM pull em lote (até `max_mensagens`, esperando até `tempo_maximo_s`); o agendamento da DAG repete o lote. Grava as mensagens cruas no bronze, carrega a silver e a quarentena e SÓ ENTÃO confirma (ack). Se algo falhar antes, nada é confirmado e as mensagens voltam.
- A silver tem uma linha por `message_id` (entrega "pelo menos uma vez"); mensagens com JSON inválido vão para `eventos_rejeitados` e são confirmadas. O backlog restante vai para a tabela de controle.
- **Limitação local:** `dataeng run local` usa um Pub/Sub em memória vazio (não há como publicar pela linha de comando), então a etapa `ingere_eventos` termina ok com 0 linhas. O teste `tests/test_local_run.py` publica mensagens sintéticas num `LocalPubSub` e o injeta em `build_context` para exercitar a ingestão.
- Troque a `assinatura` de exemplo pela real; no GCP o backlog fica nulo (exige Cloud Monitoring).

## Pub/Sub: operação

- **Dead-letter:** configure na assinatura `max_delivery_attempts` e um tópico dead-letter. Sem isso, um lote que falha sempre (ex.: verificação de qualidade bloqueante que falha por causa do conteúdo de uma mensagem) volta para sempre: a silver já foi gravada (de forma idempotente), mas nada é confirmado e o backlog não drena. Use verificação bloqueante sobre o conteúdo das mensagens com cuidado; prefira rejeitar por mensagem com `rejeitar_se` (vai para a quarentena e é confirmada).
- **Prazo de ack:** a biblioteca não estende o prazo de ack durante o processamento. Configure o `ackDeadlineSeconds` da assinatura (até 600 s) para cobrir um lote completo (pull + bronze + silver + quarentena + `dml_pos_carga`) e reduza `max_mensagens` se necessário. Um ack depois do prazo pode ser ignorado pelo serviço: as mensagens voltam (a carga é idempotente, mas o backlog não drena).
- **`modo_carga: merge` com chaves de negócio:** em cada lote fica a mensagem de maior `publish_time` por chave; as superadas são confirmadas sem ir para a silver nem para a quarentena. O merge é incondicional: um evento atrasado (mais antigo) que chegue numa execução posterior sobrescreve o mais novo já gravado.
- **Quarentena:** `_linha_original` guarda a mensagem inteira em base64 (com `attributes`), cerca de 33% maior que o original; mensagens grandes inflam a tabela `<tabela>_rejeitados`.
{% endif %}
## O que editar

{%- if cookiecutter.tipo_projeto == "relatorio" %}
- `config/pipeline.yaml`: as etapas do relatório. Parâmetros nos YAML e no SQL são `${nome}`. Remova `semeia_dados` (e `sql/ddl/semeia_vendas_exemplo.sql`) quando apontar para tabelas reais.
- `sql/views/`, `sql/dml/`, `sql/ddl/`: view, `MERGE` e DDL de exemplo; troque pelo seu relatório.
{%- else %}
- `config/pipeline.yaml`: origem, schema, transformação, qualidade e modo de carga{% if cookiecutter.tipo_projeto == "hibrido" %}, mais as etapas do relatório (`garante_tabela_base`, `view_resumo_por_nome`, `agrega_por_nome`{% if cookiecutter.usa_python == "sim" %}, `enriquece`{% endif %}, `checagem_final`{% if cookiecutter.usa_spark == "sim" %}, `recalcula_spark`{% endif %}) que leem a tabela `silver.{{ cookiecutter.__tabela_base }}`{% endif %}. Parâmetros nos YAML e no SQL são `${nome}`.
- `schemas/` e `sql/ddl/`: as colunas de exemplo (`id`, `nome`, `valor`) são genéricas; troque pelas suas.
{%- if cookiecutter.tipo_projeto == "hibrido" %}
- `sql/views/`, `sql/dml/`: view e `MERGE` de exemplo sobre a silver; troque pelo seu relatório.
{%- endif %}
{%- endif %}
- `config/ambientes/{local,dev,prod}.yaml`: os valores de projeto, bucket e Service Account são FICTÍCIOS (`meu-projeto-dev`); troque pelos reais.
- `custom/`: funções próprias, referenciadas no YAML como `modulo:funcao`{% if cookiecutter.tipo_projeto != "ingestao" and cookiecutter.usa_python == "sim" %} (`custom/enriquecimento.py` é o exemplo da etapa `python`){% endif %}.{% if cookiecutter.tipo_projeto != "ingestao" and cookiecutter.usa_spark == "sim" %}
- `spark/`: scripts PySpark da etapa `spark`.{% endif %}
{%- if "api" in cookiecutter.origens and cookiecutter.tipo_projeto != "relatorio" %}

## Segredos da origem API

O YAML guarda só o NOME da variável de ambiente (`segredo_env: API_TOKEN`). O valor nunca entra no repositório, na imagem nem nos logs: no Cloud Run, injete-o pelo Secret Manager; localmente, `export API_TOKEN=...` no seu shell. Troque também o `endpoint` de exemplo. Não há teste local de execução para a origem API (sem rede nos testes); só `validate-config` e `plan`.
{%- endif %}

## Implantação (NÃO validada aqui)

- `Dockerfile`: instala `dataeng-core[gcp]` na versão fixada ({{ cookiecutter.versao_biblioteca }}) e usa `python -m dataeng_core.jobs.run_step` como ENTRYPOINT; `--config`, `--ambiente`, `--etapa` e `--run-id` vêm da execução. O Cloud Run Job precisa ter o mesmo nome que a biblioteca calcula a partir de `nome` (minúsculas, dígitos e `-`: `{{ cookiecutter.nome_projeto.lower() }}`).
- `dags/`: monta a DAG com `build_dag(config_path, job_config_path=...)`. O Airflow é dependência do ambiente de DAGs (Linux), não deste projeto.
{%- if cookiecutter.tipo_projeto != "relatorio" and cookiecutter.motor_ingestao == "ambos" %}
- Motor `ambos`: duas DAGs, `{{ cookiecutter.nome_projeto }}_cloud_run` (`config/pipeline.yaml`) e `{{ cookiecutter.nome_projeto }}_dataproc` (`config/pipeline_dataproc.yaml`, igual ao primeiro com `motor: dataproc`). Se editar um, replique no outro.
{%- endif %}
{%- if cookiecutter.tipo_projeto != "relatorio" and cookiecutter.motor_ingestao != "cloud_run" %}
- Motor Dataproc (`ingestao`/`python`): exige uma IMAGEM do lote com o `dataeng-core` instalado (suposição de infraestrutura, a biblioteca não a cria). O `container_image` em `execucao_padrao.dataproc` é um EXEMPLO a trocar (alternativa: `ingest_entrypoint_uri`/`run_step_entrypoint_uri`); sem uma imagem real o lote falha.
{%- endif %}
- `qualidade` roda SEMPRE no Cloud Run (mesmo na DAG Dataproc): a DAG precisa do Cloud Run Job. `sql` e `view` viram tarefas do BigQuery; `python` roda no Cloud Run{% if cookiecutter.tipo_projeto != "ingestao" and cookiecutter.usa_spark == "sim" %}; `spark` vira lote do Dataproc Serverless{% endif %}.
- Nada disso (DAG, Cloud Run, Dataproc, BigQuery reais) foi executado na geração do template; só `validate-config`, `plan`, `run local` e checagem estática da DAG.
