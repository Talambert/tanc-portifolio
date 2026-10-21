# template-pipeline

Template [Cookiecutter](https://cookiecutter.readthedocs.io/) de pipelines de engenharia de dados sobre a biblioteca `dataeng-core` (`../lib-dataeng-core`). Gera três tipos de projeto: **`ingestao`** (origens `arquivo`, `api` e `pubsub`), **`relatorio`** (view, `MERGE`, `python`/`spark` opcionais e `qualidade` sobre dados de exemplo semeados) e **`hibrido`** (os dois numa só `pipeline.yaml`).

```bash
pip install cookiecutter
cookiecutter template-pipeline            # pergunta as variáveis
cookiecutter template-pipeline --no-input origens=arquivo,api motor_ingestao=ambos modo_carga=merge
cookiecutter template-pipeline --no-input tipo_projeto=relatorio usa_python=sim
cookiecutter template-pipeline --no-input tipo_projeto=hibrido origens=arquivo usa_python=sim usa_spark=sim
```

## Variáveis

| Variável | Valores |
|---|---|
| `nome_projeto` | letras, dígitos, `-`, `_` (nome do pipeline, do Cloud Run Job e da DAG) |
| `tipo_projeto` | `ingestao`, `relatorio` ou `hibrido` (veja "Tipos de projeto") |
| `origens` | só para `ingestao` e `hibrido`; texto separado por vírgula: `arquivo`, `api`, `pubsub` (ex.: `arquivo,pubsub`) |
| `motor_ingestao` | `cloud_run`, `dataproc` ou `ambos` (duas DAGs e dois YAMLs); vale para `ingestao` e `hibrido` |
| `usa_python`, `usa_spark` | `nao` (padrão) ou `sim`; valem para `relatorio` e `hibrido`: habilitam a etapa `python` (`custom/enriquecimento.py`) e a etapa `spark` (`spark/recalcula.py`) |
| `modo_carga` | `append`, `overwrite_particao`, `merge` (`merge` usa `chaves_merge: [id]`). No bloco `pubsub`, `overwrite_particao` vira `append` (merge por `message_id`; sobrescrever a partição apagaria os lotes anteriores) e `merge` usa `chaves_merge: [message_id]` |
| `projeto_gcp`, `regiao`, `bucket_bronze`, `dataset_silver`, `dataset_gold`, `agenda_cron` | valores de exemplo em `dev.yaml` |
| `origem_da_biblioteca` | `git` (`dataeng-core[local] @ git+<repositorio_biblioteca>@dataeng-core-vX.Y.Z#subdirectory=lib-dataeng-core`) ou `artifact_registry` (`dataeng-core[local]==X.Y.*` + `requirements.txt` com `--extra-index-url`) |
| `versao_biblioteca`, `repositorio_biblioteca`, `indice_artifact_registry` | versão fixada, URL do repositório Git e índice do Artifact Registry (exemplo fictício) |

## Tipos de projeto

| Tipo | Etapas geradas | Pastas |
|---|---|---|
| `ingestao` | uma `ingestao` por origem | `schemas/`, `sql/ddl/`, `data/` (origem `arquivo`), `custom/`, `dags/` |
| `relatorio` | `semeia_dados` (sql), `view_resumo_por_nome` (view), `agrega_por_nome` (sql com `MERGE`), `enriquece` (python, se `usa_python`), `checagem_final` (qualidade), `recalcula_spark` (spark, se `usa_spark`) | `sql/ddl/`, `sql/dml/`, `sql/views/`, `custom/`, `spark/` (se `usa_spark`), `dags/`. **Sem** `schemas/` nem `data/`: não há origem |
| `hibrido` | as `ingestao` das origens + `garante_tabela_base` (cria a tabela que o relatório lê se a ingestão não gravou nada) + as etapas do relatório (sem `semeia_dados`: a entrada vem da ingestão) | a união dos dois |

- **Variáveis que não valem para o tipo são ignoradas** (não dá erro, e não sobra arquivo): `origens`, `motor_ingestao` e `modo_carga` não afetam o `relatorio` (uma só DAG, nenhuma etapa de origem); `usa_python`/`usa_spark` não afetam a `ingestao`. O formato continua sendo validado para todas as variáveis. O Cookiecutter não distingue "padrão" de "informado", por isso a recusa seria inconsistente.
- **Dados semeados (`relatorio`):** a etapa `semeia_dados` cria e popula `silver.vendas_exemplo` com `CREATE OR REPLACE TABLE ... AS SELECT ... UNION ALL ...` (sem `VALUES`, que o BigQuery não aceita como tabela). Em produção, remova a etapa.
- **Tabela base do `hibrido`:** o relatório lê a silver da primeira origem disponível, na ordem `arquivo`, `api`, `pubsub` (`registros_arquivo`, `registros_api`, `eventos`); todas têm `id`, `nome` e `valor`.
- **DAG única por motor:** o `hibrido` usa o mesmo esquema da `ingestao`. Com um só motor, `dags/pipeline.py`; com `motor_ingestao=ambos`, `config/pipeline.yaml` (ingestão no Cloud Run) e `config/pipeline_dataproc.yaml` (igual, com `motor: dataproc`), e duas DAGs `<nome>_cloud_run` e `<nome>_dataproc`, ambas com o relatório inteiro. Não há `motor_relatorio`: `sql`/`view` viram tarefas do BigQuery, `python` e `qualidade` o Cloud Run Job, `spark` o lote do Dataproc.
- **`qualidade` roda sempre no Cloud Run** (também na DAG Dataproc); o texto antigo da especificação (operador do BigQuery) está desatualizado.
- **Spark:** a etapa é a última do YAML. `dataeng run local` precisa de `spark-submit` + Java; sem eles ele executa as outras etapas e falha só nela. Os testes do projeto gerado excluem a etapa `spark` da execução local; o template só testa geração, `validate-config` e `plan` para ela. **Spark nunca foi executado no desenvolvimento deste template.**
- **Dataproc exige imagem:** `execucao_padrao.dataproc.container_image` (e, com `spark`, `script_base_uri`) vêm como EXEMPLOS a trocar; o planejamento da DAG recusa a ingestão/`python` em Dataproc sem imagem.
- **SQL portátil:** o SQL de relatório usa só o subconjunto aceito por DuckDB (run local) e pela sintaxe BigQuery (`CREATE OR REPLACE ... AS SELECT`, `MERGE`, `UNION ALL`, `COUNT/SUM/AVG`, tipos `STRING`/`BIGINT`/`DECIMAL`). A sintaxe é validada como BigQuery; nunca rodou no BigQuery real.

O gancho `hooks/pre_gen_project.py` valida as respostas; `hooks/post_gen_project.py` remove o que não se aplica (DAG do outro motor, bloco da origem não escolhida, `sql/views/`, `spark/`; com a origem `api` não gera `tests/test_local_run.py`, que faria rede). Com `pubsub`, o `tests/test_local_run.py` do projeto publica mensagens sintéticas num `LocalPubSub` e o injeta em `build_context`: `dataeng run local` sozinho usa uma fila em memória vazia e termina ok com 0 linhas.

## Pub/Sub: operação

- **Dead-letter:** configure na assinatura `max_delivery_attempts` e um tópico dead-letter. Sem isso, um lote que falha sempre (ex.: verificação de qualidade bloqueante que falha por causa do conteúdo de uma mensagem) volta para sempre: a silver já foi gravada (de forma idempotente), mas nada é confirmado e o backlog não drena. Use verificação bloqueante sobre o conteúdo das mensagens com cuidado; prefira rejeitar por mensagem com `rejeitar_se` (vai para a quarentena e é confirmada).
- **Prazo de ack:** a biblioteca não estende o prazo de ack durante o processamento. Configure o `ackDeadlineSeconds` da assinatura (até 600 s) para cobrir um lote completo (pull + bronze + silver + quarentena + `dml_pos_carga`) e reduza `max_mensagens` se necessário. Um ack depois do prazo pode ser ignorado pelo serviço: as mensagens voltam (a carga é idempotente, mas o backlog não drena).
- **`modo_carga: merge` com chaves de negócio:** em cada lote fica a mensagem de maior `publish_time` por chave; as superadas são confirmadas sem ir para a silver nem para a quarentena. O merge é incondicional: um evento atrasado (mais antigo) que chegue numa execução posterior sobrescreve o mais novo já gravado.
- **Quarentena:** `_linha_original` guarda a mensagem inteira em base64 (com `attributes`), cerca de 33% maior que o original; mensagens grandes inflam a tabela `<tabela>_rejeitados`.

## Testes

```bash
pytest template-pipeline/tests -q     # a partir da raiz do repositório; precisa de dataeng-core[local]
```

Gera a matriz origem x motor x modo de carga (ingestão), `tipo_projeto` x `usa_python` x `usa_spark` (os 12 pares) e uma amostra de `hibrido` x origem x motor x modo de carga (cada valor de cada eixo aparece; o produto cartesiano levaria minutos), valida/planeja cada projeto (inclusive o planejamento das tarefas da DAG em `prod`, sem Airflow), executa os exemplos locais e roda os testes de projetos gerados.

`tests/test_hybrid_anp.py` é o **aceite híbrido da ANP**: gera um `hibrido`, troca `pipeline.yaml`, ambiente, schemas e SQL pelos de `tests/fixtures/anp/` (amostra 100% sintética de 49 linhas, com uma coluna `uf` a mais que a da biblioteca), roda `dataeng plan` e `dataeng run local` duas vezes e confere: silver com 44 linhas, 3 linhas na quarentena, view de preço médio por UF e agregado por `MERGE` com valores calculados à mão, ausência de duplicatas e uma linha de controle por etapa por execução. No Windows, o caminho do bronze local (`.local/...`) pode estourar o limite de 260 caracteres com pastas temporárias longas; por isso o projeto do aceite se chama `anp`.

## Não validado

DAG real no Airflow, Cloud Run Job, Dataproc, BigQuery (inclusive o SQL do relatório), Spark/PySpark (nunca executado), Pub/Sub real (assinatura, ack e reentrega) e a instalação da biblioteca via Git/Artifact Registry não foram executados; a checagem é estática (texto do `pyproject.toml`/`Dockerfile`, AST das DAGs).

Motor Dataproc (`ingestao`/`python`): exige uma IMAGEM do lote com o `dataeng-core` instalado (suposição de infraestrutura, a biblioteca não a cria). Informe `execucao_padrao.dataproc.container_image` no `pipeline_dataproc.yaml` (ou `ingest_entrypoint_uri`/`run_step_entrypoint_uri`); sem isso o planejamento da DAG falha com erro explicativo. A etapa `qualidade` roda sempre no Cloud Run, mesmo nessa DAG.

## Notas de segurança e limites

- **Validação:** `hooks/pre_gen_project.py` recusa, antes de gerar qualquer arquivo, valores fora de um formato estrito (cron, ids de projeto/bucket/dataset, região, versão `X.Y.Z`, URLs `https` sem metacaracteres de shell). Por isso os valores entram nos YAML/TOML/Dockerfile sem escape adicional. Valores de escolha (`tipo_projeto`, `motor_ingestao`, `modo_carga`, `origem_da_biblioteca`) são recusados pelo próprio Cookiecutter. Nomes com `"` falham no Windows antes mesmo do gancho (limite do sistema de arquivos).
- **Nome longo:** a biblioteca trunca o nome do Cloud Run Job em 63 caracteres; prefira nomes curtos.
- **Artifact Registry:** `--extra-index-url` mistura o índice privado com o PyPI público e abre espaço para *dependency confusion*. Prefira um índice privado com o nome `dataeng-core` reservado ou, quando possível, `--index-url` único apontando para um repositório virtual.
- **Imagem:** a imagem base (`python:3.12-slim`) não é fixada por digest e, no modo `git`, o `git` permanece na imagem. Aceitável nesta versão.
- **dev.yaml e prod.yaml:** vêm das variáveis em `dev.yaml`: `projeto_gcp`, `regiao`, `bucket_bronze`, `dataset_silver`, `dataset_gold`. Em `prod.yaml` só `regiao`, `dataset_silver` e `dataset_gold` vêm das variáveis; projeto, bucket e Service Accounts (`meu-projeto-prod...`, `pipeline-*@...`) são exemplos fixos e fictícios a editar.
