# dataeng-core

Biblioteca Python de engenharia de dados (distribuição `dataeng-core`, importação `dataeng_core`): ingestão de arquivos, APIs e Pub/Sub para o bronze, transformação declarativa para a silver, qualidade, SQL parametrizado, etapas tipadas, jobs, montagem de DAGs do Airflow e a CLI `dataeng`.

Ela é a parte comum dos pipelines gerados pelo [`template-pipeline`](../template-pipeline/README.md): o projeto gerado tem só configuração, schemas, SQL, DAG e ganchos; a lógica mora aqui, versionada. A versão **0.1.0 ainda não foi lançada** (veja o [CHANGELOG](CHANGELOG.md)).

> **Status: implementado e testado com a versão local** (DuckDB, pasta local, Pub/Sub em memória, clientes falsos). A execução em GCP, no Airflow, no Spark e no CI do GitHub **não foi validada**. Veja [Limitações e o que não foi validado](#limitações-e-o-que-não-foi-validado).

## Instalação

Requer Python 3.11 ou mais novo. O pacote base não depende de nada de GCP. Extras:

| Extra | Traz | Quando usar |
|---|---|---|
| `local` | DuckDB | desenvolvimento e testes sem GCP |
| `gcp` | Cloud Storage, BigQuery, Pub/Sub (e `pyarrow`, `db-dtypes`) | Cloud Run e Composer |
| `spark` | PySpark | Dataproc e transformação em Spark |
| `airflow` | Apache Airflow e provedor Google | montagem das DAGs |
| `dev` | pytest, ruff, mypy, import-linter, cookiecutter e demais ferramentas | desenvolvimento da biblioteca |

```bash
pip install -e ".[local,dev]"     # a partir de plataforma-engenharia/lib-dataeng-core/
pytest -q                         # testes locais; os de GCP, Airflow e Spark são pulados se faltar o ambiente
```

O projeto gerado fixa a versão (`dataeng-core[gcp]==X.Y.*`) e instala por Git (`...@dataeng-core-vX.Y.Z#subdirectory=plataforma-engenharia/lib-dataeng-core`) ou pelo Artifact Registry. As tags têm o prefixo `dataeng-core-v`, por causa das várias pastas do repositório.

## Arquitetura em camadas

Da base para o topo: `base → storage, warehouse, pubsub → sources, transform, quality, sql → steps → jobs → airflow, cli`. Cada camada só importa das de baixo, e o `import-linter` verifica isso (`lint-imports`; o CI roda o mesmo comando).

| Camada | O que tem |
|---|---|
| `base` | leitura do YAML e dos ambientes, modelos Pydantic do `pipeline.yaml`, erros tipados (o que aconteceu e o que fazer), retry com espera crescente, log estruturado, redação de segredos, `run_id` |
| `storage` | interface `Storage`: `LocalStorage` e `GcsStorage` |
| `warehouse` | interface `Warehouse`: `LocalWarehouse` (DuckDB) e `BigQueryWarehouse` |
| `pubsub` | interface `PubSubClient` (`pull`, `ack`, `nack`): `LocalPubSub` (em memória) e `GcpPubSub` |
| `sources` | arquivos (`csv`, `json`, `jsonl`, `xlsx`, `zip`), API (paginação, autenticação, limite de taxa) e Pub/Sub (pull em lote) |
| `transform` | transformação declarativa em dois motores: pandas e PySpark |
| `quality` | verificações `nao_nulo`, `unico`, `faixa`, `conjunto`, `formato` |
| `sql` | preenche `${param}`, divide em instruções, valida a sintaxe (`sqlglot`, dialeto BigQuery) |
| `steps` | executor e registro dos tipos de etapa |
| `jobs` | pontos de entrada: `ingest`, `ingest_spark`, `run_step` |
| `airflow` | planejador de tarefas (puro, sem importar o Airflow) e `build_dag` |
| `cli` | `dataeng` |

## CLI

`dataeng` (ou `python -m dataeng_core.cli`). Códigos de saída: 0 ok, 1 falha de validação ou de execução, 2 uso incorreto.

| Comando | O que faz |
|---|---|
| `dataeng validate-config --config config/pipeline.yaml [--ambiente A] [--param k=v]` | valida o YAML, os arquivos referenciados e a sintaxe do SQL |
| `dataeng plan --config config/pipeline.yaml [--ambiente A] [--run-id ID]` | mostra a ordem das etapas, o motor e o SQL já renderizado |
| `dataeng run local --config config/pipeline.yaml [--run-id ID] [--param k=v]` | executa o pipeline com DuckDB e pasta local |

`validate-config` e `plan` usam o ambiente `local` por padrão. Os jobs de nuvem (`python -m dataeng_core.jobs.run_step`, `jobs.ingest`, `jobs.ingest_spark`) exigem `--ambiente` explícito.

## Tipos de etapa, origens e motores

| Tipo | O que faz | Motor |
|---|---|---|
| `ingestao` | origem → bronze → silver, com quarentena e qualidade | `cloud_run` (padrão) ou `dataproc` |
| `sql` | arquivos `.sql` com `${param}`, executados em ordem | BigQuery |
| `view` | uma view (`CREATE OR REPLACE VIEW`) | BigQuery |
| `python` | função própria `modulo:funcao` | `cloud_run` ou `dataproc` |
| `spark` | script PySpark | Dataproc |
| `qualidade` | verificações sobre tabelas | Cloud Run Job (veja as limitações) |

Origens da ingestão: `arquivo` (`csv`, `json`, `jsonl`, `xlsx`, também em `.zip`; caminho local, `gs://` ou `https://`), `api` e `pubsub`. O template aceita ainda `motor_ingestao: ambos` (dois YAMLs e duas DAGs); isso é uma variável do template, não um valor do `pipeline.yaml`.

## O `pipeline.yaml`

Resumo do contrato (o esquema completo são os modelos Pydantic em [`src/dataeng_core/base/pipeline.py`](src/dataeng_core/base/pipeline.py)):

- `schema_version: 1`, `nome`, `agenda` (cron), `parametros`, `etapas`, `execucao_padrao` (recursos por motor).
- Cada etapa tem `id`, `tipo`, `depende_de` (sem ele, depende da anterior) e `motor` quando o tipo aceita. A validação rejeita ids repetidos, ciclos, motor incompatível com o tipo e arquivos inexistentes ou fora da pasta do projeto.
- Parâmetros em YAML e SQL são **`${nome}`**, nunca `{{ }}`. Precedência: `parametros` do YAML, depois o ambiente (`config/ambientes/<ambiente>.yaml`), depois `--param`. `${nome}` não definido é erro antes de executar.
- `destino.modo_carga`: `append`, `overwrite_particao` ou `merge` (este exige `chaves_merge`). Com a origem `pubsub`, `overwrite_particao` é recusado.
- Exemplo completo e anotado: o projeto gerado pelo template, e o [guia de novo pipeline](../template-pipeline/docs/guia-novo-pipeline.md).

## Contratos importantes

- **Nulos:** célula vazia, `NaN` e `null` viram `None` ao ler arquivos.
- **`run_id` com data:** o `run_id` começa por `AAAAMMDD` (formato de `new_run_id()`: `AAAAMMDDTHHMMSS-xxxxxx`) e define a `_ingestion_date`; sem data e sem `ingestion_date` explícita, a execução falha antes de gravar. O `run_id` nativo do Airflow (`scheduled__...`) não serve; a DAG usa `{{ ts_nodash }}`, estável entre as novas tentativas.
- **Bronze:** `<bucket>/bronze/<nome>/ingestion_date=AAAA-MM-DD/run_id=<id>/<arquivo>`, com manifesto (tamanho e SHA-256).
- **Silver:** colunas de controle `_run_id`, `_source_file`, `_loaded_at` e `_ingestion_date` (nomes reservados: uma origem que traga uma delas falha).
- **Quarentena:** `<tabela>_rejeitados`, com a linha original e o motivo (o motivo não repete o valor da célula).
- **Controle:** `_controle_execucoes`, no dataset da silver; uma linha por etapa e execução, só acrescenta (falha e reexecução ficam as duas registradas).
- **Idempotência:** reexecutar o mesmo `run_id` não duplica (por `run_id` e partição). O `append` da ingestão usa essa regra. O SQL do usuário é idempotente só se ele o escrever assim; a validação apenas avisa ao ver um `INSERT` sem padrão idempotente.
- **Pub/Sub:** pull em lote; **confirma só depois de gravar** no bronze, na silver, na quarentena e depois da qualidade; deduplica por `message_id`; mensagem inválida vai para a quarentena e é confirmada; registra o backlog restante (`NULL` com o `GcpPubSub`).
- **Segredos:** o YAML guarda só o **nome** da variável de ambiente (`segredo_env`); o valor é redigido de logs, mensagens de erro e da tabela de controle.

## Limitações e o que não foi validado

Tudo foi implementado e testado com a versão local; validar no GCP antes de confiar. Nada abaixo foi provado.

| Limite ou risco | Situação |
|---|---|
| BigQuery, Cloud Storage e Pub/Sub reais | Só testados com clientes falsos. Os testes de contrato (`@pytest.mark.gcp`) existem e são pulados sem `GOOGLE_APPLICATION_CREDENTIALS` e variáveis `DATAENG_GCP_*`; os de Pub/Sub e Storage esvaziam/listam o recurso, então exigem recursos dedicados. Sem confirmação: o script transacional do BigQuery (`BEGIN`/`EXCEPTION`/`RAISE`), o `DELETE` com `EXISTS` correlacionado, o `MERGE` com `IS NOT DISTINCT FROM`, `load_table_from_dataframe` com pandas 3 e `pyarrow`, o cliente mínimo `google-cloud-bigquery>=3.17` |
| Pub/Sub real | Sem confirmação: ack/nack e reentrega, `return_immediately`, formato de `publish_time`, ordem. O `ackDeadlineSeconds` da assinatura precisa cobrir um lote inteiro (a biblioteca não estende o prazo), e um lote que falha sempre só sai da fila com dead-letter configurado |
| Airflow real | A montagem da DAG com o Airflow e os operadores do Google nunca rodou (o Airflow não roda no Windows). Só o planejador, a checagem estática e um Airflow falso foram exercitados. O formato dos `overrides` do Cloud Run, o Jinja em `args` e `batch_id` e a reanexação do lote do Dataproc dependem da versão do provedor |
| Cloud Run Job | Nunca executado. Premissa: a imagem tem `ENTRYPOINT python -m dataeng_core.jobs.run_step` e a execução só sobrescreve os argumentos e o `timeout` (CPU e memória são do job) |
| Dataproc Serverless | Nunca executado. A ingestão e a etapa `python` em Dataproc **exigem a imagem do lote** com o `dataeng-core` (`execucao_padrao.dataproc.container_image`); os arquivos de entrada do lote (`ingest_entrypoint_uri`, `run_step_entrypoint_uri`) são premissas de infraestrutura, e a etapa `spark` exige `script_base_uri` ou `main_python_file_uri` |
| Spark | **O motor PySpark nunca foi executado no desenvolvimento** (sem Java nem PySpark na máquina): os testes `@pytest.mark.spark` e a equivalência pandas x Spark foram sempre pulados. O CI em Linux roda o job `spark` pela primeira vez e é quem confirma as premissas da API do PySpark. Diferenças conhecidas: no Spark o `formato` de data aceita só `%Y %y %m %d %H %M %S %f %%`; ganchos recebem DataFrame do Spark, não do pandas; o texto de timestamp depende do fuso da sessão |
| `docker build` e instalação do pacote | A imagem do template nunca foi construída. A instalação por Git (tag `dataeng-core-v0.1.0`, ainda inexistente) e pelo Artifact Registry nunca foi exercitada |
| GitHub Actions | Nenhum fluxo foi executado no GitHub. Os comandos foram rodados localmente e o `actionlint` não acusou erro, mas a primeira execução pode falhar (job `spark`, resolução de dependências com as constraints do Airflow 3.3.2, `build_dag` com o Airflow real). As versões e SHAs das actions foram conferidos pelo implementador e não por uma segunda pessoa |
| `qualidade` sempre roda como Cloud Run Job | Ela executa `run_checks` sobre um DataFrame (regex e semântica de nulos em Python, escrita na tabela de controle), por isso **não** vira operador do BigQuery nem roda em Dataproc: a DAG Dataproc também precisa do Cloud Run Job |
| SQL | Validado só por sintaxe (`sqlglot`, dialeto BigQuery); a execução local usa o DuckDB, que não fala o dialeto do BigQuery, então os exemplos usam um subconjunto portátil (`STRING`, `INT64`, `TIMESTAMP`, `DECIMAL`, `DATE`; `MERGE`, que exige DuckDB `>=1.4`; `CREATE OR REPLACE`). O SQL nunca rodou no BigQuery. **Scripts procedurais do BigQuery (`BEGIN`, `IF`, `LOOP`...) não são suportados** e falham de forma explícita. Uma string bruta `r'a\'b;c'` também falha (com erro, não silenciosamente) |
| Etapa `sql` | Executa uma instrução por vez e para na primeira falha, **sem atomicidade**; as já executadas ficam |
| `overwrite_particao` por `_ingestion_date` | Substitui **tudo** o que foi carregado no mesmo dia lógico, inclusive por outro `run_id`: dois arquivos ingeridos no mesmo dia se sobrescrevem. Prefira `merge` ou partição por coluna de negócio quando mais de um arquivo por dia for esperado |
| `merge` por chave de negócio | **Incondicional entre execuções**: um evento atrasado (mais antigo) que chegue depois sobrescreve o mais novo já gravado. Dentro de um lote do Pub/Sub, vale a mensagem de maior `publish_time` |
| Quarentena e reexecução | A quarentena usa a chave da silver: reexecutar sem linhas rejeitadas mantém as rejeitadas da tentativa anterior; reexecutar o mesmo `run_id` com a origem alterada mantém as linhas antigas que sumiram |
| Origem `arquivo` por URL | Uma tentativa de download, sem redirecionamento, com limite de 512 MiB; o Airflow refaz a tarefa |
| BigQuery e DataFrame vazio | Carregar um DataFrame vazio numa tabela inexistente não a cria (o `LocalWarehouse` cria) |
| pandas | Piso `>=2.1`; testado em 3.x e em 2.2.3, não em 2.1 (sem roda para Python 3.13) |
| Windows | O Airflow não roda; caminhos longos de pasta temporária podem passar de 260 caracteres no bronze local |

## Desenvolvimento

```bash
ruff check . && ruff format --check .
mypy src
lint-imports
pytest -q
```

## Nota

Esta pasta é tratada como um repositório independente: README, `pyproject.toml`, CHANGELOG e CI próprios (`.github/workflows/lib-dataeng-core.yml`, na raiz do monorepo).
