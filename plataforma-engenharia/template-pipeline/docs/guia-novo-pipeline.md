# Guia: criar um novo pipeline

Este guia leva de zero a um pipeline que passa em `validate-config`, `plan` e `run local`, com a DAG do Airflow pronta para ser montada a partir do YAML. Tudo que vem antes de "Levar para o GCP" roda na sua máquina, sem nuvem. **A parte de GCP não foi validada** (veja o fim do guia).

Os blocos de comando marcados no texto fazem parte do teste `tests/test_docs_commands.py`: ele gera um projeto e executa cada um deles, esperando código de saída 0. Os blocos sem marca (`pip`, `gcloud`, `docker`) são só ilustrativos.

## 1. Pré-requisitos

- Python 3.11 ou mais novo.
- A biblioteca `dataeng-core` com o extra `local` e o `cookiecutter`. Na raiz do repositório do portfólio:

```bash
pip install -e "plataforma-engenharia/lib-dataeng-core[local]" cookiecutter
```

No Windows, use o Git Bash. O Airflow não roda no Windows: localmente a DAG só passa por checagem estática, e a importação real fica para o CI em Linux.

## 2. Gerar o projeto

Escolha o tipo (`ingestao`, `relatorio` ou `hibrido`, veja a seção 5) e rode o `cookiecutter`. O exemplo abaixo gera um **híbrido** com a origem `arquivo` e a etapa `python`, que é o projeto que o teste do guia usa:

```bash
cookiecutter plataforma-engenharia/template-pipeline --no-input nome_projeto=meu-pipeline tipo_projeto=hibrido origens=arquivo usa_python=sim
cd meu-pipeline
```

Sem `--no-input`, o `cookiecutter` pergunta cada variável. As principais:

| Variável | O que decide |
|---|---|
| `nome_projeto` | letras, dígitos, `-` e `_`; vira o nome do pipeline, da DAG e do Cloud Run Job |
| `tipo_projeto` | `ingestao`, `relatorio` ou `hibrido` |
| `origens` | texto separado por vírgula, entre `arquivo`, `api` e `pubsub` (ex.: `arquivo,pubsub`); só vale para `ingestao` e `hibrido` |
| `motor_ingestao` | `cloud_run`, `dataproc` ou `ambos` (com `ambos` saem dois YAMLs e duas DAGs, `<nome>_cloud_run` e `<nome>_dataproc`) |
| `modo_carga` | `append`, `overwrite_particao` ou `merge` |
| `usa_python`, `usa_spark` | `nao` (padrão) ou `sim`; habilitam as etapas `python` e `spark` em `relatorio` e `hibrido` |
| `projeto_gcp`, `regiao`, `bucket_bronze`, `dataset_silver`, `dataset_gold` | valores de exemplo, para trocar por reais depois |
| `origem_da_biblioteca` | `git` ou `artifact_registry`: como a imagem instala o `dataeng-core` |

As respostas são validadas antes de gerar qualquer arquivo; um valor fora do formato é recusado com a explicação. A lista completa e as regras de cada variável estão no [README do template](../README.md).

O `pyproject.toml` do projeto gerado instala a biblioteca por Git, na tag `dataeng-core-v0.1.0`, que ainda não existe (a versão 0.1.0 não foi lançada). Por isso, neste repositório, use o `dataeng-core` instalado em modo editável na seção 1 e acrescente só o `pytest` (`pip install pytest`) se quiser rodar os testes do projeto.

## 3. Entender o YAML

Tudo que é comum mora na biblioteca; o projeto tem **configuração**. O arquivo central é `config/pipeline.yaml`: uma lista de **etapas** tipadas, com dependências. No projeto gerado acima, as etapas são:

| Etapa | Tipo | O que faz |
|---|---|---|
| `ingere_arquivo` | `ingestao` | lê `data/amostra.csv`, grava no bronze e na silver, manda linhas ruins para a quarentena |
| `garante_tabela_base` | `sql` | cria a tabela da silver se a ingestão não gravou nada (DDL idempotente) |
| `view_resumo_por_nome` | `view` | cria `gold.resumo_por_nome` |
| `agrega_por_nome` | `sql` | `MERGE` em `gold.agregado_por_nome` |
| `enriquece` | `python` | função `custom.enriquecimento:rodar` |
| `checagem_final` | `qualidade` | `nao_nulo` e `unico` sobre o agregado |

Regras do YAML que mais pegam gente:

- `schema_version: 1` é obrigatório.
- Parâmetros em arquivos SQL e em YAML são **`${nome}`** (nunca `{{ }}`, que é do Jinja do Cookiecutter e do Airflow). Eles vêm de `parametros:`, do ambiente (`config/ambientes/<ambiente>.yaml`) e de `--param chave=valor`, nessa ordem de precedência crescente.
- Sem `depende_de`, a etapa depende da anterior. Ids repetidos e ciclos são recusados.
- Todo arquivo citado no YAML (SQL, schema, script) precisa existir **dentro da pasta do projeto**; um caminho que sai dela é recusado.
- `motor` só vale para `ingestao` e `python` (`cloud_run` ou `dataproc`); `sql`, `view` e `qualidade` não aceitam `motor`. Detalhes na seção 7.

O esquema completo é o conjunto de modelos Pydantic em [`lib-dataeng-core/src/dataeng_core/base/pipeline.py`](../../lib-dataeng-core/src/dataeng_core/base/pipeline.py); o [README da biblioteca](../../lib-dataeng-core/README.md) resume o contrato.

## 4. Validar, planejar e rodar local

`validate-config` confere o YAML, os arquivos referenciados e a sintaxe do SQL (dialeto BigQuery, com `sqlglot`). **Sintaxe válida não prova que o SQL roda no BigQuery.**

<!-- testar -->
```bash
dataeng validate-config --config config/pipeline.yaml
```

`plan` mostra a ordem das etapas, o motor de cada uma e o SQL já com os parâmetros preenchidos:

<!-- testar -->
```bash
dataeng plan --config config/pipeline.yaml
```

Com `--ambiente dev` ou `prod`, o `plan` usa os valores de `config/ambientes/dev.yaml` ou `prod.yaml`:

<!-- testar -->
```bash
dataeng plan --config config/pipeline.yaml --ambiente dev
```

`run local` executa o pipeline com DuckDB e uma pasta local (`.local/`), sem nuvem:

<!-- testar -->
```bash
dataeng run local --config config/pipeline.yaml --run-id 20260115T060000-guia01
```

Cada etapa imprime uma linha `[ok]` (ou `[falha]`), com as linhas lidas, gravadas e rejeitadas; o código de saída é 0 só se todas passaram.

### O `run_id` precisa ter data

O `run_id` identifica a execução e **também define a data de ingestão** (`_ingestion_date` e a pasta `ingestion_date=AAAA-MM-DD` do bronze): ele precisa começar por `AAAAMMDD`, como `20260115T060000-guia01`. Sem `--run-id`, a CLI cria um com a data de agora. Um valor sem data é recusado antes de gravar qualquer coisa:

```text
Detalhe: o run_id 'sem-data' não contém data (AAAAMMDD...). Use new_run_id() ou informe ingestion_date explicitamente.
```

Reexecutar com o **mesmo** `--run-id` é a forma de repetir a mesma tentativa: a carga é idempotente por `run_id` e partição, então a segunda execução não duplica linhas. Execute de novo o comando acima e a silver continua com as 5 linhas do `data/amostra.csv`. No Airflow, o `run_id` nativo (`scheduled__2026-01-01T06:00:00+00:00`) não serve; a DAG passa `{{ ts_nodash }}` (por exemplo `20260101T060000`), que se mantém igual nas novas tentativas da mesma tarefa.

Para ver o resultado, abra o DuckDB com Python:

```bash
python -c "import duckdb; c = duckdb.connect('.local/warehouse.duckdb', read_only=True); print(c.sql('select * from gold.agregado_por_nome').fetchall())"
```

A tabela `silver._controle_execucoes` guarda uma linha por etapa e execução (status, linhas, detalhe). Ela só cresce: a falha e a reexecução bem-sucedida ficam as duas registradas.

### Quando algo está errado

Os erros dizem o que aconteceu, o detalhe e como corrigir, e trazem uma referência que também aparece no log. Por exemplo, um arquivo que não existe:

```text
ERRO: Configuração inválida
Detalhe: arquivo config\pipeline.yaml: arquivos inexistentes (...): etapa 'view_resumo_por_nome' campo sql: sql/views/nao_existe.sql (operação: load_pipeline)
Como corrigir: Corrija o campo indicado no YAML e rode `dataeng validate-config`.
```

Códigos de saída da CLI: 0 ok, 1 falha de validação ou de execução, 2 uso incorreto dos argumentos.

## 5. Tipos de projeto

| Tipo | Para que serve | Etapas geradas |
|---|---|---|
| `ingestao` | levar uma origem para o bronze e a silver | uma `ingestao` por origem |
| `relatorio` | montar camadas curated/refined com SQL | `semeia_dados`, view, `MERGE`, `python` e `spark` opcionais, `qualidade` |
| `hibrido` | os dois no mesmo repositório | as ingestões + `garante_tabela_base` + as etapas do relatório |

No `relatorio` não existe origem: ele roda sobre dados semeados (`semeia_dados`, que você remove em produção). Variáveis que não valem para o tipo (por exemplo `origens` num `relatorio`) são ignoradas, sem erro e sem deixar arquivo para trás. O [README do template](../README.md) tem as pastas geradas de cada tipo.

## 6. Preencher o pipeline com os seus dados

### Origens da ingestão

- **`arquivo`:** `csv`, `json`, `jsonl` e `xlsx`, também dentro de `.zip`; `localizacao` pode ser um caminho do projeto, `gs://` ou `https://`. Informe `encoding` e `delimitador`. O `schema` descreve as colunas do arquivo (tudo texto; a tipagem é feita em `transformacao`).
- **`api`:** `endpoint`, `autenticacao` (`bearer`, `header` ou `basic`, sempre com `segredo_env`: o **nome** da variável de ambiente, nunca o segredo no YAML), `paginacao` (`pagina`, `offset` ou `cursor`) e `limite_taxa`. O projeto gerado com `api` não traz o teste de execução local, porque ele faria chamadas de rede.
- **`pubsub`:** pull em lote por execução (`assinatura`, `max_mensagens`, `tempo_maximo_s`); confirma (ack) só depois de gravar no bronze e na silver, e deduplica por `message_id`. A silver exige a coluna `message_id` no schema de destino. Localmente, `dataeng run local` sozinho usa uma fila em memória vazia e termina ok com 0 linhas; o `tests/test_local_run.py` do projeto publica mensagens sintéticas num `LocalPubSub`.

### Transformação, quarentena e qualidade da ingestão

A `transformacao` é declarativa: `renomear`, `tipar` (`texto`, `inteiro`, `decimal`, `data`, `booleano`, `timestamp`), `normalizar`, `deduplicar`, `rejeitar_se` e `hook` (uma função sua em `custom/`, referenciada como `modulo:funcao`). Uma linha reprovada em `rejeitar_se` (ou que não passa na tipagem) vai para `<tabela>_rejeitados`, com a linha original e o motivo, em vez de derrubar a carga. Valores nulos e vazios viram `None`.

A silver recebe as colunas de controle `_run_id`, `_source_file`, `_loaded_at` e `_ingestion_date`. Os nomes dessas colunas são reservados: uma origem que traga uma delas falha com o nome da coluna.

O bloco `qualidade` da ingestão (e a etapa `qualidade`) aceita `nao_nulo`, `unico`, `faixa`, `conjunto` e `formato`, com `limiar` e `bloqueante`. O resultado vai para a tabela de controle.

### Motores e modo de carga

`modo_carga: append` é idempotente por `run_id` (reexecutar o mesmo `run_id` não duplica); `overwrite_particao` reescreve a partição (por `_ingestion_date`, substitui tudo o que foi carregado no mesmo dia lógico, inclusive por outro `run_id`: prefira `merge` ou partição por coluna de negócio quando mais de um arquivo por dia for esperado); `merge` exige `chaves_merge`. Atenção ao `merge` entre execuções: ele é **incondicional**, então um registro mais antigo que chegue depois sobrescreve o mais novo já gravado.

## 7. Adicionar etapas

Acrescente a etapa em `etapas:` e o arquivo que ela cita; rode `validate-config` e `plan` a cada passo.

- **`sql`** (lista de arquivos, executados na ordem; para na primeira falha, sem atomicidade): escreva SQL **idempotente** (`MERGE`, `CREATE OR REPLACE`, sobrescrever partição). A validação avisa ao ver um `INSERT` sem padrão idempotente, mas não garante nada. Scripts procedurais do BigQuery (`DECLARE`, `BEGIN ... END`, `IF`, `LOOP`) **não são suportados**: viram erro explícito.

  ```yaml
  - id: agrega_semanal
    tipo: sql
    depende_de: [ingere_arquivo]
    sql: [sql/dml/agrega_semanal.sql]
  ```

- **`view`** (exatamente uma instrução `CREATE OR REPLACE VIEW`, ou um `SELECT` com `nome_view`):

  ```yaml
  - id: view_resumo
    tipo: view
    sql: sql/views/resumo.sql
  ```

- **`qualidade`:** verificações sobre tabelas já gravadas. Roda **sempre como Cloud Run Job** (mesmo numa DAG Dataproc), porque executa as verificações em Python sobre os dados lidos e grava o resultado na tabela de controle; ela não vira operador do BigQuery.

  ```yaml
  - id: checagem_final
    tipo: qualidade
    verificacoes:
      - nao_nulo: {tabela: "${dataset_gold}.agregado_por_nome", colunas: [nome]}
  ```

- **`python`:** `funcao: custom.modulo:funcao`, que recebe o contexto da etapa e devolve um resultado (veja `custom/enriquecimento.py`, gerado com `usa_python=sim`). Escreva-a idempotente, usando `merge` por chave. Com `motor: dataproc`, exige a imagem do lote (seção 8).
- **`spark`:** `script: spark/recalcula.py` (gerado com `usa_spark=sim`), sempre no Dataproc Serverless na DAG. O script recebe `--etapa`, `--run-id` e `--param chave=valor`. Localmente, `dataeng run local` precisa de `spark-submit` e Java; sem eles ele executa as demais etapas e falha só nessa, e por isso ela fica por último. **O Spark nunca foi executado no desenvolvimento** (veja a seção 9).

Todo SQL dos exemplos e dos DDLs do template usa só o subconjunto que o DuckDB e o BigQuery aceitam, com os tipos `STRING`, `INT64`, `TIMESTAMP`, `DECIMAL(p, s)` e `DATE`. Não use `VARCHAR`, `TIMESTAMPTZ`, `BIGINT` ou `INTEGER`: o DuckDB aceita, o BigQuery não, e o `validate-config` só confere a sintaxe.

## 8. Levar para o GCP

Esta parte **não foi executada**. Ela lista o que muda e o que você precisa conferir.

1. **Ambientes:** troque os valores fictícios de `config/ambientes/dev.yaml` e `prod.yaml` (`projeto_gcp`, `regiao`, `bucket_bronze`, `dataset_silver`, `dataset_gold`, `service_account`). Segredos de API ficam em variáveis de ambiente (Secret Manager no Cloud Run), nunca no YAML nem na imagem.
2. **Imagem:** o `Dockerfile` instala `dataeng-core[gcp]` na versão fixada (por Git ou pelo Artifact Registry, conforme `origem_da_biblioteca`) e usa `python -m dataeng_core.jobs.run_step` como `ENTRYPOINT`. Construa e publique a imagem; o Cloud Run Job precisa ter o mesmo nome que a biblioteca calcula a partir de `nome` (minúsculas, dígitos e `-`).

   ```bash
   docker build -t REGIAO-docker.pkg.dev/PROJETO/REPOSITORIO/meu-pipeline:0.1.0 .
   ```

3. **DAG:** `dags/pipeline.py` chama `build_dag(...)`; a biblioteca cria tarefas que executam o Cloud Run Job (já existente) para `ingestao`, `python` e `qualidade`, tarefas do BigQuery para `sql` e `view` e um lote do Dataproc Serverless para `spark` e para a ingestão com `motor: dataproc`. Com `motor_ingestao=ambos` saem duas DAGs que escrevem nas **mesmas tabelas**: não ative as duas ao mesmo tempo.
4. **Dataproc:** a ingestão e a etapa `python` em Dataproc exigem uma **imagem do lote** com o `dataeng-core` instalado (`execucao_padrao.dataproc.container_image`); sem ela o planejamento da DAG recusa. O valor gerado é um exemplo a trocar. Os arquivos de entrada do lote (`ingest_entrypoint_uri`, `run_step_entrypoint_uri`) e, com `spark`, o `script_base_uri` são suposições de infraestrutura a conferir.
5. **Pub/Sub:** configure na assinatura o prazo de ack (`ackDeadlineSeconds`, até 600 s) para cobrir um lote inteiro e um tópico dead-letter com `max_delivery_attempts`. A biblioteca não estende o prazo de ack. Detalhes no README do template.

A importação real da DAG com o Airflow só acontece no CI, em Linux (`plataforma-engenharia/lib-dataeng-core/scripts/ci_import_generated_dags.py` gera três projetos e importa cada `dags/*.py`); localmente ela só passa por checagem estática. Esse fluxo nunca foi executado no GitHub.

## 9. O que não foi validado

Tudo acima foi implementado e testado **com a versão local** (DuckDB, pasta local, Pub/Sub em memória e clientes falsos). Não houve credenciais nem rede de GCP durante o desenvolvimento. Não está provado:

- BigQuery, Cloud Storage e Pub/Sub reais: o SQL e as chamadas só foram verificados por sintaxe e com clientes falsos; os testes de contrato (`@pytest.mark.gcp`) existem e são pulados sem credenciais.
- Airflow: a montagem real da DAG com os operadores do Google não rodou (o Airflow não roda no Windows); o CI em Linux é quem a importa pela primeira vez.
- Cloud Run Job, Dataproc Serverless e a imagem (`docker build`): nada disso foi executado.
- Spark: o motor PySpark nunca foi executado no desenvolvimento (sem Java nem PySpark na máquina); o CI em Linux roda o job `spark` pela primeira vez.
- Nenhum fluxo do GitHub Actions foi executado.

A tabela completa de limitações está no [README da biblioteca](../../lib-dataeng-core/README.md#limitações-e-o-que-não-foi-validado).
