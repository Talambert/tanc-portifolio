# Templates e bibliotecas de engenharia de dados: especificação de desenho

**Data:** 19/10/2026 · **Status:** revisada, aguardando nova leitura · **Escopo:** biblioteca comum + template de pipeline (tipos `ingestao`, `relatorio` e `hibrido`).

## 1. Objetivo

Quando chegar uma demanda, gerar um repositório novo a partir de um template, preencher a configuração e ter o pipeline funcionando, **sem reescrever o que é comum** a várias demandas. A demanda pode ser de **ingestão**, de **relatório** (camadas curated/refined) ou **híbrida** (as duas no mesmo repositório). O que é comum vive numa biblioteca versionada. O que é específico da demanda vive no repositório gerado.

**Para quem:** engenheiros de dados que recebem demandas em GCP, com Airflow/Cloud Composer.
**Como saber que deu certo:** seção 10.

## 2. Decisões

| Decisão | Escolha | Motivo |
|---|---|---|
| Geração dos repositórios | **Cookiecutter, sem atualização.** O repositório é gerado uma vez e fica independente | Simplicidade de manter. Consequência direta: o que precisa evoluir mora na **biblioteca**, e o template fica fino |
| Organização das bibliotecas | **Um repositório, um pacote** (`dataeng-core`), módulos por função e extras de instalação | Para equipe pequena, uma versão e uma publicação. As camadas internas são verificadas, então separar em vários pacotes depois é mecânico. Repositório por função foi descartado: custo de coordenação alto |
| Templates | **Um único `template-pipeline`**, com a pergunta `tipo_projeto` = `ingestao`, `relatorio` ou `hibrido` | O híbrido é a união dos outros dois. Três templates duplicariam CI, Dockerfile, estrutura de DAG e ambientes |
| Modelo de pipeline | Uma **lista de etapas tipadas** (`ingestao`, `sql`, `view`, `python`, `spark`, `qualidade`) com dependências | Ingestão, relatório e híbrido são combinações das mesmas etapas |
| Motores | Cloud Run (Job), Dataproc (lote Serverless) e BigQuery (SQL e views), escolhidos **por etapa** | Um template cobre todos os casos |
| Pub/Sub | **Pull em lote agendado**, confirmando o recebimento só depois de gravar no bronze | Encaixa no modelo de DAGs agendadas, é idempotente e testável sem GCP |
| Relatório | **SQL puro orquestrado pelo Airflow**. O `dbt` entra no futuro como um novo tipo de etapa | Atende ao pedido (inserts, DML e views) sem nova ferramenta |
| DAG | Arquivo no repositório que **monta a DAG a partir do `pipeline.yaml`** (função da biblioteca), com gancho para tarefas extras | O padrão vem da configuração, e o projeto pode editar |
| Onde fica a lógica | Na biblioteca. O repositório gerado tem configuração, schemas, DDL/DML/views, DAG e ganchos | Melhorias chegam por atualização de versão |
| Teste sem GCP | Interfaces `Storage`, `Warehouse` e `PubSubClient` com implementação local | O projeto ainda não está conectado ao GCP, e a lógica precisa ser verificável |
| Pastas | Duas pastas na raiz do portfólio, com prefixo, tratadas como repositórios independentes | Podem ser extraídas depois sem renomear |

## 3. Estrutura e ordem

```text
lib-dataeng-core/     pacote Python `dataeng_core` (distribuição `dataeng-core`)
template-pipeline/    template Cookiecutter: ingestao | relatorio | hibrido
```

O repositório gerado depende de `dataeng-core[...]==X.Y`. O CI da biblioteca **gera um projeto de cada tipo e roda os testes dele**, para nenhuma mudança quebrar o template sem aviso. Outras famílias de template (por exemplo, serviço de API, infraestrutura) seriam pastas à parte, no futuro.

**Fases de construção** (cada fase termina com algo verificável, seção 13):
1. Núcleo da biblioteca e o tipo `ingestao` (arquivo e API).
2. Pub/Sub em pull.
3. Etapas de relatório (`sql`, `view`, `python`, `spark`), o montador de DAG e os tipos `relatorio` e `hibrido`.
4. Acabamento: guias, CI completo e adaptação do `combustiveis-pipeline-gcp`.

## 4. Biblioteca `dataeng_core`

### 4.1 Camadas

Cada camada só importa das de baixo. Uma verificação automática (`import-linter`) no CI impede o contrário.

```text
base       ler YAML, variáveis de ambiente e argumentos; validar o esquema; log estruturado;
           erros tipados; retry com espera crescente; id de execução
storage    interface Storage                  → GcsStorage | LocalStorage
warehouse  interface Warehouse                → BigQueryWarehouse | LocalWarehouse (DuckDB)
pubsub     interface PubSubClient             → GcpPubSub | LocalPubSub
sources    api (paginação, autenticação, limite de taxa) | files (csv, json, xlsx, zip, encoding)
           | pubsub (pull em lote)
transform  limpeza declarativa: tipar, renomear, normalizar, deduplicar, separar linhas rejeitadas
quality    verificações (não nulo, único, faixa, conjunto, formato) com resultado em tabela de controle
sql        preencher parâmetros, dividir em instruções, validar a sintaxe e, com GCP, fazer dry-run
steps      executor de etapas e registro de tipos: ingestao, sql, view, python, spark, qualidade
jobs       pontos de entrada: ingest (Python), ingest_spark (PySpark), run_step
airflow    montador de DAG a partir do pipeline.yaml, operadores e callbacks
cli        `dataeng validate-config`, `dataeng plan` e `dataeng run local`
```

Ordem das dependências: `base → storage, warehouse, pubsub → sources, transform, quality, sql → steps → jobs → airflow, cli`.

### 4.2 Extras de instalação

| Extra | Traz | Quando usar |
|---|---|---|
| (nenhum) | Núcleo, configuração, `sources` (arquivo e API), `transform`, `quality`, `sql` | Base |
| `[local]` | DuckDB | Desenvolvimento e testes sem GCP |
| `[gcp]` | `google-cloud-storage`, `google-cloud-bigquery`, `google-cloud-pubsub` | Cloud Run e Composer |
| `[spark]` | PySpark | Dataproc |
| `[airflow]` | Montador de DAG e operadores | DAGs do repositório gerado |
| `[dev]` | Ferramentas de teste e lint | Desenvolvimento da biblioteca |

### 4.3 Interfaces

- **`Storage`:** `read`, `write`, `list`, `exists`, `delete`.
- **`Warehouse`:** `execute_ddl`, `run_sql` (arquivos e instruções, com parâmetros), `create_or_replace_view`, `load` (modos `append`, `overwrite_partition`, `merge`), `query`, `table_exists`, `row_count`.
- **`PubSubClient`:** `pull(assinatura, max_mensagens, timeout)`, `ack(ids)` e `nack(ids)`. A versão local é um Pub/Sub falso em memória. O `GcpPubSub` não informa o backlog (fica `NULL`; Tarefa 18).
- **Limite do DuckDB:** ele não fala o dialeto do BigQuery. DDLs, DMLs e views do projeto são **validados na sintaxe** com `sqlglot` (dialeto BigQuery) e só executados no BigQuery real. Os SQL de **exemplo e de teste** são escritos num **subconjunto portátil**, que também roda no DuckDB.

### 4.4 Transformação em dois motores

A transformação é **declarativa** (no YAML) e tem duas implementações: Python (pandas) e PySpark. Um conjunto comum de casos de equivalência roda nas duas. O PySpark roda em modo local no CI (Linux).

### 4.5 Regras de execução

- Cada rodada tem um `run_id`, que **precisa começar por `AAAAMMDD`** (ou o chamador informa `ingestion_date`); sem data a execução falha (Tarefa 12). No Airflow, o `run_id` nativo é recusado e a DAG usa `{{ ts_nodash }}` (Tarefa 16). A carga é **idempotente**: reexecutar não duplica.
- **Bronze:** `<bucket>/bronze/<nome>/ingestion_date=AAAA-MM-DD/run_id=<id>/<arquivo>`, com um manifesto (tamanho e SHA-256).
- **Silver:** tabela particionada por `_ingestion_date`, com as colunas de controle `_run_id`, `_source_file`, `_loaded_at` e `_ingestion_date` (item 8 da seção 14).
- **Quarentena:** `<tabela>_rejeitados`, com a linha original e o motivo.
- **Controle:** `_controle_execucoes` (run_id, etapa, status, linhas lidas, gravadas e rejeitadas, backlog restante, resultado da qualidade).
- **Pub/Sub:** puxa até `max_mensagens` ou `tempo_maximo_s`, grava em lotes JSON no bronze **com os metadados da mensagem** (id, horário de publicação, atributos) e **só então confirma**. Se a gravação falhar, nada é confirmado e as mensagens voltam. Como a entrega é "pelo menos uma vez", a silver deduplica por `message_id`. O backlog restante é registrado na tabela de controle.
- **Etapas `sql` e `view`:** os arquivos usam parâmetros no formato `${nome}` (preenchidos pela biblioteca, por exemplo `${data_ref}` e `${dataset_gold}`), que não colidem com o Jinja do Cookiecutter nem com a sintaxe do BigQuery. O guia recomenda padrões idempotentes (`MERGE`, sobrescrever partição, `CREATE OR REPLACE`). A validação **avisa** ao ver um `INSERT` sem padrão idempotente, sem garantir nada.
- **Erros:** tipados, com "o que aconteceu" e "o que fazer", no mesmo espírito do portal de governança.

### 4.6 API pública e versões

A API pública é: pontos de entrada de `jobs`, o esquema do `pipeline.yaml`, o registro de tipos de etapa e as interfaces `Storage`, `Warehouse` e `PubSubClient`. Versionamento semântico, `CHANGELOG.md` e `schema_version` no YAML. Uma versão nova da biblioteca continua lendo o `schema_version` anterior por pelo menos uma versão menor, com aviso de descontinuação. *(Na 0.1.0 não existe versão anterior: qualquer `schema_version` diferente de 1 falha com mensagem clara; ruling R11.)*

## 5. Template `template-pipeline`

### 5.1 Perguntas do gerador

| Variável | Valores |
|---|---|
| `nome_projeto` | texto, vira o nome do repositório e do pacote |
| `tipo_projeto` | `ingestao`, `relatorio` ou `hibrido` |
| `origens` | para `ingestao` e `hibrido`: uma ou mais entre `arquivo`, `api` e `pubsub`, como **texto separado por vírgula** (ex.: `arquivo,pubsub`; ruling R2) |
| `motor_ingestao` | `cloud_run`, `dataproc` ou `ambos` (para `ingestao` e `hibrido`). `ambos` é **variável do template**, não valor do `pipeline.yaml`: gera dois YAMLs e duas DAGs (ruling da Tarefa 22) |
| `usa_python`, `usa_spark` | para `relatorio` e `hibrido`: habilitam as etapas `python` e `spark` (e a pasta `spark/`) |
| `modo_carga` | `append`, `overwrite_particao` ou `merge` |
| `projeto_gcp`, `regiao` | texto |
| `bucket_bronze`, `dataset_silver`, `dataset_gold` | texto |
| `agenda_cron` | texto (padrão `0 6 * * 1`) |
| `origem_da_biblioteca` | `git` ou `artifact_registry` (define como instalar `dataeng-core`) |

Um gancho `post_gen_project` remove o que não se aplica ao tipo e às escolhas feitas.

### 5.2 Repositório gerado

```text
<nome-do-projeto>/
├── config/pipeline.yaml
├── config/ambientes/{local,dev,prod}.yaml
├── schemas/                       schemas de origem e de destino (ingestao e hibrido)
├── sql/ddl/ · sql/dml/ · sql/views/
├── dags/pipeline.py               monta a DAG a partir do pipeline.yaml
├── custom/                        ganchos e funções próprias (sempre gerada)
├── spark/                         scripts PySpark (só se usa_spark)
├── Dockerfile · pyproject.toml · README.md
└── tests/
```

`tipo_projeto: ingestao` não gera `sql/views/` nem `spark/`, e `relatorio` não gera os `schemas/` de origem. O `hibrido` gera tudo. A pasta `custom/` existe nos três tipos, porque os ganchos de transformação da ingestão também vivem nela.

### 5.3 `config/pipeline.yaml` (exemplo híbrido, mostrando todos os tipos de etapa)

```yaml
schema_version: 1
nome: precos_combustiveis
agenda: "0 6 * * 1"
parametros:                       # disponíveis nos SQL como ${...}
  dataset_silver: silver
  dataset_gold: gold

etapas:
  - id: ingere_precos
    tipo: ingestao
    motor: cloud_run              # ou: dataproc
    origem:
      tipo: arquivo               # ou: api | pubsub
      localizacao: "https://exemplo.gov.br/dados/ca-${ano}-${semestre}.zip"
      formato: csv
      encoding: utf-8-sig
      delimitador: ";"
      schema: schemas/origem_precos.yaml
      # tipo: api -> endpoint, autenticacao (tipo e nome do segredo), paginacao, limite_taxa
    destino:
      bronze: "gs://${bucket_bronze}/bronze/precos_combustiveis/"
      tabela: "${dataset_silver}.precos_combustiveis"
      schema: schemas/destino_precos.yaml
      modo_carga: overwrite_particao   # ou: append | merge (o merge exige chaves_merge)
      particao: _ingestion_date
      chaves_merge: []                 # obrigatória quando modo_carga = merge
      ddl: [sql/ddl/silver_precos.sql]
    transformacao:
      - renomear: {"Valor de Venda": valor_venda}
      - tipar: {valor_venda: {tipo: decimal, separador_decimal: ","}}
      - normalizar: {cnpj_revenda: [apenas_digitos]}
      - deduplicar: [cnpj_revenda, produto, data_coleta]
      - rejeitar_se: [{coluna: cnpj_revenda, regra: nao_nulo}]
      - hook: custom.ajustes:corrigir_linhas_deslocadas   # opcional
    qualidade:
      - nao_nulo: [cnpj_revenda, produto]
      - faixa: {coluna: valor_venda, minimo: 0.5, maximo: 30, limiar: 0.99}

  - id: ingere_eventos
    tipo: ingestao
    motor: cloud_run
    origem:
      tipo: pubsub
      assinatura: "projects/${projeto_gcp}/subscriptions/precos-eventos"
      max_mensagens: 100000
      tempo_maximo_s: 600
      formato: json
      dedupe_por: message_id
      schema: schemas/origem_eventos.yaml
    destino:
      bronze: "gs://${bucket_bronze}/bronze/eventos/"
      tabela: "${dataset_silver}.eventos_preco"
      schema: schemas/destino_eventos.yaml
      modo_carga: append
      particao: _ingestion_date
      ddl: [sql/ddl/silver_eventos.sql]

  - id: view_preco_medio_uf
    tipo: view
    depende_de: [ingere_precos]
    sql: sql/views/preco_medio_uf.sql           # CREATE OR REPLACE VIEW ${dataset_gold}.preco_medio_uf

  - id: agrega_semanal
    tipo: sql
    depende_de: [ingere_precos, ingere_eventos]
    sql: [sql/dml/agrega_semanal.sql]           # MERGE idempotente

  - id: enriquece
    tipo: python
    motor: cloud_run
    depende_de: [agrega_semanal]
    funcao: custom.enriquecimento:rodar

  - id: recalcula_grande
    tipo: spark
    depende_de: [agrega_semanal]
    script: spark/recalcula.py
    dataproc: {runtime: "2.2", executores: 2}

  - id: checagem_final
    tipo: qualidade
    depende_de: [view_preco_medio_uf, enriquece, recalcula_grande]
    verificacoes:
      - nao_nulo: {tabela: "${dataset_gold}.agregado_semanal", colunas: [uf, semana]}

execucao_padrao:
  cloud_run: {cpu: 1, memoria: 2Gi, timeout: 3600}
```

- Sem `depende_de`, a etapa depende da anterior.
- A validação rejeita ids repetidos, ciclos, motor incompatível com o tipo (por exemplo, `view` com `motor`) e arquivos inexistentes.
- Os ambientes (`config/ambientes/*.yaml`) definem projeto, bucket, dataset, Service Account e região, e preenchem as chaves `${...}`.

### 5.4 DAG

`dags/pipeline.py` tem poucas linhas e chama `dataeng_core.airflow.build_dag("config/pipeline.yaml")`. A biblioteca cria as tarefas por tipo: `sql` e `view` viram tarefas do BigQuery (sem contêiner); `ingestao`, `python` e **`qualidade`** viram Cloud Run Job (a `qualidade` executa `run_step`, porque `run_checks` opera sobre um DataFrame; Tarefa 22); `spark` e a ingestão ou `python` em Dataproc viram lote do Dataproc Serverless, que **exige uma imagem do lote com o `dataeng-core`** (`execucao_padrao.dataproc.container_image`) e tem entrypoints configuráveis (Tarefas 21 e 22). Um parâmetro permite acrescentar tarefas extras. Com `motor_ingestao=ambos` no template, geram-se dois YAMLs e duas DAGs (`<nome>_cloud_run` e `<nome>_dataproc`), que escrevem nas mesmas tabelas, o que permite comparar os motores.

### 5.5 Ganchos

O que for muito específico de uma origem ou de um processamento entra em `custom/` e é referenciado no YAML como `modulo:funcao`. Todo o resto é configuração.

## 6. Testes e CI

1. **Unitários da biblioteca**, nas implementações locais.
2. **Contrato das interfaces:** a mesma bateria roda contra a implementação local e contra a do GCP. A do GCP usa a marca `gcp` e é pulada sem credenciais.
3. **Pub/Sub local:** publica N mensagens, o job puxa e grava, só confirma depois de gravar, deduplica uma mensagem repetida, e uma falha ao gravar **não confirma** (as mensagens voltam).
4. **Geração do template em matriz:** `tipo_projeto` × origens × motor × modo de carga. Cada projeto gerado precisa passar em `validate-config`, `plan`, na sintaxe do SQL e nos próprios testes.
5. **Equivalência Python × PySpark** da transformação, com PySpark em modo local.
6. **Exemplo híbrido da ANP, local** (seção 9).

**CI (GitHub Actions, com filtro por pasta):** `ruff`, `mypy`, `import-linter`, testes, geração dos templates, build do pacote. Um job em Linux importa as DAGs com o Airflow (ele não roda no Windows). Localmente, as DAGs só passam por checagem estática.

## 7. Distribuição e versões

- Tags com prefixo por causa das várias pastas num repositório: `dataeng-core-vX.Y.Z`.
- Uso real: publicação no Artifact Registry (Python).
- Portfólio: instalação por Git (`git+https://...@dataeng-core-vX.Y.Z#subdirectory=lib-dataeng-core`), escolhida pela variável `origem_da_biblioteca`.
- O repositório gerado fixa a versão (`dataeng-core[gcp]==X.Y.*`). Atualizar é trocar a versão e rodar os testes.

## 8. Fora do escopo

Streaming contínuo (Dataflow ou serviço) e assinatura direta do Pub/Sub no BigQuery; origens em banco relacional (JDBC); `dbt` e views materializadas (candidatos a tipos de etapa futuros); atualização automática dos repositórios gerados; interface gráfica; outras famílias de template.

## 9. Validação com o caso da ANP

Uma amostra **sintética e pequena** (cerca de 50 linhas) reproduz os defeitos reais já mapeados nos dados da ANP: BOM, separador `;`, vírgula decimal, data `dd/mm/aaaa`, CNPJ com espaço no início e linha com `;` no endereço (colunas deslocadas). Um projeto `hibrido` gerado pelo template, com a origem `arquivo` e uma view de preço médio por UF mais um `MERGE` de agregação (em SQL portátil), deve:

- rodar `dataeng plan` e mostrar a ordem das etapas com o SQL já renderizado;
- rodar `dataeng run local` do arquivo até a view e a agregação;
- mandar as linhas defeituosas para a quarentena, com o motivo;
- não duplicar nada ao rodar de novo;
- gravar o resultado de cada etapa na tabela de controle.

O `combustiveis-pipeline-gcp` passa a ser o primeiro consumidor real: o plano dele é ajustado para usar o template.

## 10. Critérios de sucesso

- [ ] Projetos gerados passam em todas as combinações do teste de geração, nos três tipos.
- [ ] A ingestão e o exemplo híbrido da ANP rodam local de ponta a ponta e são idempotentes (seção 9).
- [ ] O Pub/Sub local confirma só depois de gravar e deduplica.
- [ ] As camadas da biblioteca são verificadas no CI e a transformação é equivalente nos dois motores.
- [ ] Existe um guia "criar um novo pipeline" que leva de zero a uma DAG planejada (a montagem real no Airflow não foi validada).
- [ ] A biblioteca e o template têm README, CHANGELOG e CI.

## 11. Limitações e riscos

| Risco ou limite | Tratamento |
|---|---|
| Sem GCP, a execução no Cloud Run, no Dataproc e no Composer, o BigQuery real, o Pub/Sub real e o build da imagem **não são provados** | Marcar no README como "implementado e testado com versão local; validar no GCP". Os testes de contrato do GCP ficam prontos para rodar quando houver credenciais |
| Dialeto do BigQuery diferente do DuckDB | Validar DDL, DML e views por sintaxe (`sqlglot`). Executar localmente só o subconjunto portátil dos exemplos |
| Idempotência do SQL é responsabilidade de quem o escreve | Guia com padrões idempotentes e aviso na validação |
| Entrega "pelo menos uma vez" do Pub/Sub e fila acumulada | Deduplicação por `message_id` e backlog na tabela de controle |
| Repositórios gerados envelhecem (sem atualização do template) | A lógica mora na biblioteca, versionada. O template fica fino |
| Duas implementações da transformação (pandas e PySpark) podem divergir | Casos de equivalência comuns, rodando no CI |
| Docker não instalado, e Airflow não roda no Windows | Builds de imagem e importação das DAGs no CI em Linux. **Nada disso foi executado**: o CI nunca rodou no GitHub e o PySpark nunca foi executado no desenvolvimento |
| Mudança de API do Cloud Run, do Dataproc, do Composer ou do Pub/Sub | Isolar o acesso em `airflow`, `warehouse`, `storage` e `pubsub`, de modo que uma mudança afete um lugar só |

## 12. Candidatos futuros

Tipo de etapa `dbt`; views materializadas; Pub/Sub por assinatura direta no BigQuery; streaming contínuo; origens em banco relacional; outras famílias de template (serviço de API, infraestrutura).

## 13. Fases e próximo passo

| Fase | Entrega verificável |
|---|---|
| 1 | Núcleo da biblioteca e `tipo_projeto: ingestao` (arquivo e API), com o caso da ANP rodando local |
| 2 | Pub/Sub em pull, com a versão local e os testes de confirmação e deduplicação |
| 3 | Etapas `sql`, `view`, `python` e `spark`, `dataeng plan`, o montador de DAG e os tipos `relatorio` e `hibrido` |
| 4 | Guias, CI completo e adaptação do `combustiveis-pipeline-gcp` |

**Próximo passo:** revisão desta especificação. Aprovada, vem o **plano de implementação** (por etapas, com testes primeiro), e só então as pastas e o código.

## 14. Atualizações pós-implementação

Pontos em que o código implementado e os rulings ratificados prevalecem sobre o texto original (a origem de cada um está entre parênteses; os rulings completos ficam no registro de execução do plano). Os trechos acima já foram corrigidos nos itens 1 a 5; os demais ficam só aqui.

1. `qualidade` roda como Cloud Run Job executando `run_step`, não como operador do BigQuery, inclusive na DAG Dataproc (Tarefa 22; seção 5.4).
2. Dataproc para `ingestao` e `python` exige imagem do lote com o `dataeng-core` e entrypoints configuráveis; a etapa `spark` exige `script_base_uri` ou `main_python_file_uri` (Tarefas 21 e 22; seção 5.4).
3. `motor: ambos` é variável do template (dois YAMLs, duas DAGs), não do modelo (Tarefa 22; seção 5.1).
4. O `run_id` precisa de data (ou `ingestion_date` explícita) e o do Airflow é mapeado para `{{ ts_nodash }}` (Tarefas 12 e 16; seção 4.5).
5. `origens` é texto separado por vírgula (R2); `schema_version` diferente de 1 falha (R11); `StepResult.status` inclui `nao_executada` (R12).
6. Células vazias, `NaN` e `null` viram `None` ao ler arquivos (Tarefa 7). A origem `arquivo` aceita também `https://` (Tarefa 12).
7. Scripts procedurais do BigQuery (`BEGIN`, `IF`, `LOOP`...) não são suportados e falham de forma explícita; a etapa `sql` não é atômica e para na primeira falha (Tarefas 11 e 20). Todo arquivo citado no YAML precisa ficar dentro da pasta do projeto (Tarefa 20).
8. A silver tem a coluna de controle `_ingestion_date` além das três da seção 4.5; `_controle_execucoes` só acrescenta linhas (Tarefa 12).
9. Pub/Sub: `overwrite_particao` é recusado; a silver faz `merge` por `message_id`; a confirmação vem depois de bronze, silver, quarentena, qualidade e DML; `merge` por chave de negócio é incondicional entre execuções (Tarefa 19).
10. `append` da ingestão é idempotente por `run_id` (Tarefa 12); `LocalWarehouse` fixa o fuso em UTC (Tarefa 23).
11. O motor PySpark, o BigQuery, o Pub/Sub, o Airflow, o Cloud Run, o Dataproc, o `docker build` e os fluxos do GitHub Actions **não foram executados** (Tarefas 14, 16, 18, 21, 22, 24). Ver as tabelas de limitações dos READMEs.
