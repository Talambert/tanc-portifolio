# 9️⃣ Plano — `portal-governanca-dados`

**Objetivo:** uma **biblioteca Python genérica** e um **portal web** (Streamlit no **Cloud Run**) que, a partir de uma lista de tabelas ou de um dataset do BigQuery, usam **Dataplex** e **Gemini no Vertex AI** para (1) **sugerir metadados**, (2) **propor e criar regras de data quality** e (3) **responder perguntas sobre os dados com citação da fonte** (catálogo com RAG). Tudo com revisão humana antes de qualquer gravação.

**Pré-requisitos:** projeto GCP com Dataplex, BigQuery, Vertex AI e Cloud Run habilitados. Os projetos 1 e 2 fornecem os **dados de demonstração** e o **gabarito** de avaliação (descrições escritas à mão no dbt), mas **a ferramenta não depende deles**.

**Entrega final:** portal publicado no Cloud Run, com login e menu de governança; biblioteca e CLI reutilizáveis; e relatórios de **qualidade das sugestões**, de **eficácia das regras** e de **qualidade das respostas do catálogo**, medidos em **dois datasets diferentes**.

> **Nota sobre nomes:** o Dataplex passou por reorganizações (hoje aparece como *Dataplex Universal Catalog*), e o Streamlit, o IAP e o Vertex AI também evoluem. **Confirmar nomes, APIs e a forma atual de configurar cada serviço na documentação oficial** antes de implementar.

## Decisões de arquitetura

| Decisão | Escolha | Por quê |
|---|---|---|
| Camadas | Biblioteca no centro; portal, CLI e Airflow/CI como interfaces | Testável, reutilizável, e a tela pode ser trocada depois |
| Interface | **Streamlit**, páginas por menu | Só Python, rápido, adequado a ferramenta interna |
| Hospedagem | **Cloud Run** | Escala a zero, custo baixo, deploy simples |
| Autenticação | Login Google via **IAP** (ou IAM do Cloud Run) | O Streamlit não autentica sozinho |
| Trabalho pesado | **Assíncrono no Dataplex**; o portal dispara e acompanha o status | Dispensa fila e API próprias |
| IA | Gemini no Vertex AI, **modelo configurável** | Somente Vertex AI, e os IDs mudam com o tempo |
| Busca do catálogo | Embeddings do Vertex AI e **busca vetorial no BigQuery** | Mantém tudo no GCP, sem banco vetorial extra |
| Genericidade | Nada específico de combustíveis; demonstrado em **2 datasets** | Prova que serve para qualquer base |
| Regras de qualidade | **YAML versionado** (regras como código) | Revisão por pull request e reprodutibilidade |

## Estrutura da pasta

```text
portal-governanca-dados/
├── src/governanca/
│   ├── core/                       # compartilhado pelos três módulos
│   │   ├── config.py               # configuração (projeto, região, modelo, limites)
│   │   ├── auth.py                 # credenciais e identidade do usuário
│   │   ├── bigquery.py             # esquema, estatísticas e amostra com limite de custo
│   │   ├── dataplex.py             # camada fina de acesso ao Dataplex
│   │   ├── masking.py              # mascaramento de dados sensíveis
│   │   ├── llm.py                  # chamada ao Gemini (Vertex AI), saída estruturada
│   │   └── audit.py                # trilha de auditoria
│   ├── metadados/
│   │   ├── context.py              # monta o contexto de cada tabela
│   │   ├── suggester.py            # sugestões estruturadas
│   │   ├── review.py               # diferença existente × proposto
│   │   ├── writer.py               # grava no BigQuery e no Dataplex
│   │   └── prompts/
│   ├── qualidade/
│   │   ├── profile.py              # profiling no Dataplex
│   │   ├── propose.py              # regras (Gemini) e baseline (recomendações do Dataplex)
│   │   ├── dry_run.py              # teste das regras em dado limpo
│   │   ├── publish.py              # scans de qualidade no Dataplex
│   │   ├── results.py              # resultados no BigQuery
│   │   └── prompts/
│   ├── catalogo/
│   │   ├── sources/                # adaptadores de fonte (plugáveis)
│   │   │   ├── dataplex_source.py
│   │   │   ├── bigquery_schema_source.py
│   │   │   ├── dbt_source.py       # opcional: manifest.json e catalog.json
│   │   │   └── files_source.py     # Markdown e PDF (dicionário de dados, regras)
│   │   ├── chunking.py             # estratégias de divisão (comparáveis)
│   │   ├── index.py                # embeddings e tabela vetorial no BigQuery
│   │   ├── retrieve.py             # busca por similaridade e filtros por metadados
│   │   ├── answer.py               # resposta com citação e recusa
│   │   └── prompts/
│   └── cli.py                      # linha de comando
├── portal/
│   ├── app.py                      # navegação e página inicial
│   └── pages/
│       ├── 1_Metadados.py
│       ├── 2_Data_Quality.py
│       ├── 3_Catalogo.py
│       └── 4_Configuracoes.py
├── rules/                          # regras de qualidade como código
├── config/
│   ├── tabelas.example.yaml        # lista de tabelas / datasets
│   └── catalogo.example.yaml       # fontes a indexar
├── evals/
│   ├── metadados/                  # gabarito e métricas
│   ├── qualidade/                  # defeitos injetados e métricas
│   └── catalogo/                   # perguntas de referência e métricas
├── tests/
├── infra/                          # Terraform: Cloud Run, Service Accounts, IAP
├── Dockerfile
└── README.md
```

## Telas do portal

```text
 🏠 Início            visão geral, busca rápida no catálogo e estado das últimas execuções
 🛡️ Governança
     ├─ 🏷️ Metadados        lista de tabelas → sugestões → revisão → gravação
     ├─ ✅ Data Quality      lista de tabelas → profiling → regras → scans → resultados
     └─ 📚 Catálogo          pergunte sobre os dados, com citação da fonte
 ⚙️ Configurações     projeto GCP, modelo, fontes, limites de custo
```

| Página | Função |
|---|---|
| **Início** | Visão geral, caixa de busca do catálogo, últimas execuções e custo acumulado |
| **Metadados** | Escolher tabelas (lista, dataset ou busca) → gerar sugestões → **tela de revisão com a diferença** → aprovar e gravar |
| **Data Quality** | Escolher tabelas → profiling → regras propostas → **teste em dado limpo** → aprovar → publicar *scans* → acompanhar resultados |
| **Catálogo** | Perguntar em português, ver a resposta **com as fontes citadas** e dar feedback (👍/👎) |
| **Configurações** | Projeto, região, modelo, fontes do catálogo, limites de custo e de amostragem |

O fluxo "propor, revisar, aprovar" é o centro das telas de Metadados e Data Quality, e é onde a interface agrega valor sobre a linha de comando.

## Módulo de Metadados

**Contexto por tabela:** entradas e aspectos do Dataplex, descrições existentes, esquema, estatísticas e uma amostra mascarada. **Saída estruturada:**

| Item | Detalhe |
|---|---|
| Descrição da tabela e das colunas | Em português, sem inventar regra de negócio |
| Classificação | Domínio e candidatas a dado sensível (a confirmar por pessoa) |
| Termos de glossário | Associa a termos **existentes**; nunca cria termo novo sem aprovação |
| Confiança e evidência | De onde veio cada sugestão |
| Lacunas | O que o modelo **não conseguiu inferir** |

Regra de ouro: **sem evidência, o modelo diz que não sabe**. Descrições escritas por pessoas **não são sobrescritas** sem confirmação.

## Módulo de Data Quality

```text
 Lista de tabelas ─▶ profiling (Dataplex) ─┐
                  ─▶ amostra (BigQuery) ───┼─▶ Gemini ─▶ regras propostas (YAML)
   recomendações do Dataplex (baseline) ───┘                     │
                                          teste em dado limpo + revisão humana
                                                                  ▼
                                    scans de qualidade (Dataplex) ─▶ resultados no BigQuery
```

Exemplo conceitual de regra:

```yaml
tabela: dataset.tabela
regras:
  - coluna: valor
    tipo: faixa
    minimo: 0.5
    maximo: 30
    limiar: 0.99
    justificativa: "Preço por litro; fora disso indica erro de unidade."
    origem: gemini            # ou: dataplex_recomendada
    confianca: alta
```

Relação com o dbt: os testes do dbt garantem o **contrato dos modelos** na transformação; os *scans* do Dataplex **vigiam as tabelas de qualquer origem**, de forma agendada. O relatório compara as duas abordagens.

## Módulo de Catálogo (RAG)

Responde perguntas como "o que significa a coluna `valor_compra`?" ou "de onde vem esta tabela?", usando **somente o que está nas fontes**, com citação.

```text
 INDEXAÇÃO (e reindexação quando a documentação muda)
   fontes ─▶ divisão em trechos ─▶ embeddings (Vertex AI) ─▶ tabela vetorial no BigQuery

 PERGUNTA
   pergunta ─▶ embedding ─▶ busca por similaridade (+ filtro por tipo de fonte) ─▶ top-k trechos
                                              ▼
                       Gemini responde SÓ com os trechos e cita a fonte
                       sem trecho que sustente a resposta? ─▶ "Não encontrei isso nas fontes"
```

**Fontes por adaptadores plugáveis** (configuradas em `catalogo.yaml`), para o módulo valer para qualquer projeto:

| Adaptador | Conteúdo | Disponível em |
|---|---|---|
| Dataplex | Entradas, aspectos, glossário e descrições (inclusive o que o módulo de Metadados gravar) | Qualquer projeto GCP |
| Esquema do BigQuery | Tabelas, colunas, tipos e descrições | Qualquer projeto GCP |
| dbt (opcional) | Modelos, colunas, testes e linhagem (`manifest.json` e `catalog.json`) | Quem usa dbt |
| Arquivos | Markdown e PDF (dicionário de dados, regras de negócio, metadados de fontes externas) | Qualquer organização |

**Sinergia com os outros módulos:** o módulo de Metadados melhora as descrições e grava no Dataplex; o catálogo as lê e reindexa. Quanto mais bem documentado, melhores as respostas.

**Esquema da tabela vetorial:** `chunk_id`, `texto`, `embedding`, `fonte`, `tipo` (tabela, coluna, métrica, regra de negócio, arquivo), `referencia`, `hash_conteudo`, `versao_fonte`, `indexado_em`.

**Decisões a comparar (viram resultados no README):**

| Decisão | Variantes |
|---|---|
| Chunking | Estruturado (um trecho por tabela e por coluna) × tamanho fixo com sobreposição × por títulos |
| Quantidade de trechos (k) | 3 × 5 × 8 |
| Filtro por tipo de fonte | Com × sem |
| Reclassificação (*rerank*) | Com × sem (verificar a oferta atual do Vertex AI) |
| Modelo de geração | Menor × maior |

> Os modelos de embeddings e de geração são configuráveis. **Confirmar nomes e dimensões na documentação oficial do Vertex AI** na hora de implementar.

## Segurança e custo

- **Amostra mínima e mascarada** antes de qualquer envio ao modelo (CPF, e-mail, telefone e nomes).
- **Nada é gravado ou criado sem aprovação**; o modo padrão simula.
- **Auditoria:** quem aprovou, valor anterior, novo valor e modelo usado, com possibilidade de desfazer.
- **Menor privilégio:** Service Accounts separadas para leitura, para escrita de metadados e criação de *scans*, e para o índice vetorial.
- **Autenticação obrigatória** no portal, com teste de que um acesso sem login é rejeitado.
- **Texto do dado não é instrução:** conteúdo de amostra, metadado ou documento indexado nunca altera o comportamento do modelo (caso adversarial nos testes).
- **Catálogo respeita permissões:** só indexar fontes que o chamador pode ver (evitar que a busca revele metadado de tabela restrita).
- **Custo:** limite de tabelas e de bytes por execução; profiling com amostragem e filtro; cache por hash; modelo menor por padrão; orçamento com alerta.

## Avaliação

### Metadados (gabarito: descrições reais dos marts do dbt, escondidas do modelo)

| Métrica | Como medir |
|---|---|
| Fidelidade semântica | Similaridade com o gabarito, mais avaliador (LLM) com rubrica e **amostra conferida por humano** |
| Alucinação | A sugestão afirma algo que não está na amostra nem nos metadados? |
| Honestidade | Em coluna ambígua, marca baixa confiança em vez de chutar? |
| Taxa de aprovação | Sugestões aceitas sem edição na revisão |
| Segurança | Nenhum dado sensível chega ao modelo (teste com dados falsos sensíveis) |

Experimentos: sem contexto × só esquema × esquema e metadados × esquema, metadados e amostra; modelo menor × maior.

### Data Quality (defeitos injetados em cópias das tabelas)

| Defeito | Exemplo |
|---|---|
| Nulos indevidos | Identificador vazio |
| Formato inválido | CNPJ com espaço; data fora do padrão |
| Valor fora da faixa | Preço negativo ou 100× maior |
| Domínio inválido | UF inexistente |
| Duplicidade | Mesma chave repetida |
| Colunas deslocadas | Linha malformada com `;` no endereço |
| Deriva de distribuição | Média muda de forma abrupta |
| Frescor | Sem dados novos há dias |

Métricas: **detecção (recall)** dos defeitos, **falsos alarmes** em dado limpo, precisão e economia (regras úteis × triviais), taxa de aceitação, e **IA × baseline do Dataplex × testes do dbt**.

### Catálogo (~50 perguntas de referência por dataset)

Categorias: respondíveis, **sem resposta nas fontes**, ambíguas e adversariais (texto malicioso em documento).

| Métrica | Como medir |
|---|---|
| **Recuperação** (*recall@k* e MRR) | O trecho correto apareceu, e em que posição? |
| **Fidelidade** | As afirmações da resposta são sustentadas pelo trecho citado? |
| **Acerto de recusa** | Perguntas sem fonte recebem "não sei"? |
| **Robustez** | Documento com instrução maliciosa não muda a resposta |
| **Custo e latência** | Tokens e tempo por pergunta |

> Os três módulos são avaliados em **dois datasets**: os dados de combustíveis e um dataset público do BigQuery, para provar a genericidade.

## Fases

### Fase 22 — Núcleo da biblioteca
- [ ] Estrutura do pacote, configuração, autenticação e acesso ao BigQuery e ao Dataplex.
- [ ] `masking.py` e `audit.py`, com testes usando dados sensíveis falsos.
- [ ] CLI mínima: ler a lista de tabelas e imprimir o contexto.
- ✅ **Pronto quando:** para uma tabela, a CLI imprime esquema, metadados e amostra mascarada, sem dado sensível.

### Fase 23 — Módulo de Metadados
- [ ] Contexto, prompts versionados, saída estruturada e relatório de diferença.
- [ ] Gravação aprovada no BigQuery e no Dataplex, com auditoria e reversão (simulação como padrão).
- ✅ **Pronto quando:** aprovar uma sugestão a grava, o catálogo mostra o texto novo e a auditoria permite desfazer.

### Fase 24 — Módulo de Data Quality
- [ ] Profiling no Dataplex, baseline de recomendações e regras do Gemini.
- [ ] `dry_run.py` em dado limpo; regras em YAML; publicação idempotente dos *scans*; resultados no BigQuery.
- ✅ **Pronto quando:** uma lista de tabelas gera regras que passam no dado limpo e os *scans* rodam sozinhos.

### Fase 25 — Portal
- [ ] Streamlit com menu: Início, Governança (Metadados e Data Quality) e Configurações.
- [ ] Telas de revisão (diferença) e de acompanhamento dos *scans*.
- [ ] Execução local contra o projeto GCP.
- ✅ **Pronto quando:** o fluxo completo dos dois módulos funciona pela interface.

### Fase 26 — Publicação no Cloud Run
- [ ] `Dockerfile`, Terraform (Cloud Run, Service Accounts, login Google via IAP ou IAM).
- [ ] Teste automatizado: acesso sem login é rejeitado.
- ✅ **Pronto quando:** o portal responde só a usuários autorizados, em URL do Cloud Run.

### Fase 27 — Módulo de Catálogo (RAG)
- [ ] Adaptadores de fonte (Dataplex, esquema do BigQuery, arquivos; dbt opcional) e `catalogo.yaml`.
- [ ] Divisão em trechos (ao menos duas estratégias), embeddings e tabela vetorial no BigQuery, com **reindexação incremental** por hash.
- [ ] Resposta com citação e **recusa** quando a fonte não sustenta.
- [ ] Página "Catálogo" no portal, com feedback 👍/👎 e a caixa de busca na página inicial.
- [ ] Reindexação quando o módulo de Metadados gravar descrições novas.
- ✅ **Pronto quando:** perguntar "o que significa a coluna X?" devolve a resposta com a fonte, e uma pergunta sem fonte recebe "não encontrei".

### Fase 28 — Avaliação
- [ ] Gabarito de metadados, injeção de defeitos de qualidade e perguntas de referência do catálogo; `evals/` para os três módulos.
- [ ] Execução nos **dois datasets** e comparativos (contexto, modelo, IA × baseline × dbt, chunking, k, rerank).
- ✅ **Pronto quando:** o README tem as tabelas de resultados e justifica as configurações escolhidas.

### Fase 29 — Acabamento
- [ ] CI: testes, mascaramento, validação dos YAML de regras e subconjunto das avaliações.
- [ ] README com diagrama, resultados, capturas de tela e GIF do portal.
- [ ] Atualizar o README da raiz.

## Riscos

| Risco | Mitigação |
|---|---|
| Descrição fluente, porém inventada | Evidência obrigatória, baixa confiança explícita, avaliação de alucinação |
| Resposta do catálogo inventada | Resposta restrita ao contexto, citação obrigatória, recusa medida nos evals |
| Recuperação ruim (trecho certo não aparece) | Chunking estruturado, filtro por tipo e métricas de recuperação separadas |
| Catálogo revelar metadado de tabela restrita | Indexar só o que o chamador pode ver; teste de permissão |
| Vazamento de dado sensível ao modelo | Mascaramento antes do modelo, amostra mínima, teste dedicado |
| Regras que geram falso alarme | Teste em dado limpo antes de publicar e limiares calibrados |
| Sobrescrever documentação boa | Nunca trocar texto humano sem confirmação; mostrar a diferença |
| Portal exposto sem autenticação | IAP/IAM, teste de acesso sem login, Cloud Run privado por padrão |
| Custo de profiling, *scans*, embeddings e IA | Amostragem, limites por execução, cache por hash, modelo menor, orçamento com alerta |
| Mudança de nomes e APIs (Dataplex, Vertex AI, IAP, Streamlit) | Camada fina de acesso, versões fixadas e verificação na documentação |
| Escopo do portal crescer demais | Catálogo só depois de Metadados e Data Quality funcionando (Fase 27) |
| Genericidade só de fachada | Demonstração e avaliação em dois datasets distintos |

## Definição de pronto

- [ ] Biblioteca e CLI reutilizáveis, com testes.
- [ ] Portal no Cloud Run com login e menu de governança (Metadados, Data Quality e Catálogo).
- [ ] Fluxo "propor, revisar, aprovar" nos módulos de Metadados e Data Quality, com auditoria.
- [ ] Catálogo respondendo com citação e recusando quando não há fonte.
- [ ] Relatórios de qualidade das sugestões, da eficácia das regras e das respostas, em dois datasets.
- [ ] Nenhum dado sensível enviado ao modelo e nada criado sem aprovação.
