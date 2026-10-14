# 2️⃣ Plano — `combustiveis-dbt`

**Objetivo:** transformar a silver (BigQuery) em um **modelo dimensional** confiável, com testes e documentação gerada, usando **dbt Core**.

**Pré-requisito:** projeto 1 concluído (tabela `silver` populada).

**Entrega final:** `dbt build` verde, site de documentação com linhagem publicado no GitHub Pages e fato/dimensões prontas para o Power BI.

## O que é o dbt (resumo para o README)

Ferramenta de **transformação (o "T" do ELT)**: cada modelo é um `SELECT` em um arquivo `.sql`; o dbt resolve a ordem de execução pelas dependências (`ref`), cria as tabelas no BigQuery, roda os testes e gera a documentação. Ele **não** ingere dados nem agenda execuções.

## Estrutura da pasta

```text
combustiveis-dbt/
├── dbt_project.yml
├── profiles.yml.example        # sem credenciais; o real fica fora do git
├── models/
│   ├── staging/                # 1:1 com a silver, nomes e tipos padronizados
│   │   ├── _sources.yml
│   │   ├── _staging.yml        # testes e descrições
│   │   └── stg_precos.sql
│   ├── intermediate/           # lógica reutilizável (deduplicação, enriquecimento)
│   │   └── int_precos_dedup.sql
│   └── marts/
│       ├── _marts.yml
│       ├── dim_produto.sql
│       ├── dim_posto.sql
│       ├── dim_localidade.sql
│       ├── dim_data.sql
│       └── fct_preco_combustivel.sql
├── tests/                      # testes singulares (regras de negócio)
├── macros/
├── seeds/                      # tabelas pequenas fixas (ex.: de-para de produtos/regiões)
├── exposures.yml               # registra o painel do Power BI como consumidor
└── README.md
```

## Modelo dimensional

**Fato** `fct_preco_combustivel` — grão: **um preço por posto, produto e data de coleta**
- Chaves: `sk_posto`, `sk_produto`, `sk_localidade`, `sk_data`
- Medidas: `valor_venda`, `valor_compra` (nulo permitido)
- Materialização **incremental** por `data_coleta`, particionada por mês.

**Dimensões**
| Dimensão | Atributos principais |
|---|---|
| `dim_produto` | produto, família (gasolina, etanol, diesel, GNV), unidade de medida |
| `dim_posto` | CNPJ, razão social, bandeira, endereço |
| `dim_localidade` | município, UF, região |
| `dim_data` | data, dia da semana, mês, trimestre, semestre, ano |

- Chaves substitutas geradas com `dbt_utils.generate_surrogate_key`.
- **Bandeira:** decidir se muda ao longo do tempo (posto que troca de bandeira). Se sim, tratar com *snapshot* (SCD tipo 2) em `dim_posto`.

## Testes (o coração do projeto)

| Tipo | Exemplos |
|---|---|
| Genéricos | `unique` e `not_null` nas chaves; `relationships` entre fato e cada dimensão |
| Valores aceitos | `uf` entre as 27 UFs; `produto` na lista controlada |
| Singulares (negócio) | preço > 0 e abaixo de um teto plausível; `data_coleta` não no futuro |
| Frescor | `source freshness` na silver (alerta se parar de atualizar) |
| Pacotes | `dbt_utils`, `dbt_expectations` (distribuição e limites) |

## Fases

### Fase 5.1 — Projeto e fontes
- [ ] `pip install dbt-bigquery` no ambiente virtual do repositório.
- [ ] `dbt_project.yml`, `profiles.yml.example` e Service Account própria do dbt (acesso só aos datasets necessários).
- [ ] Declarar a silver como `source` com testes de frescor.
- ✅ **Pronto quando:** `dbt debug` conecta e `dbt parse` passa.

### Fase 5.2 — Staging e intermediate
- [ ] `stg_precos`: renomeia e padroniza, sem regra de negócio.
- [ ] `int_precos_dedup`: garante uma linha por `cnpj + produto + data`.
- ✅ **Pronto quando:** testes de staging verdes.

### Fase 5.3 — Marts
- [ ] Dimensões e fato incremental.
- [ ] Testes de relacionamento entre fato e dimensões.
- [ ] Decidir e implementar o snapshot de `dim_posto`, se aplicável.
- ✅ **Pronto quando:** `dbt build` verde do zero e em execução incremental.

### Fase 5.4 — Documentação e linhagem
- [ ] Descrição de **todas** as colunas dos marts nos `.yml`.
- [ ] `dbt docs generate` e publicação no **GitHub Pages**.
- [ ] `exposures.yml` apontando para o painel do Power BI (completar após a Fase 7).
- ✅ **Pronto quando:** o site abre, mostra o grafo da silver ao painel e tem descrição em tudo.

### Fase 5.5 — CI
- [ ] GitHub Actions: `sqlfluff` (lint) e `dbt parse`; `dbt build` completo se houver credencial segura.
- ✅ **Pronto quando:** o workflow roda em cada push.

## Perguntas de negócio que o modelo deve responder

1. Qual o preço médio da gasolina por UF, mês a mês?
2. Qual a diferença de preço entre etanol e gasolina (paridade) por região?
3. Quais bandeiras praticam os preços mais altos e mais baixos?
4. Como o preço varia por município dentro de um estado?

## Riscos

| Risco | Mitigação |
|---|---|
| Fato incremental duplica ao reprocessar | Estratégia `merge` com `unique_key` e teste de unicidade |
| Custo de consultas no BigQuery | Partição, `maximum_bytes_billed` e amostra em desenvolvimento |
| Credenciais vazando | `profiles.yml` fora do git; só o `.example` é versionado |

## Definição de pronto do projeto

- [ ] `dbt build` verde, com testes em todas as chaves.
- [ ] Documentação publicada, com linhagem.
- [ ] Modelo responde às 4 perguntas acima com SQL de exemplo no README.
- [ ] CI configurado.
