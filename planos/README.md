# 🗺️ Planos de Desenvolvimento

Roadmap dos próximos projetos do portfólio. Os quatro projetos contam **uma única história**, com o mesmo dataset em etapas: *do arquivo cru no bucket ao painel no Power BI*.

| # | Projeto (pasta) | Plano | Fases |
|:-:|---|---|:-:|
| 1 | [`combustiveis-pipeline-gcp`](../serie-combustiveis/combustiveis-pipeline-gcp/) | [01-combustiveis-pipeline-gcp.md](01-combustiveis-pipeline-gcp.md) | 0, 1, 2, 3, 4 |
| 2 | [`combustiveis-dbt`](../serie-combustiveis/combustiveis-dbt/) | [02-combustiveis-dbt.md](02-combustiveis-dbt.md) | 5 |
| 3 | [`combustiveis-orquestracao`](../serie-combustiveis/combustiveis-orquestracao/) | [03-combustiveis-orquestracao.md](03-combustiveis-orquestracao.md) | 6 |
| 4 | [`combustiveis-bi-powerbi`](../serie-combustiveis/combustiveis-bi-powerbi/) | [04-combustiveis-bi-powerbi.md](04-combustiveis-bi-powerbi.md) | 7 |
| 5 | [`combustiveis-mcp-bigquery`](../ia-e-agentes/combustiveis-mcp-bigquery/) | [05-combustiveis-mcp-e-agente.md](05-combustiveis-mcp-e-agente.md) (Parte A) | 9 |
| 6 | [`combustiveis-agente-dados`](../ia-e-agentes/combustiveis-agente-dados/) | [05-combustiveis-mcp-e-agente.md](05-combustiveis-mcp-e-agente.md) (Parte B) | 10 a 13 |

Fase 8 (acabamento dos projetos 1 a 4) está no fim deste arquivo. Os projetos 5 e 6 têm o próprio acabamento (Fase 13) no plano deles.

### 🤖 Projetos de IA (somente Vertex AI)

| # | Projeto (pasta) | Plano | Fases | Depende de |
|:-:|---|---|:-:|---|
| 7 | [`combustiveis-agente-observabilidade`](../ia-e-agentes/combustiveis-agente-observabilidade/) | [06-combustiveis-agente-observabilidade.md](06-combustiveis-agente-observabilidade.md) | 14 a 17 | Projetos 2 (dbt) e 3 (Airflow) |
| 8 | [`extracao-documentos-ia`](../ia-e-agentes/extracao-documentos-ia/) | [07-extracao-documentos-ia.md](07-extracao-documentos-ia.md) | 18 a 21 | Independente; pode alimentar o projeto 1 |
| 9 | [`portal-governanca-dados`](../governanca/portal-governanca-dados/) | [08-portal-governanca-dados.md](08-portal-governanca-dados.md) | 22 a 29 | **Genérico** (qualquer dataset do BigQuery). Os projetos 1 e 2 fornecem dados de demonstração e o gabarito de avaliação |

> Os arquivos de plano seguem a numeração dos projetos, com uma exceção: `05` cobre dois projetos (o servidor MCP e o agente de dados).
>
> 🛡️ O projeto 9 é uma **biblioteca + portal no Cloud Run** com menu de governança e três módulos: **Metadados**, **Data Quality** e **Catálogo (RAG)**. Ele **não é específico de combustíveis**, e o menu foi pensado para receber outros módulos no futuro.

> 🤖 Os projetos 5 e 6 usam **somente o Vertex AI** (Gemini) e dependem dos marts do projeto 2. Não dependem do Airflow (projeto 3) nem do Power BI (projeto 4).

### 🧩 Plataforma de templates e bibliotecas de engenharia de dados

| # | Projeto (pasta) | Especificação | Plano | Estado |
|:-:|---|---|---|---|
| 10 | [`lib-dataeng-core`](../plataforma-engenharia/lib-dataeng-core/) e [`template-pipeline`](../plataforma-engenharia/template-pipeline/) | [especificação de desenho](../docs/superpowers/specs/2026-10-19-templates-e-bibliotecas-de-engenharia-design.md) | [plano de implementação](../docs/superpowers/plans/2026-10-19-templates-e-bibliotecas-de-engenharia.md) | Implementado e testado com a versão local; GCP, Airflow e Spark reais não validados |

> O projeto 10 é **independente** dos projetos 1 a 9. O `combustiveis-pipeline-gcp` (projeto 1) é o primeiro consumidor previsto: o plano dele é ajustado para usar o template. Os planos deste projeto ficam em `docs/superpowers/`, e não nesta pasta.

## Fluxo de ponta a ponta

```text
 ANP (gov.br)                       CSV / ZIP públicos
      │
      ▼
 ┌─────────┐  Python (Cloud Run Jobs)   ┌─────────┐       dbt        ┌─────────┐
 │ BRONZE  │ ─────────────────────────▶ │ SILVER  │ ───────────────▶ │  GOLD   │ ──▶ Power BI
 │ Storage │  PySpark (Dataproc Server.)│ BigQuery│  staging→marts   │ BigQuery│
 └─────────┘                            └─────────┘                  └─────────┘
      ▲                                       ▲                           ▲
      └───────────── Airflow local / Cloud Composer (orquestra tudo) ─────┘
```

## Decisões de arquitetura

| Decisão | Escolha | Por quê |
|---|---|---|
| Dataset | Preços de combustíveis da ANP | Público, real, na área de atuação, e com problemas reais de qualidade |
| Escopo inicial | 2023 em diante | Caber no free tier; ampliar depois para o histórico completo (desde 2004) |
| Motores de silver | Python (Cloud Run Jobs) **e** PySpark (Dataproc Serverless) | Mostrar que a ferramenta é escolhida pela necessidade; comparar custo e tempo |
| Camada gold | dbt Core sobre BigQuery | SQL versionado, testado e documentado |
| Orquestração | Airflow local (Docker) **e** Cloud Composer | O mesmo DAG nos dois ambientes |
| BI | Power BI conectado ao BigQuery | Domínio do autor |
| Infra | Terraform | Reprodutível e destruível, essencial para controlar custo |
| Região | `southamerica-east1` | Menor latência; confirmar disponibilidade dos serviços |

> ⚖️ **Honestidade técnica:** um semestre da ANP tem ~422 mil linhas (72 MB). Spark é desproporcional para esse volume, e o comparativo do projeto 1 deve **mostrar isso com números**, em vez de esconder. É um ponto a favor do portfólio: saber quando *não* usar uma ferramenta.

## Fonte de dados (verificada em 13/10/2026)

Base: `https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/`

| Arquivo | Caminho | Formato |
|---|---|---|
| Combustíveis automotivos, por semestre | `dsas/ca/ca-AAAA-0S.zip` (ex.: `ca-2026-01.zip`, ~8 MB zipado) | ZIP com um CSV |
| Gasolina/etanol e diesel/GNV, por mês | `dsan/AAAA/MM-dados-abertos-precos-AAAA-MM-<produto>.csv` (2026); `precos-<produto>-MM.csv` (2025 e antes) | CSV |
| Últimas 4 semanas | `qus/ultimas-4-semanas-<produto>.csv` | CSV |

**Formato observado:** UTF-8 com BOM, fim de linha CRLF, separador `;`, decimal com vírgula, data `dd/mm/aaaa`. 16 colunas:
`Regiao - Sigla; Estado - Sigla; Municipio; Revenda; CNPJ da Revenda; Nome da Rua; Numero Rua; Complemento; Bairro; Cep; Produto; Data da Coleta; Valor de Venda; Valor de Compra; Unidade de Medida; Bandeira`

**Problemas de qualidade encontrados** (viram requisitos e testes):
- CNPJ com **espaço no início** (` 88.587.589/0001-17`).
- Linhas com **`;` dentro do endereço, sem aspas**, que deslocam colunas (valores como `PRAIA DE ITAPARICA` ou um CEP aparecendo na coluna `Produto`).
- `Valor de Compra` quase sempre vazio.
- Unidades variam (`R$ / litro`, `R$ / m³` no GNV).
- **Nomes de arquivo irregulares** nos arquivos mensais (ex.: fevereiro/2026 com nome truncado, abril/2026 sem extensão). A ingestão deve usar um **manifesto explícito de URLs**, e não adivinhar o padrão.

> Os metadados oficiais estão em `metadados-serie-historica-precos-combustiveis-1.pdf` na mesma base. Confirmar o dicionário de dados lá antes de modelar.

## Custos e salvaguardas

- 🔔 **Alerta de orçamento** no GCP antes de criar qualquer recurso pago (ex.: R$ 25 e R$ 50).
- **Cloud Composer é caro** se ficar ligado (ordem de centenas de dólares por mês). Criar, rodar a demonstração, capturar evidências e **destruir no mesmo dia**, com Terraform.
- **Dataproc:** apenas **Serverless (batch)**. Nunca cluster fixo.
- **BigQuery:** tabelas particionadas por data e `maximum_bytes_billed` nas consultas.
- **Segredos:** nenhuma chave no git. Cada serviço usa uma Service Account própria, com menor privilégio. O repositório é público.
- Os dados da ANP são públicos, então não há dado de empresa em nenhum projeto.

## Ordem de execução

1. Fase 0 → 1 → 2 (base, bronze, silver em Python)
2. Fase 3 → 4 (PySpark e comparativo)
3. Fase 5 (dbt: staging e marts)
4. Fase 6 (orquestração local, depois Composer)
5. Fase 7 (Power BI)
6. Fase 8 (acabamento dos projetos 1 a 4)
7. Fases 9 a 13 (servidor MCP e agente de IA), que podem começar assim que o projeto 2 terminar, em paralelo às fases 6 e 7

Cada fase termina com algo que **funciona e pode ser mostrado**. Não se avança com a fase anterior quebrada.

## Fase 8 — Acabamento do repositório

- [ ] README de cada projeto com: diagrama, pergunta de negócio, decisões, resultado e como rodar.
- [ ] README da raiz atualizado (tabela de projetos, destaques).
- [ ] CI no GitHub Actions: lint, testes unitários e `dbt build` (com `dbt parse` se não houver credencial no CI).
- [ ] Documentação do dbt publicada no GitHub Pages.
- [ ] Revisão de segurança: nenhum segredo, `.gitignore` conferido, histórico limpo.
- [ ] Teste de "clone limpo": outra pessoa roda seguindo só o README.

## Pré-requisitos (a cargo do autor)

- [ ] Conta GCP com billing ativo e projeto criado (ID e região).
- [ ] Google Cloud SDK (`gcloud`) instalado e autenticado.
- [ ] Terraform instalado.
- [ ] Docker Desktop (para o Airflow local).
- [ ] Power BI Desktop (para a Fase 7).
- [ ] Python 3.11+ (todos os comandos neste roadmap usam Git Bash).
