# 4️⃣ Plano — `combustiveis-bi-powerbi`

**Objetivo:** painel no **Power BI** sobre a camada gold do BigQuery, que responda perguntas de negócio reais sobre preços de combustíveis, e fecha a história "dado bruto → decisão".

**Pré-requisito:** projeto 2 (dbt) com os marts publicados.

**Entrega final:** relatório `.pbix` (sem credenciais), capturas de tela, documentação do modelo semântico e das medidas DAX.

## Estrutura da pasta

```text
combustiveis-bi-powerbi/
├── relatorio/
│   └── combustiveis.pbix            # sem credenciais embutidas
├── docs/
│   ├── modelo-semantico.md          # tabelas, relações e medidas
│   └── medidas-dax.md               # todas as medidas, comentadas
├── imagens/                         # capturas de cada página
└── README.md                        # pergunta de negócio, decisões e como abrir
```

## Perguntas de negócio

1. **Visão geral:** como estão os preços hoje e como variaram no último mês?
2. **Geografia:** onde a gasolina está mais cara/barata (UF e município)?
3. **Paridade etanol/gasolina:** em quais regiões compensa abastecer com etanol?
4. **Bandeiras:** quem pratica os maiores e menores preços?
5. **Série temporal:** sazonalidade e tendência por produto.

## Páginas do relatório

| Página | Conteúdo |
|---|---|
| **Resumo** | Cartões (preço médio, variação 4 semanas, nº de postos), tendência por produto |
| **Mapa** | Preço médio por UF e município, com filtro de produto |
| **Paridade** | Razão etanol/gasolina por região, com a linha de referência de 70% |
| **Bandeiras e postos** | Ranking de bandeiras e dispersão de preço |
| **Sobre os dados** | Fonte, atualização, limitações e dicionário |

## Modelo semântico

- Conexão ao **BigQuery** pelo conector nativo. Modo **Import** (agendável) com opção de DirectQuery documentada como alternativa.
- Esquema **estrela**: fato `fct_preco_combustivel` no centro e as quatro dimensões do dbt, com relações 1:N e filtro unidirecional.
- Tabela de datas própria do dbt (`dim_data`) marcada como tabela de data no Power BI.
- **Sem colunas calculadas** pesadas; a lógica fica em **medidas DAX** e no dbt.

**Medidas principais (a documentar em `medidas-dax.md`)**
- Preço médio, preço mínimo, preço máximo
- Variação vs. período anterior (4 semanas, mês, ano)
- Paridade etanol/gasolina
- Nº de postos pesquisados
- Ranking de bandeira

## Fases

### Fase 7.1 — Conexão e modelo
- [ ] Conectar ao BigQuery e importar apenas os marts.
- [ ] Criar as relações e marcar a tabela de datas.
- [ ] Medidas básicas e validação cruzada com SQL no BigQuery (os números **têm que bater**).
- ✅ **Pronto quando:** 3 números do relatório conferem com a consulta direta.

### Fase 7.2 — Páginas e visuais
- [ ] Construir as 5 páginas, com tema consistente e acessível (contraste e legenda legível).
- [ ] Segmentações coerentes entre páginas (produto, UF, período).
- [ ] *Tooltips* e *drill-through* de município para postos.

### Fase 7.3 — Publicação e documentação
- [ ] Capturas de cada página em `imagens/` (alta resolução).
- [ ] `modelo-semantico.md` e `medidas-dax.md` completos.
- [ ] Atualizar `exposures.yml` do projeto dbt apontando para este relatório.
- [ ] Revisar o `.pbix`: sem credenciais, sem dados sensíveis, parâmetros genéricos.
- ✅ **Pronto quando:** outra pessoa abre o `.pbix`, informa o projeto GCP e vê o relatório funcionando.

## Observações

- O Power BI Desktop é **gratuito**. Publicar no Power BI Service pode exigir licença; o portfólio funciona só com `.pbix` e capturas, sem depender de publicação.
- Evitar colocar o `.pbix` com dados importados se ele ficar grande (limite do GitHub: 100 MB por arquivo). Alternativa: versionar o `.pbix` sem dados (apenas modelo e visuais) e documentar como atualizar.
- Dados públicos da ANP: citar a fonte e a data da última atualização no relatório.

## Riscos

| Risco | Mitigação |
|---|---|
| `.pbix` pesado ou com credenciais | Versionar sem dados; revisar fontes e parâmetros antes do commit |
| Números diferentes do BigQuery | Validação cruzada obrigatória na Fase 7.1 |
| Medidas DAX lentas | Preferir agregações no dbt e medidas simples |

## Definição de pronto do projeto

- [ ] 5 páginas completas, respondendo às perguntas de negócio.
- [ ] Números validados contra o BigQuery.
- [ ] Capturas e documentação do modelo e das medidas.
- [ ] README com a pergunta de negócio, decisões e como abrir o relatório.
