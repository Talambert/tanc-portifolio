# 7️⃣ Plano — `combustiveis-agente-observabilidade`

**Objetivo:** um agente (Gemini no **Vertex AI**) que, quando uma tarefa do Airflow ou um teste do dbt falha, **coleta as evidências, diagnostica a causa provável e propõe a correção**, abrindo uma *issue* ou um *pull request* em rascunho. Um humano revisa. O agente **nunca altera nada em produção**.

**Pré-requisitos:** projeto 2 (dbt, com artefatos `run_results.json` e `manifest.json`) e projeto 3 (Airflow, local ou Composer). O projeto 1 fornece as falhas reais que vamos reproduzir.

**Entrega final:** relatório de **taxa de diagnósticos corretos** sobre um catálogo de falhas injetadas de propósito, mais uma demonstração ponta a ponta: falha → diagnóstico → PR em rascunho.

## Estrutura da pasta

```text
combustiveis-agente-observabilidade/
├── agent/
│   ├── agent.py              # agente (ADK + Gemini no Vertex AI)
│   ├── prompts/              # instruções versionadas
│   ├── schemas.py            # saída estruturada do diagnóstico (Pydantic)
│   └── tools/                # ferramentas somente leitura (abaixo)
├── collectors/
│   ├── airflow_logs.py       # logs e estado das tarefas
│   ├── dbt_artifacts.py      # run_results.json e manifest.json
│   └── sanitize.py           # remove segredos e dados pessoais antes de enviar ao modelo
├── actions/
│   └── github_actions.py     # abre issue e PR em rascunho (permissões mínimas)
├── memory/                   # histórico de incidentes (BigQuery)
├── chaos/                    # injeção de falhas e catálogo de cenários
├── evals/
├── infra/
└── README.md
```

## Como o agente é acionado

```text
 falha no Airflow/dbt ──▶ callback ──▶ Pub/Sub ──▶ Cloud Run (agente)
                                                       │ lê logs e artefatos (somente leitura)
                                                       ▼
                                      diagnóstico estruturado ──▶ issue / PR em rascunho
```

## Ferramentas do agente (todas somente leitura)

| Ferramenta | O que devolve |
|---|---|
| `obter_logs_tarefa` | Trechos relevantes do log da tarefa que falhou |
| `obter_resultado_dbt` | Testes que falharam e o SQL do modelo envolvido |
| `historico_execucoes` | Últimas execuções da mesma tarefa (mudou algo? falhou antes?) |
| `verificar_fonte` | A URL da ANP ainda responde? O nome do arquivo mudou? |
| `buscar_incidentes_parecidos` | Falhas anteriores semelhantes e como foram resolvidas |

## Saída do diagnóstico (esquema fixo)

`causa_provavel` · `evidencias` (trechos de log citados) · `confianca` (alta, média, baixa) · `hipoteses_alternativas` · `acao_sugerida` · `severidade` · `precisa_humano` (sim/não)

Sem evidência citada, o diagnóstico é rejeitado.

## Segurança e confiabilidade

- **Somente leitura** nos sistemas observados; a única escrita é abrir *issue* ou PR **em rascunho** no GitHub, com token de permissão mínima. **Nunca** grava na `main`.
- **Sanitização:** segredos, tokens e dados pessoais são removidos dos logs antes de irem ao modelo.
- **Logs são dados:** conteúdo de log nunca é instrução (defesa contra *prompt injection* indireta).
- **Anti-tempestade:** deduplicação de incidentes e limite de execuções do agente por hora, para uma falha em cascata não gerar custo nem dezenas de *issues*.
- **Confiança honesta:** diagnóstico de baixa confiança vira *issue* com hipóteses, nunca uma correção afirmada.

## Fases

### Fase 14 — Falhas e coleta
- [ ] Catálogo de **cenários de falha** (`chaos/`): fonte com nome alterado (404), linha malformada que desloca colunas, mudança de esquema, teste do dbt quebrado, permissão negada no BigQuery, *timeout*, arquivo vazio e mudança de encoding.
- [ ] Coletores de logs do Airflow e dos artefatos do dbt, com sanitização.
- ✅ **Pronto quando:** cada cenário pode ser provocado sob demanda e gera logs e artefatos reais.

### Fase 15 — Agente de diagnóstico
- [ ] Agente com ADK e Gemini no Vertex AI, ferramentas somente leitura e saída estruturada.
- [ ] Modo de teste local: recebe os logs de um cenário e imprime o diagnóstico.
- ✅ **Pronto quando:** acerta a causa de pelo menos metade dos cenários e **sempre** cita evidência.

### Fase 16 — Ação e memória
- [ ] Abertura de *issue* e de PR **em rascunho** com o diagnóstico e a correção sugerida.
- [ ] Memória de incidentes no BigQuery e ferramenta de busca por incidentes parecidos.
- [ ] Gatilho real: callback do Airflow → Pub/Sub → Cloud Run.
- ✅ **Pronto quando:** uma falha induzida no Airflow gera sozinha uma *issue* com diagnóstico, sem ninguém intervir.

### Fase 17 — Avaliação e acabamento
- [ ] Avaliação sobre todos os cenários, repetida mais de uma vez para medir a variação.
- [ ] Métricas e comparativo de modelo (menor × maior), como no projeto 6.
- [ ] CI com testes do sanitizador e um subconjunto dos cenários.
- [ ] README com diagrama, resultados e uma demonstração em vídeo ou GIF.
- ✅ **Pronto quando:** o README tem a tabela de acurácia, custo e latência por cenário.

## Avaliação

| Métrica | Como medir |
|---|---|
| **Diagnóstico correto** | Comparar a causa proposta com a causa conhecida do cenário (rubrica; amostra conferida por humano) |
| **Evidência fiel** | Os trechos de log citados existem de fato no log? |
| **Correção sugerida plausível** | Resolveria o cenário? (revisão humana de amostra) |
| **Honestidade** | Em cenários sem solução clara, o agente declara baixa confiança? |
| **Custo e latência** | Tokens e tempo por incidente |
| **Robustez** | Log com texto malicioso não muda o comportamento |

## Riscos

| Risco | Mitigação |
|---|---|
| Diagnóstico plausível, porém errado | Evidência obrigatória, confiança explícita, humano decide |
| Vazamento de segredo em log | Sanitização antes do modelo, teste dedicado com segredos falsos |
| Laço de falhas e custo | Deduplicação, limite por hora, orçamento com alerta |
| Mudança de nomes e modelos no Vertex AI | Modelo por configuração e verificação na documentação oficial |
| Falhas artificiais não representarem as reais | Priorizar cenários vindos de falhas reais do projeto 1 |

## Definição de pronto

- [ ] Catálogo de ao menos 15 cenários de falha reprodutíveis.
- [ ] Diagnóstico estruturado com evidência obrigatória.
- [ ] Fluxo ponta a ponta até a *issue* ou PR em rascunho.
- [ ] Relatório de avaliação com acurácia, custo e latência.
- [ ] Nenhuma escrita em produção e nenhum segredo enviado ao modelo.
