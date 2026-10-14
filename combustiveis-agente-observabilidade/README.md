# 🛡️ Agente de Observabilidade de Pipelines de Dados

![Status](https://img.shields.io/badge/status-planejado-lightgrey)
![Vertex AI](https://img.shields.io/badge/Vertex_AI-4285F4?logo=googlecloud&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?logo=apacheairflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)

Agente de IA (Gemini no **Vertex AI**) que acompanha o **Airflow** e o **dbt**: quando uma tarefa ou um teste falha, ele **lê os logs, investiga a causa provável, explica em linguagem clara e propõe a correção**, abrindo uma *issue* ou um *pull request* em rascunho para um humano revisar.

> Projeto da série `combustiveis-*`. Observa o pipeline dos projetos [1](../combustiveis-pipeline-gcp/), [2](../combustiveis-dbt/) e [3](../combustiveis-orquestracao/).

## 🎯 O problema

Quando um pipeline quebra de madrugada, alguém precisa abrir logs enormes, descobrir o que mudou e decidir o que fazer. Boa parte dessas falhas tem causas recorrentes, como um arquivo da fonte com nome diferente, uma coluna deslocada por um `;` no endereço ou uma carga atrasada, e o diagnóstico inicial pode ser automatizado.

## 🧩 O que será entregue

- 🔍 **Diagnóstico automático:** lê logs do Airflow e resultados de testes do dbt e resume a causa provável
- 🧠 **Memória de incidentes:** consulta falhas parecidas já ocorridas
- ✍️ **Proposta de correção:** sugere a mudança e abre uma *issue* ou um *pull request* **em rascunho**
- 🙋 **Humano no controle:** o agente **propõe e nunca aplica** mudanças em produção
- 📏 **Avaliação:** conjunto de falhas simuladas, com a taxa de diagnósticos corretos
- 📈 **Observabilidade do próprio agente:** custo, tokens e latência por incidente

## 🏗️ Arquitetura (visão geral)

```text
 Airflow / dbt ──falha──▶ Agente (ADK + Gemini · Vertex AI)
                              │ lê logs e metadados (somente leitura)
                              ▼
                  diagnóstico + proposta ──▶ issue / PR em rascunho ──▶ revisão humana
```

## 📍 Status

⬜ Aguardando os projetos [2 (dbt)](../combustiveis-dbt/) e [3 (orquestração)](../combustiveis-orquestracao/).

📋 Plano completo: [planos/06-combustiveis-agente-observabilidade.md](../planos/06-combustiveis-agente-observabilidade.md) (Fases 14 a 17)

## 🛡️ Princípios de projeto

- **Somente leitura** nos sistemas observados.
- **Propor, não aplicar:** toda mudança passa por revisão humana.
- **Logs são dados:** o texto vindo de logs nunca é tratado como instrução para o modelo.
- **Admitir incerteza:** quando a causa não está clara, o agente diz isso e lista as hipóteses.
