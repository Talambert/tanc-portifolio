# 🤖 Agente de Dados com Gemini — Preços de Combustíveis

![Status](https://img.shields.io/badge/status-planejado-lightgrey)
![Vertex AI](https://img.shields.io/badge/Vertex_AI-4285F4?logo=googlecloud&logoColor=white)
![Gemini](https://img.shields.io/badge/Gemini-8E75B2?logo=googlegemini&logoColor=white)
![ADK](https://img.shields.io/badge/Agent_Development_Kit-4285F4)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)

Agente de IA que responde **perguntas em português sobre preços de combustíveis**, consultando o BigQuery pelo [servidor MCP](../combustiveis-mcp-bigquery/), e que **mede a própria qualidade**.

> **Projeto 6 de 6** da série `combustiveis-*`. Usa **somente o Vertex AI** (Gemini).

> 💬 *"Quais as 5 UFs com maior alta no preço do etanol nos últimos 3 meses?"*  
> → tabela, resumo em português, **o SQL usado** e o custo da consulta.

## 🎯 O problema

Agentes que geram SQL costumam parecer corretos e errar em silêncio. O foco deste projeto não é só responder, e sim **provar o quanto acerta, quanto custa e como se comporta diante de casos difíceis**.

## 🧩 O que será entregue

- 💬 **Agente em português** com Gemini no Vertex AI, construído com o Agent Development Kit
- 🧠 **Camada semântica:** usa as descrições e métricas do dbt como contexto
- 🔎 **Transparência:** toda resposta mostra o SQL e o custo
- 🙋 **Pede esclarecimento** quando a pergunta é ambígua e **recusa** o que está fora do escopo ou tenta alterar dados
- 📏 **Avaliação medida:** ~40 perguntas de referência, com acurácia de execução, custo e latência
- 🧪 **Experimentos comparativos:** com × sem camada semântica, e modelo menor × maior
- 💸 **Controle de gasto:** orçamento, limite diário e cache
- 📈 **Observabilidade:** pergunta, SQL, bytes, tokens, latência e feedback registrados

## 🏗️ Arquitetura

```text
 Usuário ─▶ Agente (ADK + Gemini · Vertex AI) ──MCP──▶ Servidor MCP ──▶ BigQuery (marts)
                         │
                         └──▶ Log de interações (pergunta · SQL · bytes · tokens · latência)
```

## 📍 Status

⬜ Aguardando o [servidor MCP](../combustiveis-mcp-bigquery/).

📋 Plano completo: [planos/05-combustiveis-mcp-e-agente.md](../planos/05-combustiveis-mcp-e-agente.md) (Parte B, Fases 10 a 13)

## 📊 Resultados (a preencher após a Fase 11)

| Configuração | Acurácia de execução | Custo médio / pergunta | Latência p95 |
|---|:-:|:-:|:-:|
| Modelo menor, sem camada semântica | — | — | — |
| Modelo menor, com camada semântica | — | — | — |
| Modelo maior, com camada semântica | — | — | — |

## 🛡️ Princípios de projeto

- **Somente leitura:** o agente nunca altera dados.
- **Mostre o trabalho:** SQL e custo sempre visíveis.
- **Admita a incerteza:** perguntar ou recusar é melhor que chutar.
- **Dados são dados:** o conteúdo vindo do banco nunca é tratado como instrução.
