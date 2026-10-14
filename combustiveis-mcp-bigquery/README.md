# 🔌 Servidor MCP para o BigQuery — Preços de Combustíveis

![Status](https://img.shields.io/badge/status-planejado-lightgrey)
![MCP](https://img.shields.io/badge/MCP-000000)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?logo=googlebigquery&logoColor=white)
![Cloud Run](https://img.shields.io/badge/Cloud_Run-4285F4?logo=googlecloud&logoColor=white)

Servidor **MCP** (*Model Context Protocol*) que expõe o modelo dimensional do BigQuery para clientes de IA, de forma **segura, limitada em custo e auditável**.

> **Projeto 5 de 6** da série `combustiveis-*`. Lê os marts do [dbt](../combustiveis-dbt/) e serve o [agente de dados](../combustiveis-agente-dados/).

## 🎯 O problema

Deixar um modelo de IA consultar um banco de dados é arriscado: ele pode gerar SQL caro, tentar alterar dados ou ser induzido por texto malicioso. Este servidor é a **camada de controle** entre a IA e os dados.

## 🧩 O que será entregue

- 🧰 **Ferramentas MCP:** listar tabelas, descrever colunas (com as descrições do dbt), consultar definições de métricas, linhagem, amostras, estimar custo e executar consultas
- 🛡️ **Somente leitura em três camadas:** validação do SQL, *dry-run* e Service Account sem permissão de escrita
- 💸 **Limites de custo:** bytes máximos por consulta, limite de linhas e *timeout*
- 🔐 **Cloud Run privado**, acessível só com identidade autorizada
- 🧾 **Auditoria** de cada chamada em uma tabela do BigQuery
- ✅ **Testes de segurança** com tentativas reais de burlar os filtros

## 🏗️ Arquitetura

```text
 Cliente MCP (agente) ──HTTP autenticado──▶ Servidor MCP (Cloud Run)
                                               │  valida · estima · limita
                                               ▼
                                      BigQuery (apenas marts, só leitura)
                                               │
                                               ▼
                                      Tabela de auditoria
```

## 📍 Status

⬜ Aguardando os projetos [1](../combustiveis-pipeline-gcp/) e [2 (dbt)](../combustiveis-dbt/).

📋 Plano completo: [planos/05-combustiveis-mcp-e-agente.md](../planos/05-combustiveis-mcp-e-agente.md) (Parte A, Fase 9)

## 🧪 O que será demonstrado

| Cenário | Resultado esperado |
|---|---|
| `SELECT` válido nos marts | Executa, com custo registrado |
| `DROP TABLE`, `INSERT`, várias instruções | **Bloqueado** e auditado |
| Consulta a dataset fora da lista | **Bloqueado** |
| Consulta que estouraria o limite de bytes | **Rejeitada** antes de executar |
| Chamada ao servidor sem token | **Rejeitada** |
