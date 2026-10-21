# 🎼 Orquestração com Airflow e Cloud Composer

![Status](https://img.shields.io/badge/status-planejado-lightgrey)
![Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Composer](https://img.shields.io/badge/Cloud_Composer-4285F4?logo=googlecloud&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)

Orquestra o pipeline inteiro (**ingestão → silver → dbt**) com **Apache Airflow**, rodando **o mesmo DAG** em dois ambientes: **Airflow local (Docker)** e **Cloud Composer** (gerenciado no GCP).

> **Projeto 3 de 4** da série `combustiveis-*`. Coordena o [pipeline](../combustiveis-pipeline-gcp/) e o [dbt](../combustiveis-dbt/).

## 🎯 O problema

Rodar cada etapa à mão não escala. É preciso agendar, ordenar dependências, tentar de novo quando a rede falha, reprocessar períodos passados e avisar quando algo quebra.

## 🧩 O que será entregue

- 🗓️ **DAG semanal** (a ANP publica toda semana) com `catchup` controlado
- 🔁 **Retries** com *backoff* e tarefas **idempotentes** por janela de dados
- 🧪 **Backfill** demonstrado
- 🚨 **Alertas** de falha
- ⚖️ **Comparativo local × Composer:** custo, setup, manutenção e observabilidade

## 💸 Nota sobre custo

O Cloud Composer cobra por hora de ambiente ligado. O plano é **subir, executar, capturar as evidências e destruir** o ambiente no mesmo dia, via Terraform, e documentar o custo real.

## 📍 Status

⬜ Aguardando os projetos [1](../combustiveis-pipeline-gcp/) e [2](../combustiveis-dbt/).

📋 Plano completo: [planos/03-combustiveis-orquestracao.md](../../planos/03-combustiveis-orquestracao.md)
