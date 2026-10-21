# 🧱 Modelagem Dimensional com dbt — Preços de Combustíveis

![Status](https://img.shields.io/badge/status-planejado-lightgrey)
![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?logo=googlebigquery&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-336791?logo=postgresql&logoColor=white)

Transforma a camada **silver** do BigQuery em um **modelo dimensional** (fato e dimensões) com **testes automáticos** e **documentação com linhagem**, usando **dbt Core**.

> **Projeto 2 de 4** da série `combustiveis-*`. Consome a saída de [combustiveis-pipeline-gcp](../combustiveis-pipeline-gcp/) e alimenta o [Power BI](../combustiveis-bi-powerbi/).

## 🎯 O problema

A silver está limpa, mas ainda é uma tabela larga e repetitiva. Para análise, o dado precisa de um modelo claro (fato e dimensões), garantias de qualidade e documentação que qualquer pessoa consiga navegar.

## 🧩 O que será entregue

- ⭐ **Esquema estrela:** `fct_preco_combustivel` + `dim_produto`, `dim_posto`, `dim_localidade`, `dim_data`
- ✅ **Testes:** unicidade, não nulos, integridade entre fato e dimensões, valores aceitos e regras de negócio
- 📖 **Documentação gerada** com grafo de linhagem, publicada no GitHub Pages
- ♻️ **Carga incremental** na fato, particionada por mês
- 🔁 **CI** no GitHub Actions

## 📍 Status

⬜ Aguardando a conclusão do [projeto 1](../combustiveis-pipeline-gcp/).

📋 Plano completo: [planos/02-combustiveis-dbt.md](../planos/02-combustiveis-dbt.md)

## ❓ Perguntas que o modelo responde

1. Qual o preço médio da gasolina por UF, mês a mês?
2. Qual a paridade etanol/gasolina por região?
3. Quais bandeiras praticam os preços mais altos e mais baixos?
4. Como o preço varia por município dentro de um estado?
