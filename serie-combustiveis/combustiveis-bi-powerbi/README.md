# 📊 Painel de Preços de Combustíveis no Power BI

![Status](https://img.shields.io/badge/status-planejado-lightgrey)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?logo=powerbi&logoColor=black)
![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?logo=googlebigquery&logoColor=white)
![DAX](https://img.shields.io/badge/DAX-F2C811?logoColor=black)

Painel no **Power BI** sobre o modelo dimensional do BigQuery, que fecha a história **do arquivo cru ao insight**: onde o combustível está mais caro, quando compensa o etanol, quais bandeiras praticam os maiores preços.

> **Projeto 4 de 4** da série `combustiveis-*`. Consome os marts do [dbt](../combustiveis-dbt/).

## 🎯 O problema

Um modelo bem feito só gera valor quando alguém consegue enxergar a resposta. O painel traduz o esquema estrela em perguntas de negócio e respostas visuais.

## 🧩 O que será entregue

- 🗺️ **5 páginas:** Resumo, Mapa, Paridade etanol/gasolina, Bandeiras e postos, Sobre os dados
- ⭐ **Modelo semântico** em estrela, com medidas DAX documentadas
- ✅ **Números validados** contra consultas diretas no BigQuery
- 🖼️ **Capturas de tela** de cada página
- 📄 `.pbix` sem credenciais, pronto para abrir e apontar para outro projeto GCP

## ❓ Perguntas de negócio

1. Como estão os preços hoje e como variaram no último mês?
2. Onde a gasolina está mais cara e mais barata?
3. Em quais regiões compensa abastecer com etanol?
4. Quem pratica os maiores e menores preços?
5. Há sazonalidade ou tendência por produto?

## 📍 Status

⬜ Aguardando o [projeto 2 (dbt)](../combustiveis-dbt/).

📋 Plano completo: [planos/04-combustiveis-bi-powerbi.md](../../planos/04-combustiveis-bi-powerbi.md)

## 📚 Fonte dos dados

[ANP — Série histórica de preços de combustíveis](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis), dados abertos.
