# 🧾 Extração Estruturada de Documentos com IA

![Status](https://img.shields.io/badge/status-planejado-lightgrey)
![Vertex AI](https://img.shields.io/badge/Vertex_AI-4285F4?logo=googlecloud&logoColor=white)
![Pydantic](https://img.shields.io/badge/Pydantic-E92063?logo=pydantic&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?logo=googlebigquery&logoColor=white)

Pipeline que recebe **PDFs e planilhas sem padrão fixo**, usa **Gemini (Vertex AI)** para **extrair os dados em um esquema definido**, **valida cada campo** e carrega no BigQuery, enviando para **quarentena** tudo o que não for confiável.

> Projeto independente da série dos combustíveis. Pode alimentar o [pipeline de ingestão](../../serie-combustiveis/combustiveis-pipeline-gcp/) quando a fonte não for CSV.

## 🎯 O problema

Muitos dados importantes chegam em formatos que ninguém padronizou: relatórios em PDF, tabelas coladas em planilhas, layouts que mudam de um mês para outro. Extrair isso à mão não escala, e confiar cegamente na IA é arriscado. O valor está em **extrair e provar que está certo**.

## 🧩 O que será entregue

- 📄 **Extração com saída estruturada:** o modelo devolve JSON dentro de um esquema definido (Pydantic)
- ✅ **Validação em camadas:** tipos, faixas plausíveis, regras de negócio e consistência entre campos (por exemplo, a soma de uma tabela confere com o total)
- 🎚️ **Nível de confiança por campo** e **quarentena** para os casos duvidosos, com o motivo
- 🙋 **Revisão humana** dos itens em quarentena
- 📏 **Avaliação:** documentos de referência com o gabarito, medindo a acurácia por campo
- 🚚 **Carga no BigQuery** com rastreabilidade (documento de origem, página, versão do esquema)
- 💸 **Custo por documento** registrado

## 🏗️ Arquitetura (visão geral)

```text
 PDF / planilha (Cloud Storage)
        │
        ▼
 Gemini (Vertex AI) ──▶ JSON no esquema ──▶ validação (Pydantic + regras)
                                                │
                            ┌───────────────────┴───────────────────┐
                            ▼                                       ▼
                   confiável → BigQuery                  duvidoso → quarentena → revisão humana
```

## 📍 Status

⬜ Planejado. Os documentos de teste serão **gerados por um script a partir de dados fictícios** (com gabarito exato), complementados, se for o caso, por poucos PDFs públicos. Nunca dados de empresas.

📋 Plano completo: [planos/07-extracao-documentos-ia.md](../../planos/07-extracao-documentos-ia.md) (Fases 18 a 21)

## 🛡️ Princípios de projeto

- **Esquema primeiro:** o formato de saída é definido antes de qualquer extração.
- **Desconfie da extração:** todo campo é validado, e o duvidoso vai para revisão.
- **Rastreabilidade total:** cada valor aponta o documento e a página de origem.
- **Documentos são dados:** o texto de um documento nunca é tratado como instrução para o modelo.
