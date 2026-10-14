# ⛽ Pipeline de Preços de Combustíveis no GCP

![Status](https://img.shields.io/badge/status-em_desenvolvimento-orange)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?logo=apachespark&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google_Cloud-4285F4?logo=googlecloud&logoColor=white)

Pipeline **ELT** que leva os preços de combustíveis da **ANP** do arquivo cru até uma tabela limpa e tipada no **BigQuery**, implementado **duas vezes**, com **Python (Cloud Run Jobs)** e com **PySpark (Dataproc Serverless)**, para comparar custo, tempo e esforço de cada abordagem.

> Este é o **projeto 1 de 4** da série `combustiveis-*`. Os demais consomem a saída deste: [dbt](../combustiveis-dbt/) → [orquestração](../combustiveis-orquestracao/) → [Power BI](../combustiveis-bi-powerbi/).

## 🎯 O problema

A ANP publica o levantamento semanal de preços em CSV, mas os arquivos têm armadilhas reais: encoding com BOM, vírgula decimal, CNPJ com espaços, linhas com `;` dentro do endereço (que deslocam as colunas) e nomes de arquivo irregulares. O projeto transforma isso em dado confiável, rastreável e pronto para análise.

## 🏗️ Arquitetura

```text
 ANP (gov.br)
      │  download com retry
      ▼
 ┌──────────┐   Python · Cloud Run Jobs     ┌──────────────┐
 │  BRONZE  │ ────────────────────────────▶ │ silver_python│
 │ Cloud    │                               └──────────────┘
 │ Storage  │   PySpark · Dataproc Serverless┌──────────────┐
 │          │ ────────────────────────────▶ │ silver_spark │
 └──────────┘                               └──────────────┘
                                                   │
                                      comparativo e prova de equivalência
```

- **Contrato único** em [`common/`](common/): os dois motores aplicam as mesmas regras.
- **Quarentena** para linhas malformadas, com o motivo, em vez de descarte silencioso.
- **Idempotente:** reexecutar não duplica nem baixa de novo.

## 📍 Status

| Fase | Entrega | Estado |
|---|---|:-:|
| 0 | Base: estrutura, manifesto de fontes, downloader local | 🟡 Em andamento |
| 0 | Base na nuvem: projeto GCP, alerta de orçamento, Terraform | ⬜ A fazer |
| 1 | Ingestão para o bucket (bronze) | ⬜ A fazer |
| 2 | Motor Python → silver | ⬜ A fazer |
| 3 | Motor PySpark → silver | ⬜ A fazer |
| 4 | Comparativo e prova de equivalência | ⬜ A fazer |

📋 Plano completo: [planos/01-combustiveis-pipeline-gcp.md](../planos/01-combustiveis-pipeline-gcp.md)

## 📁 Estrutura

```text
combustiveis-pipeline-gcp/
├── common/            contrato de dados e manifesto de fontes (sources.yaml)
├── ingestion/         download da ANP → local / Cloud Storage
├── python-cloudrun/   motor Python
├── pyspark-dataproc/  motor PySpark
├── sql/               verificação de equivalência
├── infra/             Terraform
└── tests/
```

## ▶️ Como rodar (o que já existe)

Download local dos arquivos da ANP, em Git Bash, a partir desta pasta:

```bash
python -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt

python ingestion/download_anp.py --check                       # as URLs respondem?
python ingestion/download_anp.py --only ca-2026-01 --unzip     # baixa um semestre
python ingestion/download_anp.py                               # baixa tudo do manifesto
```

Os arquivos vão para `data/raw/` (ignorada pelo git) junto com um `manifest.json` com tamanho e SHA-256 de cada um. A ingestão é idempotente: o que já existe não é baixado de novo (use `--force` para refazer).

## 📚 Fonte dos dados

[ANP — Série histórica de preços de combustíveis](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis), dados abertos.
