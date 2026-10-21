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

- **Base:** o pipeline segue o [`template-pipeline`](../template-pipeline/) (tipo `hibrido`, origem `arquivo`) sobre a biblioteca [`dataeng-core`](../lib-dataeng-core/): o contrato e as regras de limpeza ficam em YAML em [`config/`](config/) e [`schemas/`](schemas/).
- **Contrato único:** os dois motores aplicam as mesmas regras (hoje só o motor Python/pandas tem execução local; o Spark é a Fase 3).
- **Quarentena** para linhas malformadas, com o motivo, em vez de descarte silencioso.
- **Idempotente:** reexecutar não duplica nem baixa de novo.

## 📍 Status

| Fase | Entrega | Estado |
|---|---|:-:|
| 0 | Base: estrutura, manifesto de fontes, downloader local | 🟡 Em andamento |
| 0 | Contrato e configuração do pipeline (template): validados e rodados local com amostra sintética | 🟡 Em andamento |
| 0 | Base na nuvem: projeto GCP, alerta de orçamento, Terraform | ⬜ A fazer |
| 1 | Ingestão para o bucket (bronze) | ⬜ A fazer |
| 2 | Motor Python → silver | ⬜ A fazer |
| 3 | Motor PySpark → silver | ⬜ A fazer |
| 4 | Comparativo e prova de equivalência | ⬜ A fazer |

📋 Plano completo: [planos/01-combustiveis-pipeline-gcp.md](../planos/01-combustiveis-pipeline-gcp.md)

## 🔬 Status do que existe

O que é **real** hoje:

- `config/pipeline.yaml` (+ ambientes `local`, `dev`, `prod` com valores fictícios) e `schemas/` descrevem o contrato da silver e as regras de limpeza (CNPJ só com dígitos, vírgula decimal, `dd/mm/aaaa`, quarentena de linhas malformadas, deduplicação por `cnpj_revenda + produto + data_coleta`).
- `dataeng validate-config` e `dataeng plan` passam nos três ambientes, e `dataeng run local` roda com uma **amostra sintética** escrita dentro do teste [`tests/test_pipeline_config.py`](tests/test_pipeline_config.py) (DuckDB local, CNPJs fictícios).
- `ingestion/download_anp.py` baixa os arquivos do manifesto `common/sources.yaml` (veja abaixo).

O que **não** existe:

- Nenhuma execução em GCP (Cloud Run, BigQuery, Cloud Storage, Dataproc, Airflow): o `dev`/`prod` são só exemplos de configuração.
- Nenhum dado real da ANP foi processado. O cabeçalho de `schemas/origem_precos.yaml` segue o layout público conhecido do CSV e ainda não foi conferido contra um arquivo real.
- O motor PySpark nunca foi executado, e a comparação de tempo e custo Python × PySpark continua sendo trabalho futuro.
- Ainda faltam: `cep` só com dígitos e hífen, lista controlada de `produto`, partição por mês de `data_coleta` (a biblioteca particiona por `_ingestion_date`) e versão Spark do gancho `custom/anp.py`.

## 📁 Estrutura

```text
combustiveis-pipeline-gcp/
├── common/            manifesto de fontes (sources.yaml); fonte única das URLs da ANP
├── config/            pipeline.yaml e ambientes/ (local, dev, prod) — do template
├── schemas/           colunas do CSV de origem e da silver — do template
├── custom/            gancho de transformação (endereco = rua + número + complemento)
├── sql/pipeline/      DDL da silver (sql/ fica livre para a verificação de equivalência)
├── ingestion/         download da ANP → local / Cloud Storage
├── python-cloudrun/   motor Python
├── pyspark-dataproc/  motor PySpark
├── infra/             Terraform
└── tests/             test_pipeline_config.py (validate-config, plan e run local sintético)
```

`common/sources.yaml` continua sendo o manifesto: o `pipeline.yaml` não repete as URLs. Cada execução ingere **um** arquivo, indicado pelo parâmetro `arquivo_anp` (padrão `data/raw/ca-2026-01.csv`, o CSV que `download_anp.py --only ca-2026-01 --unzip` extrai); o `id` do arquivo está no manifesto.

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

### Validar e rodar o pipeline (config)

A biblioteca `dataeng-core` ainda **não está publicada**: instale-a em modo editável a partir da **raiz do repositório** do portfólio (extra `local` traz o DuckDB) e rode os comandos nesta pasta:

```bash
pip install -e "lib-dataeng-core[local]" pytest

cd combustiveis-pipeline-gcp
dataeng validate-config --config config/pipeline.yaml
dataeng plan --config config/pipeline.yaml --ambiente dev
python -m pytest tests -q          # inclui um run local com amostra sintética

# com o CSV da ANP já baixado e extraído em data/raw/ (confira o nome do arquivo):
dataeng run local --config config/pipeline.yaml --param arquivo_anp=data/raw/<arquivo>.csv
```

O `requirements.txt` continua só com o necessário ao downloader; `dataeng-core` não entra nele enquanto não houver publicação.

## 📚 Fonte dos dados

[ANP — Série histórica de preços de combustíveis](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis), dados abertos.
