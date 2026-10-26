# Changelog

Formato baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/), com versionamento semântico.

## [0.1.0] - Não lançado

Implementado e testado com a versão local (DuckDB, pasta local, Pub/Sub em memória, clientes falsos). A execução em GCP, no Airflow, no Spark e no CI do GitHub não foi validada; veja a tabela de limitações no [README](README.md#limitações-e-o-que-não-foi-validado).

### Adicionado

- Pacote `dataeng_core` em camadas (`base`, `storage`, `warehouse`, `pubsub`, `sources`, `transform`, `quality`, `sql`, `steps`, `jobs`, `airflow`, `cli`), com o contrato verificado pelo `import-linter`. Extras `local`, `gcp`, `spark`, `airflow` e `dev`.
- `base`: modelos Pydantic do `pipeline.yaml` (`schema_version: 1`), ambientes, parâmetros `${nome}`, erros tipados, retry, log estruturado com redação de segredos e `run_id`.
- `storage`, `warehouse` e `pubsub`: interfaces com implementações locais (pasta, DuckDB, memória) e de GCP (Cloud Storage, BigQuery, Pub/Sub), com bateria de contrato compartilhada.
- `sources`: arquivos (`csv`, `json`, `jsonl`, `xlsx`, `zip`), API (paginação, autenticação, limite de taxa) e Pub/Sub em pull.
- `transform`: transformação declarativa (`renomear`, `tipar`, `normalizar`, `deduplicar`, `rejeitar_se`, `hook`) em pandas e em PySpark, com quarentena das linhas rejeitadas.
- `quality`: verificações `nao_nulo`, `unico`, `faixa`, `conjunto` e `formato`.
- `sql`: preenchimento de parâmetros, divisão em instruções e validação de sintaxe com `sqlglot`.
- `steps`: tipos `ingestao` (origens `arquivo`, `api`, `pubsub`), `sql`, `view`, `python`, `spark` e `qualidade`; tabela de controle `_controle_execucoes`.
- `jobs`: `ingest`, `ingest_spark` e `run_step`.
- `airflow`: planejador de tarefas (Cloud Run Job, Dataproc Serverless, BigQuery) e `build_dag`.
- CLI `dataeng`: `validate-config`, `plan` e `run local`.
- CI em `.github/workflows/lib-dataeng-core.yml` (lint, testes, Spark, Airflow e build), ainda não executado no GitHub.

### Limitações conhecidas

Detalhadas no README. Em resumo: nada foi validado contra GCP, Airflow, Cloud Run, Dataproc ou Spark reais; `qualidade` roda sempre como Cloud Run Job; scripts procedurais do BigQuery não são suportados; o `merge` por chave de negócio é incondicional entre execuções.
