# dataeng-core

Biblioteca Python de engenharia de dados (distribuição `dataeng-core`, importação `dataeng_core`):
ingestão de arquivos, APIs e Pub/Sub para o bronze, transformação para a silver, qualidade, SQL
parametrizado, etapas, jobs, DAGs do Airflow e CLI `dataeng`.

## Extras

- `local`: DuckDB, para execução local.
- `gcp`: Cloud Storage, BigQuery e Pub/Sub.
- `spark`: PySpark.
- `airflow`: Apache Airflow e provedor Google.
- `dev`: pytest, ruff, mypy, import-linter e demais ferramentas de desenvolvimento.

```bash
pip install -e ".[local,dev]"
```

## Nota

Esta pasta é tratada como um repositório independente: README, `pyproject.toml`, CHANGELOG e CI próprios.
