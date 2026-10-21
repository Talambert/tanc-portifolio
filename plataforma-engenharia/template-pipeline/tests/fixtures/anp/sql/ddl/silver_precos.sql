CREATE TABLE IF NOT EXISTS ${dataset_silver}.precos_combustiveis (
  cnpj_revenda STRING,
  razao_social STRING,
  endereco STRING,
  uf STRING,
  produto STRING,
  data_coleta DATE,
  valor_venda DECIMAL(10, 2),
  _run_id STRING,
  _source_file STRING,
  _loaded_at TIMESTAMP,
  _ingestion_date DATE
);
