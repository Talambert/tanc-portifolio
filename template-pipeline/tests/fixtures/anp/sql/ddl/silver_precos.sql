CREATE TABLE IF NOT EXISTS ${dataset_silver}.precos_combustiveis (
  cnpj_revenda VARCHAR,
  razao_social VARCHAR,
  endereco VARCHAR,
  uf VARCHAR,
  produto VARCHAR,
  data_coleta DATE,
  valor_venda DECIMAL(10, 2),
  _run_id VARCHAR,
  _source_file VARCHAR,
  _loaded_at TIMESTAMPTZ,
  _ingestion_date DATE
);
