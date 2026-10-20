CREATE TABLE IF NOT EXISTS ${dataset_silver}.registros_arquivo (
  id BIGINT,
  nome VARCHAR,
  valor DECIMAL(18, 2),
  _run_id VARCHAR,
  _source_file VARCHAR,
  _loaded_at TIMESTAMPTZ,
  _ingestion_date DATE
);
