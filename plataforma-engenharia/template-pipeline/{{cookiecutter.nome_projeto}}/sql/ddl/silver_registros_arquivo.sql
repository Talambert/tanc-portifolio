CREATE TABLE IF NOT EXISTS ${dataset_silver}.registros_arquivo (
  id INT64,
  nome STRING,
  valor DECIMAL(18, 2),
  _run_id STRING,
  _source_file STRING,
  _loaded_at TIMESTAMP,
  _ingestion_date DATE
);
