CREATE TABLE IF NOT EXISTS ${dataset_silver}.eventos (
  message_id VARCHAR,
  publish_time VARCHAR,
  id BIGINT,
  nome VARCHAR,
  valor DECIMAL(18, 2),
  _run_id VARCHAR,
  _source_file VARCHAR,
  _loaded_at TIMESTAMPTZ,
  _ingestion_date DATE
);
