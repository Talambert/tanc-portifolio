CREATE TABLE IF NOT EXISTS ${dataset_silver}.eventos (
  message_id STRING,
  publish_time STRING,
  id INT64,
  nome STRING,
  valor DECIMAL(18, 2),
  _run_id STRING,
  _source_file STRING,
  _loaded_at TIMESTAMP,
  _ingestion_date DATE
);
