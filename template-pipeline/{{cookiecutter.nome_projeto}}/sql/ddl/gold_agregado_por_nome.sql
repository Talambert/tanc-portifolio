-- Tabela de destino do MERGE de `sql/dml/agrega_por_nome.sql` (tipos aceitos por DuckDB e BigQuery).
CREATE TABLE IF NOT EXISTS ${dataset_gold}.agregado_por_nome (
  nome STRING,
  qtd BIGINT,
  total DECIMAL(18, 2)
);
