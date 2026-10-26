-- Resumo por nome sobre a tabela base ({{ cookiecutter.__tabela_base }}). SQL portátil (DuckDB e BigQuery).
CREATE OR REPLACE VIEW ${dataset_gold}.resumo_por_nome AS
SELECT
  nome,
  COUNT(*) AS qtd,
  SUM(valor) AS total
FROM ${dataset_silver}.{{ cookiecutter.__tabela_base }}
GROUP BY nome;
