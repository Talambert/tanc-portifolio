-- Dados de EXEMPLO (sintéticos) para o `dataeng run local` do tipo `relatorio`: o relatório precisa de uma tabela
-- de entrada. Em produção, remova a etapa `semeia_dados` e aponte as consultas para as tabelas reais.
-- SQL portátil (DuckDB e BigQuery): CREATE OR REPLACE ... AS SELECT com UNION ALL, sem VALUES; reexecutar é inofensivo.
CREATE OR REPLACE TABLE ${dataset_silver}.vendas_exemplo AS
SELECT 1 AS id, 'Alfa' AS nome, CAST(10.50 AS DECIMAL(18, 2)) AS valor
UNION ALL SELECT 2, 'Alfa', CAST(4.50 AS DECIMAL(18, 2))
UNION ALL SELECT 3, 'Beta', CAST(20.00 AS DECIMAL(18, 2))
UNION ALL SELECT 4, 'Beta', CAST(5.00 AS DECIMAL(18, 2))
UNION ALL SELECT 5, 'Gama', CAST(7.25 AS DECIMAL(18, 2))
UNION ALL SELECT 6, 'Delta', CAST(3.10 AS DECIMAL(18, 2));
