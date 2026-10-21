CREATE OR REPLACE VIEW ${dataset_gold}.preco_medio_uf AS
SELECT
  uf,
  COUNT(*) AS qtd,
  CAST(AVG(valor_venda) AS DECIMAL(10, 4)) AS preco_medio
FROM ${dataset_silver}.precos_combustiveis
GROUP BY uf;
