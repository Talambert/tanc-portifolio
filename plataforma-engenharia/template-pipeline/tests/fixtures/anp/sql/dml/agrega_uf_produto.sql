MERGE INTO ${dataset_gold}.agregado_uf_produto AS t
USING (
  SELECT
    uf,
    produto,
    COUNT(*) AS qtd,
    CAST(AVG(valor_venda) AS DECIMAL(10, 4)) AS preco_medio
  FROM ${dataset_silver}.precos_combustiveis
  GROUP BY uf, produto
) AS s
ON t.uf = s.uf AND t.produto = s.produto
WHEN MATCHED THEN UPDATE SET qtd = s.qtd, preco_medio = s.preco_medio
WHEN NOT MATCHED THEN INSERT (uf, produto, qtd, preco_medio) VALUES (s.uf, s.produto, s.qtd, s.preco_medio);
