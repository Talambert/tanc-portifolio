-- Agrega por nome e grava no gold com MERGE: recalcula tudo a cada execução, então reexecutar não duplica.
-- SQL portátil (DuckDB e BigQuery); no BigQuery o UPDATE SET não leva alias na coluna.
MERGE INTO ${dataset_gold}.agregado_por_nome AS t
USING (
  SELECT nome, COUNT(*) AS qtd, SUM(valor) AS total
  FROM ${dataset_silver}.{{ cookiecutter.__tabela_base }}
  WHERE nome IS NOT NULL
  GROUP BY nome
) AS s
ON t.nome = s.nome
WHEN MATCHED THEN UPDATE SET qtd = s.qtd, total = s.total
WHEN NOT MATCHED THEN INSERT (nome, qtd, total) VALUES (s.nome, s.qtd, s.total);
