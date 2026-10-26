# ⛽ Série de combustíveis

O pipeline de preços de combustíveis da ANP ponta a ponta: do arquivo cru ao painel de decisão. Os projetos contam uma única história com o mesmo dataset.

- [`combustiveis-pipeline-gcp`](combustiveis-pipeline-gcp/): ELT da ANP até o BigQuery, em Python e PySpark. 🔨 Em andamento (GCP, Spark e dados reais da ANP não validados).
- [`combustiveis-dbt`](combustiveis-dbt/): modelagem dimensional com dbt. 🚧 Planejado.
- [`combustiveis-orquestracao`](combustiveis-orquestracao/): Airflow local e Cloud Composer. 🚧 Planejado.
- [`combustiveis-bi-powerbi`](combustiveis-bi-powerbi/): painel no Power BI. 🚧 Planejado.

O pipeline foi gerado pelo template da [plataforma de engenharia](../plataforma-engenharia/); o que não foi validado está no [README raiz](../README.md).
