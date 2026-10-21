"""Etapa `spark` de EXEMPLO (NÃO executada nem validada pelo template): recalcula o agregado com PySpark.

Contrato do script (o mesmo no `spark-submit` local e no lote do Dataproc Serverless): recebe
`--etapa <id> --run-id <run_id>` e um `--param chave=valor` por parâmetro do pipeline e do ambiente (nunca
segredos). Lê e grava no BigQuery pelo conector `spark-bigquery`, que o lote do Dataproc traz; num
`spark-submit` local o conector não existe, então adapte a leitura/escrita para testar localmente.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--etapa", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--param", action="append", default=[], metavar="CHAVE=VALOR")
    args = parser.parse_args()
    params = dict(item.split("=", 1) for item in args.param)

    spark = SparkSession.builder.appName(f"{args.etapa}-{args.run_id}").getOrCreate()
    projeto, silver, gold = params["projeto_gcp"], params["dataset_silver"], params["dataset_gold"]
    base = spark.read.format("bigquery").load(f"{projeto}.{silver}.{{ cookiecutter.__tabela_base }}")
    agregado = base.groupBy("nome").agg(F.count("*").alias("qtd"), F.sum("valor").alias("total"))
    (
        agregado.write.format("bigquery")
        .option("table", f"{projeto}.{gold}.agregado_spark")
        .option("writeMethod", "direct")
        .mode("overwrite")
        .save()
    )
    spark.stop()


if __name__ == "__main__":
    main()
