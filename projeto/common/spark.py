"""Construção da SparkSession para execução local e na AWS.

Local: SparkSession padrão escrevendo Parquet em ``lake/``.
AWS  : os mesmos scripts rodam no AWS Glue, que injeta a sessão e as libs S3;
       aqui adicionamos os pacotes hadoop-aws apenas quando fora do Glue.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def build_spark(app_name: str = "tc2-alfabetizacao", aws: bool = False) -> SparkSession:
    """Cria (ou reaproveita) uma SparkSession.

    Parameters
    ----------
    app_name: nome da aplicação Spark.
    aws: quando ``True`` e rodando fora do Glue, habilita o conector S3A para
        ler/escrever em ``s3://``. Dentro do Glue isso já vem configurado.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        # Parquet: dictionary + snappy já são padrão; garantimos overwrite dinâmico
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )

    if aws and "GLUE_PYTHON_VERSION" not in os.environ:
        # Fora do Glue: baixa o conector S3A para acessar s3://
        builder = builder.config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4"
        )
        endpoint = os.environ.get("AWS_ENDPOINT_URL")
        if endpoint:  # LocalStack
            hconf = "spark.hadoop.fs.s3a."
            builder = (
                builder.config(hconf + "endpoint", endpoint)
                .config(hconf + "path.style.access", "true")
            )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
