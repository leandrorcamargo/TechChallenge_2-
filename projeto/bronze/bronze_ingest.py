"""Bronze — ingestão batch (PySpark, compatível com AWS Glue).

Lê a zona ``raw/`` e grava a camada ``bronze/`` em Parquet, aplicando apenas as
transformações TÉCNICAS acordadas (nenhuma regra de negócio):

1. Converte CSV(.gz)/CSV → Parquet + snappy.
2. Lê **todas as colunas como STRING** (fidelidade máxima; nomes originais preservados).
3. Adiciona metadados: ``_ingestion_ts``, ``_ingestion_run_id``, ``_source_file``, ``_source_year``.
4. Une os anos dos microdados por tabela.
5. Particiona por ``ano`` (nativo na Base dos Dados; derivado do ano do arquivo nos microdados).

Uso local:
    python projeto/bronze/bronze_ingest.py         # requer a zona raw populada (landing.py)

No AWS Glue este arquivo é enviado como ``glue/bronze-ingest.py`` e lê/grava em s3://.
"""

from __future__ import annotations

import re
import sys
import uuid
from pathlib import Path

# Torna ``common`` importável como script standalone (fora do Glue).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession, functions as F  # noqa: E402

from common.config import Settings, load_settings  # noqa: E402
from common.io import layer_path  # noqa: E402
from common.logging_setup import get_logger  # noqa: E402
from common.spark import build_spark  # noqa: E402

log = get_logger("bronze.ingest")
_YEAR_RE = re.compile(r"(20\d{2})")


def read_csv_all_string(
    spark: SparkSession, path: str, sep: str, encoding: str
) -> DataFrame:
    """Lê um CSV com todas as colunas como STRING (fidelidade máxima).

    Com ``inferSchema=False`` e sem schema explícito, o Spark lê todas as colunas
    como StringType, preservando o conteúdo bruto (nomes originais inclusive).
    """
    return (
        spark.read.option("header", True)
        .option("sep", sep)
        .option("encoding", encoding)
        .option("inferSchema", False)
        .csv(path)
    )


def add_metadata(
    df: DataFrame, source_file: str, source_year: str | None, run_id: str
) -> DataFrame:
    """Adiciona as colunas técnicas de linhagem à Bronze."""
    return (
        df.withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_ingestion_run_id", F.lit(run_id))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_source_year", F.lit(source_year).cast("string"))
    )


def write_bronze(df: DataFrame, path: str, partition_by: list[str]) -> None:
    """Grava a tabela Bronze em Parquet+snappy, particionada."""
    (
        df.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy(*partition_by)
        .save(path)
    )


def ingest_base_dos_dados(spark: SparkSession, settings: Settings, run_id: str) -> None:
    """Ingesta as 5 tabelas agregadas da Base dos Dados (particiona pelo ``ano`` nativo)."""
    for table, filename in settings.get("sources.base_dos_dados", {}).items():
        raw_path = layer_path(settings, "raw", f"base_dos_dados/{filename}")
        df = read_csv_all_string(spark, raw_path, sep=",", encoding="utf-8")
        df = add_metadata(df, source_file=filename, source_year=None, run_id=run_id)
        out = layer_path(settings, "bronze", table)
        write_bronze(df, out, partition_by=["ano"])
        log.info("bronze gravada", extra={"extra": {"tabela": table, "linhas": df.count()}})


def ingest_microdados(spark: SparkSession, settings: Settings, run_id: str) -> None:
    """Ingesta TS_ALUNO/TS_MUNICIPIO/TS_ESTADO unindo os anos; particiona por ``ano``."""
    tables = settings.get("sources.microdados.bronze_tables", [])
    sep = settings.get("sources.microdados.csv_sep", ";")
    enc = settings.get("sources.microdados.encoding", "latin1")
    years = [_YEAR_RE.search(z).group(1) for z in settings.get("sources.microdados.zips", [])]

    for table in tables:
        parts: list[DataFrame] = []
        for year in years:
            path = layer_path(settings, "raw", f"microdados/{year}/{table}.csv")
            df = read_csv_all_string(spark, path, sep=sep, encoding=enc)
            # 'ano' de partição vem do ano do arquivo (confiável e sempre presente);
            # a coluna original NU_ANO_AVALIACAO permanece intacta nos dados.
            df = df.withColumn("ano", F.lit(year))
            df = add_metadata(
                df, source_file=f"{table}.csv", source_year=year, run_id=run_id
            )
            parts.append(df)

        unified = parts[0]
        for extra in parts[1:]:
            unified = unified.unionByName(extra, allowMissingColumns=True)

        out = layer_path(settings, "bronze", table.lower())
        write_bronze(unified, out, partition_by=["ano"])
        log.info("bronze gravada", extra={"extra": {"tabela": table.lower(), "anos": years}})


def main() -> None:
    settings = load_settings()
    run_id = uuid.uuid4().hex
    spark = build_spark("bronze-ingest", aws=settings.is_aws)
    log.info("bronze iniciada", extra={"extra": {"env": settings.env, "run_id": run_id}})

    ingest_base_dos_dados(spark, settings, run_id)
    ingest_microdados(spark, settings, run_id)

    log.info("bronze concluída", extra={"extra": {"run_id": run_id}})
    print(f"Bronze OK ({settings.env}) run_id={run_id}")
    spark.stop()


if __name__ == "__main__":
    main()
