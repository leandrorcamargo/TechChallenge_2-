"""Abstração de I/O das camadas do data lake (local ``lake/`` ou ``s3://``).

Centraliza a resolução de caminhos por camada e a leitura dos arquivos-fonte
(``.csv.gz`` da Base dos Dados e ``TS_*.csv`` dentro dos zips de microdados),
para que os jobs bronze/silver/gold não repitam essa lógica.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

from .config import REPO_DIR, Settings

if TYPE_CHECKING:  # evita importar pyspark quando só se resolve caminhos
    from pyspark.sql import DataFrame, SparkSession

DATA_DIR = REPO_DIR / "data"


def layer_path(settings: Settings, layer: str, dataset: str = "") -> str:
    """Resolve o caminho base de uma camada (raw/bronze/silver/gold).

    Em ``local`` retorna um caminho de sistema de arquivos sob ``lake/``;
    em ``aws`` retorna uma URI ``s3://<bucket>/<layer>/<dataset>``.
    """
    layer_name = settings.get(f"lake.layers.{layer}", layer)
    if settings.is_aws:
        bucket = settings.get("lake.s3_bucket")
        base = f"s3://{bucket}/{layer_name}"
    else:
        local_base = settings.get("lake.local_base", "../lake")
        base = str((REPO_DIR / "lake" / layer_name).resolve()) \
            if local_base in ("../lake", "lake") else f"{local_base}/{layer_name}"
    return f"{base}/{dataset}" if dataset else base


def read_source_csv(
    spark: "SparkSession",
    settings: Settings,
    filename: str,
    sep: str = ",",
    encoding: str = "utf-8",
) -> "DataFrame":
    """Lê um ``.csv.gz`` de ``data/`` como DataFrame Spark (schema inferido)."""
    path = str(DATA_DIR / filename)
    return (
        spark.read.option("header", True)
        .option("sep", sep)
        .option("encoding", encoding)
        .option("inferSchema", True)
        .csv(path)
    )


def extract_ts_table(zip_name: str, table: str) -> bytes:
    """Extrai o conteúdo bruto de ``DADOS/<table>.csv`` de um zip de microdados.

    Retorna os bytes do CSV; o job bronze decide como materializá-lo (ex.: gravar
    em staging e ler com Spark). Mantido aqui para centralizar o acesso ao zip.
    """
    zip_path = DATA_DIR / zip_name
    member = f"DADOS/{table}.csv"
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open(member) as fh:
            return fh.read()


def list_ts_members(zip_name: str) -> list[str]:
    """Lista as tabelas ``TS_*`` disponíveis dentro de um zip de microdados."""
    zip_path = DATA_DIR / zip_name
    with zipfile.ZipFile(zip_path) as zf:
        return [
            Path(n).stem
            for n in zf.namelist()
            if n.startswith("DADOS/") and n.upper().endswith(".CSV")
        ]


# Evita warning de import não usado; ``io`` é reexportado para conveniência.
_ = io
