"""Landing — popula a zona ``raw/`` a partir de ``data/``.

Etapa anterior à Bronze: coloca os arquivos-fonte na zona ``raw`` do data lake,
**sem alterar conteúdo**. É aqui que os ``TS_*.csv`` são extraídos dos zips de
microdados para ``raw/microdados/<ano>/`` — o que também é o local observado pelo
gatilho de streaming (``s3:ObjectCreated`` em ``raw/microdados/``).

Uso:
    python projeto/bronze/landing.py            # popula lake/raw (APP_ENV=local)
    APP_ENV=aws python projeto/bronze/landing.py # envia para s3://<bucket>/raw/

Não depende de PySpark (apenas biblioteca padrão + boto3 no modo AWS).
"""

from __future__ import annotations

import os
import re
import shutil
import sys
import zipfile
from pathlib import Path

# Torna o pacote ``common`` importável ao rodar como script standalone.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.config import load_settings, Settings  # noqa: E402
from common.io import DATA_DIR, layer_path  # noqa: E402
from common.logging_setup import get_logger  # noqa: E402

log = get_logger("bronze.landing")

_YEAR_RE = re.compile(r"(20\d{2})")


def _year_from_name(name: str) -> str:
    """Extrai o ano (AAAA) do nome de um arquivo/zip de microdados."""
    m = _YEAR_RE.search(name)
    if not m:
        raise ValueError(f"Não foi possível inferir o ano do arquivo: {name}")
    return m.group(1)


def run_landing(settings: Settings) -> dict[str, int]:
    """Popula a zona raw. Retorna um resumo (nº de objetos por grupo)."""
    if settings.is_aws:
        return _landing_aws(settings)
    return _landing_local(settings)


# --------------------------------------------------------------------------
# Modo local: escreve em lake/raw/
# --------------------------------------------------------------------------
def _landing_local(settings: Settings) -> dict[str, int]:
    raw_base = Path(layer_path(settings, "raw"))
    counts = {"base_dos_dados": 0, "microdados": 0}

    # 1) Base dos Dados: copia os .csv.gz como estão.
    bdd_dir = raw_base / "base_dos_dados"
    bdd_dir.mkdir(parents=True, exist_ok=True)
    for _, filename in settings.get("sources.base_dos_dados", {}).items():
        src = DATA_DIR / filename
        shutil.copy2(src, bdd_dir / filename)
        counts["base_dos_dados"] += 1
        log.info("copiado", extra={"extra": {"arquivo": filename}})

    # 2) Microdados: extrai TS_* de cada zip para microdados/<ano>/.
    tables = set(settings.get("sources.microdados.bronze_tables", []))
    for zip_name in settings.get("sources.microdados.zips", []):
        year = _year_from_name(zip_name)
        dest = raw_base / "microdados" / year
        dest.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(DATA_DIR / zip_name) as zf:
            for member in zf.namelist():
                stem = Path(member).stem
                if member.startswith("DADOS/") and stem in tables:
                    target = dest / f"{stem}.csv"
                    # Extração em streaming (não carrega o CSV inteiro em memória).
                    with zf.open(member) as fin, open(target, "wb") as fout:
                        shutil.copyfileobj(fin, fout, length=1024 * 1024)
                    counts["microdados"] += 1
                    log.info("extraído", extra={"extra": {"ano": year, "tabela": stem}})
    return counts


# --------------------------------------------------------------------------
# Modo AWS: envia para s3://<bucket>/raw/
# --------------------------------------------------------------------------
def _landing_aws(settings: Settings) -> dict[str, int]:
    import boto3  # import tardio: só necessário no modo AWS

    s3 = boto3.client("s3", region_name=settings.region)
    bucket = settings.get("lake.s3_bucket")
    raw_layer = settings.get("lake.layers.raw", "raw")
    counts = {"base_dos_dados": 0, "microdados": 0}

    for _, filename in settings.get("sources.base_dos_dados", {}).items():
        key = f"{raw_layer}/base_dos_dados/{filename}"
        s3.upload_file(str(DATA_DIR / filename), bucket, key)
        counts["base_dos_dados"] += 1
        log.info("upload", extra={"extra": {"key": key}})

    tables = set(settings.get("sources.microdados.bronze_tables", []))
    for zip_name in settings.get("sources.microdados.zips", []):
        year = _year_from_name(zip_name)
        with zipfile.ZipFile(DATA_DIR / zip_name) as zf:
            for member in zf.namelist():
                stem = Path(member).stem
                if member.startswith("DADOS/") and stem in tables:
                    key = f"{raw_layer}/microdados/{year}/{stem}.csv"
                    with zf.open(member) as fin:
                        s3.upload_fileobj(fin, bucket, key)
                    counts["microdados"] += 1
                    log.info("upload", extra={"extra": {"key": key}})
    return counts


def main() -> None:
    settings = load_settings()
    log.info("landing iniciado", extra={"extra": {"env": settings.env}})
    counts = run_landing(settings)
    log.info("landing concluído", extra={"extra": counts})
    print(f"Landing OK ({settings.env}): {counts}")


if __name__ == "__main__":
    main()
