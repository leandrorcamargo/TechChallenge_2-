"""Preparacao da fonte (E1) — extrai data/ para source/ pronto para o Databricks.

Nao faz transformacao de dados: apenas descompacta/organiza os arquivos que ja
temos em data/ numa pasta source/ com nomes logicos, para depois subir ao
workspace do Databricks (a pasta que a camada Bronze le).

Saida (source/):
    indicador_alfabetizacao_uf.csv.gz
    indicador_alfabetizacao_municipio.csv.gz
    meta_alfabetizacao_brasil.csv.gz
    meta_alfabetizacao_uf.csv.gz
    meta_alfabetizacao_municipio.csv.gz
    ts_aluno/2023.csv, 2024.csv, 2025.csv
    ts_municipio/2023.csv, 2024.csv, 2025.csv
    ts_estado/2023.csv, 2024.csv, 2025.csv

Uso:
    python projeto/bronze/prep_source.py
"""

from __future__ import annotations

import re
import shutil
import zipfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DATA = REPO / "data"
SOURCE = REPO / "source"

# Base dos Dados (.csv.gz): arquivo original -> nome logico na source/
BASE_DOS_DADOS = {
    "br_inep_avaliacao_alfabetizacao_uf.csv.gz": "indicador_alfabetizacao_uf.csv.gz",
    "br_inep_avaliacao_alfabetizacao_municipio.csv.gz": "indicador_alfabetizacao_municipio.csv.gz",
    "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv.gz": "meta_alfabetizacao_brasil.csv.gz",
    "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv.gz": "meta_alfabetizacao_uf.csv.gz",
    "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv.gz": "meta_alfabetizacao_municipio.csv.gz",
}

# Microdados: tabelas extraidas de cada zip (TS_ITEM fica de fora).
MICRODADOS_ZIPS = [
    "microdados_avaliacao_da_alfabetizacao_2023.zip",
    "microdados_avaliacao_da_alfabetizacao_2024.zip",
    "microdados_AEEB_2025.zip",
]
TS_TABLES = {"TS_ALUNO": "ts_aluno", "TS_MUNICIPIO": "ts_municipio", "TS_ESTADO": "ts_estado"}
_YEAR = re.compile(r"(20\d{2})")


def preparar() -> None:
    SOURCE.mkdir(exist_ok=True)

    # 1) Base dos Dados: copia os .csv.gz com nomes logicos.
    for origem, destino in BASE_DOS_DADOS.items():
        shutil.copy2(DATA / origem, SOURCE / destino)
        print(f"copiado: {destino}")

    # 2) Microdados: extrai TS_ALUNO/TS_MUNICIPIO/TS_ESTADO por ano.
    for zip_name in MICRODADOS_ZIPS:
        year = _YEAR.search(zip_name).group(1)
        with zipfile.ZipFile(DATA / zip_name) as zf:
            for member in zf.namelist():
                stem = Path(member).stem.upper()
                if member.startswith("DADOS/") and stem in TS_TABLES:
                    dest_dir = SOURCE / TS_TABLES[stem]
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    target = dest_dir / f"{year}.csv"
                    with zf.open(member) as fin, open(target, "wb") as fout:
                        shutil.copyfileobj(fin, fout, length=1024 * 1024)
                    print(f"extraido: {TS_TABLES[stem]}/{year}.csv")

    print(f"\nOK -> {SOURCE}")


if __name__ == "__main__":
    preparar()
