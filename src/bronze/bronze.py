from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

try:
    from colorama import Fore, Style, init

    init(autoreset=True)
except ImportError:
    class _NoColor:
        CYAN = ""
        GREEN = ""
        RED = ""
        YELLOW = ""
        RESET_ALL = ""

    Fore = Style = _NoColor()


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
PARQUET_INPUT_DIR = BRONZE_DIR
MANIFEST_FILE = "_manifest.json"
DEFAULT_YEARS = [2023, 2024, 2025]
MICRODATA_TABLES = ["TS_ALUNO", "TS_ESTADO", "TS_ITEM", "TS_MUNICIPIO"]
COMMON_PARQUET_FILES = [
    "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.parquet",
    "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.parquet",
]


def display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def inspect_parquet(parquet_path: Path) -> dict[str, Any]:
    parquet_file = pq.ParquetFile(parquet_path)
    schema = parquet_file.schema_arrow
    return {
        "rows": parquet_file.metadata.num_rows,
        "columns": schema.names,
        "column_types": {
            field.name: str(field.type)
            for field in schema
        },
    }


def register_parquet_in_bronze(
    parquet_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    """
    Registra uma base Parquet na bronze sem padronizar colunas ou valores.

    Quando a origem e o destino sao diferentes, copia o Parquet preservando o
    conteudo original. Quando sao iguais, apenas inspeciona o arquivo existente.
    """
    if parquet_path.resolve() != output_path.resolve():
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_output_path = output_path.with_suffix(".parquet.tmp")

        if temporary_output_path.exists():
            temporary_output_path.unlink()
        shutil.copy2(parquet_path, temporary_output_path)
        temporary_output_path.replace(output_path)

    inspection = inspect_parquet(output_path)
    return {
        "source": display_path(parquet_path),
        "target": display_path(output_path),
        "source_size_bytes": parquet_path.stat().st_size,
        "target_size_bytes": output_path.stat().st_size,
        **inspection,
    }


def build_bronze_layer(
    input_dir: Path = PARQUET_INPUT_DIR,
    output_dir: Path = BRONZE_DIR,
    years: list[int] | None = None,
    skip_missing: bool = False,
) -> list[dict[str, Any]]:
    years = years or DEFAULT_YEARS

    manifest = []
    missing_files = []

    for year in years:
        for table in MICRODATA_TABLES:
            parquet_path = input_dir / f"ano={year}" / f"{table}.parquet"
            if not parquet_path.exists():
                missing_files.append(display_path(parquet_path))
                continue

            output_path = output_dir / f"ano={year}" / f"{table}.parquet"
            print(f"{Fore.CYAN}Registrando {display_path(parquet_path)} -> {display_path(output_path)}...")
            item = register_parquet_in_bronze(parquet_path, output_path)
            item["ano_safra"] = year
            item["table"] = table
            manifest.append(item)

    for filename in COMMON_PARQUET_FILES:
        parquet_path = input_dir / "common" / filename
        if not parquet_path.exists():
            missing_files.append(display_path(parquet_path))
            continue

        output_path = output_dir / "common" / filename
        print(f"{Fore.CYAN}Registrando {display_path(parquet_path)} -> {display_path(output_path)}...")
        item = register_parquet_in_bronze(parquet_path, output_path)
        item["scope"] = "common"
        manifest.append(item)

    if missing_files and not skip_missing:
        missing = "\n".join(f"- {path}" for path in missing_files)
        raise FileNotFoundError(f"Arquivos esperados nao encontrados:\n{missing}")
    if not manifest:
        raise FileNotFoundError("Nenhum Parquet bruto encontrado para gerar a bronze")

    manifest_path = output_dir / MANIFEST_FILE
    manifest_path.write_text(
        json.dumps(
            {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "input_dir": display_path(input_dir),
                "bronze_dir": display_path(output_dir),
                "format": "parquet",
                "years": years,
                "missing_files": missing_files,
                "preservation_rule": (
                    "A camada bronze registra bases Parquet brutas, mantendo "
                    "nomes de colunas, tipos e valores de origem sem padronizacao."
                ),
                "files": manifest,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera o manifesto da camada bronze a partir das bases Parquet brutas."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=PARQUET_INPUT_DIR,
        help=f"Pasta de entrada com bases Parquet. Padrao: {display_path(PARQUET_INPUT_DIR)}",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=DEFAULT_YEARS,
        help="Anos/safras a processar. Padrao: 2023 2024 2025",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=BRONZE_DIR,
        help=f"Pasta de saida da bronze. Padrao: {display_path(BRONZE_DIR)}",
    )
    parser.add_argument(
        "--skip-missing",
        action="store_true",
        help="Continua a execucao mesmo se algum Parquet esperado nao existir.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = build_bronze_layer(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        years=args.years,
        skip_missing=args.skip_missing,
    )

    print(f"\n{Fore.GREEN}Camada bronze gerada com sucesso:{Style.RESET_ALL}")
    for item in result:
        print(
            f"{item['source']} -> {item['target']} "
            f"({item['rows']} linhas, {len(item['columns'])} colunas)"
        )
    print(f"Manifest: {display_path(args.output_dir / MANIFEST_FILE)}")


if __name__ == "__main__":
    main()
