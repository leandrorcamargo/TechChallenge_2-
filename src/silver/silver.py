from __future__ import annotations

import argparse
import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

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
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
MANIFEST_FILE = "_manifest.json"

YEARLY_INPUTS = {
    "TS_ALUNO": "TS_ALUNO.parquet",
    "TS_ESTADO": "TS_ESTADO.parquet",
    "TS_ITEM": "TS_ITEM.parquet",
    "TS_MUNICIPIO": "TS_MUNICIPIO.parquet",
}

COMMON_INPUTS = {
    "META_BRASIL": "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.parquet",
    "META_UF": "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.parquet",
}

PERCENTAGE_COLUMNS = {
    "pc_aluno_alfabetizado",
    "pc_aluno_nivel_0_lp",
    "pc_aluno_nivel_1_lp",
    "pc_aluno_nivel_2_lp",
    "pc_aluno_nivel_3_lp",
    "pc_aluno_nivel_4_lp",
    "pc_aluno_nivel_5_lp",
    "pc_aluno_nivel_6_lp",
    "pc_aluno_nivel_7_lp",
    "pc_aluno_nivel_8_lp",
    "taxa_alfabetizacao",
    "percentual_participacao",
    "meta_alfabetizacao_2024",
    "meta_alfabetizacao_2025",
    "meta_alfabetizacao_2026",
    "meta_alfabetizacao_2027",
    "meta_alfabetizacao_2028",
    "meta_alfabetizacao_2029",
    "meta_alfabetizacao_2030",
}

INTEGER_COLUMNS = {
    "ano",
    "ano_safra",
    "ano_avaliacao",
    "co_uf",
    "id_aluno",
    "tp_serie",
    "id_escola",
    "tp_dependencia",
    "co_municipio",
    "in_presenca_lp",
    "in_preenchimento_lp",
    "co_caderno_lp",
    "co_bloco",
    "co_bloco_1",
    "co_bloco_2",
    "co_bloco_3",
    "co_bloco_4",
    "nu_posicao",
    "co_item",
    "tp_disciplina",
    "tp_resposta_item",
    "tp_modelo_tri",
    "in_item_comum",
    "id_tipo_rede",
}

DEPENDENCIA_ADMINISTRATIVA = {
    1: "FEDERAL",
    2: "ESTADUAL",
    3: "MUNICIPAL",
    4: "PRIVADA",
}

TIPO_REDE = {
    1: "FEDERAL",
    2: "ESTADUAL",
    3: "MUNICIPAL",
    4: "PRIVADA",
    5: "PUBLICA",
}


def display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def normalize_column_name(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", name)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    snake = re.sub(r"[^0-9a-zA-Z]+", "_", ascii_name).strip("_").lower()
    if snake == "nu_ano_avaliacao":
        return "ano_avaliacao"
    return snake


def clean_text(value: Any) -> Any:
    if pd.isna(value):
        return pd.NA
    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)
    return text if text else pd.NA


def normalize_key_text(value: Any) -> Any:
    if pd.isna(value):
        return pd.NA
    normalized = unicodedata.normalize("NFKD", str(value))
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    key = re.sub(r"[^0-9A-Za-z]+", "_", ascii_value).strip("_").upper()
    return key if key else pd.NA


def normalize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.select_dtypes(include=["object", "string"]).columns:
        df[column] = df[column].map(clean_text).astype("string")
    return df


def normalize_types(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        if column in INTEGER_COLUMNS:
            df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
        elif column in PERCENTAGE_COLUMNS or column.startswith("vl_") or column.startswith("nu_param_"):
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def normalize_key_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "sg_uf" in df.columns:
        df["sg_uf"] = df["sg_uf"].str.upper()
    if "sigla_uf" in df.columns:
        df["sigla_uf"] = df["sigla_uf"].str.upper()
    if "rede" in df.columns:
        df["rede"] = df["rede"].map(normalize_key_text).astype("string")
    return df


def add_missing_indicators(df: pd.DataFrame) -> pd.DataFrame:
    business_columns = list(df.columns)
    df["qt_campos_ausentes"] = df[business_columns].isna().sum(axis=1).astype("Int64")
    df["in_possui_valor_ausente"] = (df["qt_campos_ausentes"] > 0).astype("boolean")
    return df


def fill_categorical_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    text_columns = df.select_dtypes(include=["object", "string"]).columns
    for column in text_columns:
        if column.startswith("tx_resposta_bloco_") or column.startswith("tx_gabarito_bloco_"):
            df[column] = df[column].fillna("NAO_APLICADO")
        else:
            df[column] = df[column].fillna("NAO_INFORMADO")
    return df


def format_key(value: Any, width: int | None = None) -> Any:
    if pd.isna(value):
        return pd.NA
    try:
        number = int(value)
    except (TypeError, ValueError):
        return pd.NA
    if width:
        return f"{number:0{width}d}"
    return str(number)


def add_normalized_keys(df: pd.DataFrame) -> pd.DataFrame:
    if "co_uf" in df.columns:
        df["chave_uf"] = df["co_uf"].map(lambda value: format_key(value, 2)).astype("string")
    elif "sigla_uf" in df.columns:
        df["chave_uf"] = df["sigla_uf"].astype("string")

    if "co_municipio" in df.columns:
        df["chave_municipio"] = df["co_municipio"].map(lambda value: format_key(value, 7)).astype("string")
    if "id_escola" in df.columns:
        df["chave_escola"] = df["id_escola"].map(format_key).astype("string")
    if "id_aluno" in df.columns:
        df["chave_aluno"] = df["id_aluno"].map(format_key).astype("string")
    if "id_tipo_rede" in df.columns:
        df["rede_normalizada"] = df["id_tipo_rede"].map(TIPO_REDE).fillna("NAO_INFORMADO").astype("string")
    if "rede" in df.columns:
        df["chave_rede"] = df["rede"].map(normalize_key_text).astype("string")
    return df


def add_aluno_features(df: pd.DataFrame) -> pd.DataFrame:
    if "in_alfabetizado" in df.columns:
        df["resultado_alfabetizacao"] = df["in_alfabetizado"].map({1: "ALFABETIZADO", 0: "NAO_ALFABETIZADO"}).fillna("NAO_INFORMADO").astype("string")
    if "in_presenca_lp" in df.columns:
        df["status_presenca_lp"] = df["in_presenca_lp"].map({1: "PRESENTE", 0: "AUSENTE"}).fillna("NAO_INFORMADO").astype("string")
    if "vl_proficiencia_lp" in df.columns:
        df["in_proficiencia_informada"] = df["vl_proficiencia_lp"].notna().astype("boolean")
    if "id_escola" in df.columns:
        df["in_escola_informada"] = df["id_escola"].notna().astype("boolean")
    if "co_municipio" in df.columns:
        df["in_municipio_informado"] = df["co_municipio"].notna().astype("boolean")
    response_columns = [column for column in df.columns if column.startswith("tx_resposta_bloco_")]
    if response_columns:
        df["qt_blocos_respondidos"] = df[response_columns].ne("NAO_APLICADO").sum(axis=1).astype("Int64")
    if "tp_dependencia" in df.columns:
        df["dependencia_administrativa"] = df["tp_dependencia"].map(DEPENDENCIA_ADMINISTRATIVA).fillna("NAO_INFORMADO").astype("string")
    return df


def classify_percentage(value: Any) -> str:
    if pd.isna(value):
        return "NAO_INFORMADO"
    if value < 50:
        return "CRITICO"
    if value < 70:
        return "ATENCAO"
    if value < 90:
        return "ADEQUADO"
    return "ALTO"


def add_indicator_features(df: pd.DataFrame) -> pd.DataFrame:
    if "pc_aluno_alfabetizado" in df.columns:
        df["faixa_alfabetizacao"] = df["pc_aluno_alfabetizado"].map(classify_percentage).astype("string")
    if "vl_media_lp" in df.columns:
        df["in_media_lp_informada"] = df["vl_media_lp"].notna().astype("boolean")
    return df


def add_item_features(df: pd.DataFrame) -> pd.DataFrame:
    key_columns = ["ano_avaliacao", "co_uf", "co_item", "co_bloco", "nu_posicao"]
    if set(key_columns).issubset(df.columns):
        df["chave_item"] = (
            df["ano_avaliacao"].map(format_key).astype("string")
            + "-"
            + df["co_uf"].map(lambda value: format_key(value, 2)).astype("string")
            + "-"
            + df["co_item"].map(format_key).astype("string")
            + "-"
            + df["co_bloco"].map(format_key).astype("string")
            + "-"
            + df["nu_posicao"].map(format_key).astype("string")
        )
    if "in_item_comum" in df.columns:
        df["in_item_comum_bool"] = df["in_item_comum"].map({1: True, 0: False}).astype("boolean")
    return df


def add_meta_features(df: pd.DataFrame) -> pd.DataFrame:
    meta_columns = [column for column in df.columns if column.startswith("meta_alfabetizacao_")]
    if meta_columns:
        df["qt_metas_ausentes"] = df[meta_columns].isna().sum(axis=1).astype("Int64")
        df["in_meta_2030_informada"] = df.get("meta_alfabetizacao_2030", pd.Series(pd.NA, index=df.index)).notna().astype("boolean")
    return df


def enrich_dataframe(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    df = add_missing_indicators(df)
    df = fill_categorical_missing_values(df)
    df = add_normalized_keys(df)

    if table_name == "TS_ALUNO":
        df = add_aluno_features(df)
    elif table_name in {"TS_ESTADO", "TS_MUNICIPIO"}:
        df = add_indicator_features(df)
    elif table_name == "TS_ITEM":
        df = add_item_features(df)
    elif table_name in {"META_BRASIL", "META_UF"}:
        df = add_meta_features(df)
    return df


def standardize_dataframe(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    df = df.copy()
    df.columns = [normalize_column_name(column) for column in df.columns]
    df = normalize_text_columns(df)
    df = normalize_types(df)
    df = normalize_key_columns(df)
    df = enrich_dataframe(df, table_name)
    return df.drop_duplicates().reset_index(drop=True)


def discover_years(input_dir: Path) -> list[int]:
    years = []
    for path in input_dir.glob("ano=*"):
        if path.is_dir():
            try:
                years.append(int(path.name.split("=", 1)[1]))
            except ValueError:
                continue
    return sorted(years)


def read_bronze_tables(
    input_dir: Path,
    years: list[int] | None = None,
) -> tuple[dict[str, dict[int, pd.DataFrame]], dict[str, pd.DataFrame], list[str]]:
    yearly_tables: dict[str, dict[int, pd.DataFrame]] = {}
    common_tables: dict[str, pd.DataFrame] = {}
    missing: list[str] = []
    years = years or discover_years(input_dir)

    for logical_name, filename in YEARLY_INPUTS.items():
        year_frames: dict[int, pd.DataFrame] = {}
        for year in years:
            path = input_dir / f"ano={year}" / filename
            if not path.exists():
                missing.append(display_path(path))
                continue
            df = standardize_dataframe(pd.read_parquet(path), logical_name)
            year_frames[year] = df

        if year_frames:
            yearly_tables[logical_name] = year_frames

    for logical_name, filename in COMMON_INPUTS.items():
        path = input_dir / "common" / filename
        if not path.exists():
            missing.append(display_path(path))
            continue
        common_tables[logical_name] = standardize_dataframe(pd.read_parquet(path), logical_name)

    return yearly_tables, common_tables, missing


def validate_required_columns(df: pd.DataFrame, table_name: str, columns: list[str]) -> list[dict[str, Any]]:
    findings = []
    for column in columns:
        if column not in df.columns:
            findings.append({"table": table_name, "severity": "error", "rule": "required_column", "column": column})
    return findings


def validate_not_null(df: pd.DataFrame, table_name: str, columns: list[str]) -> list[dict[str, Any]]:
    findings = []
    for column in columns:
        if column not in df.columns:
            continue
        nulls = int(df[column].isna().sum())
        if nulls:
            findings.append(
                {"table": table_name, "severity": "warning", "rule": "not_null", "column": column, "invalid_rows": nulls}
            )
    return findings


def validate_percentage_ranges(df: pd.DataFrame, table_name: str) -> list[dict[str, Any]]:
    findings = []
    for column in sorted(set(df.columns) & PERCENTAGE_COLUMNS):
        invalid = int(((df[column] < 0) | (df[column] > 100)).fillna(False).sum())
        if invalid:
            findings.append(
                {"table": table_name, "severity": "error", "rule": "percentage_0_100", "column": column, "invalid_rows": invalid}
            )
    return findings


def validate_unique_key(df: pd.DataFrame, table_name: str, key: list[str]) -> list[dict[str, Any]]:
    if not set(key).issubset(df.columns):
        return []
    duplicated = int(df.duplicated(subset=key).sum())
    if duplicated:
        return [{"table": table_name, "severity": "error", "rule": "unique_key", "columns": key, "invalid_rows": duplicated}]
    return []


def validate_referential_integrity(
    child: pd.DataFrame,
    parent: pd.DataFrame,
    child_name: str,
    parent_name: str,
    keys: list[str],
) -> list[dict[str, Any]]:
    if not set(keys).issubset(child.columns) or not set(keys).issubset(parent.columns):
        return []
    child_keys = child[keys].drop_duplicates()
    parent_keys = parent[keys].drop_duplicates()
    missing = child_keys.merge(parent_keys, on=keys, how="left", indicator=True)
    invalid = int((missing["_merge"] == "left_only").sum())
    if invalid:
        return [
            {
                "table": child_name,
                "severity": "warning",
                "rule": "referential_integrity",
                "parent_table": parent_name,
                "columns": keys,
                "invalid_keys": invalid,
            }
        ]
    return []


def build_quality_report(tables: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []

    for name, df in tables.items():
        findings.extend(validate_percentage_ranges(df, name))

    if "TS_ALUNO" in tables:
        findings.extend(validate_required_columns(tables["TS_ALUNO"], "TS_ALUNO", ["ano_avaliacao", "id_aluno", "co_municipio", "co_uf"]))
        findings.extend(validate_not_null(tables["TS_ALUNO"], "TS_ALUNO", ["ano_avaliacao", "id_aluno", "co_uf"]))
        findings.extend(validate_unique_key(tables["TS_ALUNO"], "TS_ALUNO", ["ano_avaliacao", "id_aluno"]))
    if "TS_MUNICIPIO" in tables:
        findings.extend(validate_unique_key(tables["TS_MUNICIPIO"], "TS_MUNICIPIO", ["ano_avaliacao", "co_municipio", "tp_serie", "id_tipo_rede"]))
    if "TS_ESTADO" in tables:
        findings.extend(validate_unique_key(tables["TS_ESTADO"], "TS_ESTADO", ["ano_avaliacao", "co_uf", "tp_serie", "id_tipo_rede"]))
    if "TS_ITEM" in tables:
        findings.extend(validate_unique_key(tables["TS_ITEM"], "TS_ITEM", ["ano_avaliacao", "co_uf", "co_bloco", "nu_posicao", "co_item"]))
    if "META_UF" in tables:
        findings.extend(validate_unique_key(tables["META_UF"], "META_UF", ["ano", "sigla_uf", "rede"]))
    if "META_BRASIL" in tables:
        findings.extend(validate_unique_key(tables["META_BRASIL"], "META_BRASIL", ["ano", "rede"]))

    if "TS_ALUNO" in tables and "TS_MUNICIPIO" in tables:
        findings.extend(
            validate_referential_integrity(
                tables["TS_ALUNO"],
                tables["TS_MUNICIPIO"],
                "TS_ALUNO",
                "TS_MUNICIPIO",
                ["ano_avaliacao", "co_municipio"],
            )
        )
    if "TS_MUNICIPIO" in tables and "TS_ESTADO" in tables:
        findings.extend(
            validate_referential_integrity(
                tables["TS_MUNICIPIO"],
                tables["TS_ESTADO"],
                "TS_MUNICIPIO",
                "TS_ESTADO",
                ["ano_avaliacao", "co_uf"],
            )
        )

    return findings


def combine_tables_for_quality(
    yearly_tables: dict[str, dict[int, pd.DataFrame]],
    common_tables: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {}
    for logical_name, year_frames in yearly_tables.items():
        tables[logical_name] = pd.concat(year_frames.values(), ignore_index=True, sort=False)
    tables.update(common_tables)
    return tables


def table_output_name(logical_name: str) -> str:
    return {
        "TS_ALUNO": "aluno",
        "TS_ESTADO": "estado",
        "TS_ITEM": "item",
        "TS_MUNICIPIO": "municipio",
        "META_BRASIL": "meta_alfabetizacao_brasil",
        "META_UF": "meta_alfabetizacao_uf",
    }.get(logical_name, logical_name.lower())


def write_parquet(df: pd.DataFrame, path: Path) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(".parquet.tmp")
    if temporary_path.exists():
        temporary_path.unlink()
    df.to_parquet(temporary_path, index=False)
    temporary_path.replace(path)
    return {"path": display_path(path), "rows": len(df), "columns": len(df.columns), "size_bytes": path.stat().st_size}


def write_quality_findings(findings: list[dict[str, Any]], output_dir: Path) -> dict[str, Any]:
    quality_path = output_dir / "quality_findings.parquet"
    quality_df = pd.DataFrame(findings)
    if quality_df.empty:
        quality_df = pd.DataFrame(
            columns=[
                "table",
                "severity",
                "rule",
                "column",
                "columns",
                "parent_table",
                "invalid_rows",
                "invalid_keys",
            ]
        )
    return write_parquet(quality_df, quality_path)


def table_year_column(df: pd.DataFrame) -> str | None:
    if "ano_safra" in df.columns:
        return "ano_safra"
    if "ano_avaliacao" in df.columns:
        return "ano_avaliacao"
    if "ano" in df.columns:
        return "ano"
    return None


def write_yearly_outputs(outputs_by_name: dict[str, pd.DataFrame], output_dir: Path) -> list[dict[str, Any]]:
    outputs = []
    for output_name, df in outputs_by_name.items():
        year_column = table_year_column(df)
        if year_column is None:
            continue

        for year in sorted(df[year_column].dropna().astype(int).unique()):
            year_df = df[df[year_column].astype("Int64") == year].reset_index(drop=True)
            if year_df.empty:
                continue
            path = output_dir / f"ano={year}" / f"{output_name}.parquet"
            print(f"{Fore.CYAN}Gravando {display_path(path)}...")
            outputs.append(write_parquet(year_df, path))
    return outputs


def write_yearly_table_outputs(
    yearly_tables: dict[str, dict[int, pd.DataFrame]],
    output_dir: Path,
) -> list[dict[str, Any]]:
    outputs = []
    for logical_name, year_frames in yearly_tables.items():
        output_name = table_output_name(logical_name)
        for year, df in sorted(year_frames.items()):
            path = output_dir / f"ano={year}" / f"{output_name}.parquet"
            print(f"{Fore.CYAN}Gravando {display_path(path)}...")
            outputs.append(write_parquet(df, path))
    return outputs


def build_silver_layer(
    input_dir: Path = BRONZE_DIR,
    output_dir: Path = SILVER_DIR,
    years: list[int] | None = None,
) -> dict[str, Any]:
    yearly_tables, common_tables, missing_inputs = read_bronze_tables(input_dir, years=years)
    if not yearly_tables and not common_tables:
        raise FileNotFoundError(f"Nenhum Parquet bronze encontrado em {input_dir}")

    tables = combine_tables_for_quality(yearly_tables, common_tables)
    quality_findings = build_quality_report(tables)

    outputs = write_yearly_table_outputs(yearly_tables, output_dir)
    common_outputs = {table_output_name(logical_name): df for logical_name, df in common_tables.items()}
    outputs.extend(write_yearly_outputs(common_outputs, output_dir))
    quality_output = write_quality_findings(quality_findings, output_dir)

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "bronze_dir": display_path(input_dir),
        "silver_dir": display_path(output_dir),
        "format": "parquet",
        "years": years or discover_years(input_dir),
        "missing_expected_inputs": missing_inputs,
        "transformations": [
            "column names converted to snake_case",
            "text fields trimmed and empty strings converted to null",
            "categorical nulls filled with controlled values such as NAO_INFORMADO and NAO_APLICADO",
            "row-level missing-value indicators added",
            "relationship keys normalized into derived chave_* columns",
            "percentage and metric columns converted to numeric types",
            "domain attributes derived for literacy result, attendance, network, item and target metadata",
            "duplicate rows removed",
            "consistency validations registered in the manifest and exported as a silver quality dataset",
        ],
        "quality_findings": quality_findings,
        "quality_output": quality_output,
        "outputs": outputs,
    }

    manifest_path = output_dir / MANIFEST_FILE
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gera a camada silver tratada em Parquet.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=BRONZE_DIR,
        help=f"Pasta de entrada bronze. Padrao: {display_path(BRONZE_DIR)}",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=SILVER_DIR,
        help=f"Pasta de saida silver. Padrao: {display_path(SILVER_DIR)}",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=None,
        help="Anos/safras a processar. Padrao: descobrir diretorios ano=* na bronze.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = build_silver_layer(input_dir=args.input_dir, output_dir=args.output_dir, years=args.years)

    print(f"\n{Fore.GREEN}Camada silver gerada com sucesso:{Style.RESET_ALL}")
    for output in manifest["outputs"]:
        print(f"{output['path']} ({output['rows']} linhas, {output['columns']} colunas)")

    errors = [finding for finding in manifest["quality_findings"] if finding["severity"] == "error"]
    warnings = [finding for finding in manifest["quality_findings"] if finding["severity"] == "warning"]
    print(f"Quality: {len(errors)} erros, {len(warnings)} avisos")
    print(f"Manifest: {display_path(args.output_dir / MANIFEST_FILE)}")


if __name__ == "__main__":
    main()
