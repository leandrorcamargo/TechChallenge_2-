from __future__ import annotations

import argparse
import json
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
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
MANIFEST_FILE = "_manifest.json"

YEARLY_TABLES = [
    "municipio",
    "estado",
    "aluno",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
]

TARGET_PREFIX = "meta_alfabetizacao_"


def display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)


def discover_years(input_dir: Path) -> list[int]:
    years = []
    for path in input_dir.glob("ano=*"):
        if not path.is_dir():
            continue
        try:
            years.append(int(path.name.split("=", 1)[1]))
        except ValueError:
            continue
    return sorted(years)


def read_yearly_table(input_dir: Path, table_name: str, years: list[int]) -> tuple[pd.DataFrame, list[str]]:
    frames = []
    missing = []

    for year in years:
        path = input_dir / f"ano={year}" / f"{table_name}.parquet"
        if not path.exists():
            missing.append(display_path(path))
            continue

        df = pd.read_parquet(path)
        if "ano_safra" not in df.columns:
            df["ano_safra"] = year
        frames.append(df)

    if not frames:
        return pd.DataFrame(), missing
    return pd.concat(frames, ignore_index=True, sort=False), missing


def read_silver_tables(input_dir: Path, years: list[int]) -> tuple[dict[str, pd.DataFrame], list[str]]:
    tables = {}
    missing = []

    for table_name in YEARLY_TABLES:
        df, table_missing = read_yearly_table(input_dir, table_name, years)
        missing.extend(table_missing)
        if not df.empty:
            tables[table_name] = df

    return tables, missing


def write_parquet(df: pd.DataFrame, path: Path) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(".parquet.tmp")
    if temporary_path.exists():
        temporary_path.unlink()
    df.to_parquet(temporary_path, index=False)
    temporary_path.replace(path)
    return {
        "path": display_path(path),
        "rows": len(df),
        "columns": len(df.columns),
        "size_bytes": path.stat().st_size,
    }


def classify_gap_to_target(value: Any) -> str:
    if pd.isna(value):
        return "SEM_META"
    if value >= 0:
        return "ATINGIU_META"
    if value >= -5:
        return "ATE_5PP_ABAIXO"
    if value >= -10:
        return "ENTRE_5_E_10PP_ABAIXO"
    return "MAIS_DE_10PP_ABAIXO"


def dependency_to_network_id(value: Any) -> pd.NA | int:
    if pd.isna(value):
        return pd.NA
    try:
        number = int(value)
    except (TypeError, ValueError):
        return pd.NA
    return number if number in {1, 2, 3, 4} else pd.NA


def add_student_aggregates(municipio: pd.DataFrame, aluno: pd.DataFrame) -> pd.DataFrame:
    if aluno.empty:
        return municipio

    required = {"ano_avaliacao", "co_municipio", "tp_dependencia"}
    if not required.issubset(aluno.columns):
        return municipio

    students = aluno.copy()
    students["id_tipo_rede"] = students["tp_dependencia"].map(dependency_to_network_id).astype("Int64")

    direct_network = aggregate_students(
        students.dropna(subset=["id_tipo_rede"]),
        ["ano_avaliacao", "co_municipio", "id_tipo_rede"],
    )

    public_students = students[students["tp_dependencia"].isin([1, 2, 3])].copy()
    public_students["id_tipo_rede"] = 5
    public_network = aggregate_students(public_students, ["ano_avaliacao", "co_municipio", "id_tipo_rede"])

    student_features = pd.concat([direct_network, public_network], ignore_index=True, sort=False)
    keys = ["ano_avaliacao", "co_municipio", "id_tipo_rede"]
    return municipio.merge(student_features, on=keys, how="left")


def aggregate_students(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    aggregations = {"qt_alunos_microdados": ("id_aluno", "nunique")}

    if "in_presenca_lp" in df.columns:
        aggregations["qt_alunos_presentes_lp"] = ("in_presenca_lp", "sum")
    if "in_alfabetizado" in df.columns:
        aggregations["qt_alunos_alfabetizados_microdados"] = ("in_alfabetizado", "sum")
    if "vl_proficiencia_lp" in df.columns:
        aggregations["media_proficiencia_microdados"] = ("vl_proficiencia_lp", "mean")
    if "in_possui_valor_ausente" in df.columns:
        aggregations["qt_registros_com_ausencia_microdados"] = ("in_possui_valor_ausente", "sum")

    result = df.groupby(keys, dropna=False).agg(**aggregations).reset_index()

    if {"qt_alunos_presentes_lp", "qt_alunos_microdados"}.issubset(result.columns):
        result["taxa_presenca_lp_microdados"] = (
            result["qt_alunos_presentes_lp"] / result["qt_alunos_microdados"] * 100
        )
    if {"qt_alunos_alfabetizados_microdados", "qt_alunos_microdados"}.issubset(result.columns):
        result["taxa_alfabetizacao_microdados"] = (
            result["qt_alunos_alfabetizados_microdados"] / result["qt_alunos_microdados"] * 100
        )

    integer_columns = [
        "qt_alunos_microdados",
        "qt_alunos_presentes_lp",
        "qt_alunos_alfabetizados_microdados",
        "qt_registros_com_ausencia_microdados",
    ]
    for column in integer_columns:
        if column in result.columns:
            result[column] = result[column].astype("Int64")

    return result


def build_indicador_municipio(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    if "municipio" not in tables:
        raise FileNotFoundError("Tabela silver municipio nao encontrada.")

    base_columns = [
        "ano_avaliacao",
        "co_uf",
        "sg_uf",
        "chave_uf",
        "co_municipio",
        "chave_municipio",
        "no_municipio",
        "tp_serie",
        "id_tipo_rede",
        "rede_normalizada",
        "pc_aluno_alfabetizado",
        "vl_media_lp",
        "faixa_alfabetizacao",
        "in_media_lp_informada",
        "qt_campos_ausentes",
        "in_possui_valor_ausente",
    ]
    municipio = tables["municipio"][[column for column in base_columns if column in tables["municipio"].columns]].copy()
    municipio = municipio.drop_duplicates()
    municipio = add_student_aggregates(municipio, tables.get("aluno", pd.DataFrame()))

    if "pc_aluno_alfabetizado" in municipio.columns:
        municipio["ranking_uf_ano_rede"] = (
            municipio.groupby(["ano_avaliacao", "sg_uf", "rede_normalizada"])["pc_aluno_alfabetizado"]
            .rank(method="dense", ascending=False)
            .astype("Int64")
        )
        municipio["ranking_brasil_ano_rede"] = (
            municipio.groupby(["ano_avaliacao", "rede_normalizada"])["pc_aluno_alfabetizado"]
            .rank(method="dense", ascending=False)
            .astype("Int64")
        )

    return municipio.sort_values(
        ["ano_avaliacao", "sg_uf", "no_municipio", "rede_normalizada"],
        na_position="last",
    ).reset_index(drop=True)


def target_column_for_year(year: int) -> str:
    return f"{TARGET_PREFIX}{year}"


def latest_meta_for_results(meta: pd.DataFrame, result_years: list[int], keys: list[str]) -> pd.DataFrame:
    if meta.empty or "ano" not in meta.columns:
        return pd.DataFrame()

    rows = []
    for result_year in result_years:
        eligible = meta[meta["ano"].astype("Int64") <= result_year].copy()
        if eligible.empty:
            continue

        eligible = eligible.sort_values("ano").drop_duplicates(subset=keys, keep="last")
        target_column = target_column_for_year(result_year)
        if target_column in eligible.columns:
            eligible["meta_alfabetizacao_ano"] = eligible[target_column]
        elif "taxa_alfabetizacao" in eligible.columns:
            eligible["meta_alfabetizacao_ano"] = eligible["taxa_alfabetizacao"]
        else:
            eligible["meta_alfabetizacao_ano"] = pd.NA

        eligible["ano_avaliacao"] = result_year
        selected = ["ano_avaliacao", *keys, "ano", "meta_alfabetizacao_ano"]
        if "taxa_alfabetizacao" in eligible.columns:
            selected.append("taxa_alfabetizacao")
        if "percentual_participacao" in eligible.columns:
            selected.append("percentual_participacao")
        rows.append(eligible[selected])

    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True, sort=False)


def build_comparacao_metas(indicador: pd.DataFrame, tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    result = indicador.copy()
    result_years = sorted(result["ano_avaliacao"].dropna().astype(int).unique())

    if "meta_alfabetizacao_uf" in tables:
        meta_uf = latest_meta_for_results(tables["meta_alfabetizacao_uf"], result_years, ["sigla_uf", "rede"])
        if not meta_uf.empty:
            meta_uf = meta_uf.rename(
                columns={
                    "sigla_uf": "sg_uf",
                    "rede": "rede_normalizada",
                    "ano": "ano_referencia_meta_uf",
                    "meta_alfabetizacao_ano": "meta_uf_alfabetizacao",
                    "taxa_alfabetizacao": "taxa_alfabetizacao_referencia_uf",
                    "percentual_participacao": "participacao_referencia_uf",
                }
            )
            result = result.merge(meta_uf, on=["ano_avaliacao", "sg_uf", "rede_normalizada"], how="left")

    if "meta_alfabetizacao_brasil" in tables:
        meta_brasil = latest_meta_for_results(tables["meta_alfabetizacao_brasil"], result_years, ["rede"])
        if not meta_brasil.empty:
            meta_brasil = meta_brasil.rename(
                columns={
                    "rede": "rede_normalizada",
                    "ano": "ano_referencia_meta_brasil",
                    "meta_alfabetizacao_ano": "meta_brasil_alfabetizacao",
                    "taxa_alfabetizacao": "taxa_alfabetizacao_referencia_brasil",
                    "percentual_participacao": "participacao_referencia_brasil",
                }
            )
            result = result.merge(meta_brasil, on=["ano_avaliacao", "rede_normalizada"], how="left")

    if {"pc_aluno_alfabetizado", "meta_uf_alfabetizacao"}.issubset(result.columns):
        result["distancia_meta_uf_pp"] = result["pc_aluno_alfabetizado"] - result["meta_uf_alfabetizacao"]
        result["in_atingiu_meta_uf"] = (result["distancia_meta_uf_pp"] >= 0).astype("boolean")
        result["faixa_distancia_meta_uf"] = result["distancia_meta_uf_pp"].map(classify_gap_to_target).astype("string")

    if {"pc_aluno_alfabetizado", "meta_brasil_alfabetizacao"}.issubset(result.columns):
        result["distancia_meta_brasil_pp"] = result["pc_aluno_alfabetizado"] - result["meta_brasil_alfabetizacao"]
        result["in_atingiu_meta_brasil"] = (result["distancia_meta_brasil_pp"] >= 0).astype("boolean")
        result["faixa_distancia_meta_brasil"] = result["distancia_meta_brasil_pp"].map(classify_gap_to_target).astype("string")

    return result.reset_index(drop=True)


def build_evolucao_temporal(indicador: pd.DataFrame) -> pd.DataFrame:
    keys = ["co_municipio", "id_tipo_rede", "tp_serie"]
    sort_columns = [*keys, "ano_avaliacao"]
    result = indicador.sort_values(sort_columns).copy()

    group = result.groupby(keys, dropna=False)
    result["pc_aluno_alfabetizado_ano_anterior"] = group["pc_aluno_alfabetizado"].shift(1)
    result["vl_media_lp_ano_anterior"] = group["vl_media_lp"].shift(1)
    result["variacao_alfabetizacao_pp"] = (
        result["pc_aluno_alfabetizado"] - result["pc_aluno_alfabetizado_ano_anterior"]
    )
    result["variacao_media_lp"] = result["vl_media_lp"] - result["vl_media_lp_ano_anterior"]
    result["media_movel_alfabetizacao_3_anos"] = (
        group["pc_aluno_alfabetizado"]
        .rolling(window=3, min_periods=1)
        .mean()
        .reset_index(level=keys, drop=True)
    )
    result["tendencia_alfabetizacao"] = result["variacao_alfabetizacao_pp"].map(classify_trend).astype("string")

    return result.reset_index(drop=True)


def classify_trend(value: Any) -> str:
    if pd.isna(value):
        return "SEM_HISTORICO"
    if value >= 2:
        return "CRESCIMENTO"
    if value <= -2:
        return "QUEDA"
    return "ESTAVEL"


def build_dashboard_resumo(comparacao: pd.DataFrame) -> pd.DataFrame:
    keys = ["ano_avaliacao", "sg_uf", "rede_normalizada"]
    aggregations = {
        "qt_municipios": ("co_municipio", "nunique"),
        "media_alfabetizacao": ("pc_aluno_alfabetizado", "mean"),
        "mediana_alfabetizacao": ("pc_aluno_alfabetizado", "median"),
        "desvio_padrao_alfabetizacao": ("pc_aluno_alfabetizado", "std"),
        "menor_alfabetizacao": ("pc_aluno_alfabetizado", "min"),
        "maior_alfabetizacao": ("pc_aluno_alfabetizado", "max"),
        "media_lp": ("vl_media_lp", "mean"),
    }
    if "in_atingiu_meta_uf" in comparacao.columns:
        aggregations["qt_municipios_atingiram_meta_uf"] = ("in_atingiu_meta_uf", "sum")
    if "distancia_meta_uf_pp" in comparacao.columns:
        aggregations["media_distancia_meta_uf_pp"] = ("distancia_meta_uf_pp", "mean")
    if "qt_alunos_microdados" in comparacao.columns:
        aggregations["qt_alunos_microdados"] = ("qt_alunos_microdados", "sum")

    result = comparacao.groupby(keys, dropna=False).agg(**aggregations).reset_index()

    if {"qt_municipios_atingiram_meta_uf", "qt_municipios"}.issubset(result.columns):
        result["pc_municipios_atingiram_meta_uf"] = (
            result["qt_municipios_atingiram_meta_uf"] / result["qt_municipios"] * 100
        )

    integer_columns = ["qt_municipios", "qt_municipios_atingiram_meta_uf", "qt_alunos_microdados"]
    for column in integer_columns:
        if column in result.columns:
            result[column] = result[column].astype("Int64")

    return result.sort_values(keys).reset_index(drop=True)


def build_ml_features(comparacao: pd.DataFrame, evolucao: pd.DataFrame) -> pd.DataFrame:
    keys = ["ano_avaliacao", "co_municipio", "id_tipo_rede", "tp_serie"]
    evolution_columns = [
        *keys,
        "pc_aluno_alfabetizado_ano_anterior",
        "vl_media_lp_ano_anterior",
        "variacao_alfabetizacao_pp",
        "variacao_media_lp",
        "media_movel_alfabetizacao_3_anos",
        "tendencia_alfabetizacao",
    ]
    available_evolution_columns = [column for column in evolution_columns if column in evolucao.columns]

    result = comparacao.merge(evolucao[available_evolution_columns], on=keys, how="left")
    result = result.sort_values(["co_municipio", "id_tipo_rede", "tp_serie", "ano_avaliacao"])
    group = result.groupby(["co_municipio", "id_tipo_rede", "tp_serie"], dropna=False)
    result["target_pc_aluno_alfabetizado_proximo_ano"] = group["pc_aluno_alfabetizado"].shift(-1)
    result["target_variacao_alfabetizacao_proximo_ano_pp"] = (
        result["target_pc_aluno_alfabetizado_proximo_ano"] - result["pc_aluno_alfabetizado"]
    )

    preferred_columns = [
        "ano_avaliacao",
        "co_uf",
        "sg_uf",
        "co_municipio",
        "chave_municipio",
        "no_municipio",
        "tp_serie",
        "id_tipo_rede",
        "rede_normalizada",
        "pc_aluno_alfabetizado",
        "vl_media_lp",
        "faixa_alfabetizacao",
        "qt_alunos_microdados",
        "qt_alunos_presentes_lp",
        "taxa_presenca_lp_microdados",
        "media_proficiencia_microdados",
        "taxa_alfabetizacao_microdados",
        "pc_aluno_alfabetizado_ano_anterior",
        "vl_media_lp_ano_anterior",
        "variacao_alfabetizacao_pp",
        "variacao_media_lp",
        "media_movel_alfabetizacao_3_anos",
        "tendencia_alfabetizacao",
        "meta_uf_alfabetizacao",
        "distancia_meta_uf_pp",
        "in_atingiu_meta_uf",
        "meta_brasil_alfabetizacao",
        "distancia_meta_brasil_pp",
        "in_atingiu_meta_brasil",
        "ranking_uf_ano_rede",
        "ranking_brasil_ano_rede",
        "target_pc_aluno_alfabetizado_proximo_ano",
        "target_variacao_alfabetizacao_proximo_ano_pp",
    ]
    available_columns = [column for column in preferred_columns if column in result.columns]
    return result[available_columns].reset_index(drop=True)


def write_year_partitioned(df: pd.DataFrame, name: str, output_dir: Path) -> list[dict[str, Any]]:
    outputs = []
    if "ano_avaliacao" not in df.columns:
        path = output_dir / f"{name}.parquet"
        print(f"{Fore.CYAN}Gravando {display_path(path)}...")
        return [write_parquet(df, path)]

    for year in sorted(df["ano_avaliacao"].dropna().astype(int).unique()):
        year_df = df[df["ano_avaliacao"].astype("Int64") == year].reset_index(drop=True)
        path = output_dir / f"ano={year}" / f"{name}.parquet"
        print(f"{Fore.CYAN}Gravando {display_path(path)}...")
        outputs.append(write_parquet(year_df, path))

    path = output_dir / "consolidado" / f"{name}.parquet"
    print(f"{Fore.CYAN}Gravando {display_path(path)}...")
    outputs.append(write_parquet(df.reset_index(drop=True), path))
    return outputs


def build_gold_layer(
    input_dir: Path = SILVER_DIR,
    output_dir: Path = GOLD_DIR,
    years: list[int] | None = None,
) -> dict[str, Any]:
    selected_years = years or discover_years(input_dir)
    if not selected_years:
        raise FileNotFoundError(f"Nenhum diretorio ano=* encontrado em {display_path(input_dir)}")

    tables, missing_inputs = read_silver_tables(input_dir, selected_years)
    indicador = build_indicador_municipio(tables)
    comparacao = build_comparacao_metas(indicador, tables)
    evolucao = build_evolucao_temporal(indicador)
    dashboard_resumo = build_dashboard_resumo(comparacao)
    ml_features = build_ml_features(comparacao, evolucao)

    datasets = {
        "indicador_alfabetizacao_municipio": indicador,
        "comparacao_metas_resultados": comparacao,
        "evolucao_temporal_indicador": evolucao,
        "dashboard_resumo_uf": dashboard_resumo,
        "ml_features_municipio": ml_features,
    }

    outputs = []
    for name, df in datasets.items():
        outputs.extend(write_year_partitioned(df, name, output_dir))

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "silver_dir": display_path(input_dir),
        "gold_dir": display_path(output_dir),
        "format": "parquet",
        "years": selected_years,
        "missing_expected_inputs": missing_inputs,
        "datasets": {
            "indicador_alfabetizacao_municipio": (
                "Indicador municipal enriquecido com ranking, rede, UF e agregados dos microdados de alunos."
            ),
            "comparacao_metas_resultados": (
                "Comparacao entre resultado municipal e metas de alfabetizacao da UF e do Brasil por ano/rede."
            ),
            "evolucao_temporal_indicador": (
                "Serie temporal municipal com variacao anual, media movel e classificacao de tendencia."
            ),
            "dashboard_resumo_uf": (
                "Resumo agregado por ano, UF e rede para paineis e analises estatisticas."
            ),
            "ml_features_municipio": (
                "Base wide com features historicas, metas, rankings e target do proximo ano para modelagem."
            ),
        },
        "preparation_targets": [
            "dashboards",
            "analises estatisticas",
            "treinamento de modelos de machine learning",
        ],
        "outputs": outputs,
    }

    manifest_path = output_dir / MANIFEST_FILE
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gera a camada gold analitica em Parquet.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=SILVER_DIR,
        help=f"Pasta de entrada silver. Padrao: {display_path(SILVER_DIR)}",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=GOLD_DIR,
        help=f"Pasta de saida gold. Padrao: {display_path(GOLD_DIR)}",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=None,
        help="Anos/safras a processar. Padrao: descobrir diretorios ano=* na silver.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = build_gold_layer(input_dir=args.input_dir, output_dir=args.output_dir, years=args.years)

    print(f"\n{Fore.GREEN}Camada gold gerada com sucesso:{Style.RESET_ALL}")
    for output in manifest["outputs"]:
        print(f"{output['path']} ({output['rows']} linhas, {output['columns']} colunas)")
    print(f"Manifest: {display_path(args.output_dir / MANIFEST_FILE)}")


if __name__ == "__main__":
    main()
