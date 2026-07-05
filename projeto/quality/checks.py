"""Checks reutilizáveis de qualidade de dados (PySpark).

Cada função recebe um ou mais DataFrames e retorna um ``CheckResult`` padronizado.
As regras que decidem QUAIS checks rodam em cada tabela ficam em ``rules.py``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from functools import reduce
from typing import Any

from pyspark.sql import DataFrame, functions as F

# Severidades
ERROR = "ERROR"  # bloqueia a pipeline (gate)
WARN = "WARN"    # registra no relatório, não bloqueia
INFO = "INFO"    # informativo (métrica)


@dataclass
class CheckResult:
    name: str
    table: str
    category: str
    severity: str
    passed: bool
    observed: Any = None
    threshold: Any = None
    message: str = ""
    details: dict = field(default_factory=dict)

    @property
    def blocking(self) -> bool:
        """True quando o check falhou e sua severidade é ERROR."""
        return (not self.passed) and self.severity == ERROR

    def to_dict(self) -> dict:
        d = asdict(self)
        d["blocking"] = self.blocking
        return d


def _missing(colname: str):
    """Condição 'valor ausente': nulo ou string vazia após trim."""
    c = F.col(colname).cast("string")
    return F.col(colname).isNull() | (F.trim(c) == F.lit(""))


# --------------------------------------------------------------------------
# 1) Nulos
# --------------------------------------------------------------------------
def check_not_null_keys(df: DataFrame, table: str, keys: list[str],
                        severity: str = ERROR) -> CheckResult:
    cond = reduce(lambda a, k: a | _missing(k), keys, F.lit(False))
    bad = df.filter(cond).count()
    return CheckResult(f"chaves_nao_nulas[{'+'.join(keys)}]", table, "nulos", severity,
                       bad == 0, bad, 0, f"{bad} linhas com alguma chave ausente")


def check_null_ratio(df: DataFrame, table: str, column: str, threshold: float,
                     severity: str = WARN, condition=None, suffix: str = "") -> CheckResult:
    """% de nulos em ``column``. Com ``condition``, mede só no subconjunto filtrado.

    Ex.: em ts_aluno, o nulo de proficiência só é relevante entre os alunos
    PRESENTES (ausentes legitimamente não têm nota).
    """
    base = df.filter(condition) if condition is not None else df
    total = base.count()
    nulls = base.filter(_missing(column)).count()
    ratio = (nulls / total) if total else 0.0
    return CheckResult(f"nulo_ratio[{column}{suffix}]", table, "nulos", severity,
                       ratio <= threshold, round(ratio, 4), threshold,
                       f"{ratio:.2%} de nulos em {column}{suffix}")


# --------------------------------------------------------------------------
# 2) Duplicidade
# --------------------------------------------------------------------------
def check_unique_key(df: DataFrame, table: str, keys: list[str],
                     severity: str = ERROR) -> CheckResult:
    dup_groups = (
        df.groupBy(*[F.col(k).cast("string") for k in keys])
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    return CheckResult(f"chave_unica[{'+'.join(keys)}]", table, "duplicidade", severity,
                       dup_groups == 0, dup_groups, 0,
                       f"{dup_groups} grupos de chave duplicada")


# --------------------------------------------------------------------------
# 3) Domínio / formato / categoria
# --------------------------------------------------------------------------
def check_domain_numeric(df: DataFrame, table: str, column: str, min_v: float,
                         max_v: float, severity: str = WARN) -> CheckResult:
    c = F.col(column).cast("double")
    bad = df.filter(c.isNotNull() & ((c < min_v) | (c > max_v))).count()
    return CheckResult(f"dominio[{column}]", table, "dominio", severity, bad == 0, bad, 0,
                       f"{bad} valores fora de [{min_v},{max_v}] em {column}")


def check_categorical(df: DataFrame, table: str, column: str, allowed: list[str],
                      severity: str = ERROR) -> CheckResult:
    allow = [str(a) for a in allowed]
    c = F.col(column).cast("string")
    bad = df.filter(c.isNotNull() & ~c.isin(allow)).count()
    return CheckResult(f"categorico[{column}]", table, "dominio", severity, bad == 0, bad,
                       allow, f"{bad} valores fora de {allow} em {column}")


def check_municipio_format(df: DataFrame, table: str, column: str,
                           severity: str = ERROR) -> CheckResult:
    c = F.col(column).cast("string")
    bad = df.filter(c.isNotNull() & ~c.rlike(r"^\d{7}$")).count()
    return CheckResult(f"formato_municipio[{column}]", table, "dominio", severity,
                       bad == 0, bad, 0, f"{bad} códigos de município não têm 7 dígitos")


# --------------------------------------------------------------------------
# 4) Integridade referencial (FK)
# --------------------------------------------------------------------------
def check_fk(child: DataFrame, child_col: str, parent: DataFrame, parent_col: str,
             table: str, severity: str = WARN) -> CheckResult:
    parent_keys = parent.select(F.col(parent_col).cast("string").alias("_k")).distinct()
    child_keys = (
        child.select(F.col(child_col).cast("string").alias("_c"))
        .filter(F.col("_c").isNotNull())
        .distinct()
    )
    orphans = child_keys.join(parent_keys, F.col("_c") == F.col("_k"), "left_anti").count()
    return CheckResult(f"fk[{child_col}->{table}.{parent_col}]", table, "integridade",
                       severity, orphans == 0, orphans, 0,
                       f"{orphans} chaves órfãs em {child_col}")


# --------------------------------------------------------------------------
# 5) Consistência com a regra de negócio e cross-source
# --------------------------------------------------------------------------
def check_corte_consistency(aluno: DataFrame, corte: int, tol: float,
                            severity: str = ERROR) -> CheckResult:
    """IN_ALFABETIZADO deve ser coerente com (VL_PROFICIENCIA_LP >= corte)."""
    prof = F.col("VL_PROFICIENCIA_LP").cast("double")
    flag = F.col("IN_ALFABETIZADO").cast("int")
    d = aluno.filter(prof.isNotNull() & flag.isNotNull())
    total = d.count()
    mism = d.filter(((prof >= corte) & (flag != 1)) | ((prof < corte) & (flag != 0))).count()
    ratio = (mism / total) if total else 0.0
    return CheckResult(f"corte_{corte}_vs_flag", "ts_aluno", "consistencia", severity,
                       ratio <= tol, round(ratio, 5), tol,
                       f"{ratio:.3%} das linhas divergem do corte {corte}")


def check_cross_source_taxa(municipio: DataFrame, ts_municipio: DataFrame, tol_pp: float,
                            severity: str = WARN) -> CheckResult:
    """Compara taxa_alfabetizacao (Base dos Dados) x PC_ALUNO_ALFABETIZADO (INEP)."""
    a = municipio.select(
        F.col("ano").cast("string").alias("ano"),
        F.col("id_municipio").cast("string").alias("cod"),
        F.col("serie").cast("string").alias("serie"),
        F.col("rede").cast("string").alias("rede"),
        F.col("taxa_alfabetizacao").cast("double").alias("taxa_bdd"),
    )
    b = ts_municipio.select(
        F.col("ano").cast("string").alias("ano"),
        F.col("CO_MUNICIPIO").cast("string").alias("cod"),
        F.col("TP_SERIE").cast("string").alias("serie"),
        F.col("ID_TIPO_REDE").cast("string").alias("rede"),
        F.col("PC_ALUNO_ALFABETIZADO").cast("double").alias("taxa_inep"),
    )
    j = (
        a.join(b, ["ano", "cod", "serie", "rede"], "inner")
        .filter(F.col("taxa_bdd").isNotNull() & F.col("taxa_inep").isNotNull())
        .withColumn("diff", F.abs(F.col("taxa_bdd") - F.col("taxa_inep")))
    )
    total = j.count()
    diverg = j.filter(F.col("diff") > tol_pp).count()
    avg_diff = j.select(F.avg("diff")).first()[0] if total else 0.0
    return CheckResult("cross_source_taxa[municipio x ts_municipio]", "municipio",
                       "consistencia", severity, diverg == 0, diverg, tol_pp,
                       f"{diverg}/{total} municípios divergem > {tol_pp}pp "
                       f"(diff médio {avg_diff:.3f}pp)")
