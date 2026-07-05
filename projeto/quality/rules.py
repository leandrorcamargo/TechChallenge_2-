"""Catálogo declarativo de regras de qualidade por tabela.

``run_all_checks`` recebe o dicionário de DataFrames da Bronze e devolve a lista
de ``CheckResult``. Concentra aqui QUAIS checks rodam onde, para facilitar auditoria.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from quality import checks as C
from quality.checks import CheckResult


def run_all_checks(
    bronze: dict[str, DataFrame],
    corte: int,
    null_ratio_warn: float,
    cross_source_tol_pp: float,
    corte_mismatch_tol: float,
) -> list[CheckResult]:
    r: list[CheckResult] = []

    # ---- Base dos Dados: uf --------------------------------------
    uf = bronze["uf"]
    r.append(C.check_not_null_keys(uf, "uf", ["ano", "sigla_uf", "serie", "rede"]))
    r.append(C.check_unique_key(uf, "uf", ["ano", "sigla_uf", "serie", "rede"]))
    r.append(C.check_domain_numeric(uf, "uf", "taxa_alfabetizacao", 0, 100))
    r.append(C.check_null_ratio(uf, "uf", "taxa_alfabetizacao", null_ratio_warn))

    # ---- Base dos Dados: municipio -------------------------------
    muni = bronze["municipio"]
    r.append(C.check_not_null_keys(muni, "municipio", ["ano", "id_municipio", "serie", "rede"]))
    r.append(C.check_unique_key(muni, "municipio", ["ano", "id_municipio", "serie", "rede"]))
    r.append(C.check_municipio_format(muni, "municipio", "id_municipio"))
    r.append(C.check_domain_numeric(muni, "municipio", "taxa_alfabetizacao", 0, 100))
    r.append(C.check_null_ratio(muni, "municipio", "taxa_alfabetizacao", null_ratio_warn))

    # ---- Base dos Dados: metas -----------------------------------
    r.append(C.check_not_null_keys(bronze["meta_brasil"], "meta_brasil", ["ano", "rede"]))
    muf = bronze["meta_uf"]
    r.append(C.check_not_null_keys(muf, "meta_uf", ["ano", "sigla_uf", "rede"]))
    r.append(C.check_unique_key(muf, "meta_uf", ["ano", "sigla_uf", "rede"]))
    mmun = bronze["meta_municipio"]
    r.append(C.check_not_null_keys(mmun, "meta_municipio", ["ano", "id_municipio", "rede"]))
    r.append(C.check_unique_key(mmun, "meta_municipio", ["ano", "id_municipio", "rede"]))
    r.append(C.check_municipio_format(mmun, "meta_municipio", "id_municipio"))

    # ---- Microdados: ts_aluno ------------------------------------
    al = bronze["ts_aluno"]
    # Identidade do registro: bloqueia se ausente.
    r.append(C.check_not_null_keys(al, "ts_aluno", ["ano", "ID_ALUNO"], severity=C.ERROR))
    # Município é chave de relacionamento: apenas alerta (não bloqueia).
    # Nota: em 2025 há ~628 alunos de 10 escolas não localizadas no Censo Escolar
    # (RO, PE, ES, SP, MT); podem ser retificados em versões futuras dos microdados.
    r.append(C.check_not_null_keys(al, "ts_aluno", ["CO_MUNICIPIO"], severity=C.WARN))
    r.append(C.check_unique_key(al, "ts_aluno", ["ano", "ID_ALUNO"]))
    r.append(C.check_municipio_format(al, "ts_aluno", "CO_MUNICIPIO", severity=C.WARN))
    r.append(C.check_categorical(al, "ts_aluno", "IN_ALFABETIZADO", [0, 1]))
    r.append(C.check_domain_numeric(al, "ts_aluno", "VL_PROFICIENCIA_LP", 0, 1000))
    # Proficiência nula só é relevante entre alunos PRESENTES (ausentes não têm nota).
    r.append(C.check_null_ratio(al, "ts_aluno", "VL_PROFICIENCIA_LP", null_ratio_warn,
                                condition=F.col("IN_PRESENCA_LP").cast("string") == "1",
                                suffix="@presentes"))
    r.append(C.check_corte_consistency(al, corte, corte_mismatch_tol))

    # ---- Microdados: ts_municipio / ts_estado --------------------
    tsm = bronze["ts_municipio"]
    r.append(C.check_not_null_keys(tsm, "ts_municipio",
                                   ["ano", "CO_MUNICIPIO", "TP_SERIE", "ID_TIPO_REDE"]))
    r.append(C.check_unique_key(tsm, "ts_municipio",
                                ["ano", "CO_MUNICIPIO", "TP_SERIE", "ID_TIPO_REDE"]))
    r.append(C.check_domain_numeric(tsm, "ts_municipio", "PC_ALUNO_ALFABETIZADO", 0, 100))

    tse = bronze["ts_estado"]
    r.append(C.check_not_null_keys(tse, "ts_estado",
                                   ["ano", "CO_UF", "TP_SERIE", "ID_TIPO_REDE"]))

    # ---- Integridade referencial (FK) ----------------------------
    # Todo aluno pertence a um município presente no agregado oficial.
    r.append(C.check_fk(al, "CO_MUNICIPIO", tsm, "CO_MUNICIPIO", "ts_municipio"))
    # Metas por município referem-se a municípios conhecidos.
    r.append(C.check_fk(mmun, "id_municipio", tsm, "CO_MUNICIPIO", "ts_municipio"))

    # ---- Consistência cross-source -------------------------------
    r.append(C.check_cross_source_taxa(muni, tsm, cross_source_tol_pp))

    return r
