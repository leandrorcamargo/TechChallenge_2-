from pyspark.sql import functions as F


def tratar_tabela(df, chaves_dedup, chaves_identidade, nome):

    antes = df.count()
    cond = None
    for c in chaves_identidade:
        cc = F.col(c).isNull()
        cond = cc if cond is None else (cond | cc)
    sem_nulo = df.filter(~cond) if cond is not None else df
    n_apos_nulo = sem_nulo.count()
    tratado = sem_nulo.dropDuplicates(chaves_dedup)
    depois = tratado.count()
    registro = {
        "tabela": nome,
        "linhas_entrada": antes,
        "removidas_identidade_nula": antes - n_apos_nulo,
        "removidas_duplicadas": n_apos_nulo - depois,
        "linhas_saida": depois,
    }
    return tratado, registro


def val_duplicados(df, chaves, nome):

    total = df.count()
    unicos = df.select(chaves).distinct().count()
    dup = total - unicos
    return {
        "tabela": nome, "tipo": "duplicidade",
        "detalhe": f"{dup} duplicados em {chaves}",
        "status": "OK" if dup == 0 else "ALERTA",
    }


def val_intervalo(df, coluna, lo, hi, nome):

    fora = df.filter(F.col(coluna).isNotNull() & ((F.col(coluna) < lo) | (F.col(coluna) > hi))).count()
    return {
        "tabela": nome, "tipo": "intervalo",
        "detalhe": f"{fora} fora de [{lo},{hi}] em {coluna}",
        "status": "OK" if fora == 0 else "ALERTA",
    }


def val_nulos(df, colunas, nome):

    cond = None
    for c in colunas:
        cc = F.col(c).isNull()
        cond = cc if cond is None else (cond | cc)
    n = df.filter(cond).count()
    return {
        "tabela": nome, "tipo": "nulos_chave",
        "detalhe": f"{n} linhas com chave nula em {colunas}",
        "status": "OK" if n == 0 else "ALERTA",
    }


def cross_source(base_df, base_taxa, ts_df, ts_taxa, chaves, nome, tolerancia=1.0):

    j = (base_df.select(*chaves, F.col(base_taxa).alias("taxa_bdd"))
         .join(ts_df.select(*chaves, F.col(ts_taxa).alias("taxa_inep")), chaves, "inner")
         .filter(F.col("taxa_bdd").isNotNull() & F.col("taxa_inep").isNotNull())
         .withColumn("diff", F.abs(F.col("taxa_bdd") - F.col("taxa_inep"))))
    total = j.count()
    diverg = j.filter(F.col("diff") > tolerancia).count()
    media = j.select(F.avg("diff")).first()[0] if total else 0.0
    status = "OK" if diverg == 0 else "ALERTA"
    return {
        "tabela": nome, "tipo": "cross_source",
        "detalhe": f"{diverg}/{total} divergem >{tolerancia}pp (diff medio {media:.4f}pp)",
        "status": status,
    }
