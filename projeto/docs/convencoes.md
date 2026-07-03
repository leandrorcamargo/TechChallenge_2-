# Convenções da Pipeline — camadas, particionamento e nomenclatura

Este documento define as convenções seguidas por todos os jobs (bronze/silver/gold),
garantindo consistência, rastreabilidade e eficiência de custo (FinOps).

## Camadas do data lake (medalhão)

| Camada | Local (`lake/`) | AWS (`s3://<bucket>/`) | Conteúdo |
|--------|-----------------|------------------------|----------|
| `raw`    | `lake/raw/`    | `s3://.../raw/`    | Cópia fiel dos arquivos de `data/` (landing). |
| `bronze` | `lake/bronze/` | `s3://.../bronze/` | Parquet bruto, sem transformação significativa, histórico preservado. |
| `silver` | `lake/silver/` | `s3://.../silver/` | Dados limpos, padronizados, chaves normalizadas e **bases integradas**. |
| `gold`   | `lake/gold/`   | `s3://.../gold/`   | Datasets analíticos prontos para BI/ML. |

O caminho de cada camada é resolvido por [`common/io.py`](../common/io.py) (`layer_path`),
alternando local×S3 conforme `env` em [`config/settings.yaml`](../config/settings.yaml).

## Formato e particionamento (decisões de FinOps)

- **Formato:** Parquet + compressão **snappy** (colunar, splittable, barato de escanear).
- **Partição padrão:** por `ano`. Tabelas de município também podem particionar por
  `sigla_uf` para reduzir o scan no Athena.
- **Overwrite dinâmico de partição:** reprocessa apenas o ano afetado, sem reescrever a tabela.

## Nomenclatura de datasets

- **Bronze:** mesmo nome lógico da fonte — `uf`, `municipio`, `meta_brasil`,
  `meta_uf`, `meta_municipio`, `ts_aluno`, `ts_municipio`, `ts_estado`, `ts_item`.
- **Silver:** dimensões `dim_uf`, `dim_municipio`; fatos `fato_alfabetizacao_municipio`,
  `fato_alfabetizacao_uf`, `fato_meta_*`, `fato_aluno` (com flag de corte 743 aplicada).
- **Gold:** `ind_alfabetizacao_municipio`, `ind_meta_vs_resultado`, `ind_evolucao_temporal`.

## Metadados de ingestão (bronze)

Todo registro bronze recebe colunas técnicas:

| Coluna | Descrição |
|--------|-----------|
| `_ingestion_ts` | timestamp UTC da ingestão. |
| `_source_file`  | nome do arquivo de origem em `data/`. |
| `_source_layer` | `base_dos_dados` ou `microdados`. |

## Chaves de negócio (normalização na silver)

- `ano` (int), `sigla_uf` (str, 2 letras maiúsculas), `id_municipio` (str, 7 dígitos),
  `rede` (categoria padronizada), `serie` (int).
- Regra de alfabetização: `VL_PROFICIENCIA_LP >= 743` ⇒ aluno alfabetizado
  (constante `business.proficiencia_corte`).
