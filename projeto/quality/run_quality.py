"""Gate de qualidade Bronze -> Silver (PySpark, compatível com AWS Glue).

Lê a Bronze, executa o catálogo de checks (``rules.py``), grava o relatório
(JSON + Markdown) e, se houver qualquer check ERROR, levanta exceção para
interromper a pipeline e disparar alerta.

Uso local:
    python projeto/quality/run_quality.py     # requer a Bronze já gerada (C4)
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

# Torna ``common`` e ``quality`` importáveis como script standalone.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from common.config import Settings, load_settings  # noqa: E402
from common.io import layer_path  # noqa: E402
from common.logging_setup import get_logger  # noqa: E402
from common.spark import build_spark  # noqa: E402
from quality import report as report_mod  # noqa: E402
from quality.rules import run_all_checks  # noqa: E402

log = get_logger("quality.gate")

# Tabelas Bronze necessárias aos checks.
BRONZE_TABLES = [
    "uf", "municipio", "meta_brasil", "meta_uf", "meta_municipio",
    "ts_aluno", "ts_municipio", "ts_estado",
]


def load_bronze(spark: SparkSession, settings: Settings) -> dict[str, DataFrame]:
    return {t: spark.read.parquet(layer_path(settings, "bronze", t)) for t in BRONZE_TABLES}


def _write_report(settings: Settings, run_id: str, json_txt: str, md_txt: str) -> str:
    """Grava o relatório na camada de qualidade (local ou S3). Retorna o destino."""
    base = layer_path(settings, settings.get("quality.report_layer", "quality"), "report")
    if settings.is_aws:
        import boto3

        # base é s3://bucket/quality/report
        _, _, rest = base.partition("s3://")
        bucket, _, prefix = rest.partition("/")
        s3 = boto3.client("s3", region_name=settings.region)
        s3.put_object(Bucket=bucket, Key=f"{prefix}/{run_id}.json",
                      Body=json_txt.encode("utf-8"))
        s3.put_object(Bucket=bucket, Key=f"{prefix}/{run_id}.md",
                      Body=md_txt.encode("utf-8"))
    else:
        out_dir = Path(base)
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / f"{run_id}.json").write_text(json_txt, encoding="utf-8")
        (out_dir / f"{run_id}.md").write_text(md_txt, encoding="utf-8")
    return base


def _emit_metrics(settings: Settings, summary: dict) -> None:
    """Publica métricas de qualidade (CloudWatch no AWS; log no local)."""
    if not settings.is_aws:
        log.info("dq_metrics", extra={"extra": summary})
        return
    import boto3

    cw = boto3.client("cloudwatch", region_name=settings.region)
    ns = settings.get("monitoring.namespace", "TC2/Alfabetizacao")
    cw.put_metric_data(
        Namespace=ns,
        MetricData=[
            {"MetricName": "dq_error_count", "Value": summary["errors"], "Unit": "Count"},
            {"MetricName": "dq_warn_count", "Value": summary["warnings"], "Unit": "Count"},
            {"MetricName": "dq_checks_total", "Value": summary["total"], "Unit": "Count"},
        ],
    )


def main() -> None:
    settings = load_settings()
    run_id = uuid.uuid4().hex
    spark = build_spark("quality-gate", aws=settings.is_aws)
    log.info("gate iniciado", extra={"extra": {"env": settings.env, "run_id": run_id}})

    bronze = load_bronze(spark, settings)
    results = run_all_checks(
        bronze,
        corte=settings.proficiencia_corte,
        null_ratio_warn=float(settings.get("quality.null_ratio_warn", 0.05)),
        cross_source_tol_pp=float(settings.get("quality.cross_source_tol_pp", 1.0)),
        corte_mismatch_tol=float(settings.get("quality.corte_mismatch_tol", 0.005)),
    )

    summary = report_mod.summarize(results)
    json_txt = report_mod.to_json(results, run_id, settings.env)
    md_txt = report_mod.to_markdown(results, run_id, settings.env)
    dest = _write_report(settings, run_id, json_txt, md_txt)
    _emit_metrics(settings, summary)

    log.info("gate concluido", extra={"extra": {**summary, "relatorio": dest}})
    print(f"Data Quality gate={summary['gate']} "
          f"(ok={summary['passed']} warn={summary['warnings']} err={summary['errors']}) -> {dest}")
    spark.stop()

    # Gate: interrompe a pipeline se houver erro bloqueante.
    if summary["errors"] > 0:
        blocking = [r.name for r in results if r.blocking]
        raise SystemExit(f"GATE REPROVADO: {summary['errors']} erro(s) bloqueante(s): {blocking}")


if __name__ == "__main__":
    main()
