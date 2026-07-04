# =====================================================================
# AWS Glue — Data Catalog e jobs PySpark (batch: bronze/silver/gold)
# =====================================================================

# Banco de dados no Glue Data Catalog (tabelas consultáveis via Athena).
resource "aws_glue_catalog_database" "this" {
  name        = local.glue_database
  description = "Catálogo do pipeline de alfabetização (medalhão)."
}

# Jobs PySpark do pipeline batch. Os scripts ficam no bucket de artefatos
# (enviados nas etapas C4/C6/C7) em glue/<job>.py.
resource "aws_glue_job" "pipeline" {
  for_each = toset(local.glue_jobs)

  name              = "${local.name_prefix}-${each.value}"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  # Interrompe jobs presos (FinOps: evita cobrança por execução travada).
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.artifacts.bucket}/glue/${each.value}.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.artifacts.bucket}/glue/tmp/"
    "--DATALAKE_BUCKET"                  = aws_s3_bucket.datalake.bucket
    "--GLUE_DATABASE"                    = aws_glue_catalog_database.this.name
  }

  tags = { Name = "${local.name_prefix}-${each.value}" }
}
