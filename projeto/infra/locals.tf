# Valores derivados e convenções de nomenclatura.
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  name_prefix = "${var.project_name}-${var.environment}"

  # Nome do banco no Glue Data Catalog (underscore; alinhado ao settings.yaml).
  glue_database = replace("${var.project_name}_${var.environment}", "-", "_")

  # Bucket de artefatos (scripts Glue, pacotes Lambda) — derivado do bucket do lake.
  artifacts_bucket_name = "${var.datalake_bucket_name}-artifacts"

  athena_output = "s3://${var.datalake_bucket_name}/athena-results/"

  # Jobs Glue (batch) do pipeline medalhão.
  glue_jobs = ["bronze-ingest", "silver-transform", "gold-build"]

  common_tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
      Challenge   = "postech-fase2"
    },
    var.tags,
  )
}
