# =====================================================================
# Athena — workgroup para consultas SQL sobre a camada Gold
# =====================================================================
resource "aws_athena_workgroup" "this" {
  name = local.name_prefix

  configuration {
    enforce_workgroup_configuration = true
    publish_cloudwatch_metrics_enabled = true

    # FinOps: teto de bytes escaneados por query (evita queries caras acidentais).
    bytes_scanned_cutoff_per_query = var.athena_bytes_scanned_cutoff

    result_configuration {
      output_location = local.athena_output
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = { Name = "${local.name_prefix}-workgroup" }
}
