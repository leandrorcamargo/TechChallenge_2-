# =====================================================================
# S3 — Data lake (medalhão) e bucket de artefatos
# Zonas raw/bronze/silver/gold são PREFIXOS dentro do bucket do lake.
# =====================================================================

# ---- Data lake --------------------------------------------------------
resource "aws_s3_bucket" "datalake" {
  bucket = var.datalake_bucket_name
  tags   = { Name = "${local.name_prefix}-datalake" }
}

# Versionamento: preserva histórico (importante para a camada bronze/raw).
resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Criptografia em repouso (SSE-S3), sem custo adicional.
resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# Bloqueia qualquer acesso público.
resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket                  = aws_s3_bucket.datalake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Habilita o envio de eventos do bucket para o EventBridge (gatilho do streaming).
resource "aws_s3_bucket_notification" "datalake" {
  bucket      = aws_s3_bucket.datalake.id
  eventbridge = true
}

# FinOps: transições de classe de armazenamento e expiração de resultados Athena.
resource "aws_s3_bucket_lifecycle_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  # raw/: dados brutos raramente reacessados -> Intelligent-Tiering após 30 dias.
  rule {
    id     = "raw-intelligent-tiering"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    transition {
      days          = 30
      storage_class = "INTELLIGENT_TIERING"
    }
  }

  # athena-results/: descartáveis -> expira em 30 dias.
  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 30
    }
  }

  # Limpa uploads multipart incompletos (evita custo oculto).
  rule {
    id     = "abort-incomplete-mpu"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# ---- Artefatos (scripts Glue, pacotes Lambda) -------------------------
resource "aws_s3_bucket" "artifacts" {
  bucket = local.artifacts_bucket_name
  tags   = { Name = "${local.name_prefix}-artifacts" }
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
