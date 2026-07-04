# =====================================================================
# IAM — Roles e políticas (privilégio mínimo) para Glue e Lambda
# =====================================================================

# ---- Role do AWS Glue -------------------------------------------------
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.name_prefix}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

# Política gerenciada da AWS com as permissões básicas do Glue.
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Acesso aos buckets do data lake e de artefatos.
data "aws_iam_policy_document" "glue_data_access" {
  statement {
    sid    = "ListBuckets"
    effect = "Allow"
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [
      aws_s3_bucket.datalake.arn,
      aws_s3_bucket.artifacts.arn,
    ]
  }
  statement {
    sid    = "ReadWriteObjects"
    effect = "Allow"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${aws_s3_bucket.datalake.arn}/*",
      "${aws_s3_bucket.artifacts.arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_data_access" {
  name   = "${local.name_prefix}-glue-data-access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_data_access.json
}

# ---- Role da Lambda produtora ----------------------------------------
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_producer" {
  name               = "${local.name_prefix}-lambda-producer-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "lambda_producer" {
  # Logs
  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:*:*:*"]
  }
  # Ler o delta recém-chegado no data lake
  statement {
    sid       = "ReadRaw"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.datalake.arn}/*"]
  }
  # Publicar eventos no Kinesis
  statement {
    sid       = "PublishKinesis"
    effect    = "Allow"
    actions   = ["kinesis:PutRecord", "kinesis:PutRecords"]
    resources = [aws_kinesis_stream.events.arn]
  }
  # Métricas customizadas de observabilidade
  statement {
    sid       = "PutMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "lambda_producer" {
  name   = "${local.name_prefix}-lambda-producer-policy"
  role   = aws_iam_role.lambda_producer.id
  policy = data.aws_iam_policy_document.lambda_producer.json
}
