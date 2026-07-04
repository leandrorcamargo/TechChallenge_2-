# =====================================================================
# Lambda produtora — reage ao evento do S3 e publica no Kinesis
# (código real na etapa C8; aqui empacotamos o stub em lambda_src/)
# =====================================================================

data "archive_file" "producer" {
  type        = "zip"
  source_dir  = "${path.module}/lambda_src"
  output_path = "${path.module}/build/producer.zip"
}

resource "aws_lambda_function" "producer" {
  function_name = "${local.name_prefix}-stream-producer"
  role          = aws_iam_role.lambda_producer.arn
  runtime       = "python3.11"
  handler       = "handler.lambda_handler"
  timeout       = 60
  memory_size   = 256

  filename         = data.archive_file.producer.output_path
  source_code_hash = data.archive_file.producer.output_base64sha256

  environment {
    variables = {
      KINESIS_STREAM    = aws_kinesis_stream.events.name
      DATALAKE_BUCKET   = aws_s3_bucket.datalake.bucket
      PUBLISH_BATCH_SIZE = "500"
      METRIC_NAMESPACE  = "TC2/Alfabetizacao"
    }
  }

  tags = { Name = "${local.name_prefix}-stream-producer" }
}

# Permite que o EventBridge invoque a função.
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.producer.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.raw_object_created.arn
}
