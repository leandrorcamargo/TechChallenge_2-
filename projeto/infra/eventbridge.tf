# =====================================================================
# EventBridge — regra que dispara o streaming quando um arquivo raw
# é criado no data lake (gatilho REAL, sem simulação)
# =====================================================================

resource "aws_cloudwatch_event_rule" "raw_object_created" {
  name        = "${local.name_prefix}-raw-object-created"
  description = "Dispara a Lambda produtora ao criar objeto no prefixo raw observado."

  event_pattern = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = {
        name = [aws_s3_bucket.datalake.bucket]
      }
      object = {
        key = [{ prefix = var.trigger_prefix }]
      }
    }
  })
}

resource "aws_cloudwatch_event_target" "to_producer" {
  rule      = aws_cloudwatch_event_rule.raw_object_created.name
  target_id = "stream-producer"
  arn       = aws_lambda_function.producer.arn
}
