# =====================================================================
# Observabilidade — CloudWatch Logs, SNS (alertas) e alarmes
# =====================================================================

# ---- Log groups -------------------------------------------------------
resource "aws_cloudwatch_log_group" "pipeline" {
  name              = "/tc2/alfabetizacao/pipeline"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "producer" {
  name              = "/aws/lambda/${aws_lambda_function.producer.function_name}"
  retention_in_days = var.log_retention_days
}

# ---- Tópico de alertas ------------------------------------------------
resource "aws_sns_topic" "alerts" {
  name = "${local.name_prefix}-alertas"
}

# Assinatura por e-mail (só criada se alert_email for informado).
resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ---- Alarmes ----------------------------------------------------------
# Falhas na Lambda produtora (falha de ingestão do streaming).
resource "aws_cloudwatch_metric_alarm" "producer_errors" {
  alarm_name          = "${local.name_prefix}-producer-errors"
  alarm_description   = "Erros na Lambda produtora do streaming."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.producer.function_name
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]
}

# Latência do pipeline de streaming: idade dos registros no consumidor do stream.
resource "aws_cloudwatch_metric_alarm" "stream_iterator_age" {
  alarm_name          = "${local.name_prefix}-stream-iterator-age"
  alarm_description   = "Consumo do Kinesis atrasado (latência alta do streaming)."
  namespace           = "AWS/Kinesis"
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 60000 # 60s
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    StreamName = aws_kinesis_stream.events.name
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
}
