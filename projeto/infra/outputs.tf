# Saídas úteis para configurar a aplicação (.env) e para os jobs.
output "datalake_bucket" {
  description = "Nome do bucket do data lake."
  value       = aws_s3_bucket.datalake.bucket
}

output "artifacts_bucket" {
  description = "Nome do bucket de artefatos (scripts Glue / pacotes Lambda)."
  value       = aws_s3_bucket.artifacts.bucket
}

output "kinesis_stream_name" {
  description = "Nome do Kinesis Data Stream de eventos."
  value       = aws_kinesis_stream.events.name
}

output "kinesis_stream_arn" {
  description = "ARN do Kinesis Data Stream de eventos."
  value       = aws_kinesis_stream.events.arn
}

output "glue_database" {
  description = "Banco no Glue Data Catalog."
  value       = aws_glue_catalog_database.this.name
}

output "glue_jobs" {
  description = "Jobs Glue do pipeline batch."
  value       = [for j in aws_glue_job.pipeline : j.name]
}

output "producer_lambda" {
  description = "Nome da Lambda produtora do streaming."
  value       = aws_lambda_function.producer.function_name
}

output "sns_alerts_topic_arn" {
  description = "ARN do tópico SNS de alertas."
  value       = aws_sns_topic.alerts.arn
}

output "athena_workgroup" {
  description = "Workgroup do Athena."
  value       = aws_athena_workgroup.this.name
}
