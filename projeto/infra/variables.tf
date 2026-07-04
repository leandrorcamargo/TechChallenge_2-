# Variáveis de entrada. Valores default alinhados com projeto/config/settings.yaml.
# Sobrescreva em terraform.tfvars (copie de terraform.tfvars.example).

variable "project_name" {
  description = "Prefixo lógico do projeto, usado na nomenclatura dos recursos."
  type        = string
  default     = "tc2-alfabetizacao"
}

variable "environment" {
  description = "Ambiente (dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "region" {
  description = "Região AWS onde a infraestrutura será provisionada."
  type        = string
  default     = "us-east-1"
}

variable "datalake_bucket_name" {
  description = "Nome GLOBALMENTE único do bucket do data lake (raw/bronze/silver/gold)."
  type        = string
  default     = "tc2-alfabetizacao-datalake"
}

variable "kinesis_shard_count" {
  description = "Número de shards do Kinesis Data Stream (provisionado)."
  type        = number
  default     = 1
}

variable "kinesis_retention_hours" {
  description = "Retenção dos registros no stream, em horas (24 a 8760)."
  type        = number
  default     = 24
}

variable "glue_version" {
  description = "Versão do AWS Glue para os jobs PySpark."
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Tipo de worker do Glue (G.1X, G.2X, ...). G.1X é o mais econômico."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Quantidade de workers por job Glue."
  type        = number
  default     = 2
}

variable "trigger_prefix" {
  description = "Prefixo no data lake observado para disparar o streaming (S3 ObjectCreated)."
  type        = string
  default     = "raw/microdados/"
}

variable "athena_bytes_scanned_cutoff" {
  description = "Limite de bytes escaneados por query no Athena (FinOps). Default 1 GB."
  type        = number
  default     = 1073741824
}

variable "log_retention_days" {
  description = "Retenção dos CloudWatch Log Groups, em dias."
  type        = number
  default     = 14
}

variable "alert_email" {
  description = "E-mail para assinar o tópico SNS de alertas (vazio = sem assinatura)."
  type        = string
  default     = ""
}

variable "tags" {
  description = "Tags adicionais aplicadas a todos os recursos."
  type        = map(string)
  default     = {}
}
