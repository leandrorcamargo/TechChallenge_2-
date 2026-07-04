# =====================================================================
# Kinesis Data Streams — transporte dos eventos reais de novas medições
# =====================================================================
resource "aws_kinesis_stream" "events" {
  name             = "${local.name_prefix}-eventos"
  shard_count      = var.kinesis_shard_count
  retention_period = var.kinesis_retention_hours

  # Criptografia com chave gerenciada pela AWS (sem custo de KMS dedicado).
  encryption_type = "KMS"
  kms_key_id      = "alias/aws/kinesis"

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = { Name = "${local.name_prefix}-eventos" }
}
