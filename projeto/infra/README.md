# infra/ — Infraestrutura como Código (Terraform)

Provisiona, de forma reprodutível e versionada, toda a infraestrutura AWS da pipeline
event-driven de alfabetização.

## Recursos provisionados

| Arquivo | Recursos |
|---------|----------|
| `s3.tf` | Bucket do **data lake** (raw/bronze/silver/gold) + bucket de **artefatos**; versionamento, criptografia, bloqueio público, lifecycle (FinOps) e notificação para o EventBridge. |
| `glue.tf` | **Glue Data Catalog** (database) e **jobs PySpark** batch: `bronze-ingest`, `silver-transform`, `gold-build`. |
| `kinesis.tf` | **Kinesis Data Stream** de eventos (criptografado). |
| `eventbridge.tf` | Regra do **EventBridge** que dispara o streaming ao criar objeto no `raw/` (gatilho real). |
| `lambda.tf` | **Lambda produtora** (stub em `lambda_src/`; código real na C8) + permissão para o EventBridge. |
| `iam.tf` | Roles/políticas de **privilégio mínimo** para Glue e Lambda. |
| `cloudwatch.tf` | **Log groups**, tópico **SNS** de alertas e **alarmes** (erros da Lambda, latência do stream). |
| `athena.tf` | **Workgroup do Athena** com teto de scan por query (FinOps). |

## Pré-requisitos

- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.5
- Credenciais AWS configuradas (`aws configure` ou variáveis `AWS_*`)
- Um nome **globalmente único** para o bucket do data lake

## Uso

```bash
cd projeto/infra
cp terraform.tfvars.example terraform.tfvars   # ajuste os valores (bucket único!)

terraform init      # baixa providers e inicializa o estado
terraform plan      # revisa o que será criado (não aplica nada)
terraform apply     # provisiona a infraestrutura na AWS
```

Após o `apply`, use os **outputs** (`terraform output`) para preencher o
`projeto/config/.env` (nome do bucket, stream, workgroup, etc.).

```bash
terraform destroy   # remove tudo (evita custos ao encerrar o uso)
```

## Notas de FinOps

- Kinesis **provisionado com 1 shard** em dev (menor custo); pode ser destruído quando ocioso.
- Glue com `G.1X` e poucos workers; `timeout` curto evita cobrança por job travado.
- S3 com **Intelligent-Tiering** no `raw/`, expiração de resultados Athena e limpeza de MPU.
- Athena com **teto de bytes escaneados** por query.

> A infra pode ser validada localmente com `terraform validate` e emulada com
> **LocalStack** antes de qualquer `apply` real, mantendo o custo em zero durante o desenvolvimento.
