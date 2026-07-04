# Versões do Terraform e providers.
terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }

  # Sugestão para trabalho em equipe: usar backend remoto (S3 + DynamoDB lock).
  # Deixado comentado para permitir `terraform init` local sem pré-provisão.
  # backend "s3" {
  #   bucket         = "tc2-alfabetizacao-tfstate"
  #   key            = "infra/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "tc2-alfabetizacao-tflock"
  #   encrypt        = true
  # }
}
