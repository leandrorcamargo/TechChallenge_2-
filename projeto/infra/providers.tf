# Configuração do provider AWS. Credenciais vêm do ambiente
# (aws configure / perfil IAM / variáveis AWS_*), nunca do código.
provider "aws" {
  region = var.region

  default_tags {
    tags = local.common_tags
  }
}
