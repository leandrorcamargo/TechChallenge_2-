# projeto/ — Código da Pipeline

Código-fonte da pipeline híbrida (batch + streaming) de análise da alfabetização.
A documentação completa da solução está no [README raiz](../README.md); aqui ficam
as instruções de setup e execução do código.

## Estrutura

```
projeto/
├── common/         # config, SparkSession, logging JSON, I/O (local/S3)
├── bronze/         # ingestão batch  (C4)
├── silver/         # limpeza, padronização e integração (C6)
├── gold/           # camada analítica + notebook de IA (C7, C10)
├── quality/        # validações de qualidade de dados (C5)
├── streaming/      # producer/consumer Kinesis (C8)
├── orchestration/  # orquestração da pipeline (C9)
├── infra/          # Terraform: S3, Glue, Kinesis, CloudWatch (C3)
├── docs/           # arquitetura, convenções, roteiro do vídeo
├── config/         # settings.yaml + .env.example
└── requirements.txt
```

## Setup local

```bash
# 1. Ambiente Python (requer Python 3.11+ e Java 11 para o PySpark)
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r projeto/requirements.txt

# 2. Configuração
cp projeto/config/.env.example projeto/config/.env
# APP_ENV=local  -> lê data/ e escreve em lake/ (sem custo de nuvem)
```

## Modos de execução

- **`APP_ENV=local`** — jobs leem `data/` e escrevem Parquet em `lake/<camada>/`.
  Ideal para desenvolver e validar sem custo de AWS.
- **`APP_ENV=aws`** — jobs leem/escrevem em `s3://` e usam Glue Catalog / Kinesis.
  Requer credenciais AWS configuradas (`aws configure`).

As convenções de camadas, particionamento e nomenclatura estão em
[`docs/convencoes.md`](docs/convencoes.md).
