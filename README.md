# Tech Challenge – Fase 2
## Pipeline Híbrido para Análise da Alfabetização no Brasil 🇧🇷📊

Projeto integrador da Fase 2 da Pós-Tech, desenvolvido por um time que atua como
equipe de engenharia de dados de uma organização pública de análise educacional.
O objetivo é construir uma **pipeline híbrida de dados (Batch + Streaming)**,
escalável em nuvem, que integre diferentes fontes relacionadas ao **Indicador
Criança Alfabetizada**, garantindo qualidade, escalabilidade e eficiência de custos.

---

## 📌 Contexto do Problema

A alfabetização na infância é um dos pilares para o desenvolvimento educacional,
social e econômico do país. O **Compromisso Nacional Criança Alfabetizada** é uma
política pública que mobiliza União, estados, Distrito Federal e municípios para
garantir que todas as crianças estejam alfabetizadas até o final do **2º ano do
ensino fundamental**.

A partir da **Pesquisa Alfabetiza Brasil (INEP, 2023)** foi definido o ponto de
corte de **743 pontos** na escala de proficiência do Saeb, nível a partir do qual
uma criança pode ser considerada alfabetizada. Com base nesse parâmetro foi criado
o **Indicador Criança Alfabetizada**, que expressa o percentual de estudantes que
atingem esse patamar. A **meta nacional** é que, até **2030**, todas as crianças
brasileiras estejam alfabetizadas ao final do 2º ano.

Compreender os fatores que influenciam a alfabetização exige integrar diferentes
fontes de dados: metas nacionais e estaduais, metas municipais, dados territoriais,
microdados educacionais e indicadores de desempenho.

**Fonte de dados:** Indicador Criança Alfabetizada – [Base dos Dados](https://basedosdados.org/)

---

## 🎯 Objetivo Técnico

Construir uma pipeline de dados escalável em nuvem que realize:

- Ingestão de diferentes fontes de dados educacionais;
- Tratamento e padronização das informações;
- Integração entre bases heterogêneas;
- Disponibilização de uma camada analítica confiável;
- Monitoramento operacional do pipeline;
- Controle de custos da infraestrutura.

---

## 🗂️ Fontes de Dados

A pipeline integra as seguintes entidades, originadas das avaliações de
alfabetização do **INEP**. Os arquivos brutos baixados ficam versionados na pasta
[`data/`](data/) no formato comprimido (`.csv.gz` / `.zip`):

| Entidade | Arquivo em `data/` | Formato |
|----------|--------------------|---------|
| UF | `br_inep_avaliacao_alfabetizacao_uf.csv.gz` | CSV (gzip) |
| Município | `br_inep_avaliacao_alfabetizacao_municipio.csv.gz` | CSV (gzip) |
| Meta Alfabetização Brasil | `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv.gz` | CSV (gzip) |
| Meta Alfabetização por UF | `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv.gz` | CSV (gzip) |
| Meta Alfabetização por Município | `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv.gz` | CSV (gzip) |
| Dados de alunos (microdados) | `microdados_avaliacao_da_alfabetizacao_2023.zip`, `microdados_avaliacao_da_alfabetizacao_2024.zip`, `microdados_AEEB_2025.zip` | ZIP |

> Os microdados (`microdados_*.zip`) contêm as tabelas `TS_ALUNO`, `TS_MUNICIPIO`,
> `TS_ESTADO` e `TS_ITEM`, além de dicionários e scripts de leitura (R / SAS / SPSS).

### Fontes externas (opcional – enriquecimento)

| Dimensão | Fonte |
|----------|-------|
| Estrutura escolar | Censo Escolar (INEP) |
| Socioeconômico | IBGE – Censo / PNAD |
| Desenvolvimento humano | Atlas do Desenvolvimento Humano |
| Vulnerabilidade social | Cadastro Único / Bolsa Família |
| Território | IBGE |
| Financiamento | FUNDEB |

---

## 🏗️ Arquitetura Esperada

Arquitetura híbrida moderna, seguindo a **Arquitetura Medalhão**.

### Ingestão Híbrida

- **Batch** — processamento periódico de dados históricos (metas educacionais,
  municípios, dados agregados nacionais).
- **Streaming** — simulação de ingestão de eventos em tempo quase real (atualização
  de indicadores, novas medições de desempenho, atualização de metas/resultados).

### Camadas Medalhão

| Camada | Descrição |
|--------|-----------|
| 🥉 **Bronze** | Dados brutos ingeridos das fontes, sem transformações significativas, com histórico completo preservado. |
| 🥈 **Silver** | Dados tratados: limpeza, tratamento de valores ausentes, padronização de nomes e tipos, validação de consistência e normalização de chaves. |
| 🥇 **Gold** | Camada analítica: datasets prontos para análise (indicador por município, comparação metas × resultados, evolução temporal), preparados para dashboards, análises estatísticas e treinamento de modelos de ML. |

---

## ✅ Regras de Qualidade de Dados

- Verificação de duplicidade;
- Detecção de valores ausentes;
- Validação de chaves de relacionamento;
- Consistência entre tabelas.

---

## 📈 Monitoramento da Pipeline

Mecanismos de observabilidade esperados:

- Falhas de ingestão;
- Latência do pipeline;
- Volume de dados processados;
- Alertas de erro.

---

## 💰 FinOps – Otimização de Custos

Boas práticas de eficiência no uso da nuvem:

- Uso eficiente de armazenamento (**Parquet**, particionamento);
- Otimização de queries;
- Controle de recursos computacionais;
- Estimativa de custo da arquitetura.

---

## ☁️ Implementação em Cloud

A solução será implementada em ambiente de nuvem (**AWS / GCP / Azure**).

---

## 🤖 Aplicação em IA

A camada Gold poderá ser usada para:

- Modelos de predição de alfabetização por município;
- Análise de desigualdade educacional / clusters de vulnerabilidade;
- Subsídio a políticas públicas baseadas em dados.

---

## 📁 Estrutura do Repositório

```
TechChallenge_2/
├── data/              # Dados brutos baixados das fontes (comprimidos: .csv.gz / .zip)
├── projeto/           # Código e artefatos da pipeline
│   ├── bronze/        # Ingestão de dados brutos
│   ├── silver/        # Tratamento, padronização e validação
│   ├── gold/          # Camada analítica
│   ├── quality/       # Scripts de validação e qualidade de dados
│   └── docs/          # Documentação técnica e diagramas
└── README.md          # Documentação da solução
```

> A separação entre [`data/`](data/) (dados de entrada) e [`projeto/`](projeto/)
> (código da pipeline) mantém os dados baixados isolados da lógica de processamento.

---

## 🚀 Tecnologias

> _A definir conforme o ambiente de nuvem escolhido (ferramentas e justificativas
> serão documentadas ao longo do desenvolvimento)._

## ⚙️ Execução Local das Camadas

A camada bronze registra as bases Parquet brutas das safras de 2023, 2024 e 2025
e mantém os microdados organizados por ano, sem padronizar colunas ou valores:

```bash
python3 src/bronze/bronze.py --years 2023 2024 2025
```

Saídas principais:

- `data/bronze/ano=2023/`
- `data/bronze/ano=2024/`
- `data/bronze/ano=2025/`
- `data/bronze/common/`

A camada silver lê a bronze por safra, trata os dados brutos e grava as tabelas
separadas por ano. Nessa etapa são aplicadas limpeza de texto, tratamento de
valores ausentes, padronização de nomes e tipos, validação de consistência e
normalização de chaves. A silver também adiciona indicadores de qualidade por
linha, chaves derivadas (`chave_uf`, `chave_municipio`, `chave_aluno`,
`chave_escola`), atributos de domínio e um dataset consolidado com os achados de
qualidade:

```bash
python3 src/silver/silver.py --years 2023 2024 2025
```

Saídas principais:

- `data/silver/ano=2023/`, `data/silver/ano=2024/`, `data/silver/ano=2025/`
- `data/silver/quality_findings.parquet`
- `data/silver/_manifest.json`

A camada gold lê os dados tratados da silver e cria datasets analíticos prontos
para dashboards, análises estatísticas e treinamento de modelos de machine
learning. As saídas incluem indicador de alfabetização por município,
comparação entre metas e resultados, evolução temporal do indicador, resumo por
UF/rede e base de features para modelagem:

```bash
python3 src/gold/gold.py --years 2023 2024 2025
```

Saídas principais:

- `data/gold/ano=2023/`, `data/gold/ano=2024/`, `data/gold/ano=2025/`
- `data/gold/consolidado/indicador_alfabetizacao_municipio.parquet`
- `data/gold/consolidado/comparacao_metas_resultados.parquet`
- `data/gold/consolidado/evolucao_temporal_indicador.parquet`
- `data/gold/consolidado/dashboard_resumo_uf.parquet`
- `data/gold/consolidado/ml_features_municipio.parquet`
- `data/gold/_manifest.json`

---

## 👥 Equipe

| Integrante | Contato |
|------------|---------|
| Isabelle Nicole Santana de Brito | isabelle_nicole@outlook.com |
| Filipe Noberto Justino | justinofilipe03@hotmail.com |
| Leandro Rebes Camargo | leandrorcamargo@hotmail.com |
| Felipe Vieira Sanches | fvieirasanches@gmail.com |

---

## 📹 Vídeo Executivo

> _Link a ser adicionado (apresentação executiva de até 5 minutos)._
