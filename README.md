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

A pipeline integra as seguintes entidades, obtidas a partir da plataforma **Base dos Dados**:

- UF
- Meta Alfabetização Brasil
- Meta Alfabetização por UF
- Meta Alfabetização por Município
- Município
- Dados de alunos

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
| 🥈 **Silver** | Dados tratados: limpeza, tratamento de valores ausentes, padronização de nomes e tipos, validação de consistência, normalização de chaves e **integração das bases**. |
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
├── bronze/        # Ingestão de dados brutos
├── silver/        # Tratamento, padronização e integração
├── gold/          # Camada analítica
├── quality/       # Scripts de validação e qualidade de dados
├── docs/          # Documentação técnica e diagramas
└── README.md      # Documentação da solução
```

---

## 🚀 Tecnologias

> _A definir conforme o ambiente de nuvem escolhido (ferramentas e justificativas
> serão documentadas ao longo do desenvolvimento)._

---

## 👥 Equipe

> _A definir._

---

## 📹 Vídeo Executivo

> _Link a ser adicionado (apresentação executiva de até 5 minutos)._
