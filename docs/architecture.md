# Documentação da Arquitetura

## Visão Geral

Este projeto implementa um pipeline de engenharia de dados usando a Arquitetura Medallion (camadas Bronze-Silver-Gold) com Apache Spark e Delta Lake no Databricks.

## Diagrama da Arquitetura

```
┌──────────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│    Dados Brutos      │    │  Camada Bronze  │    │  Camada Silver  │
│                      │    │                 │    │                 │
│ wiki_pages.json.gz   │ -> │ bronze_creators │ -> │silver_creators  │
│ posts_creator.json.gz│    │ bronze_posts    │    │silver_posts     │
└──────────────────────┘    └─────────────────┘    └─────────────────┘
```

## Camadas de Dados

### Camada Bronze (Ingestão de Dados Brutos)
- **Propósito**: Armazenar dados brutos e não processados dos sistemas de origem
- **Formato de Dados**: Arquivos JSON (comprimidos .gz)
- **Características**:
  - Fidelidade completa aos dados de origem
  - Colunas de metadados para rastreamento de linhagem
  - Processamento em lotes com IDs de lote únicos

### Camada Silver (Limpos e Transformados)
- **Propósito**: Dados prontos para negócios com verificações de qualidade e transformações
- **Características**:
  - Validação de qualidade de dados (verificações de completude)
  - Aplicação de esquema
  - Transformações de timestamp
  - Lógica de negócios aplicada

## Estrutura do Projeto

```
dataeng/
├── config/                       # Gerenciamento de configuração
│   ├── pipeline_config.py        # Configurações do pipeline
│   └── __init__.py
├── src/                          # Módulos de código fonte
│   ├── utils/                    # Funções utilitárias
│   │   ├── data_validation.py    # Lógica de validação de dados
│   │   ├── metadata_handler.py   # Gerenciamento de metadados
│   │   ├── table_operations.py   # Operações de tabela Delta
│   │   └── __init__.py
│   ├── transformations/          # Transformações de dados
│   │   ├── posts_transformations.py
│   │   └── __init__.py
│   └── __init__.py
├── notebooks/                    # Notebooks do Databricks
│   ├── creators_pipeline.py      # Pipeline de dados de criadores
│   ├── posts_pipeline.py         # Pipeline de dados de posts
│   └── __init__.py
├── docs/                         # Documentação
│   ├── architecture.md           # Este arquivo
│   └── deployment_guide.md       # Instruções de implantação
└── data/                         # Armazenamento de dados
    ├── raw/                      # Arquivos de origem brutos
    ├── bronze/                   # Saídas da camada bronze
    └── silver/                   # Saídas da camada silver
```

## Componentes Principais

### Validação de Dados (`src/utils/data_validation.py`)
- Validação de conectividade da fonte
- Avaliação de qualidade de dados
- Validação de contagem de linhas e colunas
- Cálculo de percentual de completude

### Manipulador de Metadados (`src/utils/metadata_handler.py`)
- Adição automática de colunas de metadados
- Rastreamento de timestamp de processamento
- Informações de linhagem da fonte
- Gerenciamento de ID de lote

### Operações de Tabela (`src/utils/table_operations.py`)
- Criação e gerenciamento de tabelas Delta
- Configurações de otimização automática
- Gerenciamento de propriedades de tabela
- Injeção de comentários e metadados

### Gerenciamento de Configuração (`config/pipeline_config.py`)
- Gerenciamento centralizado de configuração
- Configurações específicas do ambiente
- Definições de parâmetros do pipeline

## Fluxo de Dados

1. **Ingestão**: Dados JSON brutos dos sistemas de origem
2. **Processamento Bronze**: 
   - Validação de dados e verificações de conectividade da fonte
   - Adição de metadados (timestamp de processamento, fonte, ID do lote)
   - Criação de tabela Delta com otimização
3. **Processamento Silver**:
   - Validação de qualidade de dados e profiling
   - Transformações de negócios (conversões de timestamp)
   - Aplicação e validação de esquema
   - Criação de tabela Delta final

## Stack Tecnológico

- **Computação**: Apache Spark no Databricks
- **Armazenamento**: Formato Delta Lake
- **Linguagem**: Python (PySpark)
- **Orquestração**: Notebooks do Databricks
- **Controle de Versão**: Git (GitHub)

## Garantia de Qualidade

- **Limite de Qualidade de Dados**: 95% de completude necessária
- **Logging**: Logging abrangente em todos os pipelines
- **Validação**: Validação de múltiplas camadas (fonte, dados, esquema)
