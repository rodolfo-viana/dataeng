# Pipeline de Engenharia de Dados

Um pipeline de engenharia de dados pronto para produção implementando a Arquitetura Medallion (camadas Bronze-Silver-Gold) usando Apache Spark e Delta Lake no Databricks.

## Início Rápido

### Pré-requisitos
- Workspace do Databricks com Unity Catalog
- Cluster com Spark 3.4+ e suporte ao Delta Lake
- Os arquivos de dados já estão incluídos no repositório em `data/raw/`

## Visão Geral do Pipeline de Dados

```mermaid
graph LR
    A[Dados Brutos<br/>Arquivos JSON.gz] --> B[Camada Bronze<br/>Ingestão bruta]
    B --> C[Camada Silver<br/>Limpos & validados]
```

### Fontes de Dados
- **wiki_pages.json.gz**: Informações de criadores da Wikipedia (em `data/raw/`)
- **posts_creator.json.gz**: Posts de redes sociais com métricas de engajamento (em `data/raw/`)

### Recursos do Pipeline
- **Arquitetura Medallion**: Camadas Bronze → Silver
- **Qualidade de Dados**: Limite de 95% de completude com validação
- **Rastreamento de Metadados**: Linhagem completa e metadados de processamento
- **Delta Lake**: Transações ACID, viagem no tempo e otimização
- **Tratamento de Erros**: Logging abrangente e gerenciamento de erros
- **Design Modular**: Componentes reutilizáveis e gerenciamento de configuração


## Documentação

- **[Guia de Arquitetura](docs/architecture.md)**: Design e componentes do sistema
- **[Guia de Implantação](docs/deployment_guide.md)**: Instruções de configuração e implantação

## Stack

- **Computação**: Apache Spark 3.4+ no Databricks
- **Armazenamento**: Delta Lake com Unity Catalog
- **Linguagem**: Python (PySpark)
- **Controle de Versão**: Git com Databricks Repos

## 📄 Licença

Este projeto está licenciado sob a Licença MIT.
