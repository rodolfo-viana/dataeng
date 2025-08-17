# Guia de Implantação

## Pré-requisitos

### Ambiente Databricks
- Acesso ao workspace do Databricks
- Cluster com Spark 3.4+ e suporte ao Delta Lake
- Unity Catalog habilitado (recomendado)
- Permissões apropriadas para criação de banco de dados/tabela

### Desenvolvimento Local (opcional)
- Python 3.8+ instalado
- Java 8 ou 11 (para Spark)
- Git configurado

## Passos de Implantação

### 1. Configuração do Repositório

#### Opção A: Usando Databricks Repos (recomendado)
```bash
# No workspace do Databricks:
# 1. Vá para Repos -> Users -> username -> Create Git folder
# 2. Cole a URL do repositório: https://github.com/rodolfo-viana/dataeng.git
# 3. Clone o repositório
```

#### Opção B: Upload Manual
```bash
# Clone o repositório localmente
git clone https://github.com/rodolfo-viana/dataeng.git

# Faça upload para o workspace do Databricks usando CLI
databricks workspace import-dir ./dataeng /Users/<username>/dataeng
```

### 2. Configuração de Dados

Os arquivos de dados estão incluídos no repositório e serão lidos automaticamente:
- `data/raw/wiki_pages.json.gz` - Dados de criadores da Wikipedia
- `data/raw/posts_creator.json.gz` - Posts de redes sociais

### 3. Execução do Pipeline

#### Método 1: Executar Notebooks Diretamente
1. Abra `notebooks/creators_pipeline.py` ou `notebooks/posts_pipeline.py`
2. Conecte ao seu cluster
3. Execute todas as células

#### Método 2: Orquestração de Jobs
```python
# Crie jobs do Databricks para execução automatizada
# Job 1: Pipeline de Criadores
{
    "name": "Pipeline de Criadores",
    "notebook_task": {
        "notebook_path": "/Repos/<username>/dataeng/notebooks/creators_pipeline.py"
    },
    "cluster": {
        "existing_cluster_id": "<cluster-id>"
    }
}

# Job 2: Pipeline de Posts  
{
    "name": "Pipeline de Posts",
    "notebook_task": {
        "notebook_path": "/Repos/<username>/dataeng/notebooks/posts_pipeline.py"
    },
    "cluster": {
        "existing_cluster_id": "<cluster-id>"
    }
}
```

### 4. Personalização de Configuração

#### Atualizar Configuração do Pipeline
Edite `config/pipeline_config.py` para seu ambiente:

```python
# Exemplo: Altere nomes de catálogo e esquema
def get_bronze_creators_config() -> Dict[str, Any]:
    import os
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_path = os.path.join(repo_root, "data", "raw", "wiki_pages.json.gz")
    
    return {
        "layer": "bronze",
        "source": data_path,              # Caminho automático do repositório
        "catalog_name": "seu_catalogo",    # Altere isto
        "schema_name": "seu_esquema",      # Altere isto
        "table_name": "bronze_creators_scrape_wiki",
        # ... resto da configuração
    }
```

## Configurações Específicas do Ambiente

### Ambiente de Desenvolvimento
- Use conjuntos de dados menores para testes
- Configure log level para DEBUG
- Habilite profiling abrangente
- Use catálogo/esquema de desenvolvimento

### Ambiente de Produção
- Use conjuntos de dados completos
- Configure log level para INFO ou WARN
- Habilite configurações de otimização
- Use catálogo/esquema de produção
- Implemente tratamento de erros adequado e alertas

## Monitoramento e Manutenção

### Monitoramento de Qualidade de Dados
```sql
-- Verificar métricas de qualidade de dados após cada execução
SELECT 
    table_name,
    processing_timestamp,
    total_rows,
    overall_completeness_pct,
    quality_issues
FROM information_schema.table_comments
WHERE table_schema = 'seu_esquema'
ORDER BY processing_timestamp DESC;
```

### Otimização de Performance
- **Auto Optimize**: Habilitado por padrão nas operações de tabela
- **Z-Ordering**: Considere para colunas frequentemente consultadas
- **Vacuum**: Manutenção regular para tabelas Delta

```sql
-- Comandos de manutenção exemplo
OPTIMIZE seu_catalogo.seu_esquema.silver_posts_creator
ZORDER BY (creator_id, platform);

VACUUM seu_catalogo.seu_esquema.silver_posts_creator RETAIN 720 HOURS;
```

## Solução de Problemas

### Problemas Comuns

#### Erros de Importação
```python
# Se módulos não forem encontrados, certifique-se de que o caminho está corretamente definido:
import sys
import os
sys.path.append('/Workspace/Repos/<username>/dataeng/src')
```

#### Fonte de Dados Não Encontrada
- Verifique caminhos de arquivos na configuração
- Verifique permissões de arquivo
- Certifique-se de que arquivos foram carregados no local correto

#### Falhas na Criação de Tabela
- Verifique se catálogo e esquema existem
- Verifique permissões do usuário para criação de tabela
- Revise configuração do cluster

### Problemas de Performance
- Aumente o tamanho do cluster para conjuntos de dados grandes
- Habilite Auto Scaling
- Verifique Spark UI para oportunidades de otimização

## Procedimentos de Rollback

### Rollback de Código
```bash
# Usando Git no Databricks Repos
git checkout <hash-commit-anterior>
git pull origin main
```

### Rollback de Dados
```sql
-- Time travel do Delta Lake para recuperação de dados
SELECT * FROM sua_tabela VERSION AS OF 123
SELECT * FROM sua_tabela TIMESTAMP AS OF '2024-01-15 10:30:00'

-- Restaurar tabela para versão anterior
RESTORE TABLE sua_tabela TO VERSION AS OF 123
```