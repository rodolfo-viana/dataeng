# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import requests
import re
import logging
import time
import sys
import os
from urllib.parse import unquote
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(os.path.abspath('')), 'src'))

from utils.data_validation import valida_fonte, ler_fonte
from utils.table_operations import cria_tabela
from config.pipeline_config import configura_users_yt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

config = configura_users_yt()

# Páginas podem ser redirecionadas, então definimos o nome do creator
# usando a API da Wikipedia para garantir que estamos pegando o nome correto
def define_creator_wiki(creator, config=config):
    try:
        url = config["api_endpoint"]
        params = {
            "action": "parse",
            "page": creator,
            "format": "json"
        }

        response = requests.get(url, params=params, timeout=10, headers=config['headers'])
        response.raise_for_status()
        data = response.json()
        text = data['parse']['text']['*']
        if 'Redirect to:' in text:
            m = re.search(r'href=[\'"]/wiki/([^\'"#?]+)', text)
            return unquote(m.group(1)) if m else None
        else:
            return creator
    except Exception as e:
        logger.error(f"Erro ao acessar a API da Wikipedia para definir o nome do creator {creator}: {e}")
        raise


def extrai_user_id_youtube(creator, config=config):
    try:
        url = config["api_endpoint"]
        params = {
            "action": "parse",
            "page": creator,
            "format": "json"
        }
        
        response = requests.get(url, params=params, timeout=10, headers=config["headers"])
        response.raise_for_status()
        data = response.json()
        
        if 'error' in data:
            logger.warning(f"Página não encontrada na Wikipedia: {creator}")
            return None
            
        if 'parse' not in data or 'text' not in data['parse']:
            logger.warning(f"Conteúdo não disponível para: {creator}")
            return None
            
        html_content = data['parse']['externallinks']
        
        youtube_patterns = r"https?://(?:www\.)?youtube\.com/(?:user/|c/|channel/|@)([a-zA-Z0-9_.-]+)"

        for item in html_content:
            matches = re.findall(youtube_patterns, item, re.IGNORECASE)
            if matches:
                valid_matches = [
                    match for match in matches 
                    if len(match) > 2 and 
                    match.lower() not in ['watch', 'embed', 'playlist', 'results', 'feed', 'trending']
                ]
                
                if valid_matches:
                    user_id = valid_matches[0]
                    logger.info(f"User ID encontrado para {creator}: {user_id}")
                    return user_id
                else:
                    logger.warning(f"Nenhum user_id do YouTube encontrado para: {creator}")
                    return None
        
    except requests.RequestException as e:
        logger.error(f"Erro na requisição para {creator}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao processar {creator}: {str(e)}")
        return None


def processa_creators_batch(creators_list, config=config):
    results = []
    total_creators = len(creators_list)
    
    logger.info(f"Processando {total_creators} creators em lotes de {config['batch_size']}")
    
    for i in range(0, total_creators, config['batch_size']):
        batch = creators_list[i:i + config['batch_size']]
        batch_num = (i // config['batch_size']) + 1
        total_batches = (total_creators + config['batch_size'] - 1) // config['batch_size']
        
        logger.info(f"Processando lote {batch_num}/{total_batches}")
        
        for wiki_page in batch:
            real_user_id = define_creator_wiki(wiki_page)
            user_id = extrai_user_id_youtube(real_user_id)
            
            if user_id:
                results.append({
                    'wiki_page': wiki_page,
                    'user_id': user_id
                })
            
            time.sleep(0.5)
        
        if i + config['batch_size'] < total_creators:
            logger.info("Pausa entre lotes...")
            time.sleep(1)
    
    return results

# COMMAND ----------

logging.basicConfig(level=getattr(logging, config["log_level"]))
logger = logging.getLogger(__name__)

logger.info(f"Pipeline iniciado em {config['processing_timestamp']}")
logger.info(f"Fonte: {config['source']}")
logger.info(f"Endpoint da API: {config['api_endpoint']}")

if not valida_fonte(spark, config):
    raise FileNotFoundError(f"Tabela fonte não encontrada: {config['source']}")

creators_df = ler_fonte(spark, config)
logger.info(f"Tabela {config['source']} carregada com sucesso")

creators_list = [row.wiki_page for row in creators_df.select("wiki_page").collect()]
logger.info(f"Total de creators para processar: {len(creators_list)}")

# COMMAND ----------

results = processa_creators_batch(creators_list, config)

logger.info(f"Processamento concluído. User IDs encontrados: {len(results)}")
logger.info(f"Taxa de sucesso: {len(results)}/{len(creators_list)} ({len(results)/len(creators_list)*100:.1f}%)")

# COMMAND ----------

if results:
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("wiki_page", StringType(), True)
    ])
    
    users_yt_df = spark.createDataFrame(results, schema)
    
    logger.info("Exemplos de dados extraídos:")
    users_yt_df.show(10, truncate=False)
    
    total_rows = users_yt_df.count()
    logger.info(f"Total de registros criados: {total_rows}")
    
else:
    logger.warning("Nenhum user_id foi extraído com sucesso")
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("wiki_page", StringType(), True)
    ])
    users_yt_df = spark.createDataFrame([], schema)

# COMMAND ----------

config.update({
    "layer": "bronze",
    "total_rows": users_yt_df.count(),
    "column_count": len(users_yt_df.columns),
    "has_data": users_yt_df.count() > 0
})

try:
    success = cria_tabela(spark, users_yt_df, config)
    
    if success:
        logger.info(f"Tabela {config['catalog_name']}.{config['schema_name']}.{config['table_name']} criada com sucesso!")
        logger.info(f"Total de registros salvos: {config['total_rows']}")
        
    else:
        raise Exception("Falha na criação da tabela")
        
except Exception as e:
    logger.error(f"Erro ao criar tabela: {str(e)}")
    raise
