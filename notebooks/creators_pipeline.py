# Databricks notebook source
from pyspark.sql import SparkSession
import logging
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.abspath('')), 'src'))

from utils.data_validation import valida_fonte, ler_fonte, valida_dados
from utils.metadata_handler import adiciona_metadados
from utils.table_operations import cria_tabela
from utils.volume_operations import cria_volume
from config.pipeline_config import configura_bronze_creators, configura_silver_creators

# COMMAND ----------

# MAGIC %md
# MAGIC ## Camada bronze

# COMMAND ----------

bronze_config = configura_bronze_creators()
filename = bronze_config["source_filename"].split('.')[0]
cria_volume(spark, filename, dbutils)

logging.basicConfig(level=getattr(logging, bronze_config["log_level"]))
logger = logging.getLogger(__name__)

print(f"Pipeline da camada bronze iniciado em {bronze_config['processing_timestamp']}")
print(f"Fonte: {bronze_config['source']}")

# COMMAND ----------

try:
    if not valida_fonte(spark, bronze_config):
        raise FileNotFoundError(f"Fonte inacessível: {bronze_config['source']}")

    bronze_df = ler_fonte(spark, bronze_config)
    bronze_config = valida_dados(bronze_df, bronze_config)
    bronze_df = adiciona_metadados(bronze_df, bronze_config)
    
    success = cria_tabela(spark, bronze_df, bronze_config)
    if success:
        print("Camada bronze criada com sucesso!")
    else:
        raise Exception("Erro na criação da tabela bronze")
        
except Exception as e:
    print(f"Erro ao processar camada bronze: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Camada silver

# COMMAND ----------

silver_config = configura_silver_creators()

logging.basicConfig(level=getattr(logging, silver_config["log_level"]))
logger = logging.getLogger(__name__)

print(f"Pipeline da camada silver iniciado em {silver_config['processing_timestamp']}")
print(f"Fonte: {silver_config['source']}")

# COMMAND ----------

try:
    if not valida_fonte(spark, silver_config):
        raise FileNotFoundError(f"Fonte inacessível: {silver_config['source']}")

    silver_df = ler_fonte(spark, silver_config)
    silver_config = valida_dados(silver_df, silver_config)
    silver_df = adiciona_metadados(silver_df, silver_config)
    
    success = cria_tabela(spark, silver_df, silver_config)
    if success:
        print("Camada silver criada com sucesso!")
    else:
        raise Exception("Erro na criação da tabela silver")
        
except Exception as e:
    print(f"Erro ao processar camada silver: {str(e)}")
    raise

# COMMAND ----------

print("Pipeline de creators concluído com sucesso!")
