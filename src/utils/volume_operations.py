from pyspark.sql import SparkSession
from typing import Dict, Any, List
import logging
import os

logger = logging.getLogger(__name__)


def carrega_volume(spark: SparkSession, file_name: str, volume_path: str = "/Volumes/workspace/default/dataeng_raw") -> bool:
    try:
        file_path = f"../data/raw/{file_name}.json.gz"
        
        print(f"Carregando arquivos no volume: {volume_path}")
        print(f"Carregando: {file_name}")
            
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {file_name}.json.gz. Arquivos devem estar contidos no reposiório, em 'data/raw'")
            
        try:
            destination_path = f"{volume_path}/{file_name}.json.gz"
            print(f"Copiando para: {destination_path}")
            dbutils.fs.cp(f"file:{file_path}", destination_path)
            print(f"Carregamento concluído: {file_name}")
                
        except Exception as e:
            raise Exception(f"Falha no carregamento de {file_name}: {str(e)}")
        
        print("Processo de carregamento concluído!")
        return True
        
    except Exception as e:
        logger.error(f"Erro no carregamento: {str(e)}")
        return False
    

def cria_volume(spark: SparkSession, file_name: str, volume_path: str = "/Volumes/workspace/default/dataeng_raw") -> bool:
    try:
        path_parts = volume_path.strip('/').split('/')
        if len(path_parts) < 4 or path_parts[0] != 'Volumes':
            raise ValueError(f"Caminho inválido: {volume_path}")
        
        catalog = path_parts[1]
        schema = path_parts[2] 
        volume_name = path_parts[3]
        
        create_sql = f"""
        CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}
        """
        
        logger.info(f"Criando volume: {catalog}.{schema}.{volume_name}")
        spark.sql(create_sql)
        logger.info(f"Volume criado ou já existente: {volume_path}")
        carrega_volume(spark, file_name, volume_path)
        return True
        
    except Exception as e:
        logger.error(f"Erro na criação do volume {volume_path}: {str(e)}")
        return False

