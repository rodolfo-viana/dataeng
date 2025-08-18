from pyspark.sql import SparkSession
from typing import Dict, Any, List
import logging
import os

logger = logging.getLogger(__name__)


def carrega_volume(spark: SparkSession, file_name: str, dbutils, volume_path: str = "/Volumes/workspace/default/dataeng_raw") -> bool:
    """
    Carrega arquivo do workspace para o volume
    """
    try:
        # Caminho do arquivo no workspace (repositório Git)
        workspace_source_path = f"/Workspace/Users/eu@rodolfoviana.com.br/dataeng/data/raw/{file_name}.json.gz"
        
        print(f"=== CARREGAMENTO DE ARQUIVO ===")
        print(f"Volume destino: {volume_path}")
        print(f"Arquivo: {file_name}")
        print(f"Origem: {workspace_source_path}")
        
        # Verifica se o arquivo existe no workspace
        print("Verificando se arquivo existe...")
        try:
            file_info = dbutils.fs.ls(workspace_source_path)
            print(f"✅ Arquivo encontrado: {workspace_source_path}")
        except Exception as e:
            error_msg = f"❌ Arquivo não encontrado no workspace: {workspace_source_path}"
            print(error_msg)
            raise FileNotFoundError(error_msg)
        
        # Copia arquivo para o volume
        destination_path = f"{volume_path}/{file_name}.json.gz"
        print(f"Copiando de: {workspace_source_path}")
        print(f"Copiando para: {destination_path}")
        
        try:
            # Usa dbutils.fs.cp SEM o prefixo file: para evitar URI errors
            dbutils.fs.cp(workspace_source_path, destination_path)
            print(f"✅ Carregamento concluído: {file_name}")
            
            # Verifica se a cópia foi bem-sucedida
            volume_files = dbutils.fs.ls(volume_path)
            file_found = any(f"{file_name}.json.gz" in f.name for f in volume_files)
            if file_found:
                print(f"✅ Arquivo confirmado no volume: {destination_path}")
            else:
                print(f"⚠️  Arquivo não confirmado no volume")
                
        except Exception as e:
            error_msg = f"Falha ao copiar {file_name}: {str(e)}"
            print(f"❌ {error_msg}")
            raise Exception(error_msg)
        
        print("=== CARREGAMENTO CONCLUÍDO ===")
        return True
        
    except Exception as e:
        logger.error(f"Erro no carregamento: {str(e)}")
        print(f"❌ ERRO: {str(e)}")
        return False


def cria_volume(spark: SparkSession, file_name: str, dbutils, volume_path: str = "/Volumes/workspace/default/dataeng_raw") -> bool:
    """
    Cria volume e carrega arquivo
    """
    try:
        print(f"=== CRIAÇÃO DE VOLUME ===")
        print(f"Volume: {volume_path}")
        
        # Extrai componentes do caminho do volume
        path_parts = volume_path.strip('/').split('/')
        if len(path_parts) < 4 or path_parts[0] != 'Volumes':
            raise ValueError(f"Caminho de volume inválido: {volume_path}")
        
        catalog = path_parts[1]
        schema = path_parts[2] 
        volume_name = path_parts[3]
        
        # Cria volume usando SQL
        create_sql = f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}"
        
        print(f"Executando: {create_sql}")
        spark.sql(create_sql)
        print(f"✅ Volume criado ou já existe: {volume_path}")
        
        # Lista conteúdo do volume para verificar
        try:
            volume_contents = dbutils.fs.ls(volume_path)
            print(f"Conteúdo atual do volume ({len(volume_contents)} itens):")
            for item in volume_contents:
                print(f"  - {item.name}")
        except Exception:
            print("Volume vazio ou criado recentemente")
        
        # Carrega o arquivo
        print(f"\n=== INICIANDO CARREGAMENTO DO ARQUIVO ===")
        success = carrega_volume(spark, file_name, dbutils, volume_path)
        
        if success:
            print(f"🎉 Volume e arquivo configurados com sucesso!")
        else:
            print(f"❌ Falha na configuração")
            
        return success
        
    except Exception as e:
        error_msg = f"Erro na criação do volume {volume_path}: {str(e)}"
        logger.error(error_msg)
        print(f"❌ {error_msg}")
        return False
