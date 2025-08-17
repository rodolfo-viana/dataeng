from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


def cria_tabela(spark: SparkSession, df: DataFrame, config: Dict[str, Any]) -> bool:
    try:
        full_table_name = f"{config.get('catalog_name')}.{config.get('schema_name')}.{config.get('table_name')}"
        layer = config.get("layer").lower()
        logger.info(f"Iniciada a criação da tabela delta na camada {layer}: {full_table_name}")
    
        table_comment = f"Tabela na camada {layer} criada em {config.get('processing_timestamp').isoformat()}. Fonte: {config.get('source')}, Linhas: {config.get('total_rows')}"

        logger.info(f"Metadados da tabela - Camada: {layer}, Salvo em: {full_table_name}")
        logger.info(f"Tabela criada em {config.get('processing_timestamp').isoformat()}")
        logger.info(f"Linhas: {config.get('total_rows')}")
        
        if layer == "bronze":
            table_comment += f", Colunas: {config.get('column_count')}, Batch ID: {config.get('batch_id')}, Contém dados: {config.get('has_data')}."
            logger.info(f"Colunas: {config.get('column_count')}")
            logger.info(f"Batch ID: {config.get('batch_id')}")
            logger.info(f"Contém dados: {config.get('has_data')}")
        elif layer == "silver":
            table_comment += f", Colunas (exceto metadados): {config.get('total_business_columns')}, Contém dados: {config.get('has_data')}, Percentual de completude: {config.get('overall_completeness_pct'):.1f}%, Problemas de qualidade: {config.get('quality_issues')}, Qualidade atestada: {config.get('passes_quality_threshold')}."
            logger.info(f"Colunas (exceto metadados): {config.get('total_business_columns')}")
            logger.info(f"Contém dados: {config.get('has_data')}")
            logger.info(f"Percentual de completude: {config.get('overall_completeness_pct'):.1f}%")
            logger.info(f"Problemas de qualidade: {config.get('quality_issues')}")
            logger.info(f"Qualidade atestada: {config.get('passes_quality_threshold')}")
        else:
            raise Exception(f"A camada precisa ser 'bronze' ou 'silver'")
        
        writer = df.write \
                  .format("delta") \
                  .mode("overwrite") \
                  .option("overwriteSchema", "true") \
                  .option("delta.autoOptimize.optimizeWrite", "true") \
                  .option("delta.autoOptimize.autoCompact", "true")
            
        writer.saveAsTable(full_table_name)
        logger.info(f"Tabela criada com sucesso: {full_table_name}")
        spark.sql(f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ('comment' = '{table_comment}')")
        logger.info(f"Comentário com metadados adicionado com sucesso")
        return True
    except Exception as e:
        logger.error(f"Erro na criação da tabela {full_table_name}: {str(e)}")
        return False
