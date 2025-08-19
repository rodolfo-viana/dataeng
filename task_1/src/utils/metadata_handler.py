from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


def adiciona_metadados(df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    try:
        if config.get("layer").lower() == "bronze":
            return df.withColumn("_processing_timestamp", F.lit(config.get("processing_timestamp"))) \
                .withColumn("_source", F.lit(config.get("source"))) \
                .withColumn("_ingestion_batch_id", F.lit(config.get("batch_id")))
        elif config.get("layer").lower() == "silver":
            return df.withColumn("processing_timestamp", F.lit(config.get("processing_timestamp"))) \
                .withColumn("source", F.lit(config.get("source")))
        else:
            raise Exception(f"A camada precisa ser 'bronze' ou 'silver'")
    except Exception as e:
        logger.error(f"Erro ao adicionar metadados. Erro: {str(e)}")
        raise
