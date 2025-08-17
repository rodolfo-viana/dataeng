from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)


def transforma_timestamp(df: DataFrame, config: Dict[str, Any]) -> DataFrame:
    try:
        if config.get("layer").lower() == "silver":
            timestamp_columns = config.get("timestamp_columns", [])
            
            if not timestamp_columns:
                logger.info("Nenhuma coluna de timestamp especificada para conversão")
                return df
            
            logger.info(f"Convertendo colunas de timestamp: {timestamp_columns}")
            columns_converted = []
            
            for col_name in timestamp_columns:
                if col_name in df.columns:
                    logger.info(f"Convertendo coluna '{col_name}' de Unix timestamp para SQL timestamp")
                    df = df.withColumn(col_name, 
                        F.when(F.col(col_name).isNotNull() & (F.col(col_name) > 0),
                               F.from_unixtime(F.col(col_name)))
                        .otherwise(F.lit(None).cast("timestamp"))
                    )
                    columns_converted.append(col_name)
                else:
                    logger.warning(f"Coluna '{col_name}' especificada para conversão não encontrada no DataFrame")
            
            if columns_converted:
                logger.info(f"Conversão concluída para colunas: {', '.join(columns_converted)}")
            else:
                logger.info("Nenhuma coluna foi convertida")
                
        return df
    except Exception as e:
        raise Exception(f"Erro ao transformar dados: {str(e)}")
