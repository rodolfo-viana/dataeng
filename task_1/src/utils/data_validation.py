from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


def valida_fonte(spark: SparkSession, config: Dict[str, Any]) -> bool:
    try:
        if config.get("source").startswith("/"):
            test_origin = spark.read.json(config.get("source")).limit(1)
        else:
            test_origin = spark.table(config.get("source")).limit(1)            
        test_origin.count()
        logger.info(f"Fonte validada: {config.get('source')}")
        return True
    except Exception as e:
        logger.error(f"Fonte inválida: {config.get('source')}. Erro: {str(e)}")
        return False


def ler_fonte(spark: SparkSession, config: Dict[str, Any]) -> DataFrame:
    try:
        if config.get("source").startswith("/"):
            origin = spark.read.json(config.get("source"))
        else:
            origin = spark.table(config.get("source"))
            origin = origin.select([col for col in origin.columns if not col.startswith('_')])         
        return origin
    except Exception as e:
        raise Exception(f"Fonte inválida: {config.get('source')}. Erro: {str(e)}")


def valida_dados(df: DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
    try:
        total_rows = df.count()
        if config.get("layer").lower() == "bronze":
            column_count = len(df.columns)
            config.update({
                "total_rows": total_rows,
                "column_count": column_count,
                "has_data": total_rows > 0
            })
            return config
        elif config.get("layer").lower() == "silver":
            column_analysis = {}
            null_counts = {}
            for col_name in df.columns:
                if col_name.startswith('_'):
                    continue
            
                null_count = df.filter(F.col(col_name).isNull()).count()
                null_counts[col_name] = null_count
                column_analysis[col_name] = {
                    "completeness_pct": ((total_rows - null_count) / total_rows * 100) if total_rows > 0 else 0
                }
            
            completeness_scores = [analysis["completeness_pct"] for analysis in column_analysis.values()]
            overall_completeness = sum(completeness_scores) / len(completeness_scores) if completeness_scores else 0

            quality_issues = []
            for col_name, analysis in column_analysis.items():
                if analysis["completeness_pct"] < 95.0:
                    quality_issues.append({
                        "column": col_name,
                        "issue": "low_completeness",
                        "completeness_pct": analysis["completeness_pct"]
                    })
            config.update({
                "total_rows": total_rows,
                "total_business_columns": len(column_analysis),
                "column_analysis": column_analysis,
                "overall_completeness_pct": overall_completeness,
                "quality_issues": quality_issues,
                "passes_quality_threshold": overall_completeness >= 95.0,
                "has_data": total_rows > 0
            })
            return config
        else:
            raise Exception(f"A camada precisa ser 'bronze' ou 'silver'")
    except Exception as e:
        logger.error(f"Erro ao validar dados. Erro: {str(e)}")
        return config
