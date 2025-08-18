from datetime import datetime
from typing import Dict, Any
import uuid


def configura_bronze_creators() -> Dict[str, Any]:
    return {
        "layer": "bronze",
        "source": "/Volumes/workspace/default/dataeng_raw/wiki_pages.json.gz",
        "source_filename": "wiki_pages.json.gz",
        "catalog_name": "workspace",
        "schema_name": "default",
        "table_name": "bronze_creators_scrape_wiki",
        "processing_timestamp": datetime.now(),
        "batch_id": str(uuid.uuid4())[:8],
        "log_level": "INFO"
    }


def configura_silver_creators() -> Dict[str, Any]:
    return {
        "layer": "silver",
        "source": "workspace.default.bronze_creators_scrape_wiki",
        "catalog_name": "workspace",
        "schema_name": "default",
        "table_name": "silver_creators_scrape_wiki",
        "processing_timestamp": datetime.now(),
        "enable_optimization": False,
        "enable_comprehensive_profiling": True,
        "data_quality_threshold": 95.0,
        "log_level": "INFO"
    }


def configura_bronze_posts() -> Dict[str, Any]:
    return {
        "layer": "bronze",
        "source": "/Volumes/workspace/default/dataeng_raw/posts_creator.json.gz",
        "source_filename": "posts_creator.json.gz",
        "catalog_name": "workspace",
        "schema_name": "default",
        "table_name": "bronze_post_creator",
        "processing_timestamp": datetime.now(),
        "batch_id": str(uuid.uuid4())[:8],
        "log_level": "INFO"
    }


def configura_silver_posts() -> Dict[str, Any]:
    return {
        "layer": "silver",
        "source": "workspace.default.bronze_post_creator",
        "catalog_name": "workspace",
        "schema_name": "default",
        "table_name": "silver_post_creator",
        "processing_timestamp": datetime.now(),
        "enable_optimization": False,
        "enable_comprehensive_profiling": True,
        "data_quality_threshold": 95.0,
        "timestamp_columns": ["published_at"],
        "log_level": "INFO"
    }
