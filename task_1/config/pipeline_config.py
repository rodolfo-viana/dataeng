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


def configura_users_yt() -> Dict[str, Any]:
    return {
        "layer": "final",
        "source": "workspace.default.bronze_creators_scrape_wiki",
        "catalog_name": "workspace",
        "schema_name": "default",
        "table_name": "users_yt",
        "processing_timestamp": datetime.now(),
        "batch_id": str(uuid.uuid4())[:8],
        "api_endpoint": "https://en.wikipedia.org/w/api.php",
        "headers": {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"},
        "batch_size": 3,
        "rate_limit_delay": 0.5,
        "batch_delay": 2,
        "log_level": "INFO"
    }
