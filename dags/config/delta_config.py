"""
Delta Lake Configuration
Configuration settings for the smart-trader Delta Lake tables
"""

import os

# Delta Lake bucket and paths
DELTA_BUCKET = 'smart-trader'
DELTA_BASE_PATH = f's3a://{DELTA_BUCKET}'

# MinIO connection settings (same as existing config)
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')

# Bronze layer table paths
DELTA_BRONZE_PATH = f'{DELTA_BASE_PATH}/bronze'
DELTA_BRONZE_TOKEN_METRICS = f'{DELTA_BRONZE_PATH}/token_metrics'
DELTA_BRONZE_WHALE_HOLDERS = f'{DELTA_BRONZE_PATH}/whale_holders'
DELTA_BRONZE_TRANSACTION_HISTORY = f'{DELTA_BRONZE_PATH}/transaction_history'

# Silver layer table paths
DELTA_SILVER_PATH = f'{DELTA_BASE_PATH}/silver'
DELTA_SILVER_WALLET_PNL = f'{DELTA_SILVER_PATH}/wallet_pnl'
DELTA_SILVER_TRACKED_TOKENS = f'{DELTA_SILVER_PATH}/tracked_tokens'
DELTA_SILVER_ENRICHED_TRANSACTIONS = f'{DELTA_SILVER_PATH}/enriched_transactions'

# Gold layer table paths
DELTA_GOLD_PATH = f'{DELTA_BASE_PATH}/gold'
DELTA_GOLD_SMART_TRADERS = f'{DELTA_GOLD_PATH}/smart_traders'
DELTA_GOLD_PORTFOLIO_ANALYTICS = f'{DELTA_GOLD_PATH}/portfolio_analytics'

# PySpark configuration for Delta Lake (conservative memory settings)
DELTA_SPARK_CONFIG = {
    "spark.driver.memory": "512m",
    "spark.executor.memory": "512m", 
    "spark.driver.maxResultSize": "256m",
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.execution.arrow.pyspark.enabled": "false",
    
    # Delta Lake extensions (when available)
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    
    # MinIO S3A configuration
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
}

# Table-specific configurations
BRONZE_TABLE_CONFIGS = {
    "token_metrics": {
        "path": DELTA_BRONZE_TOKEN_METRICS,
        "partition_cols": ["ingestion_year", "ingestion_month", "ingestion_day"],
        "z_order_cols": ["liquidity", "volume_24h_usd"],
        "merge_keys": ["token_address", "ingestion_date"]
    },
    "whale_holders": {
        "path": DELTA_BRONZE_WHALE_HOLDERS,
        "partition_cols": ["rank_year", "rank_month"],
        "z_order_cols": ["holdings_value_usd", "rank"],
        "merge_keys": ["token_address", "wallet_address", "rank_date"]
    },
    "transaction_history": {
        "path": DELTA_BRONZE_TRANSACTION_HISTORY,
        "partition_cols": ["transaction_year", "transaction_month"],
        "z_order_cols": ["wallet_address", "transaction_timestamp"],
        "merge_keys": ["wallet_address", "transaction_hash"]
    }
}

# Migration settings
MIGRATION_BATCH_SIZE = 1000  # Records per batch during migration
MIGRATION_MAX_RETRIES = 3
MIGRATION_TIMEOUT_SECONDS = 300

# Data quality settings
DATA_QUALITY_THRESHOLDS = {
    "token_metrics": {
        "min_liquidity": 1000.0,  # Minimum liquidity for quality score
        "required_fields": ["token_address", "symbol", "liquidity", "price"]
    },
    "whale_holders": {
        "min_holdings_value": 100.0,  # Minimum USD value for quality
        "max_rank": 100,  # Maximum allowed rank
        "required_fields": ["token_address", "wallet_address", "rank"]
    },
    "transaction_history": {
        "min_usd_volume": 1.0,  # Minimum transaction value
        "required_fields": ["wallet_address", "transaction_hash", "transaction_timestamp"]
    }
}

# Optimization settings
OPTIMIZATION_SETTINGS = {
    "vacuum_retention_hours": 168,  # 7 days
    "optimize_frequency_hours": 24,  # Daily optimization
    "checkpoint_frequency": 10  # Every 10 commits
}

def get_table_path(layer: str, table_name: str) -> str:
    """Get the full S3 path for a specific table"""
    if layer == "bronze":
        return BRONZE_TABLE_CONFIGS[table_name]["path"]
    elif layer == "silver":
        return f"{DELTA_SILVER_PATH}/{table_name}"
    elif layer == "gold":
        return f"{DELTA_GOLD_PATH}/{table_name}"
    else:
        raise ValueError(f"Unknown layer: {layer}")

def get_table_config(table_name: str) -> dict:
    """Get configuration for a specific bronze table"""
    if table_name in BRONZE_TABLE_CONFIGS:
        return BRONZE_TABLE_CONFIGS[table_name]
    else:
        raise ValueError(f"Unknown table: {table_name}")

def get_spark_config_dict() -> dict:
    """Get Spark configuration as dictionary"""
    return DELTA_SPARK_CONFIG.copy()

# Legacy path mappings (for migration from old system)
LEGACY_PATHS = {
    "bronze_token_list": "s3://solana-data/bronze/token_list_v3",
    "bronze_whale_holders": "s3://solana-data/bronze/token_whales", 
    "bronze_transactions": "s3://solana-data/bronze/wallet_transactions"
}

def get_legacy_path(table_name: str) -> str:
    """Get legacy Parquet path for migration source"""
    if table_name in LEGACY_PATHS:
        return LEGACY_PATHS[table_name]
    else:
        raise ValueError(f"Unknown legacy table: {table_name}")

# Validation settings
VALIDATION_SETTINGS = {
    "compare_record_counts": True,
    "compare_schema_compatibility": True,
    "sample_data_comparison": True,
    "sample_size": 100
}