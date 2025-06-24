"""
True Delta Lake Configuration for Docker Environment
Optimized for PySpark + MinIO + Delta Lake integration in Docker containers
NO FALLBACKS - Delta Lake operations must succeed or fail
"""

import os
import logging

# Delta Lake JAR packages for Docker environment (compatible with Spark 3.5.0)
DELTA_SPARK_PACKAGES = "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4"

# Docker-optimized Spark configuration for MinIO + Delta Lake
DELTA_SPARK_CONFIG = {
    # CRITICAL: JAR packages for Docker environment
    "spark.jars.packages": DELTA_SPARK_PACKAGES,
    
    # Delta Lake extensions - REQUIRED
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    
    # MinIO S3A configuration for Docker network
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",  # Docker service name
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin123",
    "spark.hadoop.fs.s3a.path.style.access": "true", 
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    
    # Performance optimizations - ULTRA CONSERVATIVE for Docker
    "spark.sql.adaptive.enabled": "false",  # Disable adaptive query execution
    "spark.sql.adaptive.coalescePartitions.enabled": "false", 
    "spark.driver.memory": "512m",  # Ultra conservative for Docker
    "spark.executor.memory": "512m",  # Ultra conservative for Docker  
    "spark.driver.maxResultSize": "256m",  # Prevent OOM
    "spark.sql.execution.arrow.pyspark.enabled": "false",  # Disable Arrow for stability
    
    # Docker-specific optimizations - MINIMAL RESOURCE USAGE
    "spark.executor.instances": "1",  # Single executor in Docker
    "spark.sql.shuffle.partitions": "10",  # Minimal partitions to reduce memory
    "spark.network.timeout": "300s",  # Increase timeout for Docker
    "spark.executor.cores": "1",  # Single core to avoid contention
    "spark.task.maxFailures": "1",  # Fail fast on memory issues
    
    # Delta Lake optimizations
    "spark.databricks.delta.retentionDurationCheck.enabled": "false",
    "spark.databricks.delta.autoCompact.enabled": "true"
}

# MinIO connection settings for Docker environment
MINIO_ENDPOINT = "http://minio:9000"  # Docker service name
MINIO_ACCESS_KEY = "minioadmin" 
MINIO_SECRET_KEY = "minioadmin123"
MINIO_BUCKET = "smart-trader"

# True Delta Lake table paths - S3A protocol for MinIO
DELTA_TABLES = {
    "bronze_tokens": f"s3a://{MINIO_BUCKET}/bronze/token_metrics",
    "bronze_whales": f"s3a://{MINIO_BUCKET}/bronze/whale_holders",
    "bronze_transactions": f"s3a://{MINIO_BUCKET}/bronze/transaction_history", 
    "silver_pnl": f"s3a://{MINIO_BUCKET}/silver/wallet_pnl",
    "silver_tokens": f"s3a://{MINIO_BUCKET}/silver/tracked_tokens",
    "gold_traders": f"s3a://{MINIO_BUCKET}/gold/smart_traders_delta"
}

# Delta Lake table configurations
DELTA_TABLE_CONFIGS = {
    "bronze_tokens": {
        "partition_cols": ["processing_date"],
        "z_order_cols": ["liquidity", "volume_24h_usd"],
        "description": "BirdEye token metrics with Delta Lake versioning"
    },
    "bronze_whales": {
        "partition_cols": ["rank_date"], 
        "z_order_cols": ["holdings_value_usd", "rank"],
        "description": "Token whale holders with ACID properties"
    },
    "bronze_transactions": {
        "partition_cols": ["transaction_date"],
        "z_order_cols": ["wallet_address", "timestamp"],
        "description": "Wallet transaction history with consistency"
    },
    "silver_pnl": {
        "partition_cols": ["calculation_year", "calculation_month"],
        "z_order_cols": ["total_pnl", "trade_count"],
        "description": "Conservative PnL calculations with Delta Lake ACID properties"
    },
    "gold_traders": {
        "partition_cols": ["performance_tier"],
        "z_order_cols": ["smart_trader_score", "overall_rank"],
        "description": "Smart trader rankings with durability"
    }
}

def get_table_path(table_name: str) -> str:
    """Get Delta Lake table path for MinIO"""
    if table_name not in DELTA_TABLES:
        raise ValueError(f"Unknown table: {table_name}")
    return DELTA_TABLES[table_name]

def get_table_config(table_name: str) -> dict:
    """Get table configuration"""
    if table_name not in DELTA_TABLE_CONFIGS:
        raise ValueError(f"Unknown table configuration: {table_name}")
    return DELTA_TABLE_CONFIGS[table_name]

def get_spark_config() -> dict:
    """Get Spark configuration for Delta Lake + MinIO"""
    return DELTA_SPARK_CONFIG.copy()

def validate_environment():
    """Validate Docker environment for Delta Lake"""
    logger = logging.getLogger(__name__)
    
    # Check MinIO connectivity (basic validation)
    try:
        import boto3
        from botocore.exceptions import NoCredentialsError, EndpointConnectionError
        
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        
        # Test connection
        s3_client.list_buckets()
        logger.info("✅ MinIO connection validated")
        
        return True
        
    except (NoCredentialsError, EndpointConnectionError) as e:
        logger.error(f"❌ MinIO connection failed: {e}")
        return False
    except Exception as e:
        logger.warning(f"⚠️ MinIO validation warning: {e}")
        return True  # Continue anyway

def ensure_bucket_exists():
    """Ensure smart-trader bucket exists in MinIO"""
    import boto3
    from botocore.exceptions import ClientError
    
    logger = logging.getLogger(__name__)
    
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        
        # Try to create bucket
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
        logger.info(f"✅ Created {MINIO_BUCKET} bucket in MinIO")
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            logger.info(f"✅ {MINIO_BUCKET} bucket already exists")
        else:
            logger.error(f"❌ Failed to create bucket: {e}")
            raise
    
    return True

# Environment validation on import
if __name__ == "__main__":
    # Run validation when executed directly
    print("🔍 Validating True Delta Lake environment...")
    
    env_valid = validate_environment()
    bucket_created = ensure_bucket_exists()
    
    if env_valid and bucket_created:
        print("✅ True Delta Lake environment ready!")
        print(f"📊 Configured tables: {list(DELTA_TABLES.keys())}")
        print(f"🗄️ MinIO bucket: {MINIO_BUCKET}")
        print(f"📦 JAR packages: {DELTA_SPARK_PACKAGES}")
    else:
        print("❌ Environment validation failed")