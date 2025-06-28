"""
Webhook Notifications Pipeline Delta Lake Configuration
True Delta Lake settings with ACID transactions and state tracking
"""

import os

# =============================================================================
# DELTA LAKE CONFIGURATION
# =============================================================================

# MinIO/S3 Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
WEBHOOK_BUCKET = 'webhook-notifications'

# Delta Lake Packages (using same versions as smart trader pipeline + Kafka for Redpanda)
DELTA_SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,io.delta:delta-spark_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

# =============================================================================
# ULTRA-SAFE SPARK CONFIGURATION
# =============================================================================

def get_webhook_delta_config(layer='bronze'):
    """Get optimized Delta Lake Spark configuration for webhook pipeline"""
    
    # Copy exact memory settings from working smart trader pipeline
    if layer == 'bronze':
        driver_memory = '768m'    # Same as smart trader pipeline
        executor_memory = '768m'  # Same as smart trader pipeline  
        driver_overhead = '128m'  # Conservative overhead
        executor_overhead = '128m'  # Conservative overhead
        max_result_size = '384m'  # Same as smart trader pipeline
    elif layer == 'silver':
        driver_memory = '2048m'  # Java-compatible format
        executor_memory = '2048m'  # Java-compatible format
        driver_overhead = '384m'
        executor_overhead = '384m'
        max_result_size = '768m'
    else:  # gold
        driver_memory = '2560m'  # Java-compatible format (2.5g = 2560m)
        executor_memory = '2560m'  # Java-compatible format
        driver_overhead = '512m'
        executor_overhead = '512m'
        max_result_size = '1024m'  # Java-compatible format
    
    return {
        # Delta Lake Extensions
        "spark.jars.packages": DELTA_SPARK_PACKAGES,
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        
        # MinIO S3A Configuration
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        
        # Optimized Memory Configuration
        "spark.driver.memory": driver_memory,
        "spark.driver.memoryOverhead": driver_overhead,
        "spark.executor.memory": executor_memory,
        "spark.executor.memoryOverhead": executor_overhead,
        "spark.driver.maxResultSize": max_result_size,
        "spark.sql.shuffle.partitions": "1",  # Minimal partitions
        "spark.executor.instances": "1",
        "spark.executor.cores": "1",  # Minimal cores
        
        # Ultra-conservative JVM settings to prevent gateway crashes
        "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:MaxHeapFreeRatio=30 -XX:MinHeapFreeRatio=10 -Xms512m",
        "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:MaxHeapFreeRatio=30 -XX:MinHeapFreeRatio=10 -Xms512m",
        
        # DISABLED Delta Lake Optimizations (causing memory issues)
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
        "spark.databricks.delta.optimizeWrite.enabled": "false",
        "spark.databricks.delta.autoCompact.enabled": "false",
        "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "false",
        "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": "false",
        
        # Enable backpressure and streaming optimizations
        "spark.streaming.backpressure.enabled": "true",
        "spark.streaming.kafka.maxRatePerPartition": "100",  # Max messages per partition per second
        "spark.sql.adaptive.enabled": "false",  # Keep disabled for predictability
        "spark.sql.adaptive.coalescePartitions.enabled": "false",
        "spark.sql.adaptive.skewJoin.enabled": "false",
        
        # Performance Tuning
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    }

# =============================================================================
# DELTA TABLE PATHS
# =============================================================================

def get_delta_table_path(table_name):
    """Get S3A path for Delta Lake table"""
    paths = {
        # Bronze Layer
        "bronze_webhooks": f"s3a://{WEBHOOK_BUCKET}/bronze/webhooks",
        
        # Silver Layer (future)
        "silver_transactions": f"s3a://{WEBHOOK_BUCKET}/silver/transactions",
        "silver_wallet_activities": f"s3a://{WEBHOOK_BUCKET}/silver/wallet_activities",
        
        # Gold Layer (future)
        "gold_trending_tokens": f"s3a://{WEBHOOK_BUCKET}/gold/trending_tokens",
        "gold_smart_trader_alerts": f"s3a://{WEBHOOK_BUCKET}/gold/smart_trader_alerts"
    }
    return paths.get(table_name)

# =============================================================================
# TRANSACTION CATEGORIZATION KEYWORDS
# =============================================================================

# Silver layer transaction categorization
SILVER_SWAP_KEYWORDS = ['swap', 'exchange', 'trade', 'dex']
SILVER_TRANSFER_KEYWORDS = ['transfer', 'send', 'receive']
SILVER_DFI_KEYWORDS = ['stake', 'unstake', 'lend', 'borrow', 'liquidity']

# =============================================================================
# BRONZE LAYER CONFIGURATION
# =============================================================================

# Redpanda/Kafka Configuration
REDPANDA_BROKERS = os.getenv('REDPANDA_BROKERS', 'redpanda:9092')
WEBHOOK_TOPIC = os.getenv('WEBHOOK_TOPIC', 'webhooks')

# Processing Configuration (OPTIMIZED for stability with backpressure)
BRONZE_BATCH_SIZE = 50  # Reduced batch size for memory safety
BRONZE_MAX_MESSAGES_PER_RUN = 200  # Limit total messages per run
BRONZE_PROCESSING_WINDOW_SECONDS = 30  # Process every 30 seconds (micro-batching)
BRONZE_MAX_OFFSETS_PER_TRIGGER = 50  # Kafka backpressure control
BRONZE_CHECKPOINT_LOCATION = f"s3a://{WEBHOOK_BUCKET}/_checkpoints/bronze_webhooks"

# State Tracking Configuration
BRONZE_STATE_REFRESH_HOURS = 24  # Re-process webhooks after 24 hours
BRONZE_DEDUP_WINDOW_HOURS = 48   # Deduplication window

# =============================================================================
# TABLE SCHEMAS & PARTITIONING
# =============================================================================

WEBHOOK_TABLE_CONFIGS = {
    "bronze_webhooks": {
        "partition_cols": ["processing_date"],
        "z_order_cols": ["message_id", "webhook_timestamp"],
        "optimize_frequency": "1 hour"
    }
}

# =============================================================================
# MONITORING & ALERTS
# =============================================================================

# Memory monitoring thresholds
MEMORY_WARNING_THRESHOLD = 70  # Warn at 70% memory usage
MEMORY_CRITICAL_THRESHOLD = 85  # Stop at 85% memory usage

# Processing metrics
ENABLE_PROCESSING_METRICS = True
METRICS_LOG_INTERVAL_SECONDS = 60