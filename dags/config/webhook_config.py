"""
Webhook Pipeline Configuration
Centralized settings for medallion architecture, processing limits, and analytics
"""

import os

# =============================================================================
# INFRASTRUCTURE CONFIGURATION
# =============================================================================

# Redpanda/Kafka Configuration
REDPANDA_BROKERS = os.getenv('REDPANDA_BROKERS', 'redpanda:9092')
WEBHOOK_TOPIC = os.getenv('WEBHOOK_TOPIC', 'webhooks')
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'webhook-processor')
KAFKA_AUTO_OFFSET_RESET = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'latest')
KAFKA_SESSION_TIMEOUT_MS = int(os.getenv('KAFKA_SESSION_TIMEOUT_MS', '30000'))
KAFKA_REQUEST_TIMEOUT_MS = int(os.getenv('KAFKA_REQUEST_TIMEOUT_MS', '40000'))

# MinIO/S3 Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'webhook-data')

# =============================================================================
# BRONZE LAYER CONFIGURATION
# =============================================================================

# Processing Limits & Batch Sizes
BRONZE_BATCH_SIZE_LIMIT = int(os.getenv('BRONZE_BATCH_SIZE_LIMIT', '1000'))  # Max messages per batch
BRONZE_PROCESSING_WINDOW_MINUTES = int(os.getenv('BRONZE_PROCESSING_WINDOW_MINUTES', '5'))
BRONZE_MAX_CONCURRENT_TASKS = int(os.getenv('BRONZE_MAX_CONCURRENT_TASKS', '3'))

# Memory Management
BRONZE_SPARK_DRIVER_MEMORY = os.getenv('BRONZE_SPARK_DRIVER_MEMORY', '2g')
BRONZE_SPARK_EXECUTOR_MEMORY = os.getenv('BRONZE_SPARK_EXECUTOR_MEMORY', '2g')
BRONZE_SPARK_MAX_RESULT_SIZE = os.getenv('BRONZE_SPARK_MAX_RESULT_SIZE', '1g')

# Data Retention & Cleanup
BRONZE_RETENTION_DAYS = int(os.getenv('BRONZE_RETENTION_DAYS', '30'))
BRONZE_CLEANUP_ENABLED = os.getenv('BRONZE_CLEANUP_ENABLED', 'true').lower() == 'true'
BRONZE_CHECKPOINT_CLEANUP_DAYS = int(os.getenv('BRONZE_CHECKPOINT_CLEANUP_DAYS', '7'))  # Clean checkpoints older than 7 days

# Schema & Validation
BRONZE_REQUIRE_WEBHOOK_ID = os.getenv('BRONZE_REQUIRE_WEBHOOK_ID', 'true').lower() == 'true'
BRONZE_VALIDATE_JSON = os.getenv('BRONZE_VALIDATE_JSON', 'true').lower() == 'true'
BRONZE_MAX_PAYLOAD_SIZE_MB = int(os.getenv('BRONZE_MAX_PAYLOAD_SIZE_MB', '10'))

# =============================================================================
# SILVER LAYER CONFIGURATION
# =============================================================================

# Processing Settings
SILVER_PROCESSING_WINDOW_MINUTES = int(os.getenv('SILVER_PROCESSING_WINDOW_MINUTES', '10'))
SILVER_BATCH_SIZE_LIMIT = int(os.getenv('SILVER_BATCH_SIZE_LIMIT', '5000'))
SILVER_MAX_CONCURRENT_TASKS = int(os.getenv('SILVER_MAX_CONCURRENT_TASKS', '2'))

# Data Quality Thresholds
SILVER_MIN_SIGNATURE_LENGTH = int(os.getenv('SILVER_MIN_SIGNATURE_LENGTH', '64'))
SILVER_MIN_ADDRESS_LENGTH = int(os.getenv('SILVER_MIN_ADDRESS_LENGTH', '32'))
SILVER_REQUIRE_TRANSACTION_DETAILS = os.getenv('SILVER_REQUIRE_TRANSACTION_DETAILS', 'true').lower() == 'true'

# Transaction Filtering
SILVER_SUPPORTED_TRANSACTION_TYPES = os.getenv('SILVER_SUPPORTED_TRANSACTION_TYPES', 'SWAP,TRANSFER,UNKNOWN').split(',')
SILVER_MIN_TOKEN_AMOUNT = float(os.getenv('SILVER_MIN_TOKEN_AMOUNT', '0.000001'))
SILVER_MAX_TOKEN_AMOUNT = float(os.getenv('SILVER_MAX_TOKEN_AMOUNT', '1000000000'))

# Event Categorization
SILVER_SWAP_KEYWORDS = ['swap', 'exchange', 'trade', 'dex']
SILVER_TRANSFER_KEYWORDS = ['transfer', 'send', 'receive']
SILVER_DFI_KEYWORDS = ['stake', 'unstake', 'lend', 'borrow', 'liquidity']

# Deduplication Settings
SILVER_DEDUP_WINDOW_HOURS = int(os.getenv('SILVER_DEDUP_WINDOW_HOURS', '24'))
SILVER_DEDUP_BY_SIGNATURE = os.getenv('SILVER_DEDUP_BY_SIGNATURE', 'true').lower() == 'true'

# Memory Configuration
SILVER_SPARK_DRIVER_MEMORY = os.getenv('SILVER_SPARK_DRIVER_MEMORY', '3g')
SILVER_SPARK_EXECUTOR_MEMORY = os.getenv('SILVER_SPARK_EXECUTOR_MEMORY', '3g')

# =============================================================================
# GOLD LAYER CONFIGURATION  
# =============================================================================

# Processing Settings
GOLD_PROCESSING_WINDOW_MINUTES = int(os.getenv('GOLD_PROCESSING_WINDOW_MINUTES', '30'))
GOLD_LOOKBACK_HOURS = int(os.getenv('GOLD_LOOKBACK_HOURS', '24'))
GOLD_MAX_CONCURRENT_TASKS = int(os.getenv('GOLD_MAX_CONCURRENT_TASKS', '1'))

# Trending Token Analysis
GOLD_TRENDING_MIN_VOLUME_USD = float(os.getenv('GOLD_TRENDING_MIN_VOLUME_USD', '100000'))
GOLD_TRENDING_MIN_TRANSACTIONS = int(os.getenv('GOLD_TRENDING_MIN_TRANSACTIONS', '100'))
GOLD_TRENDING_TOP_N = int(os.getenv('GOLD_TRENDING_TOP_N', '50'))
GOLD_TRENDING_TIME_WINDOWS = ['1h', '4h', '24h']

# Whale Activity Tracking
GOLD_WHALE_MIN_USD_VALUE = float(os.getenv('GOLD_WHALE_MIN_USD_VALUE', '10000'))
GOLD_WHALE_TOP_N_TRANSACTIONS = int(os.getenv('GOLD_WHALE_TOP_N_TRANSACTIONS', '100'))
GOLD_WHALE_ADDRESS_WHITELIST = []  # Known institutional wallets

# DEX Volume Analytics
GOLD_DEX_MIN_DAILY_VOLUME = float(os.getenv('GOLD_DEX_MIN_DAILY_VOLUME', '1000000'))
GOLD_SUPPORTED_DEXES = ['Raydium', 'Orca', 'Jupiter', 'Serum', 'Saber']

# Real-time Metrics
GOLD_METRICS_UPDATE_FREQUENCY_MINUTES = int(os.getenv('GOLD_METRICS_UPDATE_FREQUENCY_MINUTES', '5'))
GOLD_ALERT_THRESHOLDS = {
    'volume_spike_multiplier': 3.0,
    'price_change_percent': 20.0,
    'whale_transaction_usd': 50000
}

# =============================================================================
# STORAGE PATHS & PARTITIONING
# =============================================================================

# Bronze Layer Paths
BRONZE_WEBHOOKS_PATH = "bronze/webhooks"
BRONZE_RAW_EVENTS_PATH = "bronze/raw_events"

# Silver Layer Paths  
SILVER_WEBHOOK_EVENTS_PATH = "silver/webhook_events"
SILVER_TRANSACTION_DETAILS_PATH = "silver/transaction_details"
SILVER_SWAP_TRANSACTIONS_PATH = "silver/swap_transactions"
SILVER_DATA_QUALITY_METRICS_PATH = "silver/data_quality_metrics"

# Gold Layer Paths
GOLD_TRENDING_TOKENS_PATH = "gold/trending_tokens"
GOLD_WHALE_ACTIVITY_PATH = "gold/whale_activity"
GOLD_DEX_VOLUMES_PATH = "gold/dex_volumes"
GOLD_REAL_TIME_METRICS_PATH = "gold/real_time_metrics"

# Partitioning Strategy
PARTITION_BY_DATE = True
PARTITION_BY_HOUR = True
PARTITION_DATE_FORMAT = "yyyy-MM-dd"
PARTITION_HOUR_FORMAT = "HH"

# =============================================================================
# PYSPARK CONFIGURATION
# =============================================================================

# Spark Packages
SPARK_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367"

# Spark Session Configuration
SPARK_APP_NAME_PREFIX = "webhook-pipeline"
SPARK_SQL_ADAPTIVE_ENABLED = True
SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS = True
SPARK_SERIALIZER = "org.apache.spark.serializer.KryoSerializer"
SPARK_SQL_EXECUTION_ARROW_PYSPARK_ENABLED = True

# Kafka Integration Settings
KAFKA_STARTING_OFFSETS = "latest"  # For streaming; can be "earliest" for batch backfill
KAFKA_MAX_OFFSETS_PER_TRIGGER = BRONZE_BATCH_SIZE_LIMIT

# =============================================================================
# MONITORING & ALERTS
# =============================================================================

# Health Check Settings
HEALTH_CHECK_ENABLED = os.getenv('HEALTH_CHECK_ENABLED', 'true').lower() == 'true'
HEALTH_CHECK_INTERVAL_MINUTES = int(os.getenv('HEALTH_CHECK_INTERVAL_MINUTES', '5'))

# Processing Status Tracking
STATUS_TRACKING_ENABLED = True
STATUS_SUCCESS_MARKER_ENABLED = True
STATUS_BATCH_ID_FORMAT = "%Y%m%d_%H%M%S"

# Data Quality Monitoring
DQ_VALIDATION_ENABLED = True
DQ_ALERT_ON_QUALITY_DROP = True
DQ_MIN_SUCCESS_RATE_PERCENT = float(os.getenv('DQ_MIN_SUCCESS_RATE_PERCENT', '95.0'))

# Performance Monitoring
PERF_MONITORING_ENABLED = True
PERF_LOG_PROCESSING_TIMES = True
PERF_ALERT_ON_SLOW_PROCESSING = True
PERF_MAX_PROCESSING_TIME_MINUTES = int(os.getenv('PERF_MAX_PROCESSING_TIME_MINUTES', '30'))

# =============================================================================
# LEGACY SUPPORT & MIGRATION
# =============================================================================

# Legacy Path Support (for migration period)
LEGACY_PROCESSED_WEBHOOKS_PATH = "processed-webhooks"
LEGACY_SUPPORT_ENABLED = True
MIGRATION_MODE = os.getenv('MIGRATION_MODE', 'false').lower() == 'true'

# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def get_spark_config(layer='bronze'):
    """Get PySpark configuration dictionary for specified layer"""
    if layer == 'bronze':
        driver_memory = BRONZE_SPARK_DRIVER_MEMORY
        executor_memory = BRONZE_SPARK_EXECUTOR_MEMORY
    elif layer == 'silver':
        driver_memory = SILVER_SPARK_DRIVER_MEMORY
        executor_memory = SILVER_SPARK_EXECUTOR_MEMORY
    else:  # gold
        driver_memory = '4g'  # Gold layer needs more memory for analytics
        executor_memory = '4g'
    
    return {
        "spark.jars.packages": SPARK_PACKAGES,
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.sql.adaptive.enabled": str(SPARK_SQL_ADAPTIVE_ENABLED).lower(),
        "spark.sql.adaptive.coalescePartitions.enabled": str(SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS).lower(),
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.driver.memory": driver_memory,
        "spark.executor.memory": executor_memory,
        "spark.driver.maxResultSize": BRONZE_SPARK_MAX_RESULT_SIZE,
        "spark.serializer": SPARK_SERIALIZER,
        "spark.sql.execution.arrow.pyspark.enabled": str(SPARK_SQL_EXECUTION_ARROW_PYSPARK_ENABLED).lower()
    }

def get_s3_path(path_suffix):
    """Get full S3 path for a dataset"""
    return f"s3a://{MINIO_BUCKET}/{path_suffix}/"

def get_kafka_config():
    """Get Kafka consumer configuration"""
    return {
        'bootstrap.servers': REDPANDA_BROKERS,
        'group.id': KAFKA_CONSUMER_GROUP,
        'auto.offset.reset': KAFKA_AUTO_OFFSET_RESET,
        'session.timeout.ms': KAFKA_SESSION_TIMEOUT_MS,
        'request.timeout.ms': KAFKA_REQUEST_TIMEOUT_MS,
        'enable.auto.commit': False  # Manual commit for better control
    }

def get_processing_status_fields():
    """Get standard processing status fields for medallion architecture"""
    return {
        'processed_for_silver': False,
        'silver_processed_at': None,
        'processed_for_gold': False, 
        'gold_processed_at': None,
        'processing_status': 'pending',
        'processing_errors': None
    }

def get_partition_path(base_path, processing_date=None, processing_hour=None):
    """Generate partitioned path for data storage"""
    path = base_path
    
    if PARTITION_BY_DATE and processing_date:
        path += f"/processing_date={processing_date}"
    
    if PARTITION_BY_HOUR and processing_hour:
        path += f"/processing_hour={processing_hour:02d}"
    
    return path

def get_layer_config(layer):
    """Get layer-specific configuration"""
    configs = {
        'bronze': {
            'batch_size_limit': BRONZE_BATCH_SIZE_LIMIT,
            'processing_window_minutes': BRONZE_PROCESSING_WINDOW_MINUTES,
            'spark_driver_memory': BRONZE_SPARK_DRIVER_MEMORY,
            'spark_executor_memory': BRONZE_SPARK_EXECUTOR_MEMORY,
            'max_concurrent_tasks': BRONZE_MAX_CONCURRENT_TASKS
        },
        'silver': {
            'batch_size_limit': SILVER_BATCH_SIZE_LIMIT,
            'processing_window_minutes': SILVER_PROCESSING_WINDOW_MINUTES,
            'spark_driver_memory': SILVER_SPARK_DRIVER_MEMORY,
            'spark_executor_memory': SILVER_SPARK_EXECUTOR_MEMORY,
            'max_concurrent_tasks': SILVER_MAX_CONCURRENT_TASKS
        },
        'gold': {
            'processing_window_minutes': GOLD_PROCESSING_WINDOW_MINUTES,
            'lookback_hours': GOLD_LOOKBACK_HOURS,
            'max_concurrent_tasks': GOLD_MAX_CONCURRENT_TASKS
        }
    }
    
    return configs.get(layer, {})

# =============================================================================
# REQUIRED AIRFLOW VARIABLES
# =============================================================================

REQUIRED_AIRFLOW_VARIABLES = [
    'WEBHOOK_LISTENER_URL',       # FastAPI webhook listener endpoint
    'REDPANDA_ADMIN_API_URL',     # Redpanda admin API for topic management
    'ALERT_WEBHOOK_URL',          # Alerts and notifications endpoint
    'DQ_ALERT_EMAIL'              # Data quality alert recipients
]

# Solana Constants
SOL_TOKEN_ADDRESS = "So11111111111111111111111111111111111111112"
WSOL_TOKEN_ADDRESS = "So11111111111111111111111111111111111111112"

# =============================================================================
# FEATURE FLAGS
# =============================================================================

# Enable/disable specific features
ENABLE_BRONZE_PROCESSING = True
ENABLE_SILVER_PROCESSING = True  
ENABLE_GOLD_PROCESSING = True
ENABLE_REAL_TIME_ALERTS = False  # Disabled by default
ENABLE_DATA_QUALITY_CHECKS = True
ENABLE_PERFORMANCE_MONITORING = True