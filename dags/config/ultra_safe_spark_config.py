"""
Ultra-Safe Spark Configuration for Docker Environment
Prevents JVM crashes with extremely conservative memory settings
"""

# Ultra-conservative memory settings to prevent crashes
ULTRA_SAFE_SPARK_CONFIG = {
    # Core memory settings - EXTREMELY CONSERVATIVE
    "spark.driver.memory": "512m",              # Half of previous 1g
    "spark.executor.memory": "512m",            # Half of previous 1g
    "spark.driver.maxResultSize": "128m",       # Quarter of previous 512m
    
    # Prevent memory spikes
    "spark.memory.fraction": "0.4",             # Less memory for execution/storage
    "spark.memory.storageFraction": "0.3",      # Less memory for caching
    "spark.sql.execution.arrow.maxRecordsPerBatch": "100",  # Tiny batches
    
    # Minimal parallelism
    "spark.default.parallelism": "1",           # Single thread
    "spark.sql.shuffle.partitions": "2",        # Minimal shuffle partitions
    "spark.executor.instances": "1",            # Single executor
    "spark.executor.cores": "1",                # Single core per executor
    
    # Aggressive spill to disk
    "spark.shuffle.spill": "true",
    "spark.sql.adaptive.shuffle.targetPostShuffleInputSize": "64MB",
    "spark.sql.files.maxPartitionBytes": "64MB",
    
    # Disable features that consume memory
    "spark.sql.adaptive.enabled": "false",      # No adaptive query execution
    "spark.sql.adaptive.coalescePartitions.enabled": "false",
    "spark.sql.execution.arrow.pyspark.enabled": "false",  # No Arrow optimization
    "spark.sql.execution.pandas.convertToArrowArraySafely": "true",
    
    # JVM settings for stability
    "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:MaxHeapFreeRatio=40 -XX:MinHeapFreeRatio=10 -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError -XX:ErrorFile=/tmp/hs_err_driver.log",
    "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:MaxHeapFreeRatio=40 -XX:MinHeapFreeRatio=10 -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError -XX:ErrorFile=/tmp/hs_err_executor.log",
    
    # Network and timeout settings
    "spark.network.timeout": "600s",
    "spark.rpc.askTimeout": "600s",
    "spark.executor.heartbeatInterval": "60s",
    "spark.task.maxFailures": "1",             # Fail fast on errors
    
    # Delta Lake specific settings
    "spark.databricks.delta.retentionDurationCheck.enabled": "false",
    "spark.databricks.delta.autoCompact.enabled": "false",  # Disable auto-compact
    "spark.databricks.delta.optimizeWrite.enabled": "false",  # Disable optimize
    "spark.databricks.delta.stalenessLimit": "1h",
    
    # S3A settings for MinIO
    "spark.hadoop.fs.s3a.connection.maximum": "5",  # Limit connections
    "spark.hadoop.fs.s3a.threads.max": "5",
    "spark.hadoop.fs.s3a.fast.upload": "false",  # Disable fast upload
    "spark.hadoop.fs.s3a.multipart.size": "32M",  # Smaller multipart size
    "spark.hadoop.fs.s3a.block.size": "32M",      # Smaller block size
}

def get_ultra_safe_config():
    """Get ultra-safe Spark configuration for Docker environment"""
    from config.smart_trader_config import (
        MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
        SPARK_PACKAGES_WITH_DELTA
    )
    
    config = ULTRA_SAFE_SPARK_CONFIG.copy()
    config.update({
        "spark.jars.packages": SPARK_PACKAGES_WITH_DELTA,
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    })
    
    return config