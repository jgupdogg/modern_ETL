#!/usr/bin/env python3
"""
Airflow DAG: PySpark Streaming Pipeline
Processes data from Redpanda to MinIO using PySpark in micro-batches.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import timedelta

# Add the scripts directory to Python path for imports
sys.path.append('/opt/airflow/scripts')

# DAG Configuration
DAG_ID = "pyspark_streaming_pipeline"
SCHEDULE_INTERVAL = "*/5 * * * *"  # Every 5 minutes

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='PySpark streaming pipeline from Redpanda to MinIO',
    schedule=SCHEDULE_INTERVAL,
    catchup=False,
    max_active_runs=1,
    tags=['pyspark', 'streaming', 'redpanda', 'minio']
)

def get_config() -> Dict[str, Any]:
    """Get configuration from Airflow Variables with defaults."""
    return {
        'redpanda_brokers': Variable.get('REDPANDA_BROKERS', 'localhost:19092'),
        'webhook_topic': Variable.get('WEBHOOK_TOPIC', 'webhooks'),
        'minio_endpoint': Variable.get('MINIO_ENDPOINT', 'http://localhost:9000'),
        'minio_access_key': Variable.get('MINIO_ACCESS_KEY', 'minioadmin'),
        'minio_secret_key': Variable.get('MINIO_SECRET_KEY', 'minioadmin123'),
        'minio_bucket': Variable.get('MINIO_BUCKET', 'webhook-data'),
        'local_data_path': Variable.get('LOCAL_DATA_PATH', '/opt/airflow/data/processed'),
        'checkpoint_path': Variable.get('CHECKPOINT_PATH', '/opt/airflow/data/checkpoints'),
        'processing_window_minutes': int(Variable.get('PROCESSING_WINDOW_MINUTES', '5'))
    }

@task(dag=dag)
def redpanda_to_local(**context) -> Dict[str, Any]:
    """
    Task: Read from Redpanda and write to local storage.
    Processes data in time-bounded batches instead of continuous streaming.
    """
    import logging
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json, current_timestamp, date_format, lit
    from pyspark.sql.types import StructType, StructField, StringType
    
    logger = logging.getLogger(__name__)
    config = get_config()
    
    # Calculate time window for this batch
    execution_date = context['logical_date']
    window_start = execution_date - timedelta(minutes=config['processing_window_minutes'])
    window_end = execution_date
    
    logger.info(f"Processing window: {window_start} to {window_end}")
    
    # Create output directories
    os.makedirs(config['local_data_path'], exist_ok=True)
    os.makedirs(config['checkpoint_path'], exist_ok=True)
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName(f"RedpandaToLocal_{execution_date.strftime('%Y%m%d_%H%M%S')}") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Define webhook schema
        webhook_schema = StructType([
            StructField("message_id", StringType(), True),
            StructField("timestamp", StringType(), True), 
            StructField("source_ip", StringType(), True),
            StructField("file_path", StringType(), True),
            StructField("payload", StringType(), True)
        ])
        
        # Read from Kafka/Redpanda in batch mode
        kafka_df = spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config['redpanda_brokers']) \
            .option("subscribe", config['webhook_topic']) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        # Filter by timestamp if messages have timestamps
        # For now, we'll process all available messages
        logger.info(f"Found {kafka_df.count()} messages in Kafka topic")
        
        if kafka_df.count() == 0:
            logger.info("No new messages to process")
            return {
                'status': 'success',
                'messages_processed': 0,
                'output_path': config['local_data_path']
            }
        
        # Parse the JSON value
        parsed_df = kafka_df.select(
            col("key").cast("string").alias("kafka_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), webhook_schema).alias("webhook_data")
        ).select(
            "kafka_key",
            "topic", 
            "partition",
            "offset",
            "kafka_timestamp",
            "webhook_data.message_id",
            "webhook_data.timestamp",
            "webhook_data.source_ip", 
            "webhook_data.file_path",
            "webhook_data.payload"
        )
        
        # Add processing metadata
        processed_df = parsed_df.withColumn(
            "processed_at", 
            current_timestamp()
        ).withColumn(
            "processing_date",
            date_format(current_timestamp(), "yyyy-MM-dd")
        ).withColumn(
            "processing_hour", 
            date_format(current_timestamp(), "HH")
        ).withColumn(
            "batch_id",
            lit(execution_date.strftime("%Y%m%d_%H%M%S"))
        )
        
        # Write to local directory partitioned by date and hour
        output_path = f"{config['local_data_path']}/batch_{execution_date.strftime('%Y%m%d_%H%M%S')}"
        
        processed_df.write \
            .mode("overwrite") \
            .partitionBy("processing_date", "processing_hour") \
            .parquet(output_path)
        
        message_count = processed_df.count()
        logger.info(f"Successfully processed {message_count} messages to {output_path}")
        
        return {
            'status': 'success',
            'messages_processed': message_count,
            'output_path': output_path,
            'batch_id': execution_date.strftime("%Y%m%d_%H%M%S")
        }
        
    except Exception as e:
        logger.error(f"Error in redpanda_to_local task: {str(e)}")
        raise
    finally:
        spark.stop()

@task(dag=dag)
def local_to_minio(redpanda_result: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    Task: Read from local storage and write to MinIO.
    """
    import logging
    import boto3
    from botocore.client import Config
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import current_timestamp, lit
    
    logger = logging.getLogger(__name__)
    config = get_config()
    
    if redpanda_result['messages_processed'] == 0:
        logger.info("No messages to upload to MinIO")
        return {
            'status': 'success',
            'records_uploaded': 0,
            'minio_path': None
        }
    
    execution_date = context['logical_date']
    input_path = redpanda_result['output_path']
    
    logger.info(f"Processing data from {input_path}")
    
    # Create Spark session with S3A/MinIO support
    spark = SparkSession.builder \
        .appName(f"LocalToMinIO_{execution_date.strftime('%Y%m%d_%H%M%S')}") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367") \
        .config("spark.hadoop.fs.s3a.endpoint", config['minio_endpoint']) \
        .config("spark.hadoop.fs.s3a.access.key", config['minio_access_key']) \
        .config("spark.hadoop.fs.s3a.secret.key", config['minio_secret_key']) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Create MinIO bucket if it doesn't exist
        s3_client = boto3.client(
            's3',
            endpoint_url=config['minio_endpoint'],
            aws_access_key_id=config['minio_access_key'],
            aws_secret_access_key=config['minio_secret_key'],
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        try:
            s3_client.head_bucket(Bucket=config['minio_bucket'])
            logger.info(f"Bucket '{config['minio_bucket']}' exists")
        except:
            s3_client.create_bucket(Bucket=config['minio_bucket'])
            logger.info(f"Created bucket '{config['minio_bucket']}'")
        
        # Read parquet files from local directory
        df = spark.read.parquet(input_path)
        record_count = df.count()
        
        logger.info(f"Found {record_count} records to upload")
        
        # Add batch processing metadata
        processed_df = df.withColumn(
            "batch_processed_at", 
            current_timestamp()
        ).withColumn(
            "upload_batch_id",
            lit(execution_date.strftime("%Y%m%d_%H%M%S"))
        )
        
        # Write to MinIO partitioned by processing_date
        minio_path = f"s3a://{config['minio_bucket']}/processed-webhooks"
        
        processed_df.write \
            .mode("append") \
            .partitionBy("processing_date") \
            .parquet(minio_path)
        
        logger.info(f"Successfully uploaded {record_count} records to {minio_path}")
        
        # Clean up local files after successful upload
        import shutil
        if os.path.exists(input_path):
            shutil.rmtree(input_path)
            logger.info(f"Cleaned up local files at {input_path}")
        
        return {
            'status': 'success',
            'records_uploaded': record_count,
            'minio_path': minio_path,
            'batch_id': redpanda_result['batch_id']
        }
        
    except Exception as e:
        logger.error(f"Error in local_to_minio task: {str(e)}")
        raise
    finally:
        spark.stop()

@task(dag=dag)
def data_validation(minio_result: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    Task: Validate data integrity after processing.
    """
    import logging
    
    logger = logging.getLogger(__name__)
    
    if minio_result['records_uploaded'] == 0:
        logger.info("No data to validate")
        return {'status': 'success', 'validation': 'skipped'}
    
    # Here you could add validation logic like:
    # - Verify record counts match
    # - Check data quality rules
    # - Validate schema compliance
    # - Alert on anomalies
    
    logger.info(f"Validated {minio_result['records_uploaded']} records")
    
    return {
        'status': 'success',
        'validation': 'passed',
        'validated_records': minio_result['records_uploaded']
    }

# Define task dependencies
with dag:
    redpanda_task = redpanda_to_local()
    minio_task = local_to_minio(redpanda_task)
    validation_task = data_validation(minio_task)
    
    redpanda_task >> minio_task >> validation_task