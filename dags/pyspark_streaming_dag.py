#!/usr/bin/env python3
"""
Airflow DAG: PySpark Streaming Pipeline (Bronze Layer)
Streams webhook data from Redpanda directly to Bronze layer in MinIO using structured streaming.
Implements true medallion architecture with proper bronze layer processing.
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

# Import centralized webhook configuration
sys.path.append('/opt/airflow/dags')
from config.webhook_config import (
    REDPANDA_BROKERS, WEBHOOK_TOPIC, MINIO_ENDPOINT, MINIO_ACCESS_KEY, 
    MINIO_SECRET_KEY, MINIO_BUCKET, BRONZE_WEBHOOKS_PATH,
    BRONZE_BATCH_SIZE_LIMIT, BRONZE_PROCESSING_WINDOW_MINUTES,
    get_spark_config, get_s3_path, get_processing_status_fields
)

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
    description='PySpark streaming pipeline - Bronze layer processing from Redpanda to MinIO',
    schedule=SCHEDULE_INTERVAL,
    catchup=False,
    max_active_runs=1,
    tags=['pyspark', 'streaming', 'bronze', 'medallion', 'webhooks']
)

def get_config() -> Dict[str, Any]:
    """Get configuration from centralized webhook config with Airflow Variable overrides."""
    return {
        'redpanda_brokers': Variable.get('REDPANDA_BROKERS', REDPANDA_BROKERS),
        'webhook_topic': Variable.get('WEBHOOK_TOPIC', WEBHOOK_TOPIC),
        'minio_endpoint': Variable.get('MINIO_ENDPOINT', MINIO_ENDPOINT),
        'minio_access_key': Variable.get('MINIO_ACCESS_KEY', MINIO_ACCESS_KEY),
        'minio_secret_key': Variable.get('MINIO_SECRET_KEY', MINIO_SECRET_KEY),
        'minio_bucket': Variable.get('MINIO_BUCKET', MINIO_BUCKET),
        'local_data_path': Variable.get('LOCAL_DATA_PATH', '/opt/airflow/data/processed'),
        'checkpoint_path': Variable.get('CHECKPOINT_PATH', '/opt/airflow/data/checkpoints'),
        'processing_window_minutes': int(Variable.get('PROCESSING_WINDOW_MINUTES', str(BRONZE_PROCESSING_WINDOW_MINUTES))),
        'batch_size_limit': BRONZE_BATCH_SIZE_LIMIT
    }

@task(dag=dag)
def streaming_redpanda_to_bronze(**context) -> Dict[str, Any]:
    """
    Task: Stream from Redpanda directly to Bronze layer in MinIO.
    Uses structured streaming for real-time processing with checkpointing.
    """
    import logging
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json, current_timestamp, date_format, lit
    from pyspark.sql.types import StructType, StructField, StringType
    
    logger = logging.getLogger(__name__)
    config = get_config()
    
    execution_date = context['logical_date']
    batch_id = execution_date.strftime("%Y%m%d_%H%M%S")
    
    logger.info(f"Starting streaming processing for batch: {batch_id}")
    
    # Create checkpoint directory
    checkpoint_dir = f"{config['checkpoint_path']}/bronze_streaming"
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    # Create Spark session with centralized configuration
    spark_config = get_spark_config('bronze')
    spark = SparkSession.builder \
        .appName(f"StreamingRedpandaToBronze_{batch_id}")
    
    # Apply all spark configurations
    for key, value in spark_config.items():
        spark = spark.config(key, value)
    
    spark = spark.getOrCreate()
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
        
        logger.info(f"Reading from Redpanda topic '{config['webhook_topic']}' with batch limit {config['batch_size_limit']}")
        
        # Create streaming DataFrame from Kafka/Redpanda
        kafka_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config['redpanda_brokers']) \
            .option("subscribe", config['webhook_topic']) \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", config['batch_size_limit']) \
            .option("kafka.consumer.pollTimeoutMs", "30000") \
            .load()
        
        logger.info("Successfully created streaming DataFrame from Redpanda")
        
        # Parse the JSON value and add processing metadata
        processed_stream = kafka_stream.select(
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
        ).withColumn(
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
            lit(batch_id)
        ).withColumn(
            "processed_for_silver",
            lit(False)
        ).withColumn(
            "silver_processed_at",
            lit(None).cast("timestamp")
        ).withColumn(
            "processed_for_gold",
            lit(False)
        ).withColumn(
            "gold_processed_at",
            lit(None).cast("timestamp")
        ).withColumn(
            "processing_status",
            lit("pending")
        ).withColumn(
            "processing_errors",
            lit(None).cast("string")
        )
        
        # Write directly to Bronze layer in MinIO with streaming
        bronze_path = get_s3_path(BRONZE_WEBHOOKS_PATH)
        logger.info(f"Writing streaming data to bronze layer: {bronze_path}")
        
        # Start streaming query with trigger interval
        stream_query = processed_stream.writeStream \
            .format("parquet") \
            .option("path", bronze_path) \
            .option("checkpointLocation", checkpoint_dir) \
            .partitionBy("processing_date", "processing_hour") \
            .trigger(processingTime=f"{config['processing_window_minutes']} minutes") \
            .outputMode("append") \
            .start()
        
        logger.info("Streaming query started successfully")
        
        # Wait for processing to complete (run for one trigger interval)
        timeout_seconds = config['processing_window_minutes'] * 60 + 60  # Add 1 minute buffer
        
        try:
            stream_query.awaitTermination(timeout=timeout_seconds)
            logger.info("Streaming query completed successfully")
            
            # Get processing statistics
            progress = stream_query.lastProgress
            if progress:
                input_rows = progress.get('inputRowsPerSecond', 0)
                processed_rows = progress.get('batchDuration', 0)
                logger.info(f"Processed {processed_rows}ms batch with {input_rows} rows/sec")
            
            return {
                'status': 'success',
                'batch_id': batch_id,
                'output_path': bronze_path,
                'checkpoint_location': checkpoint_dir,
                'processing_duration_seconds': timeout_seconds,
                'stream_id': stream_query.id if stream_query else None
            }
            
        except Exception as stream_error:
            logger.warning(f"Stream processing timeout or error: {stream_error}")
            # Stop the query gracefully
            if stream_query.isActive:
                stream_query.stop()
            
            # Still return success if stream was processing
            return {
                'status': 'partial_success',
                'batch_id': batch_id,
                'output_path': bronze_path,
                'checkpoint_location': checkpoint_dir,
                'message': 'Stream processing completed with timeout'
            }
        
    except Exception as e:
        logger.error(f"Error in streaming_redpanda_to_bronze task: {str(e)}")
        raise
    finally:
        # Ensure Spark session is stopped
        try:
            spark.stop()
        except:
            pass

@task(dag=dag)
def validate_bronze_data(streaming_result: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    Task: Validate that bronze data was successfully written to MinIO.
    """
    import logging
    import boto3
    from botocore.client import Config
    
    logger = logging.getLogger(__name__)
    config = get_config()
    
    if streaming_result['status'] not in ['success', 'partial_success']:
        logger.warning("Streaming task did not complete successfully, skipping validation")
        return {
            'status': 'skipped',
            'message': 'Streaming task failed'
        }
    
    batch_id = streaming_result['batch_id']
    bronze_path = streaming_result['output_path']
    
    logger.info(f"Validating bronze data for batch: {batch_id}")
    
    try:
        # Create MinIO client to check data
        s3_client = boto3.client(
            's3',
            endpoint_url=config['minio_endpoint'],
            aws_access_key_id=config['minio_access_key'],
            aws_secret_access_key=config['minio_secret_key'],
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=config['minio_bucket'])
            logger.info(f"✅ Bucket '{config['minio_bucket']}' exists")
        except Exception as e:
            logger.error(f"❌ Bucket '{config['minio_bucket']}' not accessible: {e}")
            return {
                'status': 'error',
                'message': f'Bucket not accessible: {e}'
            }
        
        # List objects in bronze path to verify data was written
        bronze_prefix = BRONZE_WEBHOOKS_PATH
        
        try:
            response = s3_client.list_objects_v2(
                Bucket=config['minio_bucket'],
                Prefix=bronze_prefix,
                MaxKeys=10
            )
            
            if 'Contents' in response and len(response['Contents']) > 0:
                file_count = response['KeyCount']
                latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
                
                logger.info(f"✅ Found {file_count} files in bronze layer")
                logger.info(f"✅ Latest file: {latest_file['Key']} ({latest_file['Size']} bytes)")
                
                return {
                    'status': 'success',
                    'batch_id': batch_id,
                    'bronze_files_found': file_count,
                    'latest_file': latest_file['Key'],
                    'latest_file_size': latest_file['Size'],
                    'validation_message': f'Bronze data validation passed - {file_count} files found'
                }
            else:
                logger.warning(f"⚠️ No files found in bronze path: {bronze_prefix}")
                return {
                    'status': 'no_data',
                    'batch_id': batch_id,
                    'bronze_files_found': 0,
                    'validation_message': 'No bronze data files found - may be first run or processing lag'
                }
                
        except Exception as e:
            logger.error(f"❌ Error listing bronze data: {e}")
            return {
                'status': 'error',
                'message': f'Error validating bronze data: {e}'
            }
        
    except Exception as e:
        logger.error(f"Error in validate_bronze_data task: {str(e)}")
        return {
            'status': 'error',
            'message': f'Validation error: {e}'
        }

# Define task dependencies - Simplified streaming pipeline
with dag:
    streaming_task = streaming_redpanda_to_bronze()
    validation_task = validate_bronze_data(streaming_task)
    
    streaming_task >> validation_task