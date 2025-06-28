"""
Simplified Helius Webhook Notifications Pipeline DAG
Bronze layer only - processes webhook files directly to Delta Lake
"""

from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 26),
    'is_paused_upon_creation': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'helius_webhook_notifications_pipeline',
    default_args=default_args,
    description='Process webhook files directly to Delta Lake bronze layer',
    schedule_interval='*/1 * * * *',  # Run every minute for real-time processing
    catchup=False,
    max_active_runs=1,  # Single DAG run to prevent PySpark resource conflicts
    tags=['webhook', 'delta-lake', 'bronze', 'helius'],
)

@task(dag=dag)
def cleanup_old_webhook_files(**context):
    """
    Clean up old webhook files that may have been stuck or orphaned.
    Runs before main processing to prevent accumulation.
    """
    import logging
    import time
    from pathlib import Path
    
    logger = logging.getLogger(__name__)
    
    try:
        pending_dir = Path("/opt/airflow/data/pending_webhooks")
        
        if not pending_dir.exists():
            logger.info("No pending webhooks directory to clean")
            return {"status": "success", "files_cleaned": 0}
        
        # Clean files older than 5 minutes (300 seconds)
        current_time = time.time()
        old_files = []
        
        for webhook_file in pending_dir.glob("*.json"):
            try:
                file_age = current_time - webhook_file.stat().st_mtime
                if file_age > 300:  # 5 minutes
                    old_files.append(webhook_file)
            except Exception as e:
                logger.warning(f"Could not check age of file {webhook_file}: {e}")
        
        # Delete old files
        deleted_count = 0
        for old_file in old_files:
            try:
                old_file.unlink()
                deleted_count += 1
            except Exception as e:
                logger.error(f"Failed to delete old file {old_file}: {e}")
        
        if deleted_count > 0:
            logger.warning(f"Cleaned up {deleted_count} old webhook files (>5 minutes old)")
        else:
            logger.info("No old webhook files to clean")
        
        return {"status": "success", "files_cleaned": deleted_count}
        
    except Exception as e:
        logger.error(f"Error in webhook file cleanup: {str(e)}")
        return {"status": "error", "files_cleaned": 0}

@task(dag=dag)
def process_pending_webhooks_to_bronze(**context):
    """
    Process webhook files written by the webhook listener service.
    Reads pending webhook JSON files and writes to Delta Lake bronze layer.
    """
    import logging
    import json
    import os
    from pathlib import Path
    from utils.true_delta_manager import TrueDeltaLakeManager
    from datetime import datetime
    
    logger = logging.getLogger(__name__)
    delta_manager = None
    
    try:
        # Use the proven TrueDeltaLakeManager from smart trader pipeline
        delta_manager = TrueDeltaLakeManager()
        
        logger.info("Processing pending webhook files to Delta Lake bronze")
        
        # Path to pending webhook files (Docker volume mounted)
        pending_dir = Path("/opt/airflow/data/pending_webhooks")
        
        if not pending_dir.exists():
            logger.info("No pending webhooks directory found")
            return {
                "status": "success",
                "records_processed": 0,
                "message": "No pending webhooks directory"
            }
        
        # Get all pending webhook files and check for accumulation
        webhook_files = list(pending_dir.glob("*.json"))
        total_files = len(webhook_files)
        
        if not webhook_files:
            logger.info("No pending webhook files to process")
            return {
                "status": "success",
                "records_processed": 0,
                "message": "No pending webhook files found"
            }
        
        # Monitor file accumulation
        if total_files > 1000:
            logger.error(f"CRITICAL: {total_files} webhook files queued - system may be overloaded!")
        elif total_files > 100:
            logger.warning(f"HIGH LOAD: {total_files} webhook files queued")
        else:
            logger.info(f"Processing {total_files} pending webhook files")
        
        logger.info(f"Found {len(webhook_files)} pending webhook files to process")
        
        # Read all webhook files
        webhook_data = []
        processed_files = []
        
        for webhook_file in webhook_files[:500]:  # Smaller batches for real-time processing
            try:
                with open(webhook_file, 'r') as f:
                    webhook_record = json.load(f)
                    webhook_data.append(webhook_record)
                    processed_files.append(webhook_file)
            except Exception as e:
                logger.warning(f"Failed to read webhook file {webhook_file}: {e}")
                continue
        
        if not webhook_data:
            logger.info("No valid webhook data found in files")
            return {
                "status": "success",
                "records_processed": 0,
                "message": "No valid webhook data found"
            }
        
        # Convert to DataFrame for Delta Lake
        from pyspark.sql.functions import current_timestamp, lit, date_format
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType
        
        # Create DataFrame from webhook data
        bronze_df = delta_manager.spark.createDataFrame(webhook_data)
        
        # Add processing metadata
        bronze_df = bronze_df.withColumn("ingested_at", current_timestamp()) \
                           .withColumn("processing_date", date_format(current_timestamp(), "yyyy-MM-dd")) \
                           .withColumn("processed_to_silver", lit(False))
        
        # Define webhook bronze table path
        webhook_table_path = "s3a://webhook-notifications/bronze/webhooks"
        
        # Use exact same pattern as smart trader pipeline
        if delta_manager.table_exists(webhook_table_path):
            # Table exists, use MERGE operation to prevent duplicates
            merge_condition = "target.webhook_id = source.webhook_id"
            
            # Use TrueDeltaLakeManager's upsert_data method (same as smart trader pipeline)
            version = delta_manager.upsert_data(
                table_path=webhook_table_path,
                source_df=bronze_df,
                merge_condition=merge_condition
            )
            logger.info(f"Used MERGE operation to prevent duplicates - version: {version}")
        else:
            # Create new table using TrueDeltaLakeManager.create_table (same as smart trader pipeline)
            version = delta_manager.create_table(
                bronze_df,
                webhook_table_path,
                partition_cols=["processing_date"]
            )
            logger.info(f"Created new Delta table at {webhook_table_path} - version: {version}")
        
        # Clean up processed files immediately
        deleted_count = 0
        for processed_file in processed_files:
            try:
                processed_file.unlink()  # Delete the file
                deleted_count += 1
            except Exception as e:
                logger.error(f"CRITICAL: Failed to delete processed file {processed_file}: {e}")
        
        logger.info(f"Cleaned up {deleted_count}/{len(processed_files)} processed webhook files")
        
        logger.info(f"Successfully processed {len(webhook_data)} webhooks to Delta Lake bronze")
        return {
            "status": "success", 
            "records_processed": len(webhook_data),
            "table_path": webhook_table_path,
            "table_version": version
        }
        
    except Exception as e:
        logger.error(f"Error in webhook bronze ingestion: {str(e)}")
        raise
    finally:
        if delta_manager:
            delta_manager.stop()

# Task dependencies: cleanup old files first, then process pending webhooks
cleanup_task = cleanup_old_webhook_files()
webhook_processing = process_pending_webhooks_to_bronze()

# Set task dependencies
cleanup_task >> webhook_processing