#!/usr/bin/env python3
"""
Airflow DAG: Silver Webhook Transformation Pipeline
Transforms bronze webhook data to silver layer swap transactions using PySpark.
"""

import os
import json
import logging
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, List

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

# Import centralized webhook configuration
sys.path.append('/opt/airflow/dags')
from config.webhook_config import (
    MINIO_BUCKET, BRONZE_WEBHOOKS_PATH, SILVER_SWAP_TRANSACTIONS_PATH,
    SILVER_BATCH_SIZE_LIMIT, SILVER_PROCESSING_WINDOW_MINUTES,
    get_spark_config, get_s3_path, get_processing_status_fields
)

# DAG Configuration
DAG_ID = "silver_webhook_transformation"
SCHEDULE_INTERVAL = f"*/{SILVER_PROCESSING_WINDOW_MINUTES} * * * *"  # From centralized config

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Transform bronze webhook data to silver swap transactions',
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    max_active_runs=1,
    tags=['pyspark', 'silver', 'webhooks', 'transformation']
)

@task(dag=dag)
def silver_webhook_swap_transformation(**context) -> Dict[str, Any]:
    """
    Transform bronze webhook data to silver layer swap transactions.
    Mimics the original transformation logic using PySpark for scalability.
    """
    import logging
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, lit, when, from_json, get_json_object, 
        current_timestamp, date_format, size, 
        element_at, expr, coalesce, collect_set, from_unixtime
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, 
        IntegerType, BooleanType, TimestampType, ArrayType
    )
    from datetime import datetime
    
    logger = logging.getLogger(__name__)
    logger.info("Starting silver webhook transformation with PySpark")
    
    # Generate batch ID
    execution_date = context['logical_date']
    batch_id = execution_date.strftime("%Y%m%d_%H%M%S")
    
    # Create Spark session with centralized configuration
    spark_config = get_spark_config('silver')
    spark = SparkSession.builder \
        .appName(f"SilverWebhookTransformation_{batch_id}")
    
    # Apply all spark configurations
    for key, value in spark_config.items():
        spark = spark.config(key, value)
    
    # Disable arrow for JSON processing compatibility
    spark = spark.config("spark.sql.execution.arrow.pyspark.enabled", "false")
    spark = spark.getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Read bronze webhook data using centralized path
        try:
            bronze_path = get_s3_path(BRONZE_WEBHOOKS_PATH)
            bronze_df = spark.read.parquet(bronze_path)
            logger.info(f"Successfully read bronze data from {bronze_path}")
            
            # Add processed_for_silver flag if it doesn't exist
            if "processed_for_silver" not in bronze_df.columns:
                bronze_df = bronze_df.withColumn("processed_for_silver", lit(False))
                logger.info("Added processed_for_silver flag to bronze data")
            
            # Filter for unprocessed records with quality checks
            clean_df = bronze_df.filter(
                (col("processed_for_silver") == False) &
                (col("payload").isNotNull()) &
                (col("message_id").isNotNull())
            )
            
            unprocessed_count = clean_df.count()
            logger.info(f"Found {unprocessed_count} unprocessed webhook records")
            
            if unprocessed_count == 0:
                logger.info("No unprocessed records - returning success")
                return {
                    "records_processed": 0,
                    "unique_tokens": [],
                    "batch_id": batch_id,
                    "status": "no_data"
                }
            
            # Process in smaller batches to avoid memory issues
            batch_size = SILVER_BATCH_SIZE_LIMIT  # Use centralized config
            if unprocessed_count > batch_size:
                logger.info(f"Large dataset detected ({unprocessed_count} records). Processing first {batch_size} records.")
                clean_df = clean_df.limit(batch_size)
            
            # Parse JSON payload - handle both string and array formats
            # First, try to parse as JSON array (Solana transactions)
            parsed_df = clean_df.withColumn(
                "payload_parsed",
                when(
                    col("payload").startswith("["),
                    from_json(col("payload"), ArrayType(StringType()))
                ).otherwise(
                    # Handle single objects as arrays
                    from_json(expr("concat('[', payload, ']')"), ArrayType(StringType()))
                )
            ).filter(
                col("payload_parsed").isNotNull() & 
                (size(col("payload_parsed")) > 0)
            )
            
            # Extract first transaction from array
            tx_df = parsed_df.withColumn(
                "first_transaction",
                element_at(col("payload_parsed"), 1)
            ).filter(col("first_transaction").isNotNull())
            
            # Parse the first transaction JSON
            tx_parsed_df = tx_df.withColumn(
                "tx_json",
                from_json(
                    col("first_transaction"),
                    StructType([
                        StructField("signature", StringType(), True),
                        StructField("source", StringType(), True),
                        StructField("timestamp", IntegerType(), True),
                        StructField("tokenTransfers", ArrayType(StructType([
                            StructField("fromUserAccount", StringType(), True),
                            StructField("mint", StringType(), True),
                            StructField("tokenAmount", DoubleType(), True)
                        ])), True)
                    ])
                )
            ).filter(
                col("tx_json").isNotNull() &
                col("tx_json.tokenTransfers").isNotNull() &
                (size(col("tx_json.tokenTransfers")) > 0)
            )
            
            valid_count = tx_parsed_df.count()
            logger.info(f"Found {valid_count} valid transaction records after parsing")
            
            if valid_count == 0:
                logger.info("No valid transactions found after parsing")
                return {
                    "records_processed": 0,
                    "unique_tokens": [],
                    "batch_id": batch_id,
                    "status": "no_valid_transactions"
                }
            
            # Extract swap details (mimicking original logic)
            silver_df = tx_parsed_df.select(
                # Raw metadata
                col("message_id").alias("raw_id"),
                
                # Swap details from tokenTransfers
                col("tx_json.tokenTransfers")[0]["fromUserAccount"].alias("user_address"),
                col("tx_json.tokenTransfers")[0]["mint"].alias("swap_from_token"),
                col("tx_json.tokenTransfers")[0]["tokenAmount"].alias("swap_from_amount"),
                
                # Last token transfer for "to" details
                expr("tx_json.tokenTransfers[size(tx_json.tokenTransfers)-1].mint").alias("swap_to_token"),
                expr("tx_json.tokenTransfers[size(tx_json.tokenTransfers)-1].tokenAmount").alias("swap_to_amount"),
                
                # Transaction metadata
                col("tx_json.signature").alias("signature"),
                col("tx_json.source").alias("source"),
                
                # Convert timestamp from unix to datetime string
                when(
                    col("tx_json.timestamp").isNotNull(),
                    date_format(from_unixtime(col("tx_json.timestamp")), "yyyy-MM-dd HH:mm:ss")
                ).otherwise(None).alias("timestamp"),
                
                # Processing flags (mimicking original logic)
                lit(False).alias("processed"),
                lit(False).alias("notification_sent"),
                
                # Medallion architecture gold processing status
                lit(False).alias("processed_for_gold"),
                lit(None).alias("gold_processed_at"),
                lit(None).alias("gold_batch_id"),
                
                # Processing metadata
                current_timestamp().alias("processed_at"),
                lit(batch_id).alias("batch_id"),
                date_format(current_timestamp(), "yyyy-MM-dd").alias("processing_date")
            ).filter(
                # Quality checks - ensure we have valid swap data
                col("signature").isNotNull() &
                col("user_address").isNotNull() &
                col("swap_from_token").isNotNull() &
                col("swap_to_token").isNotNull()
            )
            
            final_count = silver_df.count()
            logger.info(f"Generated {final_count} valid swap transaction records")
            
            if final_count == 0:
                logger.info("No valid swap records generated")
                return {
                    "records_processed": 0,
                    "unique_tokens": [],
                    "batch_id": batch_id,
                    "status": "no_valid_swaps"
                }
            
            # Write to silver layer using centralized path
            output_path = get_s3_path(SILVER_SWAP_TRANSACTIONS_PATH)
            
            silver_df.write \
                .partitionBy("processing_date", "source") \
                .mode("append") \
                .parquet(output_path)
            
            logger.info(f"Successfully wrote {final_count} records to {output_path}")
            
            # Extract unique token addresses (mimicking original return logic)
            unique_tokens_df = silver_df.select(
                col("swap_from_token").alias("token")
            ).union(
                silver_df.select(col("swap_to_token").alias("token"))
            ).filter(
                col("token").isNotNull()
            ).distinct()
            
            unique_tokens = [row["token"] for row in unique_tokens_df.collect()]
            logger.info(f"Extracted {len(unique_tokens)} unique token addresses")
            
            # Update bronze processing status
            processed_ids = tx_parsed_df.select("message_id").distinct()
            bronze_updated = bronze_df.join(
                processed_ids.withColumn("was_processed", lit(True)),
                on="message_id",
                how="left"
            ).withColumn(
                "processed_for_silver",
                when(col("was_processed") == True, True).otherwise(col("processed_for_silver"))
            ).withColumn(
                "silver_processed_at",
                when(col("was_processed") == True, current_timestamp()).otherwise(
                    col("silver_processed_at") if "silver_processed_at" in bronze_df.columns else lit(None)
                )
            ).withColumn(
                "silver_batch_id",
                when(col("was_processed") == True, lit(batch_id)).otherwise(
                    col("silver_batch_id") if "silver_batch_id" in bronze_df.columns else lit(None)
                )
            ).drop("was_processed")
            
            # Write updated bronze data back to bronze layer using centralized path
            bronze_updated_path = get_s3_path(BRONZE_WEBHOOKS_PATH)
            bronze_updated.write.mode("overwrite").partitionBy("processing_date").parquet(bronze_updated_path)
            logger.info(f"Updated bronze processing status for {final_count} records")
            
            # Write success marker
            success_path = f"{output_path}_SUCCESS_{batch_id}"
            spark.createDataFrame([("success",)], ["status"]).coalesce(1).write.mode("overwrite").text(success_path)
            
            return {
                "records_processed": final_count,
                "unique_tokens": unique_tokens,
                "batch_id": batch_id,
                "output_path": output_path,
                "status": "success"
            }
            
        except Exception as e:
            logger.warning(f"Could not read or process bronze data: {e}")
            return {
                "records_processed": 0,
                "unique_tokens": [],
                "batch_id": batch_id,
                "status": "error_reading_bronze"
            }
            
    except Exception as e:
        logger.error(f"Error in silver webhook transformation task: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        spark.stop()

# Task Dependencies
with dag:
    transformation_task = silver_webhook_swap_transformation()