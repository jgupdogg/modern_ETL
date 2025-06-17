#!/usr/bin/env python3
"""
Airflow DAG: Gold Webhook Analytics Pipeline
Processes silver layer webhook data to create high-level analytics and insights.
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

# Import centralized webhook configuration
sys.path.append('/opt/airflow/dags')
from config.webhook_config import (
    SILVER_SWAP_TRANSACTIONS_PATH, GOLD_TRENDING_TOKENS_PATH, GOLD_WHALE_ACTIVITY_PATH,
    GOLD_DEX_VOLUMES_PATH, GOLD_REAL_TIME_METRICS_PATH, GOLD_PROCESSING_WINDOW_MINUTES,
    GOLD_TRENDING_MIN_VOLUME_USD, GOLD_TRENDING_TOP_N, GOLD_WHALE_MIN_USD_VALUE,
    GOLD_WHALE_TOP_N_TRANSACTIONS, GOLD_LOOKBACK_HOURS,
    get_spark_config, get_s3_path, get_layer_config
)

# DAG Configuration
DAG_ID = "gold_webhook_analytics"
SCHEDULE_INTERVAL = f"*/{GOLD_PROCESSING_WINDOW_MINUTES} * * * *"  # From centralized config

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Generate gold layer analytics from silver webhook data',
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'analytics', 'webhooks', 'trending', 'whales']
)

@task(dag=dag)
def trending_tokens_analysis(**context) -> Dict[str, Any]:
    """
    Analyze trending tokens based on swap volume and transaction frequency.
    """
    import logging
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, sum as spark_sum, count, avg, max as spark_max,
        current_timestamp, date_format, lit, desc, 
        when, unix_timestamp, from_unixtime
    )
    from datetime import datetime, timedelta
    
    logger = logging.getLogger(__name__)
    logger.info("Starting trending tokens analysis")
    
    execution_date = context['logical_date']
    batch_id = execution_date.strftime("%Y%m%d_%H%M%S")
    
    # Create Spark session with gold layer configuration
    spark_config = get_spark_config('gold')
    spark = SparkSession.builder \
        .appName(f"GoldTrendingTokens_{batch_id}")
    
    for key, value in spark_config.items():
        spark = spark.config(key, value)
    
    spark = spark.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Read silver swap transactions
        silver_path = get_s3_path(SILVER_SWAP_TRANSACTIONS_PATH)
        logger.info(f"Reading silver data from {silver_path}")
        
        try:
            silver_df = spark.read.parquet(silver_path)
        except Exception as e:
            logger.warning(f"Could not read silver data: {e}")
            return {
                'status': 'no_data',
                'trending_tokens_count': 0,
                'batch_id': batch_id
            }
        
        # Filter for recent data (last N hours)
        lookback_timestamp = execution_date - timedelta(hours=GOLD_LOOKBACK_HOURS)
        
        recent_df = silver_df.filter(
            (col("processed_for_gold") == False) &
            (col("processed_at") >= lit(lookback_timestamp)) &
            (col("swap_from_amount").isNotNull()) &
            (col("swap_to_amount").isNotNull())
        )
        
        record_count = recent_df.count()
        logger.info(f"Found {record_count} recent unprocessed swap records")
        
        if record_count == 0:
            return {
                'status': 'no_new_data',
                'trending_tokens_count': 0,
                'batch_id': batch_id
            }
        
        # Analyze trending "from" tokens (tokens being sold)
        from_tokens_df = recent_df.groupBy("swap_from_token").agg(
            spark_sum("swap_from_amount").alias("total_volume"),
            count("*").alias("transaction_count"),
            avg("swap_from_amount").alias("avg_amount"),
            spark_max("swap_from_amount").alias("max_amount")
        ).withColumn("token_role", lit("from"))
        
        # Analyze trending "to" tokens (tokens being bought)  
        to_tokens_df = recent_df.groupBy("swap_to_token").agg(
            spark_sum("swap_to_amount").alias("total_volume"),
            count("*").alias("transaction_count"),
            avg("swap_to_amount").alias("avg_amount"),
            spark_max("swap_to_amount").alias("max_amount")
        ).withColumn("token_role", lit("to")).withColumnRenamed("swap_to_token", "swap_from_token")
        
        # Combine and rank tokens
        combined_df = from_tokens_df.union(to_tokens_df)
        
        trending_df = combined_df.groupBy("swap_from_token").agg(
            spark_sum("total_volume").alias("combined_volume"),
            spark_sum("transaction_count").alias("combined_transactions"),
            avg("avg_amount").alias("overall_avg_amount"),
            spark_max("max_amount").alias("overall_max_amount")
        ).filter(
            # Apply minimum volume threshold
            col("combined_volume") >= GOLD_TRENDING_MIN_VOLUME_USD
        ).withColumn(
            "trending_score",
            col("combined_volume") * col("combined_transactions")
        ).orderBy(desc("trending_score")).limit(GOLD_TRENDING_TOP_N)
        
        # Add metadata
        trending_final_df = trending_df.withColumn(
            "analysis_timestamp", current_timestamp()
        ).withColumn(
            "analysis_date", date_format(current_timestamp(), "yyyy-MM-dd")
        ).withColumn(
            "analysis_hour", date_format(current_timestamp(), "HH")
        ).withColumn(
            "batch_id", lit(batch_id)
        ).withColumn(
            "lookback_hours", lit(GOLD_LOOKBACK_HOURS)
        ).withColumn(
            "min_volume_threshold", lit(GOLD_TRENDING_MIN_VOLUME_USD)
        )
        
        trending_count = trending_final_df.count()
        logger.info(f"Identified {trending_count} trending tokens")
        
        if trending_count > 0:
            # Write to gold layer
            output_path = get_s3_path(GOLD_TRENDING_TOKENS_PATH)
            
            trending_final_df.write \
                .partitionBy("analysis_date", "analysis_hour") \
                .mode("append") \
                .parquet(output_path)
            
            logger.info(f"Successfully wrote {trending_count} trending tokens to {output_path}")
        
        return {
            'status': 'success',
            'trending_tokens_count': trending_count,
            'records_analyzed': record_count,
            'batch_id': batch_id,
            'output_path': output_path if trending_count > 0 else None
        }
        
    except Exception as e:
        logger.error(f"Error in trending tokens analysis: {e}")
        raise
    finally:
        spark.stop()

@task(dag=dag)
def whale_activity_analysis(**context) -> Dict[str, Any]:
    """
    Analyze whale activity based on large transaction values.
    """
    import logging
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, when, current_timestamp, date_format, lit, desc, 
        greatest, coalesce
    )
    from datetime import datetime, timedelta
    
    logger = logging.getLogger(__name__)
    logger.info("Starting whale activity analysis")
    
    execution_date = context['logical_date']
    batch_id = execution_date.strftime("%Y%m%d_%H%M%S")
    
    # Create Spark session
    spark_config = get_spark_config('gold')
    spark = SparkSession.builder \
        .appName(f"GoldWhaleActivity_{batch_id}")
    
    for key, value in spark_config.items():
        spark = spark.config(key, value)
    
    spark = spark.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Read silver swap transactions
        silver_path = get_s3_path(SILVER_SWAP_TRANSACTIONS_PATH)
        
        try:
            silver_df = spark.read.parquet(silver_path)
        except Exception as e:
            logger.warning(f"Could not read silver data: {e}")
            return {
                'status': 'no_data',
                'whale_transactions_count': 0,
                'batch_id': batch_id
            }
        
        # Filter for recent unprocessed whale transactions
        lookback_timestamp = execution_date - timedelta(hours=GOLD_LOOKBACK_HOURS)
        
        # Estimate USD value (simplified - assumes token amounts represent USD values)
        whale_df = silver_df.filter(
            (col("processed_for_gold") == False) &
            (col("processed_at") >= lit(lookback_timestamp))
        ).withColumn(
            "estimated_usd_value",
            greatest(
                coalesce(col("swap_from_amount"), lit(0.0)),
                coalesce(col("swap_to_amount"), lit(0.0))
            )
        ).filter(
            col("estimated_usd_value") >= GOLD_WHALE_MIN_USD_VALUE
        )
        
        whale_count = whale_df.count()
        logger.info(f"Found {whale_count} whale transactions")
        
        if whale_count == 0:
            return {
                'status': 'no_whale_activity',
                'whale_transactions_count': 0,
                'batch_id': batch_id
            }
        
        # Select top whale transactions
        whale_final_df = whale_df.select(
            col("raw_id"),
            col("user_address").alias("whale_address"),
            col("swap_from_token"),
            col("swap_to_token"),
            col("swap_from_amount"),
            col("swap_to_amount"),
            col("estimated_usd_value"),
            col("signature"),
            col("source"),
            col("timestamp"),
            current_timestamp().alias("analysis_timestamp"),
            date_format(current_timestamp(), "yyyy-MM-dd").alias("analysis_date"),
            date_format(current_timestamp(), "HH").alias("analysis_hour"),
            lit(batch_id).alias("batch_id"),
            lit(GOLD_WHALE_MIN_USD_VALUE).alias("min_whale_threshold")
        ).orderBy(desc("estimated_usd_value")).limit(GOLD_WHALE_TOP_N_TRANSACTIONS)
        
        final_count = whale_final_df.count()
        
        if final_count > 0:
            # Write to gold layer
            output_path = get_s3_path(GOLD_WHALE_ACTIVITY_PATH)
            
            whale_final_df.write \
                .partitionBy("analysis_date", "analysis_hour") \
                .mode("append") \
                .parquet(output_path)
            
            logger.info(f"Successfully wrote {final_count} whale transactions to {output_path}")
        
        return {
            'status': 'success',
            'whale_transactions_count': final_count,
            'total_candidates': whale_count,
            'batch_id': batch_id,
            'output_path': output_path if final_count > 0 else None
        }
        
    except Exception as e:
        logger.error(f"Error in whale activity analysis: {e}")
        raise
    finally:
        spark.stop()

@task(dag=dag)
def update_silver_processing_status(trending_result: Dict[str, Any], whale_result: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    Update silver layer records to mark them as processed for gold layer.
    """
    import logging
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when, current_timestamp, lit
    from datetime import datetime, timedelta
    
    logger = logging.getLogger(__name__)
    logger.info("Updating silver layer processing status")
    
    execution_date = context['logical_date']
    batch_id = execution_date.strftime("%Y%m%d_%H%M%S")
    
    # Skip if no data was processed
    if (trending_result.get('status') == 'no_data' and 
        whale_result.get('status') == 'no_data'):
        logger.info("No data processed, skipping status update")
        return {'status': 'skipped', 'batch_id': batch_id}
    
    # Create Spark session
    spark_config = get_spark_config('gold')
    spark = SparkSession.builder \
        .appName(f"UpdateSilverStatus_{batch_id}")
    
    for key, value in spark_config.items():
        spark = spark.config(key, value)
    
    spark = spark.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Read silver data
        silver_path = get_s3_path(SILVER_SWAP_TRANSACTIONS_PATH)
        silver_df = spark.read.parquet(silver_path)
        
        # Update processing status for recent records
        lookback_timestamp = execution_date - timedelta(hours=GOLD_LOOKBACK_HOURS)
        
        updated_df = silver_df.withColumn(
            "processed_for_gold",
            when(
                (col("processed_for_gold") == False) &
                (col("processed_at") >= lit(lookback_timestamp)),
                True
            ).otherwise(col("processed_for_gold"))
        ).withColumn(
            "gold_processed_at",
            when(
                (col("processed_for_gold") == True) &
                (col("gold_processed_at").isNull()),
                current_timestamp()
            ).otherwise(col("gold_processed_at"))
        ).withColumn(
            "gold_batch_id",
            when(
                (col("processed_for_gold") == True) &
                (col("gold_batch_id").isNull()),
                lit(batch_id)
            ).otherwise(col("gold_batch_id"))
        )
        
        # Write back to silver layer
        updated_df.write.mode("overwrite").partitionBy("processing_date", "source").parquet(silver_path)
        
        logger.info(f"Updated silver layer processing status for batch {batch_id}")
        
        return {
            'status': 'success',
            'batch_id': batch_id,
            'records_updated': 'bulk_update_completed'
        }
        
    except Exception as e:
        logger.error(f"Error updating silver processing status: {e}")
        raise
    finally:
        spark.stop()

# Task Dependencies
with dag:
    trending_task = trending_tokens_analysis()
    whale_task = whale_activity_analysis()
    status_update_task = update_silver_processing_status(trending_task, whale_task)
    
    [trending_task, whale_task] >> status_update_task