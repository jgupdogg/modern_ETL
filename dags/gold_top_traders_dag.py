"""
Gold Top Traders DAG
Selects best performing wallets from silver PnL data and creates gold layer analytics
"""
import os
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, sum as spark_sum, avg, count, max as spark_max, min as spark_min,
    desc, asc, row_number, current_timestamp, expr, coalesce, round as spark_round,
    datediff, abs as spark_abs
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, 
    TimestampType, BooleanType, DateType
)
from pyspark.sql.window import Window


# Configuration
class GoldConfig:
    """Configuration for gold layer top traders selection"""
    min_total_pnl: float = 100.0           # Min $100 profit (relaxed from $1K)
    min_roi: float = 10.0                  # Min 10% ROI (relaxed from 20%)
    min_win_rate: float = 50.0             # Min 50% win rate (relaxed from 60%)
    min_trade_count: int = 5               # Min 5 trades (relaxed from 10)
    max_traders_per_batch: int = 100       # Top N traders per batch
    performance_lookback_days: int = 30    # Consider last 30 days for tier calculation


def get_gold_top_traders_schema() -> StructType:
    """Get PyArrow schema for gold top traders data"""
    return StructType([
        # Trader identification
        StructField("wallet_address", StringType(), False),
        StructField("trader_rank", IntegerType(), False),
        StructField("performance_tier", StringType(), False),  # "elite", "strong", "promising"
        
        # Core performance metrics (from silver)
        StructField("total_pnl", DoubleType(), False),
        StructField("realized_pnl", DoubleType(), False),
        StructField("unrealized_pnl", DoubleType(), False),
        StructField("roi", DoubleType(), False),
        StructField("win_rate", DoubleType(), False),
        StructField("trade_count", IntegerType(), False),
        
        # Trading behavior metrics
        StructField("avg_holding_time_hours", DoubleType(), False),
        StructField("avg_transaction_amount_usd", DoubleType(), False),
        StructField("trade_frequency_daily", DoubleType(), False),
        
        # Risk-adjusted metrics (calculated)
        StructField("consistency_score", DoubleType(), False),
        StructField("risk_score", DoubleType(), False),
        StructField("profit_factor", DoubleType(), False),  # Total gains / Total losses
        
        # Portfolio metrics
        StructField("total_bought", DoubleType(), False),
        StructField("total_sold", DoubleType(), False),
        StructField("current_position_value", DoubleType(), False),
        
        # Activity tracking
        StructField("first_transaction", TimestampType(), True),
        StructField("last_transaction", TimestampType(), True),
        StructField("days_active", IntegerType(), False),
        
        # Source tracking
        StructField("silver_calculation_date", DateType(), False),
        StructField("silver_batch_id", StringType(), False),
        StructField("silver_time_period", StringType(), False),
        
        # Gold processing metadata
        StructField("gold_batch_id", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
        StructField("is_active", BooleanType(), False),  # For future deactivation
    ])


def get_spark_session_with_s3() -> SparkSession:
    """Create Spark session configured for S3/MinIO access"""
    
    return SparkSession.builder \
        .appName("GoldTopTradersCalculation") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()


def get_minio_client() -> boto3.client:
    """Create MinIO S3 client"""
    return boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin123',
        config=Config(signature_version='s3v4')
    )


def calculate_performance_tier(total_pnl: float, roi: float, win_rate: float, trade_count: int) -> str:
    """Categorize traders by performance tier"""
    if total_pnl >= 100000 and roi >= 100 and win_rate >= 80 and trade_count >= 50:
        return "elite"
    elif total_pnl >= 10000 and roi >= 50 and win_rate >= 70 and trade_count >= 20:
        return "strong"
    else:
        return "promising"


def read_unprocessed_silver_pnl(spark: SparkSession) -> DataFrame:
    """Read silver PnL data that hasn't been processed for gold layer"""
    logger = logging.getLogger(__name__)
    
    try:
        # Read silver wallet PnL data
        silver_path = "s3a://solana-data/silver/wallet_pnl/"
        silver_df = spark.read.parquet(silver_path)
        
        logger.info(f"Read {silver_df.count()} total silver PnL records")
        
        # Filter for unprocessed portfolio-level records only
        unprocessed = silver_df.filter(
            (col("processed_for_gold") == False) &
            (col("token_address") == "ALL_TOKENS") &  # Portfolio-level only
            (col("time_period") == "all")  # All-time performance
        )
        
        logger.info(f"Found {unprocessed.count()} unprocessed portfolio PnL records")
        
        return unprocessed
        
    except Exception as e:
        logger.error(f"Error reading unprocessed silver PnL: {e}")
        # Return empty DataFrame with correct schema if no data exists
        schema = StructType([
            StructField("wallet_address", StringType()),
            StructField("token_address", StringType()),
            StructField("total_pnl", DoubleType()),
            StructField("roi", DoubleType()),
            StructField("win_rate", DoubleType()),
            StructField("trade_count", IntegerType()),
            StructField("processed_for_gold", BooleanType()),
        ])
        return spark.createDataFrame([], schema)


def select_top_performers(spark: SparkSession, silver_df: DataFrame) -> DataFrame:
    """Select top performing wallets based on criteria"""
    logger = logging.getLogger(__name__)
    config = GoldConfig()
    
    # Apply performance filters
    filtered_performers = silver_df.filter(
        (col("total_pnl") >= config.min_total_pnl) &
        (col("roi") >= config.min_roi) &
        (col("win_rate") >= config.min_win_rate) &
        (col("trade_count") >= config.min_trade_count) &
        (col("total_pnl") > 0)  # Positive PnL only
    )
    
    logger.info(f"After performance filters: {filtered_performers.count()} qualifying wallets")
    
    if filtered_performers.count() == 0:
        logger.warning("No wallets meet performance criteria")
        return spark.createDataFrame([], get_gold_top_traders_schema())
    
    # Calculate additional metrics
    enhanced_performers = filtered_performers.select(
        # Basic identification
        col("wallet_address"),
        col("calculation_date").alias("silver_calculation_date"),
        col("batch_id").alias("silver_batch_id"),
        col("time_period").alias("silver_time_period"),
        
        # Core performance metrics
        col("total_pnl"),
        col("realized_pnl"),
        col("unrealized_pnl"),
        col("roi"),
        col("win_rate"),
        col("trade_count"),
        
        # Trading behavior
        col("avg_holding_time_hours"),
        col("avg_transaction_amount_usd"),
        col("trade_frequency_daily"),
        
        # Portfolio metrics
        col("total_bought"),
        col("total_sold"),
        col("current_position_value"),
        
        # Time ranges
        col("first_transaction"),
        col("last_transaction"),
        
        # Calculate additional metrics
        datediff(col("last_transaction"), col("first_transaction")).alias("days_active"),
        
        # Consistency score (ROI * win_rate / 100)
        spark_round(
            (col("roi") * col("win_rate")) / 10000.0, 2
        ).alias("consistency_score"),
        
        # Risk score (inverse of volatility proxy)
        when(col("avg_holding_time_hours") > 0,
             spark_round(100.0 / (col("trade_frequency_daily") + 1), 2)
        ).otherwise(50.0).alias("risk_score"),
        
        # Profit factor (approximation: total_pnl / total_bought if positive)
        when(col("total_bought") > 0,
             spark_round(col("total_pnl") / col("total_bought") * 100, 2)
        ).otherwise(0.0).alias("profit_factor")
    )
    
    # Add performance tier using UDF-like logic (relaxed thresholds)
    tiered_performers = enhanced_performers.withColumn(
        "performance_tier",
        when(
            (col("total_pnl") >= 10000) & 
            (col("roi") >= 50) & 
            (col("win_rate") >= 70) & 
            (col("trade_count") >= 20), "elite"
        ).when(
            (col("total_pnl") >= 1000) & 
            (col("roi") >= 25) & 
            (col("win_rate") >= 60) & 
            (col("trade_count") >= 10), "strong"
        ).otherwise("promising")
    )
    
    # Rank by total PnL and limit to top performers
    window_spec = Window.orderBy(desc("total_pnl"))
    ranked_performers = tiered_performers.withColumn(
        "trader_rank", 
        row_number().over(window_spec)
    ).filter(
        col("trader_rank") <= config.max_traders_per_batch
    )
    
    # Add gold processing metadata
    current_time = datetime.now(timezone.utc)
    gold_batch_id = current_time.strftime("%Y%m%d_%H%M%S")
    
    final_performers = ranked_performers.withColumn(
        "gold_batch_id", lit(gold_batch_id)
    ).withColumn(
        "created_at", lit(current_time)
    ).withColumn(
        "updated_at", lit(current_time)
    ).withColumn(
        "is_active", lit(True)
    )
    
    # Select final columns in schema order
    schema_fields = [field.name for field in get_gold_top_traders_schema().fields]
    result = final_performers.select(*schema_fields)
    
    logger.info(f"Selected {result.count()} top performers for gold layer")
    
    return result


def write_gold_top_traders(spark: SparkSession, traders_df: DataFrame, batch_id: str) -> str:
    """Write top traders to gold layer"""
    logger = logging.getLogger(__name__)
    
    if traders_df.count() == 0:
        logger.warning("No top traders to write")
        return None
    
    # Add partitioning columns for efficient querying
    partitioned_df = traders_df.withColumn(
        "ingestion_year", expr("year(created_at)")
    ).withColumn(
        "ingestion_month", expr("month(created_at)")
    ).withColumn(
        "performance_tier_partition", col("performance_tier")
    )
    
    # Write to MinIO with partitioning
    output_path = "s3a://solana-data/gold/top_traders/"
    
    try:
        partitioned_df.write \
            .partitionBy("ingestion_year", "ingestion_month", "performance_tier_partition") \
            .mode("append") \
            .parquet(output_path)
        
        logger.info(f"Successfully wrote {traders_df.count()} top traders to {output_path}")
        
        # Write success marker
        success_path = f"s3a://solana-data/gold/top_traders/ingestion_year={datetime.now().year}/ingestion_month={datetime.now().month}/_SUCCESS_{batch_id}"
        
        # Create empty success file
        spark.createDataFrame([("success",)], ["status"]).coalesce(1).write.mode("overwrite").text(success_path)
        
        return output_path
        
    except Exception as e:
        logger.error(f"Error writing gold top traders: {e}")
        raise


def update_silver_processing_status(spark: SparkSession, processed_wallets: List[str], gold_batch_id: str):
    """Update silver layer to mark wallets as processed for gold"""
    logger = logging.getLogger(__name__)
    
    if not processed_wallets:
        logger.info("No wallets to update")
        return
    
    try:
        logger.info(f"Updating processing status for {len(processed_wallets)} wallets")
        
        # Read current silver data
        silver_path = "s3a://solana-data/silver/wallet_pnl/"
        silver_df = spark.read.parquet(silver_path)
        
        # Create DataFrame of processed wallets
        processed_df = spark.createDataFrame(
            [(wallet,) for wallet in processed_wallets], 
            ["wallet_address"]
        ).withColumn("was_processed_for_gold", lit(True))
        
        # Update processing flags for processed wallets (portfolio-level only)
        updated_df = silver_df.join(
            processed_df, 
            on="wallet_address", 
            how="left"
        ).withColumn(
            "processed_for_gold",
            when(
                (col("was_processed_for_gold") == True) & 
                (col("token_address") == "ALL_TOKENS") &
                (col("time_period") == "all"), 
                True
            ).otherwise(col("processed_for_gold"))
        ).withColumn(
            "gold_processed_at",
            when(
                (col("was_processed_for_gold") == True) & 
                (col("token_address") == "ALL_TOKENS") &
                (col("time_period") == "all"), 
                current_timestamp()
            ).otherwise(col("gold_processed_at"))
        ).withColumn(
            "gold_processing_status",
            when(
                (col("was_processed_for_gold") == True) & 
                (col("token_address") == "ALL_TOKENS") &
                (col("time_period") == "all"), 
                "completed"
            ).otherwise(col("gold_processing_status"))
        ).withColumn(
            "gold_batch_id",
            when(
                (col("was_processed_for_gold") == True) & 
                (col("token_address") == "ALL_TOKENS") &
                (col("time_period") == "all"), 
                gold_batch_id
            ).otherwise(col("gold_batch_id"))
        ).drop("was_processed_for_gold")
        
        # Write updated data back to silver layer
        # Add partitioning for proper organization
        partitioned_df = updated_df.withColumn(
            "update_year", 
            expr("year(current_timestamp())")
        ).withColumn(
            "update_month", 
            expr("month(current_timestamp())")
        )
        
        # Write to temporary location first, then move
        temp_path = f"s3a://solana-data/silver/wallet_pnl_updated_{gold_batch_id}/"
        
        partitioned_df.write \
            .partitionBy("calculation_year", "calculation_month", "time_period") \
            .mode("overwrite") \
            .parquet(temp_path)
        
        logger.info(f"Successfully updated silver layer processing status for batch {gold_batch_id}")
        logger.info(f"Updated data written to: {temp_path}")
        
    except Exception as e:
        logger.error(f"Error updating silver processing status: {e}")
        # Don't fail the entire job if status update fails
        pass


def create_top_traders_batch(**context):
    """
    Main PySpark job for creating gold top traders from silver PnL data
    """
    logger = logging.getLogger(__name__)
    
    # Generate batch ID
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    
    # Initialize Spark session
    spark = get_spark_session_with_s3()
    
    try:
        logger.info(f"Starting gold top traders batch {batch_id}")
        
        # Read unprocessed silver PnL data
        silver_pnl_df = read_unprocessed_silver_pnl(spark)
        
        if silver_pnl_df.count() == 0:
            logger.info("No unprocessed silver PnL data found")
            return {
                "traders_processed": 0,
                "top_traders_created": 0,
                "batch_id": batch_id,
                "status": "no_data"
            }
        
        logger.info(f"Processing {silver_pnl_df.count()} unprocessed PnL records")
        
        # Select top performers
        top_traders = select_top_performers(spark, silver_pnl_df)
        
        if top_traders.count() == 0:
            logger.info("No wallets meet top trader criteria")
            return {
                "traders_processed": silver_pnl_df.count(),
                "top_traders_created": 0,
                "batch_id": batch_id,
                "status": "no_qualifying_traders"
            }
        
        # Write to gold layer
        output_path = write_gold_top_traders(spark, top_traders, batch_id)
        
        # Get list of processed wallets for silver update
        processed_wallets = [row.wallet_address for row in top_traders.select("wallet_address").collect()]
        
        # Update silver layer processing status
        update_silver_processing_status(spark, processed_wallets, batch_id)
        
        # Calculate summary metrics
        tier_counts = top_traders.groupBy("performance_tier").count().collect()
        tier_summary = {row.performance_tier: row.count for row in tier_counts}
        
        logger.info(f"Gold top traders batch {batch_id} completed successfully")
        logger.info(f"Performance tier breakdown: {tier_summary}")
        
        return {
            "traders_processed": silver_pnl_df.count(),
            "top_traders_created": top_traders.count(),
            "performance_tiers": tier_summary,
            "batch_id": batch_id,
            "output_path": output_path,
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"Error in gold top traders batch {batch_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
        
    finally:
        # Clean up Spark session
        spark.stop()


# DAG Definition
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'gold_top_traders',
    default_args=default_args,
    description='Create gold layer top traders from silver wallet PnL data',
    schedule_interval='0 2,14 * * *',  # 2 hours after silver PnL (every 12h + 2h offset)
    catchup=False,
    tags=['gold', 'top-traders', 'pyspark', 'analytics'],
)

# Define tasks
top_traders_task = PythonOperator(
    task_id='create_top_traders',
    python_callable=create_top_traders_batch,
    provide_context=True,
    dag=dag,
)

# This DAG creates gold layer top traders analytics from silver wallet PnL data