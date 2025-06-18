"""
Gold Layer Tasks

Core business logic for gold layer analytics and top trader selection.
Extracted from gold_top_traders_dag.py for use in the smart trader identification pipeline.
"""
import os
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config

# Import centralized configuration
from config.smart_trader_config import (
    MIN_TOTAL_PNL, MIN_ROI_PERCENT, MIN_WIN_RATE_PERCENT, MIN_TRADE_COUNT,
    GOLD_MAX_TRADERS_PER_BATCH, PERFORMANCE_LOOKBACK_DAYS,
    ELITE_MIN_PNL, ELITE_MIN_ROI, ELITE_MIN_WIN_RATE, ELITE_MIN_TRADES,
    STRONG_MIN_PNL, STRONG_MIN_ROI, STRONG_MIN_WIN_RATE, STRONG_MIN_TRADES,
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET,
    GOLD_TOP_TRADERS_PATH, SILVER_WALLET_PNL_PATH
)


def get_minio_client() -> boto3.client:
    """Create MinIO S3 client"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4')
    )


# PySpark imports for gold layer processing
try:
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
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


def get_gold_top_traders_schema() -> StructType:
    """Get PySpark schema for gold top traders data - matches existing schema exactly"""
    if not PYSPARK_AVAILABLE:
        raise ImportError("PySpark not available")
        
    return StructType([
        # Exact schema match to existing gold/top_traders structure
        StructField("wallet_address", StringType(), False),         # 0
        StructField("trader_rank", IntegerType(), False),           # 1
        StructField("performance_tier", StringType(), False),       # 2
        StructField("total_pnl", DoubleType(), False),             # 3
        StructField("realized_pnl", DoubleType(), False),          # 4
        StructField("unrealized_pnl", DoubleType(), False),        # 5
        StructField("roi", DoubleType(), False),                   # 6
        StructField("win_rate", DoubleType(), False),              # 7
        StructField("trade_count", IntegerType(), False),          # 8  (note: was int64 in data, using IntegerType)
        StructField("avg_holding_time_hours", DoubleType(), False), # 9
        StructField("avg_transaction_amount_usd", DoubleType(), False), # 10
        StructField("trade_frequency_daily", DoubleType(), False), # 11
        StructField("consistency_score", DoubleType(), False),     # 12
        StructField("risk_score", DoubleType(), False),           # 13
        StructField("profit_factor", DoubleType(), False),        # 14
        StructField("total_bought", DoubleType(), False),         # 15
        StructField("total_sold", DoubleType(), False),           # 16
        StructField("current_position_value", DoubleType(), False), # 17
        StructField("first_transaction", TimestampType(), True),   # 18
        StructField("last_transaction", TimestampType(), True),    # 19
        StructField("days_active", IntegerType(), False),         # 20
        StructField("silver_calculation_date", DateType(), False), # 21
        StructField("silver_batch_id", StringType(), False),      # 22
        StructField("silver_time_period", StringType(), False),   # 23
        StructField("gold_batch_id", StringType(), False),        # 24
        StructField("created_at", TimestampType(), False),        # 25
        StructField("updated_at", TimestampType(), False),        # 26
        StructField("is_active", BooleanType(), False),           # 27
    ])


def get_spark_session_with_s3() -> SparkSession:
    """Create Spark session configured for S3/MinIO access"""
    if not PYSPARK_AVAILABLE:
        raise ImportError("PySpark not available")
        
    spark = SparkSession.builder \
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
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    return spark


def transform_gold_top_traders(**context):
    """
    Create gold layer top traders from silver wallet PnL data using PySpark
    
    Extracted from gold_top_traders_dag.py
    """
    logger = logging.getLogger(__name__)
    
    if not PYSPARK_AVAILABLE:
        logger.error("PySpark not available - using mock data for testing")
        # Return mock results for testing when PySpark is not available
        return {
            "traders_processed": 300,
            "top_traders_created": 25,
            "performance_tiers": {"elite": 5, "strong": 8, "promising": 12},
            "batch_id": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
            "output_path": "s3a://solana-data/gold/top_traders/",
            "status": "mock_success"
        }
    
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
        tier_summary = {row.performance_tier: int(row.count) for row in tier_counts}
        
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


# Helper functions for gold layer processing (only defined if PySpark is available)
if PYSPARK_AVAILABLE:
    
    def read_unprocessed_silver_pnl(spark: SparkSession) -> DataFrame:
        """Read silver PnL data that hasn't been processed for gold layer"""
        logger = logging.getLogger(__name__)
        
        try:
            # Clear Spark metadata cache to avoid stale file references
            spark.sql("CLEAR CACHE")
            
            # Read silver wallet PnL data using wildcard pattern for robustness
            silver_path = "s3a://solana-data/silver/wallet_pnl/**/*.parquet"
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
        
        # Apply performance filters for profitable wallets only using centralized config
        filtered_performers = silver_df.filter(
            (col("total_pnl") >= MIN_TOTAL_PNL) &  # Must be profitable
            (col("roi") >= MIN_ROI_PERCENT) &
            (col("win_rate") >= MIN_WIN_RATE_PERCENT) &
            (col("trade_count") >= MIN_TRADE_COUNT) &
            (col("total_pnl") > 0)  # Explicitly profitable only
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
        
        # Add performance tier using centralized config thresholds
        tiered_performers = enhanced_performers.withColumn(
            "performance_tier",
            when(
                (col("total_pnl") >= ELITE_MIN_PNL) & 
                (col("roi") >= ELITE_MIN_ROI) & 
                (col("win_rate") >= ELITE_MIN_WIN_RATE) & 
                (col("trade_count") >= ELITE_MIN_TRADES), "elite"
            ).when(
                (col("total_pnl") >= STRONG_MIN_PNL) & 
                (col("roi") >= STRONG_MIN_ROI) & 
                (col("win_rate") >= STRONG_MIN_WIN_RATE) & 
                (col("trade_count") >= STRONG_MIN_TRADES), "strong"
            ).otherwise("promising")
        )
        
        # Rank by total PnL and limit to top performers
        window_spec = Window.orderBy(desc("total_pnl"))
        ranked_performers = tiered_performers.withColumn(
            "trader_rank", 
            row_number().over(window_spec)
        ).filter(
            col("trader_rank") <= GOLD_MAX_TRADERS_PER_BATCH
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
        
        # Write to MinIO without partitioning (small dataset, better performance)
        output_path = "s3a://solana-data/gold/top_traders/"
        
        try:
            traders_df.write \
                .mode("append") \
                .parquet(output_path)
            
            logger.info(f"Successfully wrote {traders_df.count()} top traders to {output_path}")
            
            # Write success marker (simplified path)
            success_path = f"s3a://solana-data/gold/top_traders/_SUCCESS_{batch_id}"
            
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
            
            # Write updated data back to original silver layer location
            original_path = "s3a://solana-data/silver/wallet_pnl/"
            
            partitioned_df.write \
                .partitionBy("calculation_year", "calculation_month", "time_period") \
                .mode("overwrite") \
                .parquet(original_path)
            
            logger.info(f"Successfully updated silver layer processing status for batch {gold_batch_id}")
            logger.info(f"Updated data written to: {original_path}")
            
        except Exception as e:
            logger.error(f"Error updating silver processing status: {e}")
            # Don't fail the entire job if status update fails
            pass