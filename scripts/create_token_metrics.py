#!/usr/bin/env python3
"""
Create Token Metrics Table - Delta Lake Compatible
Create the enhanced token metrics table structure in smart-trader bucket
Uses current Parquet format with Delta-compatible schema for now
"""

import sys
import os
import json
import logging
from datetime import datetime, date
from typing import Dict, List, Any

# Add paths for imports - adjust for host execution
sys.path.append('/home/jgupdogg/dev/claude_pipeline/dags')

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType, DateType, LongType
    from pyspark.sql.functions import lit, current_timestamp, current_date, col, when, isnan, isnull
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

def get_spark_session():
    """Create Spark session with S3A configuration"""
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark not available")
    
    spark = SparkSession.builder \
        .appName("CreateTokenMetricsTable") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "512m") \
        .config("spark.driver.maxResultSize", "256m") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_enhanced_token_metrics_schema():
    """Enhanced schema for token metrics (37 columns)"""
    return StructType([
        # Core identity
        StructField("token_address", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("name", StringType(), True),
        
        # Market data (cleaned)
        StructField("liquidity", DoubleType(), False),
        StructField("price", DoubleType(), False),
        StructField("market_cap", DoubleType(), True),
        StructField("fdv", DoubleType(), True),
        StructField("volume_24h_usd", DoubleType(), True),
        
        # Volume metrics (all cleaned from NaN)
        StructField("volume_1h_usd", DoubleType(), True),
        StructField("volume_6h_usd", DoubleType(), True),
        StructField("volume_12h_usd", DoubleType(), True),
        
        # Price changes
        StructField("price_change_1h_percent", DoubleType(), True),
        StructField("price_change_6h_percent", DoubleType(), True),
        StructField("price_change_12h_percent", DoubleType(), True),
        StructField("price_change_24h_percent", DoubleType(), True),
        
        # Trading activity
        StructField("trade_1h_count", LongType(), True),
        StructField("trade_6h_count", LongType(), True),
        StructField("trade_12h_count", LongType(), True),
        StructField("trade_24h_count", LongType(), True),
        
        # Quality flags (NEW - Enhanced)
        StructField("data_quality_score", DoubleType(), True),
        StructField("has_complete_metrics", BooleanType(), True),
        StructField("volume_metrics_available", BooleanType(), True),
        StructField("price_change_available", BooleanType(), True),
        
        # Processing metadata
        StructField("ingestion_date", DateType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("batch_id", StringType(), False),
        
        # Partitioning helper columns (NEW)
        StructField("ingestion_year", IntegerType(), True),
        StructField("ingestion_month", IntegerType(), True),
        StructField("ingestion_day", IntegerType(), True)
    ])

def create_sample_data(spark, schema):
    """Create sample token metrics data for testing"""
    logger = logging.getLogger(__name__)
    
    # Sample data representing the type of enhanced token metrics we'll have
    sample_data = [
        # High quality token example
        (
            "85VBFQZC9TZkfaptBWjvUw7YbZjy52A6mjtPGjstQAmQ",  # token_address
            "W",  # symbol
            "Wrapped Token",  # name
            380000.0,  # liquidity
            0.65,  # price
            1200000.0,  # market_cap
            1500000.0,  # fdv
            95000.0,  # volume_24h_usd
            12000.0,  # volume_1h_usd
            45000.0,  # volume_6h_usd
            78000.0,  # volume_12h_usd
            -2.5,  # price_change_1h_percent
            1.8,  # price_change_6h_percent
            4.2,  # price_change_12h_percent
            -1.1,  # price_change_24h_percent
            25,  # trade_1h_count
            128,  # trade_6h_count
            245,  # trade_12h_count
            456,  # trade_24h_count
            0.95,  # data_quality_score (high quality)
            True,  # has_complete_metrics
            True,  # volume_metrics_available
            True,  # price_change_available
            date.today(),  # ingestion_date
            datetime.now(),  # ingested_at
            f"test_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",  # batch_id
            date.today().year,  # ingestion_year
            date.today().month,  # ingestion_month
            date.today().day  # ingestion_day
        ),
        # Medium quality token example (some missing data)
        (
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # token_address (USDC)
            "USDC",  # symbol
            "USD Coin",  # name
            2500000.0,  # liquidity
            1.0,  # price
            None,  # market_cap (missing)
            None,  # fdv (missing)
            250000.0,  # volume_24h_usd
            None,  # volume_1h_usd (missing)
            None,  # volume_6h_usd (missing)
            180000.0,  # volume_12h_usd
            0.0,  # price_change_1h_percent
            0.1,  # price_change_6h_percent
            -0.05,  # price_change_12h_percent
            0.02,  # price_change_24h_percent
            45,  # trade_1h_count
            None,  # trade_6h_count (missing)
            289,  # trade_12h_count
            612,  # trade_24h_count
            0.65,  # data_quality_score (medium quality)
            False,  # has_complete_metrics
            False,  # volume_metrics_available (some missing)
            True,  # price_change_available
            date.today(),  # ingestion_date
            datetime.now(),  # ingested_at
            f"test_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",  # batch_id
            date.today().year,  # ingestion_year
            date.today().month,  # ingestion_month
            date.today().day  # ingestion_day
        )
    ]
    
    df = spark.createDataFrame(sample_data, schema)
    
    # Apply data cleaning transformations
    df_cleaned = df \
        .fillna(0.0, subset=["volume_1h_usd", "volume_6h_usd", "volume_12h_usd"]) \
        .fillna(0, subset=["trade_1h_count", "trade_6h_count", "trade_12h_count", "trade_24h_count"]) \
        .withColumn("volume_metrics_available", 
                   col("volume_1h_usd").isNotNull() & 
                   col("volume_6h_usd").isNotNull() & 
                   col("volume_12h_usd").isNotNull()) \
        .withColumn("price_change_available",
                   col("price_change_1h_percent").isNotNull() &
                   col("price_change_24h_percent").isNotNull())
    
    logger.info(f"Created sample data with {df_cleaned.count()} records")
    return df_cleaned

def create_token_metrics_table():
    """Create the enhanced token metrics table structure"""
    logger = setup_logging()
    
    if not PYSPARK_AVAILABLE:
        logger.error("❌ PySpark not available")
        return False
    
    logger.info("🚀 Creating Enhanced Token Metrics Table")
    
    try:
        # Create Spark session
        spark = get_spark_session()
        logger.info("✅ Spark session created")
        
        # Get enhanced schema
        schema = get_enhanced_token_metrics_schema()
        logger.info(f"✅ Schema defined: {len(schema.fields)} columns")
        
        # Create sample data
        df = create_sample_data(spark, schema)
        logger.info("✅ Sample data created")
        
        # Define output path
        output_path = "s3a://smart-trader/bronze/token_metrics"
        
        # Write table with partitioning (Delta-compatible structure)
        logger.info(f"📊 Writing to: {output_path}")
        
        df.write \
            .mode("overwrite") \
            .partitionBy("ingestion_year", "ingestion_month", "ingestion_day") \
            .parquet(output_path)
        
        logger.info("✅ Table written successfully")
        
        # Verify table creation
        logger.info("🔍 Verifying table creation...")
        test_df = spark.read.parquet(output_path)
        record_count = test_df.count()
        column_count = len(test_df.columns)
        
        logger.info(f"✅ Verification successful: {record_count} records, {column_count} columns")
        
        # Show sample of created data
        logger.info("📋 Sample data from created table:")
        test_df.select("token_address", "symbol", "liquidity", "data_quality_score", "ingestion_date").show()
        
        # Show partitions created
        logger.info("📁 Partitions created:")
        partitions = test_df.select("ingestion_year", "ingestion_month", "ingestion_day").distinct().collect()
        for partition in partitions:
            logger.info(f"  - year={partition.ingestion_year}/month={partition.ingestion_month}/day={partition.ingestion_day}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Table creation failed: {e}")
        return False
    finally:
        if 'spark' in locals():
            spark.stop()

def main():
    """Main execution function"""
    logger = setup_logging()
    
    logger.info("🎯 Token Metrics Table Creation - Phase 1 Proof of Concept")
    logger.info("=" * 60)
    
    success = create_token_metrics_table()
    
    logger.info("=" * 60)
    if success:
        logger.info("🎉 Token metrics table created successfully!")
        logger.info("📊 Ready for: MERGE operations, data migration, Delta conversion")
        logger.info("🔗 Table location: s3a://smart-trader/bronze/token_metrics")
    else:
        logger.info("❌ Table creation failed - check logs for details")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)