#!/usr/bin/env python3
"""
Test script to verify silver wallet PnL data can be read by gold task.
"""
import logging
import sys
import os

# Add the dags directory to the path so we can import from config
sys.path.append('/home/jgupdogg/dev/claude_pipeline/dags')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    
    # Create Spark session like the gold task
    spark = SparkSession.builder \
        .appName("TestSilverGoldPath") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Testing silver wallet PnL data read...")
    
    # Try to read the silver data exactly like the gold task does
    silver_path = "s3a://solana-data/silver/wallet_pnl/"
    logger.info(f"Reading from: {silver_path}")
    
    silver_df = spark.read.parquet(silver_path)
    total_count = silver_df.count()
    logger.info(f"Total silver PnL records: {total_count}")
    
    # Show the schema
    logger.info("Schema:")
    silver_df.printSchema()
    
    # Check what time_periods exist
    time_periods = silver_df.select("time_period").distinct().collect()
    logger.info("Available time_periods:")
    for row in time_periods:
        logger.info(f"  - {row.time_period}")
    
    # Check what token_address values exist
    token_addresses = silver_df.select("token_address").distinct().collect()
    logger.info("Available token_addresses:")
    for row in token_addresses:
        logger.info(f"  - {row.token_address}")
    
    # Filter exactly like the gold task
    unprocessed = silver_df.filter(
        (col("processed_for_gold") == False) &
        (col("token_address") == "ALL_TOKENS") &
        (col("time_period") == "all")
    )
    
    unprocessed_count = unprocessed.count()
    logger.info(f"Unprocessed portfolio PnL records for time_period='all': {unprocessed_count}")
    
    if unprocessed_count > 0:
        logger.info("Sample unprocessed record:")
        unprocessed.select("wallet_address", "time_period", "token_address", "total_pnl", "processed_for_gold").show(1)
    
    # Try different time period filters to see what works
    for period in ["all", "week", "month", "quarter", "7day", "30day", "60day"]:
        period_count = silver_df.filter(col("time_period") == period).count()
        logger.info(f"Records with time_period='{period}': {period_count}")

    spark.stop()
    logger.info("Test completed successfully!")
    
except Exception as e:
    logger.error(f"Test failed: {e}")
    import traceback
    logger.error(traceback.format_exc())
    sys.exit(1)