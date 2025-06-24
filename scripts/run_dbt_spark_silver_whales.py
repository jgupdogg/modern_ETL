#!/usr/bin/env python3
"""
Run dbt silver whale transformation using PySpark for Delta Lake
This script creates a Spark session with Delta Lake support and runs the silver whales transformation
"""

import logging
import sys
import os
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_pyspark_silver_whales_transformation():
    """
    Run the silver whales transformation using PySpark with Delta Lake support
    """
    logger.info("🚀 Starting PySpark silver whales transformation for Delta Lake...")
    
    try:
        # Import required libraries
        from pyspark.sql import SparkSession
        from delta.tables import DeltaTable
        
        logger.info("✅ PySpark and Delta Lake libraries imported")
        
        # Create Spark session with Delta Lake support
        spark = (
            SparkSession.builder.appName("DBTSilverWhalesTransformation")
            
            # JAR packages for Delta Lake + MinIO (same as bronze)
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4")
            
            # Delta Lake extensions
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            
            # MinIO S3A configuration
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            
            .getOrCreate()
        )
        
        logger.info("✅ Spark session created with Delta Lake support")
        
        # Define paths
        bronze_delta_path = "s3a://smart-trader/bronze/whale_holders"
        silver_delta_path = "s3a://smart-trader/silver/tracked_whales_delta"
        
        logger.info(f"📖 Reading from bronze Delta table: {bronze_delta_path}")
        
        # Read from bronze Delta Lake table
        bronze_df = spark.read.format("delta").load(bronze_delta_path)
        bronze_count = bronze_df.count()
        
        logger.info(f"✅ Read {bronze_count} records from bronze Delta table")
        
        # Apply silver transformation logic (same as dbt model)
        logger.info("🔄 Applying silver whales transformation logic...")
        
        # Basic filtering - use holdings_amount since holdings_value_usd is 0 in current data
        filtered_df = bronze_df.filter(
            (bronze_df.wallet_address.isNotNull()) &
            (bronze_df.token_address.isNotNull()) &
            (bronze_df.holdings_amount > 0) &  # Only whales with actual token holdings
            (bronze_df.rank.isNotNull()) &  # Must have a valid rank
            (bronze_df._delta_operation.isin(['BRONZE_WHALE_CREATE', 'BRONZE_WHALE_APPEND']))
        )
        
        logger.info(f"📊 Filtered to {filtered_df.count()} records meeting basic criteria")
        
        # Enhanced transformations
        from pyspark.sql.functions import col, when, round as spark_round, current_timestamp, lit, row_number, concat, coalesce
        from pyspark.sql.window import Window
        
        enhanced_df = filtered_df.select(
            # Create composite whale ID for unique tracking
            concat(col("wallet_address"), lit("_"), col("token_address")).alias("whale_id"),
            
            # Core whale information
            col("wallet_address"),
            col("token_address"),
            col("token_symbol"),
            col("token_name"),
            col("rank"),
            col("holdings_amount"),
            col("holdings_value_usd"),
            col("holdings_percentage"),
            
            # Calculate whale tier based on holdings amount (token quantity)
            when(col("holdings_amount") >= 1000000, "MEGA")
            .when(col("holdings_amount") >= 100000, "LARGE")
            .when(col("holdings_amount") >= 10000, "MEDIUM")
            .when(col("holdings_amount") >= 1000, "SMALL")
            .otherwise("MINIMAL").alias("whale_tier"),
            
            # Calculate rank tier for easier filtering
            when(col("rank") <= 3, "TOP_3")
            .when(col("rank") <= 10, "TOP_10")
            .when(col("rank") <= 50, "TOP_50")
            .otherwise("OTHER").alias("rank_tier"),
            
            # Transaction tracking status
            col("txns_fetched"),
            col("txns_last_fetched_at"),
            coalesce(col("txns_fetch_status"), lit("pending")).alias("txns_fetch_status"),
            
            # Metadata fields
            col("rank_date"),
            col("batch_id").alias("bronze_batch_id"),
            col("_delta_timestamp").alias("bronze_delta_timestamp"),
            col("_delta_operation").alias("source_operation"),
            col("_delta_created_at").alias("bronze_created_at"),
            current_timestamp().alias("silver_created_at"),
            
            # Processing status logic
            when(col("txns_fetched") == True, "completed")
            .when(col("txns_fetch_status") == "pending", "pending")
            .otherwise("ready").alias("processing_status"),
            
            lit(True).alias("is_newly_tracked")
        )
        
        # Deduplication window
        window = Window.partitionBy("wallet_address", "token_address").orderBy(
            col("rank_date").desc(), 
            col("bronze_delta_timestamp").desc()
        )
        
        # Add row numbers and filter to latest
        final_df = enhanced_df.withColumn("rn", row_number().over(window)) \
                            .filter(col("rn") == 1) \
                            .drop("rn") \
                            .orderBy(col("holdings_amount").desc(), col("rank").asc())
        
        final_count = final_df.count()
        logger.info(f"✅ Final silver whales dataset: {final_count} unique whale-token pairs")
        
        # Show sample data
        logger.info("📋 Sample silver whales data:")
        final_df.select("whale_id", "token_symbol", "wallet_address", "whale_tier", "rank_tier", "holdings_amount", "processing_status") \
                .show(5, truncate=False)
        
        # Write to silver Delta Lake table
        logger.info(f"💾 Writing to silver Delta table: {silver_delta_path}")
        
        final_df.write.format("delta") \
                     .mode("overwrite") \
                     .option("overwriteSchema", "true") \
                     .save(silver_delta_path)
        
        logger.info("✅ Silver whales Delta table created successfully!")
        
        # Verify the table
        silver_table = DeltaTable.forPath(spark, silver_delta_path)
        history = silver_table.history(1).collect()
        
        logger.info(f"📈 Silver whales table version: {history[0]['version']}")
        logger.info(f"📊 Silver whales table operation: {history[0]['operation']}")
        
        # Read back to verify
        verify_df = spark.read.format("delta").load(silver_delta_path)
        verify_count = verify_df.count()
        
        logger.info(f"✅ Verification: Read back {verify_count} records from silver whales table")
        
        # Show whale tier distribution
        logger.info("📊 Whale tier distribution:")
        verify_df.groupBy("whale_tier").count().orderBy("count", ascending=False).show()
        
        # Show processing status distribution
        logger.info("📊 Processing status distribution:")
        verify_df.groupBy("processing_status").count().show()
        
        # Stop Spark session
        spark.stop()
        logger.info("✅ Spark session stopped")
        
        logger.info("🎉 PySpark silver whales transformation completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ PySpark silver whales transformation failed: {e}")
        return False

if __name__ == "__main__":
    success = run_pyspark_silver_whales_transformation()
    if success:
        print("\n🚀 Silver whales transformation ready!")
    else:
        print("\n❌ Fix issues before proceeding")
    
    sys.exit(0 if success else 1)