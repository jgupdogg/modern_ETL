#!/usr/bin/env python3
"""
Run dbt silver transformation using PySpark for Delta Lake
This script creates a Spark session with Delta Lake support and runs the silver transformation
"""

import logging
import sys
import os
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_pyspark_silver_transformation():
    """
    Run the silver transformation using PySpark with Delta Lake support
    """
    logger.info("🚀 Starting PySpark silver transformation for Delta Lake...")
    
    try:
        # Import required libraries
        from pyspark.sql import SparkSession
        from delta.tables import DeltaTable
        
        logger.info("✅ PySpark and Delta Lake libraries imported")
        
        # Create Spark session with Delta Lake support
        spark = (
            SparkSession.builder.appName("DBTSilverTransformation")
            
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
        bronze_delta_path = "s3a://smart-trader/bronze/token_metrics"
        silver_delta_path = "s3a://smart-trader/silver/tracked_tokens_delta"
        
        logger.info(f"📖 Reading from bronze Delta table: {bronze_delta_path}")
        
        # Read from bronze Delta Lake table
        bronze_df = spark.read.format("delta").load(bronze_delta_path)
        bronze_count = bronze_df.count()
        
        logger.info(f"✅ Read {bronze_count} records from bronze Delta table")
        
        # Apply silver transformation logic (same as dbt model)
        logger.info("🔄 Applying silver transformation logic...")
        
        # Basic filtering
        filtered_df = bronze_df.filter(
            (bronze_df.liquidity >= 100000) &  # Min $100K liquidity
            (bronze_df.token_address.isNotNull()) &
            (bronze_df.symbol.isNotNull()) &
            (bronze_df.liquidity.isNotNull()) &
            (bronze_df.price.isNotNull()) &
            (bronze_df._delta_operation.isin(['BRONZE_TOKEN_CREATE', 'BRONZE_TOKEN_APPEND']))
        )
        
        logger.info(f"📊 Filtered to {filtered_df.count()} records meeting basic criteria")
        
        # Enhanced transformations
        from pyspark.sql.functions import col, when, round as spark_round, current_timestamp, lit, row_number
        from pyspark.sql.window import Window
        
        enhanced_df = filtered_df.select(
            col("token_address"),
            col("symbol"),
            col("name"),
            col("decimals"),
            col("liquidity"),
            col("price"),
            col("fdv"),
            col("holder"),
            
            # Calculate liquidity tier
            when(col("liquidity") >= 1000000, "HIGH")
            .when(col("liquidity") >= 500000, "MEDIUM")
            .when(col("liquidity") >= 100000, "LOW")
            .otherwise("MINIMAL").alias("liquidity_tier"),
            
            # Calculate FDV per holder
            when(col("holder") > 0, spark_round(col("fdv") / col("holder"), 2))
            .otherwise(None).alias("fdv_per_holder"),
            
            # Metadata fields
            col("processing_date"),
            col("batch_id").alias("bronze_batch_id"),
            col("_delta_timestamp").alias("bronze_delta_timestamp"),
            col("_delta_operation").alias("source_operation"),
            col("_delta_created_at").alias("bronze_created_at"),
            current_timestamp().alias("silver_created_at"),
            
            # Tracking fields
            lit("pending").alias("whale_fetch_status"),
            lit(None).cast("timestamp").alias("whale_fetched_at"),
            lit(True).alias("is_newly_tracked")
        )
        
        # Deduplication window
        window = Window.partitionBy("token_address").orderBy(
            col("processing_date").desc(), 
            col("bronze_delta_timestamp").desc()
        )
        
        # Add row numbers and filter to latest
        final_df = enhanced_df.withColumn("rn", row_number().over(window)) \
                            .filter(col("rn") == 1) \
                            .drop("rn") \
                            .orderBy(col("liquidity").desc())
        
        final_count = final_df.count()
        logger.info(f"✅ Final silver dataset: {final_count} unique tokens")
        
        # Show sample data
        logger.info("📋 Sample silver data:")
        final_df.select("symbol", "name", "liquidity", "liquidity_tier", "fdv_per_holder") \
                .show(5, truncate=False)
        
        # Write to silver Delta Lake table
        logger.info(f"💾 Writing to silver Delta table: {silver_delta_path}")
        
        final_df.write.format("delta") \
                     .mode("overwrite") \
                     .option("overwriteSchema", "true") \
                     .save(silver_delta_path)
        
        logger.info("✅ Silver Delta table created successfully!")
        
        # Verify the table
        silver_table = DeltaTable.forPath(spark, silver_delta_path)
        history = silver_table.history(1).collect()
        
        logger.info(f"📈 Silver table version: {history[0]['version']}")
        logger.info(f"📊 Silver table operation: {history[0]['operation']}")
        
        # Read back to verify
        verify_df = spark.read.format("delta").load(silver_delta_path)
        verify_count = verify_df.count()
        
        logger.info(f"✅ Verification: Read back {verify_count} records from silver table")
        
        # Stop Spark session
        spark.stop()
        logger.info("✅ Spark session stopped")
        
        logger.info("🎉 PySpark silver transformation completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ PySpark silver transformation failed: {e}")
        return False

if __name__ == "__main__":
    success = run_pyspark_silver_transformation()
    if success:
        print("\n🚀 Silver transformation ready!")
    else:
        print("\n❌ Fix issues before proceeding")
    
    sys.exit(0 if success else 1)