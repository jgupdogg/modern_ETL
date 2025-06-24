#!/usr/bin/env python3
"""
Run dbt gold smart traders transformation using PySpark for Delta Lake
This script creates smart trader rankings from silver wallet PnL data
"""

import logging
import sys
import os
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_pyspark_gold_smart_traders_transformation():
    """
    Run the gold smart traders transformation using PySpark with Delta Lake support
    """
    logger.info("🚀 Starting PySpark gold smart traders transformation for Delta Lake...")
    
    try:
        # Import required libraries
        from pyspark.sql import SparkSession
        from delta.tables import DeltaTable
        
        logger.info("✅ PySpark and Delta Lake libraries imported")
        
        # Create Spark session with Delta Lake support
        spark = (
            SparkSession.builder.appName("DBTGoldSmartTradersTransformation")
            
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
        silver_delta_path = "s3a://smart-trader/silver/wallet_pnl"
        gold_delta_path = "s3a://smart-trader/gold/smart_traders_delta"
        
        logger.info(f"📖 Reading from silver Delta table: {silver_delta_path}")
        
        # Read from silver Delta Lake table
        silver_df = spark.read.format("delta").load(silver_delta_path)
        silver_count = silver_df.count()
        
        logger.info(f"✅ Read {silver_count} records from silver Delta table")
        
        # Apply gold transformation logic (same as dbt model)
        logger.info("🔄 Applying gold smart traders transformation logic...")
        
        # Step 1: Filter for qualified traders
        qualified_traders = silver_df.filter(
            # Smart trader qualification criteria: profitable or successful
            ((silver_df.win_rate > 0) | (silver_df.total_pnl > 0)) &
            
            # Additional quality filters
            (silver_df.trade_count >= 1) &  # Must have at least 1 trade
            (silver_df.total_bought > 0) &  # Must have meaningful trading volume
            (silver_df.wallet_address.isNotNull()) &
            (silver_df.calculation_date.isNotNull())
        )
        
        qualified_count = qualified_traders.count()
        logger.info(f"📊 Filtered to {qualified_count} qualified traders")
        
        if qualified_count == 0:
            logger.warning("⚠️ No qualified traders found")
            return False
        
        # Step 2: Enhanced transformations
        from pyspark.sql.functions import (
            col, when, round as spark_round, current_timestamp, lit, row_number, 
            datediff, least, expr
        )
        from pyspark.sql.window import Window
        
        enhanced_df = qualified_traders.select(
            # Core trader identification
            col("wallet_address"),
            
            # Performance metrics
            col("total_pnl"),
            col("portfolio_roi"),
            col("win_rate"),
            col("trade_count"),
            col("total_bought"),
            col("total_sold"),
            col("realized_pnl"),
            col("unrealized_pnl"),
            col("current_position_cost_basis"),
            col("current_position_value"),
            
            # Calculate performance tier based on combined metrics
            when(
                (col("total_pnl") >= 10000) & (col("portfolio_roi") >= 50) & (col("win_rate") >= 30), "ELITE"
            ).when(
                (col("total_pnl") >= 1000) & (col("portfolio_roi") >= 20) & (col("win_rate") >= 20), "STRONG"
            ).when(
                (col("total_pnl") >= 100) & (col("portfolio_roi") >= 10) & (col("win_rate") >= 10), "PROMISING"
            ).when(
                (col("total_pnl") > 0) | (col("win_rate") > 0), "QUALIFIED"
            ).otherwise("UNQUALIFIED").alias("performance_tier"),
            
            # Calculate smart trader score (weighted combination)
            spark_round(
                (when(col("total_pnl") > 0, least(col("total_pnl") / 1000, lit(100))).otherwise(0) * 0.4) +  # 40% profitability
                (when(col("portfolio_roi") > 0, least(col("portfolio_roi"), lit(100))).otherwise(0) * 0.3) +  # 30% ROI
                (when(col("win_rate") > 0, col("win_rate")).otherwise(0) * 0.2) +  # 20% win rate
                (when(col("trade_count") > 10, lit(10)).otherwise(col("trade_count")) * 0.1),  # 10% activity bonus
                2
            ).alias("smart_trader_score"),
            
            # Trading behavior metrics
            col("avg_holding_time_hours"),
            col("trade_frequency_daily"),
            col("first_transaction"),
            col("last_transaction"),
            
            # Calculate trading experience in days
            (datediff(col("last_transaction"), col("first_transaction")) + 1).alias("trading_experience_days"),
            
            # Calculate average trade size
            spark_round((col("total_bought") + col("total_sold")) / col("trade_count"), 2).alias("avg_trade_size_usd"),
            
            # Risk metrics
            when(
                col("total_bought") > 0, 
                spark_round((col("total_sold") / col("total_bought")) * 100, 2)
            ).otherwise(0).alias("sell_ratio_percent"),
            
            # Metadata
            col("calculation_date"),
            col("batch_id").alias("source_batch_id"),
            col("processed_at").alias("source_processed_at"),
            col("data_source").alias("source_data_source"),
            current_timestamp().alias("gold_created_at")
        )
        
        # Step 3: Add tier ranking
        tier_window = Window.partitionBy("performance_tier").orderBy(col("smart_trader_score").desc(), col("total_pnl").desc())
        overall_window = Window.orderBy(col("smart_trader_score").desc(), col("total_pnl").desc())
        
        final_df = enhanced_df.withColumn("tier_rank", row_number().over(tier_window)) \
                             .withColumn("overall_rank", row_number().over(overall_window)) \
                             .orderBy(col("smart_trader_score").desc(), col("total_pnl").desc())
        
        final_count = final_df.count()
        logger.info(f"✅ Final gold smart traders dataset: {final_count} traders")
        
        # Show sample data
        logger.info("📋 Sample gold smart traders data:")
        final_df.select("wallet_address", "performance_tier", "smart_trader_score", "overall_rank", "total_pnl", "portfolio_roi", "win_rate") \
                .show(5, truncate=False)
        
        # Show performance tier distribution
        logger.info("📊 Performance tier distribution:")
        final_df.groupBy("performance_tier").count().orderBy("count", ascending=False).show()
        
        # Write to gold Delta Lake table
        logger.info(f"💾 Writing to gold Delta table: {gold_delta_path}")
        
        final_df.write.format("delta") \
                     .mode("overwrite") \
                     .option("overwriteSchema", "true") \
                     .partitionBy("performance_tier") \
                     .save(gold_delta_path)
        
        logger.info("✅ Gold smart traders Delta table created successfully!")
        
        # Verify the table
        gold_table = DeltaTable.forPath(spark, gold_delta_path)
        history = gold_table.history(1).collect()
        
        logger.info(f"📈 Gold table version: {history[0]['version']}")
        logger.info(f"📊 Gold table operation: {history[0]['operation']}")
        
        # Read back to verify
        verify_df = spark.read.format("delta").load(gold_delta_path)
        verify_count = verify_df.count()
        
        logger.info(f"✅ Verification: Read back {verify_count} records from gold table")
        
        # Show top smart traders
        logger.info("🏆 TOP 3 SMART TRADERS:")
        verify_df.select("wallet_address", "performance_tier", "smart_trader_score", "total_pnl", "portfolio_roi", "win_rate") \
                 .orderBy(col("smart_trader_score").desc()) \
                 .show(3, truncate=False)
        
        # Stop Spark session
        spark.stop()
        logger.info("✅ Spark session stopped")
        
        logger.info("🎉 PySpark gold smart traders transformation completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ PySpark gold smart traders transformation failed: {e}")
        return False

if __name__ == "__main__":
    success = run_pyspark_gold_smart_traders_transformation()
    if success:
        print("\n🚀 Gold smart traders transformation ready!")
    else:
        print("\n❌ Fix issues before proceeding")
    
    sys.exit(0 if success else 1)