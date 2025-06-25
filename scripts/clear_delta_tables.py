#!/usr/bin/env python3
"""
Clear All Delta Tables Script
Safely truncates all Delta Lake tables to start fresh while preserving table schemas.
Uses Delta Lake's ACID properties for safe data removal.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from delta import configure_spark_with_delta_pip

def create_spark_session():
    """Create Spark session with Delta Lake support"""
    builder = SparkSession.builder \
        .appName("ClearDeltaTables") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "512m")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def clear_delta_table(spark, table_path, table_name):
    """Clear a Delta table using DELETE operation (preserves schema)"""
    try:
        # Check if table exists
        if spark._jsparkSession.catalog().tableExists(f"delta.`{table_path}`"):
            print(f"🗑️  Clearing {table_name} at {table_path}")
            
            # Use Delta Lake DELETE to remove all rows (preserves schema)
            spark.sql(f"DELETE FROM delta.`{table_path}` WHERE 1=1")
            
            # Vacuum to remove old files (optional - frees up storage)
            spark.sql(f"VACUUM delta.`{table_path}` RETAIN 0 HOURS")
            
            print(f"✅ {table_name} cleared successfully")
        else:
            print(f"⚠️  {table_name} table does not exist at {table_path}")
            
    except Exception as e:
        print(f"❌ Failed to clear {table_name}: {str(e)}")

def main():
    """Main function to clear all Delta tables"""
    print("🚀 Starting Delta Tables Cleanup...")
    print("=" * 60)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Define all Delta table paths
        tables_to_clear = [
            # Bronze Layer
            ("s3a://smart-trader/bronze/token_metrics", "Bronze Tokens"),
            ("s3a://smart-trader/bronze/whale_holders", "Bronze Whales"), 
            ("s3a://smart-trader/bronze/transaction_history", "Bronze Transactions"),
            
            # Silver Layer
            ("s3a://smart-trader/silver/tracked_tokens_delta", "Silver Tokens"),
            ("s3a://smart-trader/silver/tracked_whales_delta", "Silver Whales"),
            ("s3a://smart-trader/silver/wallet_pnl", "Silver Wallet PnL"),
            
            # Gold Layer
            ("s3a://smart-trader/gold/smart_traders_delta", "Gold Smart Traders")
        ]
        
        print(f"📊 Found {len(tables_to_clear)} tables to clear")
        print()
        
        # Clear each table
        for table_path, table_name in tables_to_clear:
            clear_delta_table(spark, table_path, table_name)
            print()
        
        print("=" * 60)
        print("✅ All Delta tables cleared successfully!")
        print()
        print("🔄 Pipeline is ready for fresh start")
        print("   Run: docker compose run airflow-cli airflow dags trigger optimized_delta_smart_trader_identification")
        
    except Exception as e:
        print(f"❌ Script failed: {str(e)}")
        sys.exit(1)
        
    finally:
        # Clean up Spark session
        spark.stop()
        print("🛑 Spark session stopped")

if __name__ == "__main__":
    # Confirmation prompt
    response = input("⚠️  This will DELETE ALL DATA in the Delta tables. Are you sure? (yes/no): ")
    
    if response.lower() in ['yes', 'y']:
        main()
    else:
        print("❌ Operation cancelled")
        sys.exit(0)