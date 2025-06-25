#!/usr/bin/env python3
"""
Simple Delta Tables Clear Script
Run this inside the Airflow worker container to clear all tables
"""

from pyspark.sql import SparkSession

def main():
    print("🚀 Clearing all Delta tables...")
    
    # Create minimal Spark session  
    spark = SparkSession.builder \
        .appName("ClearTables") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.driver.memory", "512m") \
        .getOrCreate()
    
    try:
        # Define all table paths
        tables = [
            "s3a://smart-trader/bronze/token_metrics",
            "s3a://smart-trader/bronze/whale_holders", 
            "s3a://smart-trader/bronze/transaction_history",
            "s3a://smart-trader/silver/tracked_tokens_delta",
            "s3a://smart-trader/silver/tracked_whales_delta",
            "s3a://smart-trader/silver/wallet_pnl",
            "s3a://smart-trader/gold/smart_traders_delta"
        ]
        
        for path in tables:
            try:
                # Try to delete all rows
                spark.sql(f"DELETE FROM delta.`{path}` WHERE 1=1")
                print(f"✅ Cleared {path.split('/')[-1]}")
            except Exception as e:
                if "Path does not exist" in str(e) or "is not a Delta table" in str(e):
                    print(f"⚠️  {path.split('/')[-1]} does not exist")
                else:
                    print(f"❌ Failed to clear {path.split('/')[-1]}: {str(e)}")
        
        print("\n🎉 All tables cleared! Pipeline ready for fresh start.")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()