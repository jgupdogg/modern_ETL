#!/bin/bash
# Clear Delta Tables Script - Docker Version
# Safely clears all Delta Lake tables using the Airflow worker container

set -e

echo "🚀 Delta Tables Cleanup Script"
echo "=============================="
echo ""

# Confirmation prompt
read -p "⚠️  This will DELETE ALL DATA in the Delta tables. Are you sure? (yes/no): " -r
echo ""

if [[ ! $REPLY =~ ^[Yy]([Ee][Ss])?$ ]]; then
    echo "❌ Operation cancelled"
    exit 0
fi

echo "🗑️  Starting cleanup of all Delta tables..."
echo ""

# Define Python script to run inside container
PYTHON_SCRIPT='
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def create_spark():
    return SparkSession.builder \
        .appName("ClearDeltaTables") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "512m") \
        .getOrCreate()

def clear_table(spark, path, name):
    try:
        df = spark.read.format("delta").load(path)
        if df.take(1):
            spark.sql(f"DELETE FROM delta.`{path}` WHERE 1=1")
            print(f"✅ Cleared {name}")
        else:
            print(f"📭 {name} already empty")
    except Exception as e:
        if "Path does not exist" in str(e) or "is not a Delta table" in str(e):
            print(f"⚠️  {name} table does not exist")
        else:
            print(f"❌ Failed to clear {name}: {str(e)}")

spark = create_spark()
try:
    tables = [
        ("s3a://smart-trader/bronze/token_metrics", "Bronze Tokens"),
        ("s3a://smart-trader/bronze/whale_holders", "Bronze Whales"),
        ("s3a://smart-trader/bronze/transaction_history", "Bronze Transactions"),
        ("s3a://smart-trader/silver/tracked_tokens_delta", "Silver Tokens"),
        ("s3a://smart-trader/silver/tracked_whales_delta", "Silver Whales"),
        ("s3a://smart-trader/silver/wallet_pnl", "Silver Wallet PnL"),
        ("s3a://smart-trader/gold/smart_traders_delta", "Gold Smart Traders")
    ]
    
    for path, name in tables:
        clear_table(spark, path, name)
    
    print("\n✅ All Delta tables cleared successfully!")
    print("🔄 Pipeline ready for fresh start")
    
finally:
    spark.stop()
'

# Run the Python script inside the Airflow worker container
echo "🐳 Executing cleanup inside Docker container..."
docker compose exec airflow-worker python3 -c "$PYTHON_SCRIPT"

echo ""
echo "=============================="
echo "✅ Cleanup completed!"
echo ""
echo "🚀 To start pipeline with fresh data:"
echo "   docker compose run airflow-cli airflow dags trigger optimized_delta_smart_trader_identification"
echo ""