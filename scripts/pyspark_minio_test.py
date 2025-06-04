#!/usr/bin/env python3
"""
Test script: Local directory → PySpark → MinIO
Reads parquet files from local directory and writes to MinIO using PySpark.
"""

import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration
INPUT_DIR = os.getenv("INPUT_DIR", "/home/jgupdogg/dev/claude_pipeline/data/test_output")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "webhook-data")
OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/processed-webhooks"

def create_spark_session():
    """Create Spark session with S3A/MinIO support."""
    return SparkSession.builder \
        .appName("LocalToMinIO") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def create_minio_bucket(spark):
    """Create MinIO bucket if it doesn't exist."""
    try:
        # Use spark to test connection and create bucket via boto3
        import boto3
        from botocore.client import Config
        
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=MINIO_BUCKET)
            print(f"Bucket '{MINIO_BUCKET}' already exists")
        except:
            # Create bucket
            s3_client.create_bucket(Bucket=MINIO_BUCKET)
            print(f"Created bucket '{MINIO_BUCKET}'")
            
    except Exception as e:
        print(f"Warning: Could not verify/create bucket: {e}")
        print("Proceeding anyway - bucket may already exist")

def main():
    """Main processing function."""
    print("Starting PySpark Local to MinIO test...")
    print(f"Input directory: {INPUT_DIR}")
    print(f"MinIO endpoint: {MINIO_ENDPOINT}")
    print(f"MinIO bucket: {MINIO_BUCKET}")
    print(f"Output path: {OUTPUT_PATH}")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Create bucket if needed
        create_minio_bucket(spark)
        
        # Read parquet files from local directory
        print(f"\nReading parquet files from {INPUT_DIR}...")
        df = spark.read.parquet(INPUT_DIR)
        
        print(f"Found {df.count()} records")
        print("Schema:")
        df.printSchema()
        
        # Add batch processing metadata
        processed_df = df.withColumn(
            "batch_processed_at", 
            current_timestamp()
        ).withColumn(
            "batch_id",
            lit(datetime.now().strftime("%Y%m%d_%H%M%S"))
        )
        
        # Show sample data
        print("\nSample data:")
        processed_df.select("message_id", "timestamp", "payload", "batch_processed_at").show(3, truncate=False)
        
        # Write to MinIO partitioned by processing_date
        print(f"\nWriting to MinIO: {OUTPUT_PATH}")
        processed_df.write \
            .mode("append") \
            .partitionBy("processing_date") \
            .parquet(OUTPUT_PATH)
        
        print("✅ Successfully wrote data to MinIO!")
        
        # Verify data was written by reading it back
        print("\nVerifying data in MinIO...")
        minio_df = spark.read.parquet(OUTPUT_PATH)
        record_count = minio_df.count()
        print(f"✅ Verified {record_count} records in MinIO")
        
        # Show partitions
        print("\nPartitions in MinIO:")
        minio_df.select("processing_date").distinct().show()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    main()