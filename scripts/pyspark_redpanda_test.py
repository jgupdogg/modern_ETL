#!/usr/bin/env python3
"""
Test script: Redpanda → PySpark → local directory
Reads streaming data from Redpanda and writes to local files using PySpark.
"""

import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration
REDPANDA_BROKERS = os.getenv("REDPANDA_BROKERS", "localhost:19092")
WEBHOOK_TOPIC = os.getenv("WEBHOOK_TOPIC", "webhooks")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/home/jgupdogg/dev/claude_pipeline/data/test_output")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/home/jgupdogg/dev/claude_pipeline/data/checkpoints")

def create_spark_session():
    """Create Spark session with Kafka support."""
    return SparkSession.builder \
        .appName("RedpandaToLocal") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def define_webhook_schema():
    """Define the schema for webhook messages."""
    return StructType([
        StructField("message_id", StringType(), True),
        StructField("timestamp", StringType(), True), 
        StructField("source_ip", StringType(), True),
        StructField("file_path", StringType(), True),
        StructField("payload", StringType(), True)  # JSON string
    ])

def main():
    """Main processing function."""
    print("Starting PySpark Redpanda consumer test...")
    print(f"Redpanda brokers: {REDPANDA_BROKERS}")
    print(f"Topic: {WEBHOOK_TOPIC}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Checkpoint directory: {CHECKPOINT_DIR}")
    
    # Create output and checkpoint directories
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Read from Kafka/Redpanda
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", REDPANDA_BROKERS) \
            .option("subscribe", WEBHOOK_TOPIC) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse the JSON value
        webhook_schema = define_webhook_schema()
        
        parsed_df = kafka_df.select(
            col("key").cast("string").alias("kafka_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), webhook_schema).alias("webhook_data")
        ).select(
            "kafka_key",
            "topic", 
            "partition",
            "offset",
            "kafka_timestamp",
            "webhook_data.message_id",
            "webhook_data.timestamp",
            "webhook_data.source_ip", 
            "webhook_data.file_path",
            "webhook_data.payload"
        )
        
        # Add processing timestamp
        processed_df = parsed_df.withColumn(
            "processed_at", 
            current_timestamp()
        ).withColumn(
            "processing_date",
            date_format(current_timestamp(), "yyyy-MM-dd")
        ).withColumn(
            "processing_hour", 
            date_format(current_timestamp(), "HH")
        )
        
        # Write to local directory partitioned by date and hour
        query = processed_df.writeStream \
            .format("parquet") \
            .option("path", OUTPUT_DIR) \
            .option("checkpointLocation", CHECKPOINT_DIR) \
            .partitionBy("processing_date", "processing_hour") \
            .outputMode("append") \
            .trigger(processingTime="10 seconds") \
            .start()
        
        print("\nStreaming started. Processing messages...")
        print("Press Ctrl+C to stop\n")
        
        # Wait for termination
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    main()