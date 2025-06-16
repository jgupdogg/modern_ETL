#!/usr/bin/env python3
"""
Analyze webhook data structure from MinIO using PySpark
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, explode, when
from pyspark.sql.types import *
import json

def main():
    # Create Spark session with MinIO configuration
    spark = SparkSession.builder \
        .appName("WebhookDataAnalysis") \
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.523") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("=" * 80)
        print("WEBHOOK DATA ANALYSIS")
        print("=" * 80)
        
        # Read webhook data from MinIO
        webhook_path = "s3a://webhook-data/processed-webhooks/"
        
        print(f"\n1. Reading data from: {webhook_path}")
        df = spark.read.parquet(webhook_path)
        
        # Basic statistics
        total_records = df.count()
        print(f"\nTotal records found: {total_records}")
        
        # Show schema
        print("\n2. Data Schema:")
        print("-" * 40)
        df.printSchema()
        
        # Sample records
        print("\n3. Sample Records (5 records):")
        print("-" * 40)
        df.show(5, truncate=False, vertical=True)
        
        # Analyze payload structure if it exists
        if 'payload' in df.columns:
            print("\n4. Analyzing Payload Structure:")
            print("-" * 40)
            
            # Get a sample payload
            sample_payload = df.filter(col("payload").isNotNull()).select("payload").first()
            if sample_payload:
                try:
                    # If payload is a string, try to parse it as JSON
                    if isinstance(sample_payload[0], str):
                        payload_dict = json.loads(sample_payload[0])
                        print("Payload fields found:")
                        for key in sorted(payload_dict.keys()):
                            print(f"  - {key}: {type(payload_dict[key]).__name__}")
                            
                        # Look for transaction-related fields
                        print("\nTransaction-related fields:")
                        transaction_keywords = ['transaction', 'tx', 'signature', 'block', 'slot', 
                                              'account', 'program', 'instruction', 'token', 'amount',
                                              'from', 'to', 'sender', 'receiver', 'mint']
                        
                        for key in payload_dict.keys():
                            if any(keyword in key.lower() for keyword in transaction_keywords):
                                print(f"  - {key}: {payload_dict[key]}")
                except Exception as e:
                    print(f"Could not parse payload as JSON: {e}")
        
        # Check for nested structures
        print("\n5. Column Analysis:")
        print("-" * 40)
        for field in df.schema.fields:
            if field.dataType.typeName() in ['struct', 'array', 'map']:
                print(f"\nNested field '{field.name}' ({field.dataType.typeName()}):")
                # Show distinct values if reasonable
                if field.dataType.typeName() != 'array':
                    distinct_count = df.select(field.name).distinct().count()
                    if distinct_count < 20:
                        df.select(field.name).distinct().show(truncate=False)
        
        # Look for specific Solana transaction fields
        print("\n6. Searching for Solana Transaction Fields:")
        print("-" * 40)
        
        # Common Solana transaction field patterns
        solana_fields = []
        for col_name in df.columns:
            col_lower = col_name.lower()
            if any(term in col_lower for term in ['signature', 'slot', 'block', 'program', 'account', 'instruction']):
                solana_fields.append(col_name)
        
        if solana_fields:
            print(f"Found potential Solana fields: {solana_fields}")
            # Show sample of these fields
            df.select(*solana_fields).show(5, truncate=False)
        else:
            print("No obvious Solana transaction fields found in column names")
        
        # Show distinct webhook types if there's a type field
        if 'type' in df.columns or 'event_type' in df.columns or 'webhook_type' in df.columns:
            type_col = next((col for col in ['type', 'event_type', 'webhook_type'] if col in df.columns), None)
            if type_col:
                print(f"\n7. Webhook Types (from '{type_col}' column):")
                print("-" * 40)
                df.groupBy(type_col).count().orderBy("count", ascending=False).show()
        
        # Summary statistics
        print("\n8. Data Summary:")
        print("-" * 40)
        print(f"Total columns: {len(df.columns)}")
        print(f"Total records: {total_records}")
        
        # Check for null values
        print("\nNull value counts:")
        null_counts = []
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                null_counts.append((col_name, null_count, f"{100*null_count/total_records:.1f}%"))
        
        if null_counts:
            for col_name, count, pct in sorted(null_counts, key=lambda x: x[1], reverse=True):
                print(f"  - {col_name}: {count} ({pct})")
        else:
            print("  No null values found")
            
    except Exception as e:
        print(f"\nError analyzing webhook data: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()