#!/usr/bin/env python3
"""
Test True Delta Lake inside Docker Container
This script creates a minimal Delta Lake table to validate the complete integration
"""

import logging
import sys
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_true_delta_lake_minimal():
    """
    Minimal test to create a Delta Lake table in MinIO
    This verifies the complete Docker + PySpark + MinIO + Delta Lake stack
    """
    logger.info("🚀 Testing TRUE Delta Lake with MinIO in Docker...")
    
    try:
        # Import required packages - will fail if not in proper environment
        from pyspark.sql import SparkSession
        from delta.tables import DeltaTable
        import boto3
        
        logger.info("✅ All packages imported successfully")
        
        # Create Spark session with Delta Lake + MinIO configuration
        spark = (
            SparkSession.builder.appName("TrueDeltaTest")
            
            # JAR packages for Delta Lake + MinIO (Spark 3.5.0 compatible)
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
        
        # Ensure bucket exists
        s3_client = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin123'
        )
        
        try:
            s3_client.create_bucket(Bucket='smart-trader')
            logger.info("✅ Created smart-trader bucket")
        except Exception as e:
            if 'BucketAlreadyOwnedByYou' in str(e):
                logger.info("✅ smart-trader bucket already exists")
            else:
                raise
        
        # Create test DataFrame
        test_data = [
            ("test_wallet_1", "test_token_1", 100.0, "2025-06-24"),
            ("test_wallet_2", "test_token_1", 200.0, "2025-06-24"),
            ("test_wallet_3", "test_token_2", 300.0, "2025-06-24"),
        ]
        
        columns = ["wallet_address", "token_address", "pnl", "test_date"]
        test_df = spark.createDataFrame(test_data, columns)
        
        logger.info(f"✅ Created test DataFrame with {test_df.count()} records")
        
        # Write to Delta Lake in MinIO
        delta_table_path = "s3a://smart-trader/test/true_delta_test"
        
        test_df.write.format("delta").mode("overwrite").save(delta_table_path)
        
        logger.info(f"✅ Wrote Delta table to: {delta_table_path}")
        
        # Verify it's a true Delta table
        is_delta = DeltaTable.isDeltaTable(spark, delta_table_path)
        if not is_delta:
            raise RuntimeError("Failed to create Delta table")
        
        logger.info("✅ Confirmed table is a TRUE Delta table")
        
        # Get Delta table info
        delta_table = DeltaTable.forPath(spark, delta_table_path)
        history = delta_table.history().collect()
        
        logger.info(f"✅ Delta table history: {len(history)} entries")
        logger.info(f"📊 Latest operation: {history[0]['operation']}")
        logger.info(f"📈 Version: {history[0]['version']}")
        
        # Read back data
        read_df = spark.read.format("delta").load(delta_table_path)
        read_count = read_df.count()
        
        logger.info(f"✅ Read back {read_count} records from Delta table")
        
        # Test time travel (read version 0)
        version_0_df = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)
        v0_count = version_0_df.count()
        
        logger.info(f"✅ Time travel test: Version 0 has {v0_count} records")
        
        # Verify _delta_log exists in MinIO
        try:
            delta_log_objects = s3_client.list_objects_v2(
                Bucket='smart-trader',
                Prefix='test/true_delta_test/_delta_log/'
            )
            
            if 'Contents' in delta_log_objects:
                log_files = len(delta_log_objects['Contents'])
                logger.info(f"✅ _delta_log verified: {log_files} transaction log files")
            else:
                raise RuntimeError("No _delta_log files found")
                
        except Exception as e:
            logger.error(f"❌ _delta_log verification failed: {e}")
            raise
        
        # Test APPEND operation
        append_data = [("test_wallet_4", "test_token_3", 400.0, "2025-06-24")]
        append_df = spark.createDataFrame(append_data, columns)
        
        append_df.write.format("delta").mode("append").save(delta_table_path)
        
        # Check new version
        new_history = delta_table.history().collect()
        new_version = new_history[0]['version']
        
        logger.info(f"✅ APPEND test: New version {new_version}")
        
        # Final verification
        final_df = spark.read.format("delta").load(delta_table_path)
        final_count = final_df.count()
        
        logger.info(f"✅ Final verification: {final_count} total records")
        
        # Stop Spark session
        spark.stop()
        logger.info("✅ Spark session stopped")
        
        logger.info("\n" + "="*60)
        logger.info("🎉 TRUE DELTA LAKE TEST SUCCESSFUL!")
        logger.info("="*60)
        logger.info("✅ PySpark + Delta Lake + MinIO integration verified")
        logger.info("✅ _delta_log transaction logs confirmed")
        logger.info("✅ ACID operations (CREATE, APPEND) tested")
        logger.info("✅ Time travel functionality validated")
        logger.info("✅ S3A filesystem working correctly")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ TRUE Delta Lake test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_true_delta_lake_minimal()
    if success:
        print("\n🚀 Ready to proceed with TRUE Delta Lake implementation!")
    else:
        print("\n❌ Fix environment issues before proceeding")
    
    sys.exit(0 if success else 1)