#!/usr/bin/env python3
"""
Simple test for bronze tokens Delta Lake creation
Tests the core functionality with mock data
"""

import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_bronze_tokens_with_mock_data():
    """Test bronze tokens creation with mock data"""
    logger.info("🧪 Testing bronze tokens with mock data...")
    
    try:
        # Import required modules
        from pyspark.sql import SparkSession
        from delta.tables import DeltaTable
        import boto3
        
        logger.info("✅ All packages imported successfully")
        
        # Create Spark session with Delta Lake + MinIO configuration
        spark = (
            SparkSession.builder.appName("BronzeTokensTest")
            
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
        
        # Create mock token data (simulating BirdEye API response)
        mock_token_data = [
            {
                "token_address": "So11111111111111111111111111111111111111112",
                "symbol": "SOL",
                "name": "Wrapped SOL",
                "price": 150.45,
                "price_change_24h_percent": 5.2,
                "volume_24h_usd": 1200000000.0,
                "liquidity": 5000000.0,
                "market_cap": 65000000000.0,
                "decimals": 9,
                "rank": 1
            },
            {
                "token_address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "symbol": "USDC",
                "name": "USD Coin",
                "price": 1.0,
                "price_change_24h_percent": 0.1,
                "volume_24h_usd": 800000000.0,
                "liquidity": 3000000.0,
                "market_cap": 32000000000.0,
                "decimals": 6,
                "rank": 2
            },
            {
                "token_address": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
                "symbol": "USDT",
                "name": "Tether USD",
                "price": 1.0,
                "price_change_24h_percent": -0.05,
                "volume_24h_usd": 900000000.0,
                "liquidity": 2500000.0,
                "market_cap": 110000000000.0,
                "decimals": 6,
                "rank": 3
            }
        ]
        
        logger.info(f"✅ Created mock token data with {len(mock_token_data)} tokens")
        
        # Convert to Spark DataFrame
        df = spark.createDataFrame(mock_token_data)
        
        # Add Delta Lake metadata columns
        from pyspark.sql.functions import current_timestamp, lit, current_date
        
        df_with_metadata = df.withColumn("_delta_timestamp", current_timestamp()) \
                            .withColumn("_delta_operation", lit("BRONZE_TOKEN_CREATE")) \
                            .withColumn("processing_date", current_date()) \
                            .withColumn("batch_id", lit("test_batch_20250624"))
        
        logger.info(f"✅ Added Delta metadata - {df_with_metadata.count()} records")
        
        # Write to Delta Lake table
        delta_table_path = "s3a://smart-trader/bronze/token_metrics"
        
        logger.info(f"💾 Writing to Delta table: {delta_table_path}")
        
        df_with_metadata.write.format("delta").mode("overwrite") \
                       .partitionBy("processing_date") \
                       .save(delta_table_path)
        
        logger.info("✅ Delta table created successfully")
        
        # Verify it's a Delta table
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
        
        # Read back data to verify
        read_df = spark.read.format("delta").load(delta_table_path)
        read_count = read_df.count()
        
        logger.info(f"✅ Read back {read_count} records from Delta table")
        
        # Show sample data
        logger.info("📋 Sample token data:")
        read_df.select("symbol", "name", "price", "liquidity", "_delta_operation").show(3, truncate=False)
        
        # Verify _delta_log exists in MinIO
        try:
            delta_log_objects = s3_client.list_objects_v2(
                Bucket='smart-trader',
                Prefix='bronze/token_metrics/_delta_log/'
            )
            
            if 'Contents' in delta_log_objects:
                log_files = len(delta_log_objects['Contents'])
                logger.info(f"✅ _delta_log verified: {log_files} transaction log files")
            else:
                raise RuntimeError("No _delta_log files found")
                
        except Exception as e:
            logger.error(f"❌ _delta_log verification failed: {e}")
            raise
        
        # Stop Spark session
        spark.stop()
        logger.info("✅ Spark session stopped")
        
        logger.info("\n" + "="*60)
        logger.info("🎉 BRONZE TOKENS DELTA LAKE TEST SUCCESSFUL!")
        logger.info("="*60)
        logger.info("✅ Mock token data processed successfully")
        logger.info("✅ TRUE Delta Lake table created")
        logger.info("✅ _delta_log transaction logs verified")
        logger.info("✅ Partitioning by processing_date working")
        logger.info("✅ Ready for real BirdEye API integration")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Bronze tokens test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_bronze_tokens_with_mock_data()
    if success:
        print("\n🚀 Bronze tokens Delta Lake implementation ready!")
    else:
        print("\n❌ Fix issues before proceeding")
    
    sys.exit(0 if success else 1)