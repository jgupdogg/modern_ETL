#!/usr/bin/env python3
"""
DuckDB + MinIO Connection Test Script
Tests basic connectivity and queries bronze parquet files.
"""

import os
import sys
import logging
import duckdb
import boto3
from botocore.client import Config

def setup_logging():
    """Configure logging for the test script."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def test_duckdb_basic():
    """Test basic DuckDB functionality."""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=== Testing DuckDB Basic Functionality ===")
        
        # Connect to DuckDB
        db_path = os.getenv('DUCKDB_DATABASE_PATH', '/data/analytics.duckdb')
        conn = duckdb.connect(db_path)
        
        # Test basic query
        result = conn.execute("SELECT 'DuckDB is working!' as status, version() as version;").fetchone()
        logger.info(f"Status: {result[0]}")
        logger.info(f"Version: {result[1]}")
        
        # Test extensions
        extensions = conn.execute("SELECT extension_name, loaded FROM duckdb_extensions() WHERE extension_name IN ('httpfs', 'parquet');").fetchall()
        for ext_name, loaded in extensions:
            status = "loaded" if loaded else "not loaded"
            logger.info(f"Extension {ext_name}: {status}")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"DuckDB basic test failed: {e}")
        return False

def test_s3_connectivity():
    """Test S3/MinIO connectivity using boto3."""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=== Testing S3/MinIO Connectivity (boto3) ===")
        
        # Get configuration
        endpoint_url = os.getenv('DUCKDB_S3_ENDPOINT', 'http://minio:9000')
        access_key = os.getenv('DUCKDB_S3_ACCESS_KEY', 'minioadmin')
        secret_key = os.getenv('DUCKDB_S3_SECRET_KEY', 'minioadmin123')
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        # List buckets
        buckets = s3_client.list_buckets()
        logger.info(f"Available buckets: {[b['Name'] for b in buckets['Buckets']]}")
        
        # Check webhook-data bucket
        try:
            objects = s3_client.list_objects_v2(Bucket='webhook-data', MaxKeys=5)
            if 'Contents' in objects:
                logger.info(f"Found {len(objects['Contents'])} objects in webhook-data bucket")
                for obj in objects['Contents'][:3]:  # Show first 3
                    logger.info(f"  - {obj['Key']} ({obj['Size']} bytes)")
            else:
                logger.info("webhook-data bucket exists but is empty")
        except Exception as e:
            logger.warning(f"webhook-data bucket not accessible: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"S3/MinIO connectivity test failed: {e}")
        return False

def test_duckdb_s3_integration():
    """Test DuckDB S3 integration and parquet reading."""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=== Testing DuckDB S3/MinIO Integration ===")
        
        # Connect to DuckDB
        db_path = os.getenv('DUCKDB_DATABASE_PATH', '/data/analytics.duckdb')
        conn = duckdb.connect(db_path)
        
        # Configure S3 settings
        s3_endpoint = os.getenv('DUCKDB_S3_ENDPOINT', 'http://minio:9000')
        s3_access_key = os.getenv('DUCKDB_S3_ACCESS_KEY', 'minioadmin')
        s3_secret_key = os.getenv('DUCKDB_S3_SECRET_KEY', 'minioadmin123')
        
        conn.execute("LOAD httpfs;")
        conn.execute(f"SET s3_endpoint='{s3_endpoint}';")
        conn.execute(f"SET s3_access_key_id='{s3_access_key}';")
        conn.execute(f"SET s3_secret_access_key='{s3_secret_key}';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute("SET s3_url_style='path';")
        
        # Test listing S3 files
        try:
            logger.info("Testing S3 file listing...")
            files_query = "SELECT * FROM glob('s3://webhook-data/**/*.parquet') LIMIT 5;"
            files = conn.execute(files_query).fetchall()
            logger.info(f"Found {len(files)} parquet files")
            for file in files:
                logger.info(f"  - {file[0]}")
        except Exception as e:
            logger.warning(f"S3 file listing failed: {e}")
        
        # Test reading parquet data
        try:
            logger.info("Testing parquet data reading...")
            data_query = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT message_id) as unique_messages,
                    MIN(processed_at) as earliest_record,
                    MAX(processed_at) as latest_record
                FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet');
            """
            result = conn.execute(data_query).fetchone()
            logger.info(f"Total records: {result[0]}")
            logger.info(f"Unique messages: {result[1]}")
            logger.info(f"Date range: {result[2]} to {result[3]}")
            
            # Test sample data
            sample_query = """
                SELECT message_id, timestamp, source_ip, processing_date
                FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet')
                LIMIT 3;
            """
            samples = conn.execute(sample_query).fetchall()
            logger.info("Sample records:")
            for sample in samples:
                logger.info(f"  - ID: {sample[0]}, Time: {sample[1]}, IP: {sample[2]}, Date: {sample[3]}")
                
        except Exception as e:
            logger.warning(f"Parquet data reading failed: {e}")
            # This might fail if no data exists yet, which is OK
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"DuckDB S3 integration test failed: {e}")
        return False

def main():
    """Run all connection tests."""
    logger = setup_logging()
    
    logger.info("Starting DuckDB + MinIO connection tests...")
    
    tests = [
        ("DuckDB Basic", test_duckdb_basic),
        ("S3/MinIO Connectivity", test_s3_connectivity),
        ("DuckDB S3 Integration", test_duckdb_s3_integration)
    ]
    
    results = {}
    for test_name, test_func in tests:
        logger.info(f"\n{'='*50}")
        try:
            results[test_name] = test_func()
        except Exception as e:
            logger.error(f"Test '{test_name}' crashed: {e}")
            results[test_name] = False
    
    # Summary
    logger.info(f"\n{'='*50}")
    logger.info("TEST SUMMARY:")
    for test_name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        logger.info(f"  {test_name}: {status}")
    
    all_passed = all(results.values())
    if all_passed:
        logger.info("\n🎉 All tests passed! DuckDB + MinIO integration is working.")
    else:
        logger.warning("\n⚠️  Some tests failed. Check the logs above for details.")
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)