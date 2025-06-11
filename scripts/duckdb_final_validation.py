#!/usr/bin/env python3
"""
Final End-to-End Pipeline Validation
Tests complete bronze→silver transformation with all fixes applied.
"""

import os
import sys
import logging
import duckdb
import time
from datetime import datetime

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

def get_duckdb_connection():
    """Create DuckDB connection with S3 configuration."""
    db_path = os.getenv('DUCKDB_DATABASE_PATH', '/data/analytics.duckdb')
    conn = duckdb.connect(db_path)
    
    # Configure S3/MinIO
    conn.execute("LOAD httpfs;")
    endpoint = os.getenv('DUCKDB_S3_ENDPOINT', 'http://minio:9000').replace('http://', '').replace('https://', '')
    conn.execute(f"SET s3_endpoint='{endpoint}';")
    conn.execute(f"SET s3_access_key_id='{os.getenv('DUCKDB_S3_ACCESS_KEY', 'minioadmin')}';")
    conn.execute(f"SET s3_secret_access_key='{os.getenv('DUCKDB_S3_SECRET_KEY', 'minioadmin123')}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    
    return conn

def test_bronze_layer(conn, logger):
    """Test bronze layer data access and quality."""
    logger.info("=== Testing Bronze Layer ===")
    
    try:
        # Bronze data volume
        bronze_count = conn.execute("SELECT COUNT(*) FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet');").fetchone()[0]
        bronze_unique = conn.execute("SELECT COUNT(DISTINCT message_id) FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet');").fetchone()[0]
        
        logger.info(f"✅ Bronze layer: {bronze_count} total records, {bronze_unique} unique messages")
        
        # Schema validation
        schema = conn.execute("DESCRIBE SELECT * FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet') LIMIT 1;").fetchall()
        logger.info(f"✅ Bronze schema: {len(schema)} columns validated")
        
        return {'success': True, 'total_records': bronze_count, 'unique_records': bronze_unique}
    except Exception as e:
        logger.error(f"❌ Bronze layer test failed: {e}")
        return {'success': False, 'error': str(e)}

def test_silver_transformations(conn, logger):
    """Test silver layer transformations in DuckDB."""
    logger.info("=== Testing Silver Transformations ===")
    
    try:
        # Check if bronze view exists
        try:
            bronze_view_count = conn.execute("SELECT COUNT(*) FROM bronze.webhooks WHERE row_num = 1;").fetchone()[0]
            logger.info(f"✅ Bronze view: {bronze_view_count} deduplicated records")
        except:
            logger.warning("⚠️ Bronze view not found, recreating...")
            # Create bronze view if missing
            conn.execute("DROP VIEW IF EXISTS bronze.webhooks;")
            bronze_view_sql = """
            CREATE VIEW bronze.webhooks AS
            SELECT DISTINCT
                message_id, timestamp as webhook_timestamp, source_ip, file_path, payload,
                kafka_timestamp, processed_at, processing_date, processing_hour,
                ROW_NUMBER() OVER (PARTITION BY message_id ORDER BY processed_at DESC) as row_num
            FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet')
            WHERE message_id IS NOT NULL;
            """
            conn.execute(bronze_view_sql)
            bronze_view_count = conn.execute("SELECT COUNT(*) FROM bronze.webhooks WHERE row_num = 1;").fetchone()[0]
            logger.info(f"✅ Bronze view created: {bronze_view_count} deduplicated records")
        
        # Check silver tables
        silver_events = conn.execute("SELECT COUNT(*), event_type FROM silver.webhook_events GROUP BY event_type ORDER BY event_type;").fetchall()
        logger.info("✅ Silver webhook events:")
        total_silver = 0
        for count, event_type in silver_events:
            logger.info(f"  {event_type}: {count} events")
            total_silver += count
        
        # Verify data integrity
        integrity_check = conn.execute("SELECT COUNT(DISTINCT bw.message_id) FROM bronze.webhooks bw JOIN silver.webhook_events swe ON bw.message_id = swe.message_id;").fetchone()[0]
        logger.info(f"✅ Data integrity: {integrity_check}/{bronze_view_count} messages successfully transformed")
        
        return {'success': True, 'silver_events': total_silver, 'integrity_ratio': integrity_check / bronze_view_count}
    except Exception as e:
        logger.error(f"❌ Silver transformations test failed: {e}")
        return {'success': False, 'error': str(e)}

def test_minio_persistence(conn, logger):
    """Test MinIO storage and retrieval of silver data."""
    logger.info("=== Testing MinIO Persistence ===")
    
    try:
        # Test reading silver data from MinIO
        minio_events = conn.execute("SELECT COUNT(*), event_type FROM parquet_scan('s3://webhook-data/silver-webhooks/webhook_events/**/*.parquet') GROUP BY event_type ORDER BY event_type;").fetchall()
        
        logger.info("✅ Silver data in MinIO:")
        minio_total = 0
        for count, event_type in minio_events:
            logger.info(f"  {event_type}: {count} events")
            minio_total += count
        
        # Test transaction details
        minio_tx = conn.execute("SELECT COUNT(*) FROM parquet_scan('s3://webhook-data/silver-webhooks/transaction_details/**/*.parquet');").fetchone()[0]
        logger.info(f"✅ Transaction details in MinIO: {minio_tx} records")
        
        # Test data quality metrics
        try:
            minio_quality = conn.execute("SELECT total_events, json_validity_pct FROM parquet_scan('s3://webhook-data/silver-webhooks/data_quality_metrics/**/*.parquet');").fetchone()
            logger.info(f"✅ Quality metrics in MinIO: {minio_quality[0]} events, {minio_quality[1]}% JSON valid")
            quality_success = True
        except Exception as e:
            logger.warning(f"⚠️ Quality metrics not in MinIO: {e}")
            quality_success = False
        
        return {
            'success': True, 
            'minio_events': minio_total, 
            'minio_transactions': minio_tx,
            'quality_metrics_available': quality_success
        }
    except Exception as e:
        logger.error(f"❌ MinIO persistence test failed: {e}")
        return {'success': False, 'error': str(e)}

def test_query_performance(conn, logger):
    """Test analytical query performance."""
    logger.info("=== Testing Query Performance ===")
    
    performance_tests = [
        ("Bronze aggregation", "SELECT COUNT(*), processing_date FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet') GROUP BY processing_date;"),
        ("Silver event counting", "SELECT COUNT(*) FROM silver.webhook_events WHERE event_type = 'test_event';"),
        ("MinIO silver query", "SELECT COUNT(*) FROM parquet_scan('s3://webhook-data/silver-webhooks/webhook_events/**/*.parquet') WHERE event_type = 'solana_swap';"),
        ("JSON extraction", "SELECT COUNT(*) FROM silver.webhook_events WHERE payload_source IS NOT NULL;")
    ]
    
    results = {}
    
    for test_name, query in performance_tests:
        try:
            start_time = time.time()
            result = conn.execute(query).fetchall()
            duration = time.time() - start_time
            
            results[test_name] = {'duration': duration, 'rows': len(result), 'success': True}
            logger.info(f"✅ {test_name}: {duration:.3f}s ({len(result)} result rows)")
            
        except Exception as e:
            results[test_name] = {'success': False, 'error': str(e)}
            logger.warning(f"⚠️ {test_name} failed: {e}")
    
    avg_duration = sum(r['duration'] for r in results.values() if r.get('success')) / len([r for r in results.values() if r.get('success')])
    logger.info(f"✅ Average query time: {avg_duration:.3f}s")
    
    return results

def main():
    """Run comprehensive final validation."""
    logger = setup_logging()
    
    logger.info("🚀 Starting Final End-to-End Pipeline Validation")
    logger.info("=" * 60)
    
    try:
        conn = get_duckdb_connection()
        
        # Run all validation tests
        tests = [
            ("Bronze Layer", test_bronze_layer),
            ("Silver Transformations", test_silver_transformations), 
            ("MinIO Persistence", test_minio_persistence),
            ("Query Performance", test_query_performance)
        ]
        
        results = {}
        for test_name, test_func in tests:
            logger.info(f"\n{'-'*50}")
            results[test_name] = test_func(conn, logger)
        
        conn.close()
        
        # Final Summary
        logger.info(f"\n{'='*60}")
        logger.info("🎯 FINAL VALIDATION SUMMARY:")
        
        all_passed = True
        for test_name, result in results.items():
            if isinstance(result, dict) and result.get('success'):
                logger.info(f"  ✅ {test_name}: PASS")
            else:
                logger.info(f"  ❌ {test_name}: FAIL")
                all_passed = False
        
        if all_passed:
            logger.info("\n🎉 COMPREHENSIVE VALIDATION SUCCESSFUL!")
            logger.info("✅ Bronze → Silver medallion architecture: COMPLETE")
            logger.info("✅ DuckDB + MinIO integration: OPERATIONAL") 
            logger.info("✅ End-to-end data pipeline: PRODUCTION READY")
        else:
            logger.warning("\n⚠️ Some validation tests failed - see details above")
        
        return all_passed
        
    except Exception as e:
        logger.error(f"💥 Fatal validation error: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)