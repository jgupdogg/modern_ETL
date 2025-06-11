#!/usr/bin/env python3
"""
Phase 2: Bronze Data Analysis
Comprehensive analysis of bronze parquet data in MinIO via DuckDB.
"""

import os
import sys
import logging
import duckdb
import json
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
    # Remove http:// prefix for DuckDB
    endpoint = os.getenv('DUCKDB_S3_ENDPOINT', 'http://minio:9000').replace('http://', '').replace('https://', '')
    conn.execute(f"SET s3_endpoint='{endpoint}';")
    conn.execute(f"SET s3_access_key_id='{os.getenv('DUCKDB_S3_ACCESS_KEY', 'minioadmin')}';")
    conn.execute(f"SET s3_secret_access_key='{os.getenv('DUCKDB_S3_SECRET_KEY', 'minioadmin123')}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    
    return conn

def analyze_bronze_schema(conn, logger):
    """Analyze bronze data schema and structure."""
    logger.info("=== Bronze Schema Analysis ===")
    
    try:
        # Get schema information
        schema_query = """
        DESCRIBE SELECT * FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet') LIMIT 1;
        """
        schema_result = conn.execute(schema_query).fetchall()
        
        logger.info("Bronze Schema:")
        for col_name, col_type, null_val, key_val, default_val, extra in schema_result:
            logger.info(f"  {col_name}: {col_type}")
        
        return True
    except Exception as e:
        logger.error(f"Schema analysis failed: {e}")
        return False

def analyze_data_volume(conn, logger):
    """Analyze data volume and distribution."""
    logger.info("=== Data Volume Analysis ===")
    
    try:
        # Total record count
        total_query = "SELECT COUNT(*) FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet');"
        total_records = conn.execute(total_query).fetchone()[0]
        logger.info(f"Total records: {total_records:,}")
        
        # Records by processing date
        date_query = """
        SELECT 
            processing_date,
            COUNT(*) as record_count,
            COUNT(DISTINCT message_id) as unique_messages
        FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet')
        GROUP BY processing_date
        ORDER BY processing_date;
        """
        date_results = conn.execute(date_query).fetchall()
        
        logger.info("Records by date:")
        for date, count, unique in date_results:
            logger.info(f"  {date}: {count:,} records ({unique:,} unique)")
        
        # Records by hour (latest date)
        if date_results:
            latest_date = date_results[-1][0]
            hour_query = f"""
            SELECT 
                processing_hour,
                COUNT(*) as record_count
            FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet')
            WHERE processing_date = '{latest_date}'
            GROUP BY processing_hour
            ORDER BY processing_hour;
            """
            hour_results = conn.execute(hour_query).fetchall()
            
            logger.info(f"Records by hour ({latest_date}):")
            for hour, count in hour_results:
                logger.info(f"  {hour}:00 - {count:,} records")
        
        return True
    except Exception as e:
        logger.error(f"Volume analysis failed: {e}")
        return False

def analyze_payload_structure(conn, logger):
    """Analyze JSON payload structure and content."""
    logger.info("=== Payload Structure Analysis ===")
    
    try:
        # Sample payloads for analysis
        sample_query = """
        SELECT payload
        FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet')
        WHERE payload IS NOT NULL
        LIMIT 5;
        """
        samples = conn.execute(sample_query).fetchall()
        
        logger.info(f"Analyzing {len(samples)} sample payloads...")
        
        payload_keys = set()
        for payload_str, in samples:
            try:
                if payload_str:
                    payload = json.loads(payload_str)
                    if isinstance(payload, dict):
                        payload_keys.update(payload.keys())
            except json.JSONDecodeError:
                continue
        
        logger.info("Common payload keys:")
        for key in sorted(payload_keys):
            logger.info(f"  - {key}")
        
        # Check for Helius-specific transaction data
        helius_query = """
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN payload LIKE '%transaction%' THEN 1 END) as has_transaction,
            COUNT(CASE WHEN payload LIKE '%SWAP%' THEN 1 END) as has_swap,
            COUNT(CASE WHEN payload LIKE '%solana%' THEN 1 END) as has_solana
        FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet')
        WHERE payload IS NOT NULL;
        """
        helius_result = conn.execute(helius_query).fetchone()
        
        logger.info("Payload content analysis:")
        logger.info(f"  Total payloads: {helius_result[0]:,}")
        logger.info(f"  With 'transaction': {helius_result[1]:,}")
        logger.info(f"  With 'SWAP': {helius_result[2]:,}")
        logger.info(f"  With 'solana': {helius_result[3]:,}")
        
        return True
    except Exception as e:
        logger.error(f"Payload analysis failed: {e}")
        return False

def test_query_performance(conn, logger):
    """Test query performance on bronze data."""
    logger.info("=== Query Performance Testing ===")
    
    queries = [
        ("Basic COUNT", "SELECT COUNT(*) FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet');"),
        ("Date aggregation", """
            SELECT processing_date, COUNT(*) 
            FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet')
            GROUP BY processing_date;
        """),
        ("JSON extraction", """
            SELECT 
                message_id,
                JSON_EXTRACT(payload, '$.transactionType') as tx_type
            FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet')
            WHERE payload IS NOT NULL
            LIMIT 10;
        """)
    ]
    
    performance_results = {}
    
    for test_name, query in queries:
        try:
            start_time = datetime.now()
            result = conn.execute(query).fetchall()
            end_time = datetime.now()
            
            duration = (end_time - start_time).total_seconds()
            performance_results[test_name] = {
                'duration': duration,
                'rows': len(result),
                'success': True
            }
            
            logger.info(f"{test_name}: {duration:.3f}s ({len(result)} rows)")
            
        except Exception as e:
            performance_results[test_name] = {'success': False, 'error': str(e)}
            logger.warning(f"{test_name} failed: {e}")
    
    return performance_results

def validate_data_quality(conn, logger):
    """Validate data quality and completeness."""
    logger.info("=== Data Quality Validation ===")
    
    try:
        # Null value analysis
        null_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(message_id) as non_null_message_id,
            COUNT(timestamp) as non_null_timestamp,
            COUNT(source_ip) as non_null_source_ip,
            COUNT(payload) as non_null_payload
        FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet');
        """
        null_result = conn.execute(null_query).fetchone()
        
        logger.info("Data completeness:")
        logger.info(f"  Total records: {null_result[0]:,}")
        logger.info(f"  Non-null message_id: {null_result[1]:,}")
        logger.info(f"  Non-null timestamp: {null_result[2]:,}")
        logger.info(f"  Non-null source_ip: {null_result[3]:,}")
        logger.info(f"  Non-null payload: {null_result[4]:,}")
        
        # Duplicate analysis
        dup_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT message_id) as unique_message_ids
        FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet')
        WHERE message_id IS NOT NULL;
        """
        dup_result = conn.execute(dup_query).fetchone()
        
        duplicates = dup_result[0] - dup_result[1]
        logger.info(f"Duplicate analysis:")
        logger.info(f"  Total records: {dup_result[0]:,}")
        logger.info(f"  Unique message IDs: {dup_result[1]:,}")
        logger.info(f"  Potential duplicates: {duplicates:,}")
        
        return True
    except Exception as e:
        logger.error(f"Data quality validation failed: {e}")
        return False

def main():
    """Run comprehensive bronze data analysis."""
    logger = setup_logging()
    
    logger.info("Starting Phase 2: Bronze Data Analysis")
    
    try:
        conn = get_duckdb_connection()
        
        tests = [
            ("Schema Analysis", analyze_bronze_schema),
            ("Data Volume Analysis", analyze_data_volume),
            ("Payload Structure Analysis", analyze_payload_structure),
            ("Query Performance Testing", test_query_performance),
            ("Data Quality Validation", validate_data_quality)
        ]
        
        results = {}
        for test_name, test_func in tests:
            logger.info(f"\n{'='*60}")
            try:
                if test_name == "Query Performance Testing":
                    results[test_name] = test_func(conn, logger)
                else:
                    results[test_name] = test_func(conn, logger)
            except Exception as e:
                logger.error(f"Test '{test_name}' failed: {e}")
                results[test_name] = False
        
        conn.close()
        
        # Summary
        logger.info(f"\n{'='*60}")
        logger.info("PHASE 2 ANALYSIS SUMMARY:")
        for test_name, result in results.items():
            if test_name == "Query Performance Testing":
                if isinstance(result, dict):
                    passed_tests = sum(1 for r in result.values() if isinstance(r, dict) and r.get('success', False))
                    total_tests = len(result)
                    logger.info(f"  {test_name}: {passed_tests}/{total_tests} queries passed")
                else:
                    logger.info(f"  {test_name}: FAILED")
            else:
                status = "PASS" if result else "FAIL"
                logger.info(f"  {test_name}: {status}")
        
        logger.info("\n🎉 Phase 2 bronze data analysis completed!")
        return True
        
    except Exception as e:
        logger.error(f"Phase 2 analysis failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)