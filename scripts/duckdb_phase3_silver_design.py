#!/usr/bin/env python3
"""
Phase 3: Silver Layer Design & Implementation
Creates silver layer transformations based on bronze analysis.
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
    endpoint = os.getenv('DUCKDB_S3_ENDPOINT', 'http://minio:9000').replace('http://', '').replace('https://', '')
    conn.execute(f"SET s3_endpoint='{endpoint}';")
    conn.execute(f"SET s3_access_key_id='{os.getenv('DUCKDB_S3_ACCESS_KEY', 'minioadmin')}';")
    conn.execute(f"SET s3_secret_access_key='{os.getenv('DUCKDB_S3_SECRET_KEY', 'minioadmin123')}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    
    return conn

def create_bronze_view(conn, logger):
    """Create a bronze view for cleaner access to bronze data."""
    logger.info("=== Creating Bronze View ===")
    
    try:
        # Drop existing view if it exists
        conn.execute("DROP VIEW IF EXISTS bronze.webhooks;")
        
        # Create bronze view with deduplication
        bronze_view_sql = """
        CREATE VIEW bronze.webhooks AS
        SELECT DISTINCT
            message_id,
            timestamp as webhook_timestamp,
            source_ip,
            file_path,
            payload,
            kafka_timestamp,
            processed_at,
            processing_date,
            processing_hour,
            -- Add row number for deduplication
            ROW_NUMBER() OVER (
                PARTITION BY message_id 
                ORDER BY processed_at DESC
            ) as row_num
        FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet')
        WHERE message_id IS NOT NULL;
        """
        
        conn.execute(bronze_view_sql)
        
        # Test the view
        count_result = conn.execute("SELECT COUNT(*) FROM bronze.webhooks WHERE row_num = 1;").fetchone()
        logger.info(f"Bronze view created: {count_result[0]} unique records")
        
        return True
    except Exception as e:
        logger.error(f"Bronze view creation failed: {e}")
        return False

def create_silver_webhook_events(conn, logger):
    """Create silver table for cleaned webhook events."""
    logger.info("=== Creating Silver Webhook Events ===")
    
    try:
        # Drop existing table if it exists
        conn.execute("DROP TABLE IF EXISTS silver.webhook_events;")
        
        # Create silver webhook events table
        silver_webhooks_sql = """
        CREATE TABLE silver.webhook_events AS
        SELECT 
            message_id,
            -- Standardize timestamp format
            CASE 
                WHEN webhook_timestamp ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}'
                THEN CAST(webhook_timestamp AS TIMESTAMP)
                ELSE NULL
            END as event_timestamp,
            source_ip,
            file_path,
            -- Extract basic payload information
            CASE 
                WHEN JSON_VALID(payload) THEN payload::JSON
                ELSE NULL
            END as payload_json,
            -- Extract specific fields from payload
            CASE 
                WHEN JSON_VALID(payload) THEN JSON_EXTRACT(payload, '$.source')
                ELSE NULL
            END as payload_source,
            CASE 
                WHEN JSON_VALID(payload) THEN JSON_EXTRACT(payload, '$.test')
                ELSE NULL
            END as payload_test,
            CASE 
                WHEN JSON_VALID(payload) THEN JSON_EXTRACT(payload, '$.value')
                ELSE NULL
            END as payload_value,
            -- Categorize event types
            CASE 
                WHEN payload LIKE '%transaction%' AND payload LIKE '%SWAP%' THEN 'solana_swap'
                WHEN payload LIKE '%transaction%' THEN 'solana_transaction'
                WHEN payload LIKE '%test%' THEN 'test_event'
                ELSE 'unknown'
            END as event_type,
            -- Quality flags
            CASE WHEN JSON_VALID(payload) THEN true ELSE false END as has_valid_json,
            CASE WHEN source_ip IS NOT NULL THEN true ELSE false END as has_source_ip,
            -- Processing metadata
            kafka_timestamp,
            processed_at,
            processing_date,
            -- Add silver processing timestamp
            CURRENT_TIMESTAMP as silver_processed_at
        FROM bronze.webhooks
        WHERE row_num = 1;  -- Only include deduplicated records
        """
        
        conn.execute(silver_webhooks_sql)
        
        # Get summary statistics
        summary_query = """
        SELECT 
            COUNT(*) as total_events,
            COUNT(CASE WHEN event_type = 'solana_swap' THEN 1 END) as solana_swaps,
            COUNT(CASE WHEN event_type = 'solana_transaction' THEN 1 END) as solana_transactions,
            COUNT(CASE WHEN event_type = 'test_event' THEN 1 END) as test_events,
            COUNT(CASE WHEN has_valid_json THEN 1 END) as valid_json_count,
            COUNT(CASE WHEN event_timestamp IS NOT NULL THEN 1 END) as valid_timestamps
        FROM silver.webhook_events;
        """
        
        summary = conn.execute(summary_query).fetchone()
        logger.info("Silver webhook events created:")
        logger.info(f"  Total events: {summary[0]}")
        logger.info(f"  Solana swaps: {summary[1]}")
        logger.info(f"  Solana transactions: {summary[2]}")
        logger.info(f"  Test events: {summary[3]}")
        logger.info(f"  Valid JSON: {summary[4]}")
        logger.info(f"  Valid timestamps: {summary[5]}")
        
        return True
    except Exception as e:
        logger.error(f"Silver webhook events creation failed: {e}")
        return False

def create_silver_transaction_details(conn, logger):
    """Create silver table for transaction-specific details."""
    logger.info("=== Creating Silver Transaction Details ===")
    
    try:
        # Drop existing table if it exists
        conn.execute("DROP TABLE IF EXISTS silver.transaction_details;")
        
        # Create transaction details table for Solana transaction events
        transaction_details_sql = """
        CREATE TABLE silver.transaction_details AS
        SELECT 
            message_id,
            event_timestamp,
            event_type,
            -- Extract transaction-specific fields (when they exist)
            CASE 
                WHEN payload_json IS NOT NULL THEN JSON_EXTRACT(payload_json, '$.transactionType')
                ELSE NULL
            END as transaction_type,
            CASE 
                WHEN payload_json IS NOT NULL THEN JSON_EXTRACT(payload_json, '$.transactionAmount')
                ELSE NULL
            END as transaction_amount,
            CASE 
                WHEN payload_json IS NOT NULL THEN JSON_EXTRACT(payload_json, '$.tokenA')
                ELSE NULL
            END as token_a,
            CASE 
                WHEN payload_json IS NOT NULL THEN JSON_EXTRACT(payload_json, '$.tokenB')
                ELSE NULL
            END as token_b,
            CASE 
                WHEN payload_json IS NOT NULL THEN JSON_EXTRACT(payload_json, '$.accountKeys')
                ELSE NULL
            END as account_keys,
            CASE 
                WHEN payload_json IS NOT NULL THEN JSON_EXTRACT(payload_json, '$.signature')
                ELSE NULL
            END as transaction_signature,
            -- Processing metadata
            processing_date,
            silver_processed_at
        FROM silver.webhook_events
        WHERE event_type IN ('solana_swap', 'solana_transaction');
        """
        
        conn.execute(transaction_details_sql)
        
        # Get transaction summary
        tx_summary = conn.execute("SELECT COUNT(*), event_type FROM silver.transaction_details GROUP BY event_type;").fetchall()
        
        logger.info("Silver transaction details created:")
        for count, event_type in tx_summary:
            logger.info(f"  {event_type}: {count} transactions")
        
        return True
    except Exception as e:
        logger.error(f"Silver transaction details creation failed: {e}")
        return False

def create_silver_data_quality_metrics(conn, logger):
    """Create data quality metrics table."""
    logger.info("=== Creating Data Quality Metrics ===")
    
    try:
        # Drop existing table if it exists
        conn.execute("DROP TABLE IF EXISTS silver.data_quality_metrics;")
        
        # Create data quality metrics
        quality_metrics_sql = """
        CREATE TABLE silver.data_quality_metrics AS
        SELECT 
            processing_date,
            COUNT(*) as total_events,
            COUNT(CASE WHEN has_valid_json THEN 1 END) as valid_json_events,
            COUNT(CASE WHEN has_source_ip THEN 1 END) as events_with_source_ip,
            COUNT(CASE WHEN event_timestamp IS NOT NULL THEN 1 END) as events_with_valid_timestamp,
            COUNT(CASE WHEN event_type = 'unknown' THEN 1 END) as unknown_event_types,
            COUNT(DISTINCT message_id) as unique_message_ids,
            -- Quality percentages
            ROUND(COUNT(CASE WHEN has_valid_json THEN 1 END) * 100.0 / COUNT(*), 2) as json_validity_pct,
            ROUND(COUNT(CASE WHEN event_timestamp IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as timestamp_validity_pct,
            -- Processing metadata
            MIN(silver_processed_at) as first_processed,
            MAX(silver_processed_at) as last_processed,
            CURRENT_TIMESTAMP as metrics_calculated_at
        FROM silver.webhook_events
        GROUP BY processing_date
        ORDER BY processing_date;
        """
        
        conn.execute(quality_metrics_sql)
        
        # Display metrics
        metrics = conn.execute("SELECT * FROM silver.data_quality_metrics;").fetchall()
        
        logger.info("Data quality metrics:")
        for row in metrics:
            date, total, valid_json, with_ip, valid_ts, unknown, unique, json_pct, ts_pct = row[:9]
            logger.info(f"  {date}: {total} events, {unique} unique, {json_pct}% valid JSON, {ts_pct}% valid timestamps")
        
        return True
    except Exception as e:
        logger.error(f"Data quality metrics creation failed: {e}")
        return False

def test_silver_queries(conn, logger):
    """Test silver layer with sample analytical queries."""
    logger.info("=== Testing Silver Layer Queries ===")
    
    queries = [
        ("Event type distribution", """
            SELECT event_type, COUNT(*) as count 
            FROM silver.webhook_events 
            GROUP BY event_type 
            ORDER BY count DESC;
        """),
        ("Events by processing date", """
            SELECT processing_date, COUNT(*) as daily_events 
            FROM silver.webhook_events 
            GROUP BY processing_date 
            ORDER BY processing_date;
        """),
        ("Data quality summary", """
            SELECT 
                AVG(json_validity_pct) as avg_json_validity,
                AVG(timestamp_validity_pct) as avg_timestamp_validity,
                SUM(total_events) as total_processed_events
            FROM silver.data_quality_metrics;
        """),
        ("Sample transaction details", """
            SELECT message_id, transaction_type, transaction_amount, token_a, token_b 
            FROM silver.transaction_details 
            WHERE transaction_type IS NOT NULL 
            LIMIT 5;
        """)
    ]
    
    test_results = {}
    
    for query_name, query in queries:
        try:
            start_time = datetime.now()
            result = conn.execute(query).fetchall()
            end_time = datetime.now()
            
            duration = (end_time - start_time).total_seconds()
            test_results[query_name] = {
                'duration': duration,
                'rows': len(result),
                'success': True,
                'sample_data': result[:3]  # First 3 rows
            }
            
            logger.info(f"{query_name}: {duration:.3f}s ({len(result)} rows)")
            if result:
                logger.info(f"  Sample: {result[0]}")
            
        except Exception as e:
            test_results[query_name] = {'success': False, 'error': str(e)}
            logger.warning(f"{query_name} failed: {e}")
    
    return test_results

def export_silver_to_minio(conn, logger):
    """Export silver tables back to MinIO for persistence."""
    logger.info("=== Exporting Silver Tables to MinIO ===")
    
    exports = [
        ("silver.webhook_events", "s3://webhook-data/silver-webhooks/webhook_events"),
        ("silver.transaction_details", "s3://webhook-data/silver-webhooks/transaction_details"),
        ("silver.data_quality_metrics", "s3://webhook-data/silver-webhooks/data_quality_metrics")
    ]
    
    export_results = {}
    
    for table_name, export_path in exports:
        try:
            # Export table to MinIO with overwrite option
            export_sql = f"""
            COPY {table_name} TO '{export_path}' 
            (FORMAT PARQUET, PARTITION_BY processing_date, OVERWRITE_OR_IGNORE);
            """
            
            conn.execute(export_sql)
            
            # Verify export
            count_sql = f"SELECT COUNT(*) FROM {table_name};"
            count = conn.execute(count_sql).fetchone()[0]
            
            export_results[table_name] = {'success': True, 'records': count}
            logger.info(f"Exported {table_name}: {count} records to {export_path}")
            
        except Exception as e:
            export_results[table_name] = {'success': False, 'error': str(e)}
            logger.warning(f"Export failed for {table_name}: {e}")
    
    return export_results

def main():
    """Run Phase 3: Silver layer design and implementation."""
    logger = setup_logging()
    
    logger.info("Starting Phase 3: Silver Layer Design & Implementation")
    
    try:
        conn = get_duckdb_connection()
        
        # Phase 3 tasks
        tasks = [
            ("Create Bronze View", create_bronze_view),
            ("Create Silver Webhook Events", create_silver_webhook_events),
            ("Create Silver Transaction Details", create_silver_transaction_details),
            ("Create Data Quality Metrics", create_silver_data_quality_metrics),
            ("Test Silver Queries", test_silver_queries),
            ("Export Silver to MinIO", export_silver_to_minio)
        ]
        
        results = {}
        for task_name, task_func in tasks:
            logger.info(f"\n{'='*60}")
            try:
                results[task_name] = task_func(conn, logger)
            except Exception as e:
                logger.error(f"Task '{task_name}' failed: {e}")
                results[task_name] = False
        
        conn.close()
        
        # Summary
        logger.info(f"\n{'='*60}")
        logger.info("PHASE 3 SILVER LAYER SUMMARY:")
        for task_name, result in results.items():
            if task_name in ["Test Silver Queries", "Export Silver to MinIO"]:
                if isinstance(result, dict):
                    success_count = sum(1 for r in result.values() if isinstance(r, dict) and r.get('success', False))
                    total_count = len(result)
                    logger.info(f"  {task_name}: {success_count}/{total_count} successful")
                else:
                    status = "PASS" if result else "FAIL"
                    logger.info(f"  {task_name}: {status}")
            else:
                status = "PASS" if result else "FAIL"
                logger.info(f"  {task_name}: {status}")
        
        logger.info("\n🎉 Phase 3 silver layer implementation completed!")
        logger.info("\nSilver layer structure:")
        logger.info("  - bronze.webhooks (view): Deduplicated bronze data")
        logger.info("  - silver.webhook_events: Cleaned and categorized events")
        logger.info("  - silver.transaction_details: Transaction-specific data")
        logger.info("  - silver.data_quality_metrics: Data quality monitoring")
        logger.info("  - MinIO exports: s3://webhook-data/silver-webhooks/")
        
        return True
        
    except Exception as e:
        logger.error(f"Phase 3 failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)