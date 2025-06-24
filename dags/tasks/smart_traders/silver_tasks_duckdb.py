"""
Silver Layer Tasks - DuckDB Implementation
Safe, reliable wallet PnL processing using DuckDB instead of PySpark
Addresses memory issues and system crashes from PySpark approach
"""

import os
import json
import logging
import duckdb
import tempfile
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import boto3
from botocore.client import Config

# Import centralized configuration
from config.smart_trader_config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET,
    SILVER_WALLET_PNL_PATH, BRONZE_WALLET_TRANSACTIONS_PATH,
    SILVER_PNL_WALLET_BATCH_SIZE
)


def get_minio_client() -> boto3.client:
    """Create MinIO S3 client"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4')
    )


def setup_duckdb_with_s3():
    """Setup DuckDB with S3/MinIO configuration for processing"""
    conn = duckdb.connect(':memory:')  # Use in-memory for processing
    
    # Configure S3/MinIO
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    endpoint = MINIO_ENDPOINT.replace('http://', '').replace('https://', '')
    conn.execute(f"SET s3_endpoint='{endpoint}';")
    conn.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    
    return conn


def get_available_transaction_partitions(conn, max_days=7):
    """Get list of available transaction partitions from the last N days"""
    logger = logging.getLogger(__name__)
    
    # Generate target dates
    target_dates = []
    for days_ago in range(max_days):
        date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        target_dates.append(date)
    
    available_partitions = []
    
    for date in target_dates:
        try:
            # Test if partition exists by trying to read schema
            test_query = f"""
            SELECT COUNT(*) 
            FROM parquet_scan('s3://solana-data/bronze/wallet_transactions/date={date}/*.parquet')
            LIMIT 1
            """
            result = conn.execute(test_query).fetchone()
            if result and result[0] >= 0:  # Even 0 records means partition exists
                available_partitions.append(date)
                logger.info(f"✅ Found partition: date={date}")
        except Exception as e:
            logger.warning(f"❌ Partition date={date} not available: {e}")
    
    return available_partitions


def get_unprocessed_wallets(conn, date_partition, batch_size=5):
    """Get a batch of unprocessed wallets from a specific date partition"""
    logger = logging.getLogger(__name__)
    
    try:
        query = f"""
        SELECT DISTINCT wallet_address
        FROM parquet_scan('s3://solana-data/bronze/wallet_transactions/date={date_partition}/*.parquet')
        WHERE (processed_for_pnl = false OR processed_for_pnl IS NULL)
          AND tx_type = 'SWAP'
          AND base_nearest_price > 0
          AND quote_nearest_price > 0
        LIMIT {batch_size}
        """
        
        result = conn.execute(query).fetchall()
        wallets = [row[0] for row in result]
        
        logger.info(f"Found {len(wallets)} unprocessed wallets in {date_partition}")
        return wallets
        
    except Exception as e:
        logger.error(f"Failed to get unprocessed wallets for {date_partition}: {e}")
        return []


def calculate_wallet_pnl_duckdb(conn, wallet_address, date_partition):
    """Calculate PnL for a single wallet using DuckDB - simplified but accurate approach"""
    logger = logging.getLogger(__name__)
    
    try:
        # Comprehensive wallet analysis query
        pnl_query = f"""
        WITH wallet_swaps AS (
            SELECT 
                wallet_address,
                base_address,
                quote_address,
                base_symbol,
                quote_symbol,
                base_ui_change_amount,
                quote_ui_change_amount,
                base_nearest_price,
                quote_nearest_price,
                timestamp,
                date,
                -- Calculate USD values for both sides of swap
                ABS(base_ui_change_amount) * COALESCE(base_nearest_price, 0) as base_usd_value,
                ABS(quote_ui_change_amount) * COALESCE(quote_nearest_price, 0) as quote_usd_value,
                -- Determine if this is buying or selling the base token
                CASE 
                    WHEN base_ui_change_amount > 0 THEN 'BUY_BASE'
                    WHEN base_ui_change_amount < 0 THEN 'SELL_BASE'
                    ELSE 'NEUTRAL'
                END as trade_direction
            FROM parquet_scan('s3://solana-data/bronze/wallet_transactions/date={date_partition}/*.parquet')
            WHERE wallet_address = '{wallet_address}'
              AND tx_type = 'SWAP'
              AND base_nearest_price > 0
              AND quote_nearest_price > 0
              AND (processed_for_pnl = false OR processed_for_pnl IS NULL)
        ),
        token_level_analysis AS (
            SELECT 
                wallet_address,
                base_address as token_address,
                base_symbol as token_symbol,
                COUNT(*) as trade_count,
                -- Volume metrics
                SUM(base_usd_value) as total_base_volume,
                SUM(quote_usd_value) as total_quote_volume,
                -- PnL approximation: net USD flow from perspective of base token
                SUM(CASE 
                    WHEN trade_direction = 'BUY_BASE' THEN -quote_usd_value  -- Spent quote to get base
                    WHEN trade_direction = 'SELL_BASE' THEN quote_usd_value   -- Received quote for base
                    ELSE 0 
                END) as net_pnl_estimate,
                -- Trading patterns
                COUNT(CASE WHEN trade_direction = 'BUY_BASE' THEN 1 END) as buy_count,
                COUNT(CASE WHEN trade_direction = 'SELL_BASE' THEN 1 END) as sell_count,
                MIN(date) as first_trade_date,
                MAX(date) as last_trade_date,
                AVG(base_usd_value) as avg_trade_size
            FROM wallet_swaps
            WHERE base_address IS NOT NULL
            GROUP BY wallet_address, base_address, base_symbol
        ),
        portfolio_summary AS (
            SELECT 
                wallet_address,
                'ALL_TOKENS' as token_address,
                'PORTFOLIO' as token_symbol,
                SUM(trade_count) as trade_count,
                SUM(total_base_volume) + SUM(total_quote_volume) as total_volume,
                SUM(net_pnl_estimate) as total_pnl,
                -- Calculate ROI as percentage of total volume
                CASE 
                    WHEN SUM(total_base_volume) + SUM(total_quote_volume) > 0 
                    THEN (SUM(net_pnl_estimate) / (SUM(total_base_volume) + SUM(total_quote_volume))) * 100
                    ELSE 0 
                END as roi,
                -- Win rate: tokens with positive PnL / total tokens traded
                (COUNT(CASE WHEN net_pnl_estimate > 0 THEN 1 END) * 100.0 / COUNT(*)) as win_rate,
                SUM(buy_count) as total_buys,
                SUM(sell_count) as total_sells,
                MIN(first_trade_date) as portfolio_first_trade,
                MAX(last_trade_date) as portfolio_last_trade,
                AVG(avg_trade_size) as avg_trade_size
            FROM token_level_analysis
            GROUP BY wallet_address
        )
        SELECT 
            wallet_address,
            token_address,
            token_symbol,
            trade_count,
            COALESCE(total_volume, 0) as total_volume,
            COALESCE(total_pnl, 0) as total_pnl,
            COALESCE(roi, 0) as roi,
            COALESCE(win_rate, 0) as win_rate,
            COALESCE(total_buys, 0) as total_buys,
            COALESCE(total_sells, 0) as total_sells,
            portfolio_first_trade,
            portfolio_last_trade,
            COALESCE(avg_trade_size, 0) as avg_trade_size,
            CURRENT_DATE as calculation_date,
            CURRENT_TIMESTAMP as processed_at
        FROM portfolio_summary
        """
        
        result = conn.execute(pnl_query).fetchone()
        
        if result:
            return {
                'wallet_address': result[0],
                'token_address': result[1], 
                'token_symbol': result[2],
                'trade_count': result[3],
                'total_volume': result[4],
                'total_pnl': result[5],
                'roi': result[6],
                'win_rate': result[7],
                'total_buys': result[8],
                'total_sells': result[9],
                'first_trade_date': result[10],
                'last_trade_date': result[11],
                'avg_trade_size': result[12],
                'calculation_date': result[13],
                'processed_at': result[14]
            }
        else:
            return None
            
    except Exception as e:
        logger.error(f"PnL calculation failed for {wallet_address}: {e}")
        return None


def export_pnl_to_minio(pnl_records, batch_id):
    """Export PnL records to MinIO in silver layer format"""
    logger = logging.getLogger(__name__)
    
    if not pnl_records:
        logger.warning("No PnL records to export")
        return False
    
    try:
        # Create temporary file for export
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            # Convert to DuckDB table and export
            conn = duckdb.connect(':memory:')
            
            # Create table from records
            conn.execute("""
            CREATE TABLE temp_pnl (
                wallet_address VARCHAR,
                token_address VARCHAR,
                token_symbol VARCHAR,
                trade_count INTEGER,
                total_volume DOUBLE,
                total_pnl DOUBLE,
                roi DOUBLE,
                win_rate DOUBLE,
                total_buys INTEGER,
                total_sells INTEGER,
                first_trade_date DATE,
                last_trade_date DATE,
                avg_trade_size DOUBLE,
                calculation_date DATE,
                processed_at TIMESTAMP
            )
            """)
            
            # Insert records
            for record in pnl_records:
                conn.execute("""
                INSERT INTO temp_pnl VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    record['wallet_address'],
                    record['token_address'],
                    record['token_symbol'], 
                    record['trade_count'],
                    record['total_volume'],
                    record['total_pnl'],
                    record['roi'],
                    record['win_rate'],
                    record['total_buys'],
                    record['total_sells'],
                    record['first_trade_date'],
                    record['last_trade_date'],
                    record['avg_trade_size'],
                    record['calculation_date'],
                    record['processed_at']
                ])
            
            # Export to parquet file
            conn.execute(f"COPY temp_pnl TO '{tmp_file.name}' (FORMAT PARQUET)")
            
        # Upload to MinIO
        s3_client = get_minio_client()
        
        # Create S3 key with partitioning
        current_date = datetime.now()
        s3_key = f"silver/wallet_pnl/calculation_year={current_date.year}/calculation_month={current_date.month:02d}/pnl_batch_{batch_id}.parquet"
        
        s3_client.upload_file(tmp_file.name, MINIO_BUCKET, s3_key)
        
        # Cleanup temp file
        os.unlink(tmp_file.name)
        
        logger.info(f"✅ Exported {len(pnl_records)} PnL records to s3://{MINIO_BUCKET}/{s3_key}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to export PnL records: {e}")
        return False


def transform_silver_wallet_pnl_duckdb(**context):
    """
    DuckDB-based wallet PnL processing - safe alternative to PySpark
    Processes wallet transactions to generate silver layer PnL metrics
    """
    logger = logging.getLogger(__name__)
    
    # Generate batch ID
    batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    logger.info(f"🚀 Starting DuckDB wallet PnL processing - Batch: {batch_id}")
    
    # Setup DuckDB connection
    try:
        conn = setup_duckdb_with_s3()
        logger.info("✅ DuckDB connection established")
    except Exception as e:
        logger.error(f"❌ Failed to setup DuckDB: {e}")
        return {
            "wallets_processed": 0,
            "total_pnl_records": 0,
            "batch_id": batch_id,
            "status": "duckdb_setup_failed"
        }
    
    try:
        # Get available partitions
        available_partitions = get_available_transaction_partitions(conn, max_days=7)
        
        if not available_partitions:
            logger.warning("No transaction partitions available for processing")
            return {
                "wallets_processed": 0,
                "total_pnl_records": 0,
                "batch_id": batch_id,
                "status": "no_partitions_available"
            }
        
        logger.info(f"Processing partitions: {available_partitions}")
        
        total_wallets_processed = 0
        total_pnl_records = 0
        all_pnl_records = []
        
        # Process each partition
        for partition_date in available_partitions[:3]:  # Limit to 3 partitions for safety
            logger.info(f"📊 Processing partition: {partition_date}")
            
            # Get batch of unprocessed wallets
            wallets = get_unprocessed_wallets(conn, partition_date, SILVER_PNL_WALLET_BATCH_SIZE)
            
            if not wallets:
                logger.info(f"No unprocessed wallets in {partition_date}")
                continue
            
            # Process each wallet
            partition_records = []
            for wallet_address in wallets:
                logger.info(f"💰 Processing wallet: {wallet_address}")
                
                pnl_data = calculate_wallet_pnl_duckdb(conn, wallet_address, partition_date)
                
                if pnl_data:
                    partition_records.append(pnl_data)
                    logger.info(f"✅ {wallet_address}: PnL=${pnl_data['total_pnl']:.2f}, ROI={pnl_data['roi']:.1f}%, Trades={pnl_data['trade_count']}")
                    total_wallets_processed += 1
                else:
                    logger.warning(f"❌ Failed to process {wallet_address}")
            
            # Add partition records to total
            all_pnl_records.extend(partition_records)
            total_pnl_records += len(partition_records)
            
            logger.info(f"📈 Partition {partition_date}: {len(partition_records)} PnL records generated")
        
        # Export all records to MinIO
        if all_pnl_records:
            export_success = export_pnl_to_minio(all_pnl_records, batch_id)
            
            if export_success:
                logger.info(f"🎉 Successfully processed {total_wallets_processed} wallets, generated {total_pnl_records} PnL records")
                status = "success"
            else:
                logger.error("❌ Failed to export PnL records to MinIO")
                status = "export_failed"
        else:
            logger.warning("⚠️ No PnL records generated")
            status = "no_records_generated"
        
        return {
            "wallets_processed": total_wallets_processed,
            "total_pnl_records": total_pnl_records,
            "batch_id": batch_id,
            "status": status,
            "partitions_processed": available_partitions[:3]
        }
        
    except Exception as e:
        logger.error(f"❌ DuckDB wallet PnL processing failed: {e}")
        return {
            "wallets_processed": 0,
            "total_pnl_records": 0,
            "batch_id": batch_id,
            "status": "processing_failed",
            "error": str(e)
        }
    
    finally:
        if conn:
            conn.close()


# Compatibility wrapper for existing DAG
def transform_silver_wallet_pnl(**context):
    """Wrapper that uses DuckDB implementation"""
    return transform_silver_wallet_pnl_duckdb(**context)