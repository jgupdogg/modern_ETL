#!/usr/bin/env python3
"""
Migrate PostgreSQL bronze.wallet_trade_history to Bronze Raw Schema

Converts PostgreSQL wallet trade history to the raw bronze schema format
expected by the silver layer PnL processing.

Author: Claude Code
Date: 2025-06-19
"""

import os
import logging
import psycopg2
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from io import BytesIO
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_raw_transaction_schema() -> pa.Schema:
    """Get PyArrow schema for raw wallet transaction data"""
    return pa.schema([
        # Core identification
        pa.field("wallet_address", pa.string(), nullable=False),
        pa.field("transaction_hash", pa.string(), nullable=False), 
        pa.field("timestamp", pa.timestamp('us', tz='UTC'), nullable=False),
        
        # Raw API response - Base token
        pa.field("base_symbol", pa.string(), nullable=True),
        pa.field("base_address", pa.string(), nullable=True),
        pa.field("base_type_swap", pa.string(), nullable=True),  # "from" or "to"
        pa.field("base_ui_change_amount", pa.float64(), nullable=True),
        pa.field("base_nearest_price", pa.float64(), nullable=True),
        pa.field("base_decimals", pa.int32(), nullable=True),
        pa.field("base_ui_amount", pa.float64(), nullable=True),
        pa.field("base_change_amount", pa.string(), nullable=True),  # Raw amount as string
        
        # Raw API response - Quote token  
        pa.field("quote_symbol", pa.string(), nullable=True),
        pa.field("quote_address", pa.string(), nullable=True),
        pa.field("quote_type_swap", pa.string(), nullable=True),  # "from" or "to"
        pa.field("quote_ui_change_amount", pa.float64(), nullable=True),
        pa.field("quote_nearest_price", pa.float64(), nullable=True),
        pa.field("quote_decimals", pa.int32(), nullable=True),
        pa.field("quote_ui_amount", pa.float64(), nullable=True),
        pa.field("quote_change_amount", pa.string(), nullable=True),  # Raw amount as string
        
        # Transaction metadata
        pa.field("source", pa.string(), nullable=True),  # DEX source
        pa.field("tx_type", pa.string(), nullable=True),  # "swap", "transfer"
        pa.field("block_unix_time", pa.int64(), nullable=True),
        pa.field("owner", pa.string(), nullable=True),  # API owner field
        
        # Processing state tracking
        pa.field("processed_for_pnl", pa.bool_(), nullable=False),  # Default: False
        pa.field("pnl_processed_at", pa.timestamp('us', tz='UTC'), nullable=True),
        pa.field("pnl_processing_batch_id", pa.string(), nullable=True),
        
        # Metadata
        pa.field("fetched_at", pa.timestamp('us', tz='UTC'), nullable=False),
        pa.field("batch_id", pa.string(), nullable=False),
        pa.field("data_source", pa.string(), nullable=False)  # "birdeye_v3", "migration"
    ])


class PostgresToBronzeRawMigrator:
    """Migrate PostgreSQL data to Bronze Raw Schema"""
    
    def __init__(self):
        # PostgreSQL connection
        self.pg_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': 'solana_pipeline',
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'St0ck!adePG')
        }
        
        # MinIO/S3 connection
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin123',
            region_name='us-east-1',
            config=Config(signature_version='s3v4')
        )
        
        self.bucket = 'solana-data'
        self.output_path = 'bronze/wallet_transactions/'
        
    def migrate_to_raw_schema(self):
        """Migrate PostgreSQL data to raw bronze schema"""
        logger.info("Starting PostgreSQL to Bronze Raw migration...")
        
        # Generate batch ID
        batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        fetched_at = pd.Timestamp.utcnow()
        
        try:
            # Connect to PostgreSQL
            with psycopg2.connect(**self.pg_config) as conn:
                # Query all data from bronze.wallet_trade_history
                query = """
                SELECT 
                    wallet_address,
                    signature,
                    block_time,
                    from_token_address,
                    from_token_symbol,
                    from_amount,
                    to_token_address,
                    to_token_symbol,
                    to_amount,
                    price_per_token,
                    usd_value,
                    processing_status,
                    created_at
                FROM bronze.wallet_trade_history
                ORDER BY block_time;
                """
                
                df = pd.read_sql_query(query, conn)
                logger.info(f"Fetched {len(df)} records from PostgreSQL")
                
            if df.empty:
                logger.warning("No records found in bronze.wallet_trade_history")
                return
            
            # Transform to raw bronze schema
            raw_records = []
            
            for _, row in df.iterrows():
                # Determine transaction type based on token flow
                # If selling SOL for USDC: base=SOL (from), quote=USDC (to)
                # If buying SOL with USDC: base=USDC (from), quote=SOL (to)
                
                raw_record = {
                    # Core identification
                    "wallet_address": row['wallet_address'],
                    "transaction_hash": row['signature'],
                    "timestamp": pd.Timestamp(row['block_time']).tz_localize('UTC') if pd.Timestamp(row['block_time']).tz is None else pd.Timestamp(row['block_time']),
                    
                    # Base token (what we're selling/from)
                    "base_symbol": row['from_token_symbol'],
                    "base_address": row['from_token_address'],
                    "base_type_swap": "from",  # We're selling the base token
                    "base_ui_change_amount": -float(row['from_amount']),  # Negative for selling
                    "base_nearest_price": float(row['price_per_token']) if row['price_per_token'] else None,
                    "base_decimals": None,  # Unknown from PostgreSQL data
                    "base_ui_amount": float(row['from_amount']),
                    "base_change_amount": str(row['from_amount']),
                    
                    # Quote token (what we're buying/to)
                    "quote_symbol": row['to_token_symbol'],
                    "quote_address": row['to_token_address'],
                    "quote_type_swap": "to",  # We're buying the quote token
                    "quote_ui_change_amount": float(row['to_amount']),  # Positive for buying
                    "quote_nearest_price": None,  # Calculate if needed
                    "quote_decimals": None,  # Unknown from PostgreSQL data
                    "quote_ui_amount": float(row['to_amount']),
                    "quote_change_amount": str(row['to_amount']),
                    
                    # Transaction metadata
                    "source": "postgresql_migration",
                    "tx_type": "swap",
                    "block_unix_time": int(pd.Timestamp(row['block_time']).timestamp()),
                    "owner": row['wallet_address'],  # Same as wallet_address
                    
                    # Processing state tracking
                    "processed_for_pnl": False,  # Not yet processed
                    "pnl_processed_at": None,
                    "pnl_processing_batch_id": None,
                    
                    # Metadata
                    "fetched_at": fetched_at,
                    "batch_id": batch_id,
                    "data_source": "postgresql_migration"
                }
                
                raw_records.append(raw_record)
            
            # Convert to DataFrame
            raw_df = pd.DataFrame(raw_records)
            logger.info(f"Transformed {len(raw_df)} records to raw schema")
            
            # Group by date for partitioning
            raw_df['partition_date'] = pd.to_datetime(raw_df['timestamp']).dt.date
            
            # Upload to MinIO by date partition
            for partition_date, date_df in raw_df.groupby('partition_date'):
                # Remove partition_date column before saving
                date_df = date_df.drop(columns=['partition_date'])
                
                # Convert to PyArrow table with schema
                table = pa.Table.from_pandas(date_df, schema=get_raw_transaction_schema())
                
                # Write to parquet
                buffer = BytesIO()
                pq.write_table(table, buffer, compression='snappy')
                buffer.seek(0)
                
                # Upload to MinIO
                date_str = partition_date.strftime('%Y-%m-%d')
                file_name = f"wallet_transactions_{batch_id}.parquet"
                s3_key = f"{self.output_path}date={date_str}/{file_name}"
                
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=s3_key,
                    Body=buffer.getvalue(),
                    ContentType='application/octet-stream'
                )
                
                logger.info(f"Uploaded {len(date_df)} records to s3://{self.bucket}/{s3_key}")
            
            # Write success marker
            success_key = f"{self.output_path}date={datetime.utcnow().strftime('%Y-%m-%d')}/_SUCCESS_{batch_id}"
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=success_key,
                Body=b''
            )
            
            logger.info(f"Migration completed successfully! Batch ID: {batch_id}")
            
            # Print summary
            print("\n" + "="*60)
            print("MIGRATION SUMMARY")
            print("="*60)
            print(f"Total Records Migrated: {len(raw_df)}")
            print(f"Unique Wallets: {raw_df['wallet_address'].nunique()}")
            print(f"Date Range: {raw_df['timestamp'].min()} to {raw_df['timestamp'].max()}")
            print(f"Output Location: s3://{self.bucket}/{self.output_path}")
            print(f"Batch ID: {batch_id}")
            print("="*60)
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            raise


def main():
    """Main entry point"""
    print("PostgreSQL to Bronze Raw Schema Migration")
    print("Converting bronze.wallet_trade_history to raw format")
    print("-" * 60)
    
    migrator = PostgresToBronzeRawMigrator()
    migrator.migrate_to_raw_schema()


if __name__ == "__main__":
    main()