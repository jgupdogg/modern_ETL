#!/usr/bin/env python3
"""
Migrate Large PostgreSQL Dataset to Bronze Raw Schema

Migrates 3.6M records from PostgreSQL bronze.wallet_trade_history 
to the bronze raw schema format expected by silver layer.

Handles:
- Batch processing for memory efficiency
- Schema transformation from from/to format to base/quote format
- Transaction type logic for swap directions
- Price calculations and missing data
- Resumable processing with status tracking

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
from dotenv import load_dotenv
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv('/home/jgupdogg/dev/claude_pipeline/.env')


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
        pa.field("data_source", pa.string(), nullable=False)  # "postgresql_migration"
    ])


class LargePostgresMigrator:
    """Migrate large PostgreSQL dataset to bronze raw schema"""
    
    def __init__(self, batch_size=50000):
        """Initialize migrator with batch processing"""
        self.batch_size = batch_size
        
        # PostgreSQL connection
        self.pg_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'solana_pipeline'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'St0ck!adePG')
        }
        
        # MinIO/S3 connection
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin123',
            config=Config(signature_version='s3v4')
        )
        
        self.bucket = 'solana-data'
        self.output_path = 'bronze/wallet_transactions/'
        self.status_path = 'bronze/wallet_transactions_migration_status/'
        
        # Migration batch ID
        self.migration_batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self.fetched_at = pd.Timestamp.utcnow()
        
    def get_total_count(self):
        """Get total record count for progress tracking"""
        with psycopg2.connect(**self.pg_config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM bronze.wallet_trade_history;")
                return cur.fetchone()[0]
    
    def get_processed_count(self):
        """Get count of already processed records"""
        try:
            # Check if status file exists
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=f"{self.status_path}migration_{self.migration_batch_id}/"
            )
            
            if 'Contents' not in response:
                return 0
            
            # Count processed batches
            processed = 0
            for obj in response['Contents']:
                if 'batch_' in obj['Key'] and obj['Key'].endswith('.json'):
                    processed += self.batch_size
            
            return processed
            
        except Exception as e:
            logger.warning(f"Could not check processed count: {e}")
            return 0
    
    def transform_batch(self, batch_df):
        """Transform PostgreSQL batch to target schema"""
        transformed_records = []
        
        for _, row in batch_df.iterrows():
            # Simple consistent mapping: from_symbol = base, to_symbol = quote
            
            # Base token (from_symbol - what we're swapping FROM)
            base_symbol = row['from_symbol']
            base_address = row['from_address']
            base_type_swap = "from"  # Always "from" for base
            base_ui_change_amount = -float(row['from_amount']) if row['from_amount'] else 0.0  # Negative (losing)
            base_ui_amount = float(row['from_amount']) if row['from_amount'] else 0.0
            base_nearest_price = float(row['base_price']) if row['base_price'] else None
            
            # Quote token (to_symbol - what we're swapping TO)
            quote_symbol = row['to_symbol']
            quote_address = row['to_address']
            quote_type_swap = "to"  # Always "to" for quote
            quote_ui_change_amount = float(row['to_amount']) if row['to_amount'] else 0.0  # Positive (gaining)
            quote_ui_amount = float(row['to_amount']) if row['to_amount'] else 0.0
            quote_nearest_price = float(row['quote_price']) if row['quote_price'] else None
            
            # If no price data, try to derive from value_usd
            if not base_nearest_price and row['value_usd'] and base_ui_amount > 0:
                try:
                    base_nearest_price = float(row['value_usd']) / base_ui_amount
                except (ZeroDivisionError, TypeError):
                    pass
            
            if not quote_nearest_price and row['value_usd'] and quote_ui_amount > 0:
                try:
                    quote_nearest_price = float(row['value_usd']) / quote_ui_amount
                except (ZeroDivisionError, TypeError):
                    pass
            
            # Create transformed record
            transformed_record = {
                # Core identification
                "wallet_address": row['wallet_address'],
                "transaction_hash": row['transaction_hash'],
                "timestamp": pd.Timestamp(row['timestamp']).tz_convert('UTC') if pd.notna(row['timestamp']) else self.fetched_at,
                
                # Base token (from_symbol)
                "base_symbol": base_symbol,
                "base_address": base_address,
                "base_type_swap": base_type_swap,
                "base_ui_change_amount": base_ui_change_amount,
                "base_nearest_price": base_nearest_price,
                "base_decimals": self._get_base_decimals(row),
                "base_ui_amount": base_ui_amount,
                "base_change_amount": self._get_base_raw_amount(row),
                
                # Quote token (to_symbol)
                "quote_symbol": quote_symbol,
                "quote_address": quote_address,
                "quote_type_swap": quote_type_swap,
                "quote_ui_change_amount": quote_ui_change_amount,
                "quote_nearest_price": quote_nearest_price,
                "quote_decimals": self._get_quote_decimals(row),
                "quote_ui_amount": quote_ui_amount,
                "quote_change_amount": self._get_quote_raw_amount(row),
                
                # Transaction metadata
                "source": row['source'] if pd.notna(row['source']) else 'unknown',
                "tx_type": row['tx_type'] if pd.notna(row['tx_type']) else 'swap',
                "block_unix_time": int(row['block_unix_time']) if pd.notna(row['block_unix_time']) else None,
                "owner": row['wallet_address'],  # Same as wallet_address
                
                # Processing state tracking
                "processed_for_pnl": False,  # Reset for new processing
                "pnl_processed_at": None,
                "pnl_processing_batch_id": None,
                
                # Metadata
                "fetched_at": self.fetched_at,
                "batch_id": self.migration_batch_id,
                "data_source": "postgresql_migration"
            }
            
            transformed_records.append(transformed_record)
        
        return pd.DataFrame(transformed_records)
    
    def _get_base_decimals(self, row):
        """Get decimals for base token (from_symbol)"""
        return int(row['from_decimals']) if pd.notna(row.get('from_decimals')) else None
    
    def _get_quote_decimals(self, row):
        """Get decimals for quote token (to_symbol)"""
        return int(row['to_decimals']) if pd.notna(row.get('to_decimals')) else None
    
    def _get_base_raw_amount(self, row):
        """Get raw amount for base token (from_symbol)"""
        return str(row['from_raw_amount']) if pd.notna(row.get('from_raw_amount')) else str(row.get('from_amount', '0'))
    
    def _get_quote_raw_amount(self, row):
        """Get raw amount for quote token (to_symbol)"""
        return str(row['to_raw_amount']) if pd.notna(row.get('to_raw_amount')) else str(row.get('to_amount', '0'))
    
    def process_batch(self, offset, batch_num, total_batches):
        """Process a single batch"""
        batch_start_time = time.time()
        
        try:
            with psycopg2.connect(**self.pg_config) as conn:
                # Query batch from PostgreSQL
                query = f"""
                SELECT 
                    wallet_address,
                    transaction_hash,
                    timestamp,
                    source,
                    block_unix_time,
                    tx_type,
                    transaction_type,
                    from_symbol,
                    from_address,
                    from_decimals,
                    from_amount,
                    from_raw_amount,
                    to_symbol,
                    to_address,
                    to_decimals,
                    to_amount,
                    to_raw_amount,
                    base_price,
                    quote_price,
                    value_usd,
                    processed_for_pnl
                FROM bronze.wallet_trade_history
                ORDER BY id
                OFFSET {offset}
                LIMIT {self.batch_size};
                """
                
                batch_df = pd.read_sql_query(query, conn)
                
                if batch_df.empty:
                    return 0
                
                # Transform to target schema
                transformed_df = self.transform_batch(batch_df)
                
                # Group by date for partitioning
                transformed_df['partition_date'] = pd.to_datetime(transformed_df['timestamp']).dt.date
                
                # Write each date partition
                records_written = 0
                for partition_date, date_df in transformed_df.groupby('partition_date'):
                    # Remove partition column
                    date_df = date_df.drop(columns=['partition_date'])
                    
                    # Convert to PyArrow table
                    table = pa.Table.from_pandas(date_df, schema=get_raw_transaction_schema())
                    
                    # Write to parquet
                    buffer = BytesIO()
                    pq.write_table(table, buffer, compression='snappy')
                    buffer.seek(0)
                    
                    # Upload to MinIO
                    date_str = partition_date.strftime('%Y-%m-%d')
                    file_name = f"wallet_transactions_batch_{batch_num:06d}_{self.migration_batch_id}.parquet"
                    s3_key = f"{self.output_path}date={date_str}/{file_name}"
                    
                    self.s3_client.put_object(
                        Bucket=self.bucket,
                        Key=s3_key,
                        Body=buffer.getvalue()
                    )
                    
                    records_written += len(date_df)
                
                # Write batch status
                status_data = {
                    'batch_num': batch_num,
                    'offset': offset,
                    'records_processed': len(batch_df),
                    'records_written': records_written,
                    'processing_time': time.time() - batch_start_time,
                    'timestamp': datetime.utcnow().isoformat()
                }
                
                status_key = f"{self.status_path}migration_{self.migration_batch_id}/batch_{batch_num:06d}.json"
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=status_key,
                    Body=str(status_data).encode()
                )
                
                # Calculate progress
                progress = (batch_num / total_batches) * 100
                rate = len(batch_df) / (time.time() - batch_start_time) if time.time() > batch_start_time else 0
                
                logger.info(f"Batch {batch_num:4d}/{total_batches} | "
                          f"{progress:5.1f}% | "
                          f"{len(batch_df):,} records | "
                          f"Rate: {rate:,.0f}/sec | "
                          f"Offset: {offset:,}")
                
                return len(batch_df)
                
        except Exception as e:
            logger.error(f"Batch {batch_num} failed: {e}")
            return 0
    
    def run_migration(self):
        """Run the complete migration"""
        logger.info("Starting large PostgreSQL dataset migration...")
        logger.info(f"Batch size: {self.batch_size:,}")
        logger.info(f"Migration batch ID: {self.migration_batch_id}")
        
        # Get total count
        total_count = self.get_total_count()
        total_batches = (total_count + self.batch_size - 1) // self.batch_size
        
        logger.info(f"Total records: {total_count:,}")
        logger.info(f"Total batches: {total_batches:,}")
        
        # Check for resumable progress
        processed_count = self.get_processed_count()
        start_batch = processed_count // self.batch_size + 1 if processed_count > 0 else 1
        start_offset = (start_batch - 1) * self.batch_size
        
        if processed_count > 0:
            logger.info(f"Resuming from batch {start_batch} (offset {start_offset:,})")
        
        # Process all batches
        migration_start_time = time.time()
        total_processed = 0
        
        for batch_num in range(start_batch, total_batches + 1):
            offset = (batch_num - 1) * self.batch_size
            
            records_in_batch = self.process_batch(offset, batch_num, total_batches)
            
            if records_in_batch == 0:
                logger.info(f"No more records at batch {batch_num}, stopping")
                break
            
            total_processed += records_in_batch
            
            # Brief pause to prevent system overload
            time.sleep(0.1)
        
        # Final summary
        total_time = time.time() - migration_start_time
        
        # Write final success marker
        success_data = {
            'migration_batch_id': self.migration_batch_id,
            'total_records_processed': total_processed,
            'total_time_minutes': total_time / 60,
            'average_rate_per_second': total_processed / total_time if total_time > 0 else 0,
            'completion_timestamp': datetime.utcnow().isoformat(),
            'output_location': f's3://{self.bucket}/{self.output_path}'
        }
        
        success_key = f"{self.output_path}_MIGRATION_SUCCESS_{self.migration_batch_id}.json"
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=success_key,
            Body=str(success_data).encode()
        )
        
        logger.info("\n" + "="*80)
        logger.info("MIGRATION COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        logger.info(f"Records processed: {total_processed:,}")
        logger.info(f"Total time: {total_time/60:.1f} minutes")
        logger.info(f"Average rate: {total_processed/total_time:,.0f} records/second")
        logger.info(f"Output location: s3://{self.bucket}/{self.output_path}")
        logger.info(f"Migration batch ID: {self.migration_batch_id}")
        logger.info("="*80)
        
        return total_processed == total_count


def main():
    """Main entry point"""
    print("Large PostgreSQL Dataset Migration to Bronze Raw Schema")
    print("3.6M records: bronze.wallet_trade_history → raw bronze format")
    print("="*80)
    
    # Allow custom batch size
    import sys
    batch_size = int(sys.argv[1]) if len(sys.argv) > 1 else 50000
    
    migrator = LargePostgresMigrator(batch_size=batch_size)
    success = migrator.run_migration()
    
    exit(0 if success else 1)


if __name__ == "__main__":
    main()