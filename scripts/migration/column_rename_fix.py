#!/usr/bin/env python3
"""
Simple Column Rename Fix
Renames columns in existing migrated data to match expected bronze schema
Much more efficient than full re-migration
"""

import logging
import time
from datetime import datetime, timezone
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ColumnRenameFix:
    """Rename columns in existing data to match bronze schema"""
    
    def __init__(self):
        self.batch_id = f"column_rename_{int(time.time())}"
        
        # MinIO S3 client
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin123',
            config=Config(signature_version='s3v4')
        )
        
        # Column mapping from current (wrong) to expected (correct)
        self.column_mapping = {
            # Current → Expected
            'transaction_type': 'tx_type',
            'token_a': 'base_symbol',
            'token_b': 'quote_symbol', 
            'token_a_address': 'base_address',
            'token_b_address': 'quote_address',
            'amount_a': 'base_ui_amount',
            'amount_b': 'quote_ui_amount',
            'base_price': 'base_nearest_price',
            'quote_price': 'quote_nearest_price',
            
            # Keep these as-is (already correct)
            'wallet_address': 'wallet_address',
            'transaction_hash': 'transaction_hash', 
            'timestamp': 'timestamp',
            'value_usd': 'value_usd',
            'nearest_price': 'nearest_price',
            'processed_for_pnl': 'processed_for_pnl',
            'migration_source': 'data_source',
            'partition_date': 'partition_date'
        }
        
        # Add missing bronze schema columns with defaults
        self.missing_columns = {
            'base_type_swap': 'from',  # Default direction
            'quote_type_swap': 'to',   # Default direction
            'base_decimals': 9,        # Default SOL decimals
            'quote_decimals': 9,       # Default decimals
            'base_change_amount': '0', # String version of amount
            'quote_change_amount': '0', # String version of amount
            'source': 'migration_dex', # DEX source
            'block_unix_time': None,   # Not available
            'owner': None,             # Use wallet_address later
            'pnl_processed_at': None,
            'pnl_processing_batch_id': None,
            'fetched_at': datetime.now(timezone.utc),
            'batch_id': self.batch_id
        }
        
        self.stats = {
            'files_processed': 0,
            'records_processed': 0,
            'start_time': datetime.now(timezone.utc)
        }

    def get_target_bronze_schema(self) -> pa.Schema:
        """Get the exact bronze schema we need to match"""
        return pa.schema([
            # Core identification
            pa.field("wallet_address", pa.string(), nullable=False),
            pa.field("transaction_hash", pa.string(), nullable=False), 
            pa.field("timestamp", pa.timestamp('us', tz='UTC'), nullable=False),
            
            # Base token
            pa.field("base_symbol", pa.string(), nullable=True),
            pa.field("base_address", pa.string(), nullable=True),
            pa.field("base_type_swap", pa.string(), nullable=True),
            pa.field("base_ui_amount", pa.float64(), nullable=True),
            pa.field("base_nearest_price", pa.float64(), nullable=True),
            pa.field("base_decimals", pa.int32(), nullable=True),
            pa.field("base_ui_change_amount", pa.float64(), nullable=True),
            pa.field("base_change_amount", pa.string(), nullable=True),
            
            # Quote token  
            pa.field("quote_symbol", pa.string(), nullable=True),
            pa.field("quote_address", pa.string(), nullable=True),
            pa.field("quote_type_swap", pa.string(), nullable=True),
            pa.field("quote_ui_amount", pa.float64(), nullable=True),
            pa.field("quote_nearest_price", pa.float64(), nullable=True),
            pa.field("quote_decimals", pa.int32(), nullable=True),
            pa.field("quote_ui_change_amount", pa.float64(), nullable=True),
            pa.field("quote_change_amount", pa.string(), nullable=True),
            
            # Transaction metadata
            pa.field("source", pa.string(), nullable=True),
            pa.field("tx_type", pa.string(), nullable=True),
            pa.field("block_unix_time", pa.int64(), nullable=True),
            pa.field("owner", pa.string(), nullable=True),
            
            # Processing state
            pa.field("processed_for_pnl", pa.bool_(), nullable=False),
            pa.field("pnl_processed_at", pa.timestamp('us', tz='UTC'), nullable=True),
            pa.field("pnl_processing_batch_id", pa.string(), nullable=True),
            
            # Metadata
            pa.field("fetched_at", pa.timestamp('us', tz='UTC'), nullable=False),
            pa.field("batch_id", pa.string(), nullable=False),
            pa.field("data_source", pa.string(), nullable=False)
        ])

    def transform_record(self, record: dict) -> dict:
        """Transform a single record to match bronze schema"""
        new_record = {}
        
        # Apply column mapping
        for old_col, new_col in self.column_mapping.items():
            if old_col in record:
                new_record[new_col] = record[old_col]
        
        # Add missing columns
        for col, default_val in self.missing_columns.items():
            if col == 'owner':
                new_record[col] = new_record.get('wallet_address')
            elif col == 'base_ui_change_amount':
                new_record[col] = new_record.get('base_ui_amount', 0.0)
            elif col == 'quote_ui_change_amount':  
                new_record[col] = new_record.get('quote_ui_amount', 0.0)
            elif col == 'base_change_amount':
                amount = new_record.get('base_ui_amount', 0.0)
                new_record[col] = str(int(amount * 1e9)) if amount else "0"
            elif col == 'quote_change_amount':
                amount = new_record.get('quote_ui_amount', 0.0) 
                new_record[col] = str(int(amount * 1e9)) if amount else "0"
            else:
                new_record[col] = default_val
        
        # Ensure required fields exist
        if 'tx_type' in new_record and new_record['tx_type'] == 'UNKNOWN':
            new_record['tx_type'] = 'swap'  # Convert UNKNOWN to swap
            
        return new_record

    def process_files(self):
        """Process all files in the current location"""
        logger.info("Starting column rename process...")
        
        # List all current wallet transaction files
        paginator = self.s3_client.get_paginator('list_objects_v2')
        files_to_process = []
        
        for page in paginator.paginate(Bucket='solana-data', Prefix='bronze/wallet_transactions/'):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.parquet'):
                    files_to_process.append(obj['Key'])
        
        logger.info(f"Found {len(files_to_process)} files to process")
        
        if not files_to_process:
            logger.error("No files found to process!")
            return
        
        # Process each file
        for file_key in files_to_process:
            try:
                self.process_single_file(file_key)
                self.stats['files_processed'] += 1
                
                if self.stats['files_processed'] % 100 == 0:
                    logger.info(f"Processed {self.stats['files_processed']}/{len(files_to_process)} files")
                    
            except Exception as e:
                logger.error(f"Failed to process {file_key}: {e}")
                continue
        
        logger.info(f"Column rename complete: {self.stats['files_processed']} files, {self.stats['records_processed']} records")

    def process_single_file(self, file_key: str):
        """Process a single parquet file"""
        # Download file
        response = self.s3_client.get_object(Bucket='solana-data', Key=file_key)
        file_content = response['Body'].read()
        
        # Read parquet
        table = pq.read_table(pa.BufferReader(file_content))
        df = table.to_pandas()
        
        # Transform records
        records = df.to_dict('records')
        transformed_records = [self.transform_record(record) for record in records]
        
        # Create new table with correct schema
        new_df = pd.DataFrame(transformed_records)
        schema = self.get_target_bronze_schema()
        new_table = pa.Table.from_pandas(new_df, schema=schema)
        
        # Write back to same location
        buffer = pa.BufferOutputStream()
        pq.write_table(new_table, buffer, compression='snappy')
        
        self.s3_client.put_object(
            Bucket='solana-data',
            Key=file_key,
            Body=buffer.getvalue().to_pybytes()
        )
        
        self.stats['records_processed'] += len(transformed_records)

    def validate_result(self):
        """Validate that the column rename worked"""
        logger.info("Validating column rename results...")
        
        try:
            # Try to read a sample file and check schema
            response = self.s3_client.list_objects_v2(
                Bucket='solana-data', 
                Prefix='bronze/wallet_transactions/',
                MaxKeys=1
            )
            
            if not response.get('Contents'):
                raise Exception("No files found after processing")
            
            first_file = response['Contents'][0]['Key']
            obj_response = self.s3_client.get_object(Bucket='solana-data', Key=first_file)
            table = pq.read_table(pa.BufferReader(obj_response['Body'].read()))
            
            # Check that expected columns exist
            expected_cols = ['tx_type', 'base_address', 'quote_address', 'base_ui_amount', 'quote_ui_amount']
            actual_cols = table.schema.names
            
            missing_cols = [col for col in expected_cols if col not in actual_cols]
            if missing_cols:
                raise Exception(f"Missing expected columns: {missing_cols}")
            
            logger.info("✅ Validation successful - all expected columns present")
            logger.info(f"Schema contains {len(actual_cols)} columns: {actual_cols[:10]}...")
            
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            raise

    def run(self):
        """Execute the column rename process"""
        try:
            self.process_files()
            self.validate_result()
            
            self.stats['end_time'] = datetime.now(timezone.utc)
            self.stats['duration'] = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            
            logger.info("=== COLUMN RENAME COMPLETE ===")
            logger.info(f"Files processed: {self.stats['files_processed']}")
            logger.info(f"Records processed: {self.stats['records_processed']}")
            logger.info(f"Duration: {self.stats['duration']:.1f} seconds")
            
            return self.stats
            
        except Exception as e:
            logger.error(f"Column rename failed: {e}")
            raise


if __name__ == "__main__":
    renamer = ColumnRenameFix()
    stats = renamer.run()
    
    print("\n✅ Column rename completed successfully!")
    print(f"📊 Processed {stats['files_processed']} files with {stats['records_processed']} records")
    print(f"⏱️  Duration: {stats['duration']:.1f} seconds")