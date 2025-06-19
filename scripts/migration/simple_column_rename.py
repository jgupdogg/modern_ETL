#!/usr/bin/env python3
"""
Simple Column Rename Fix
Just renames the problematic columns without changing the full schema
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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleColumnRename:
    """Simple rename of problematic columns only"""
    
    def __init__(self):
        self.batch_id = f"simple_rename_{int(time.time())}"
        
        # MinIO S3 client
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin123',
            config=Config(signature_version='s3v4')
        )
        
        # Simple column mapping - only rename what needs to be renamed
        self.column_mapping = {
            'transaction_type': 'tx_type',
            'token_a_address': 'base_address',
            'token_b_address': 'quote_address'
        }
        
        self.stats = {'files_processed': 0, 'records_processed': 0}

    def process_files(self):
        """Process all files and rename columns"""
        logger.info("Starting simple column rename process...")
        
        # List all files
        paginator = self.s3_client.get_paginator('list_objects_v2')
        files_to_process = []
        
        for page in paginator.paginate(Bucket='solana-data', Prefix='bronze/wallet_transactions/'):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.parquet'):
                    files_to_process.append(obj['Key'])
        
        logger.info(f"Found {len(files_to_process)} files to process")
        
        # Process each file
        for i, file_key in enumerate(files_to_process):
            try:
                self.process_single_file(file_key)
                self.stats['files_processed'] += 1
                
                if (i + 1) % 100 == 0:
                    logger.info(f"Processed {i + 1}/{len(files_to_process)} files")
                    
            except Exception as e:
                logger.error(f"Failed to process {file_key}: {e}")
                continue
        
        logger.info(f"Simple rename complete: {self.stats['files_processed']} files, {self.stats['records_processed']} records")

    def process_single_file(self, file_key: str):
        """Process a single parquet file"""
        # Download file
        response = self.s3_client.get_object(Bucket='solana-data', Key=file_key)
        file_content = response['Body'].read()
        
        # Read parquet
        table = pq.read_table(pa.BufferReader(file_content))
        df = table.to_pandas()
        
        # Rename columns
        df.rename(columns=self.column_mapping, inplace=True)
        
        # Keep the same schema structure but with renamed columns
        schema = table.schema
        
        # Update schema field names
        new_fields = []
        for field in schema:
            field_name = field.name
            if field_name in self.column_mapping:
                new_field_name = self.column_mapping[field_name]
                new_fields.append(pa.field(new_field_name, field.type, nullable=field.nullable))
            else:
                new_fields.append(field)
        
        new_schema = pa.schema(new_fields)
        new_table = pa.Table.from_pandas(df, schema=new_schema)
        
        # Write back to same location
        buffer = pa.BufferOutputStream()
        pq.write_table(new_table, buffer, compression='snappy')
        
        self.s3_client.put_object(
            Bucket='solana-data',
            Key=file_key,
            Body=buffer.getvalue().to_pybytes()
        )
        
        self.stats['records_processed'] += len(df)

    def validate_result(self):
        """Validate that the column rename worked"""
        logger.info("Validating simple column rename results...")
        
        try:
            # Read a sample file and check that renamed columns exist
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
            
            # Check that renamed columns exist
            expected_cols = ['tx_type', 'base_address', 'quote_address']
            actual_cols = table.schema.names
            
            missing_cols = [col for col in expected_cols if col not in actual_cols]
            if missing_cols:
                raise Exception(f"Missing renamed columns: {missing_cols}")
            
            # Check that old columns are gone
            old_cols = ['transaction_type', 'token_a_address', 'token_b_address']
            present_old_cols = [col for col in old_cols if col in actual_cols]
            if present_old_cols:
                raise Exception(f"Old column names still present: {present_old_cols}")
            
            logger.info("✅ Validation successful - column rename completed")
            logger.info(f"Sample schema: {actual_cols[:10]}...")
            
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            raise

    def run(self):
        """Execute the simple column rename process"""
        try:
            start_time = datetime.now()
            
            self.process_files()
            self.validate_result()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("=== SIMPLE COLUMN RENAME COMPLETE ===")
            logger.info(f"Files processed: {self.stats['files_processed']}")
            logger.info(f"Records processed: {self.stats['records_processed']}")
            logger.info(f"Duration: {duration:.1f} seconds")
            
            return self.stats
            
        except Exception as e:
            logger.error(f"Simple column rename failed: {e}")
            raise


if __name__ == "__main__":
    renamer = SimpleColumnRename()
    stats = renamer.run()
    
    print("\n✅ Simple column rename completed successfully!")
    print(f"📊 Processed {stats['files_processed']} files with {stats['records_processed']} records")