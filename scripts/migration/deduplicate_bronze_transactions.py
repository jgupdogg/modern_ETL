#!/usr/bin/env python3
"""
Deduplicate Bronze Wallet Transactions

Removes duplicate records from bronze/wallet_transactions/ keeping the most recent version
of each wallet_address + transaction_hash combination.

Author: Claude Code
Date: 2025-06-19
"""

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import logging
from datetime import datetime
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BronzeTransactionDeduplicator:
    """Deduplicate bronze wallet transactions"""
    
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin123',
            config=Config(signature_version='s3v4')
        )
        
        self.bucket = 'solana-data'
        self.source_prefix = 'bronze/wallet_transactions/'
        self.backup_prefix = 'bronze/wallet_transactions_with_duplicates/'
        
    def backup_existing_files(self):
        """Backup existing files before deduplication"""
        logger.info("Backing up existing files...")
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        backed_up = 0
        
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.source_prefix):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.parquet') and 'wallet_transactions_' in obj['Key']:
                    # Create backup key
                    backup_key = obj['Key'].replace(self.source_prefix, self.backup_prefix)
                    
                    try:
                        # Copy to backup
                        self.s3_client.copy_object(
                            CopySource={'Bucket': self.bucket, 'Key': obj['Key']},
                            Bucket=self.bucket,
                            Key=backup_key
                        )
                        backed_up += 1
                    except Exception as e:
                        logger.error(f"Error backing up {obj['Key']}: {e}")
        
        logger.info(f"Backed up {backed_up} files")
        return backed_up
    
    def deduplicate_transactions(self):
        """Deduplicate all wallet transactions"""
        logger.info("Starting deduplication process...")
        
        # Read all transaction files
        all_transactions = []
        file_count = 0
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.source_prefix):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.parquet') and 'wallet_transactions_' in obj['Key']:
                    try:
                        # Read file
                        response = self.s3_client.get_object(Bucket=self.bucket, Key=obj['Key'])
                        table = pq.read_table(BytesIO(response['Body'].read()))
                        df = table.to_pandas()
                        
                        # Add source file info
                        df['source_file'] = obj['Key']
                        df['file_modified'] = obj['LastModified']
                        
                        all_transactions.append(df)
                        file_count += 1
                        
                        logger.info(f"Read {len(df)} records from {obj['Key']}")
                        
                    except Exception as e:
                        logger.error(f"Error reading {obj['Key']}: {e}")
        
        if not all_transactions:
            logger.warning("No transactions found to deduplicate")
            return
        
        # Combine all transactions
        combined_df = pd.concat(all_transactions, ignore_index=True)
        logger.info(f"Combined {len(combined_df)} total records from {file_count} files")
        
        # Deduplicate: Keep the record with the latest timestamp for each wallet+transaction
        # Sort by timestamp descending to keep the most recent
        combined_df = combined_df.sort_values(['timestamp', 'file_modified'], ascending=[False, False])
        
        # Drop duplicates keeping the first (most recent) occurrence
        deduped_df = combined_df.drop_duplicates(
            subset=['wallet_address', 'transaction_hash'],
            keep='first'
        )
        
        # Remove the temporary columns
        deduped_df = deduped_df.drop(columns=['source_file', 'file_modified'])
        
        duplicates_removed = len(combined_df) - len(deduped_df)
        logger.info(f"Removed {duplicates_removed} duplicate records")
        logger.info(f"Unique records remaining: {len(deduped_df)}")
        
        # Delete existing files
        logger.info("Removing old files...")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.source_prefix):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.parquet') and 'wallet_transactions_' in obj['Key']:
                    try:
                        self.s3_client.delete_object(Bucket=self.bucket, Key=obj['Key'])
                    except Exception as e:
                        logger.error(f"Error deleting {obj['Key']}: {e}")
        
        # Write deduplicated data back, partitioned by date
        logger.info("Writing deduplicated data...")
        batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        # Group by date
        deduped_df['partition_date'] = pd.to_datetime(deduped_df['timestamp']).dt.date
        
        for partition_date, date_df in deduped_df.groupby('partition_date'):
            # Remove partition_date column
            date_df = date_df.drop(columns=['partition_date'])
            
            # Write to parquet
            buffer = BytesIO()
            date_df.to_parquet(buffer, compression='snappy', index=False)
            buffer.seek(0)
            
            # Upload
            date_str = partition_date.strftime('%Y-%m-%d')
            file_name = f"wallet_transactions_{batch_id}_deduped.parquet"
            s3_key = f"{self.source_prefix}date={date_str}/{file_name}"
            
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=buffer.getvalue()
            )
            
            logger.info(f"Wrote {len(date_df)} records to {s3_key}")
        
        # Write success marker
        success_key = f"{self.source_prefix}_DEDUPLICATION_SUCCESS_{batch_id}"
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=success_key,
            Body=f"Deduplication completed. Removed {duplicates_removed} duplicates from {len(combined_df)} total records.".encode()
        )
        
        return {
            'total_records': len(combined_df),
            'unique_records': len(deduped_df),
            'duplicates_removed': duplicates_removed,
            'batch_id': batch_id
        }


def main():
    """Main entry point"""
    print("Bronze Wallet Transactions Deduplication")
    print("=" * 60)
    
    deduplicator = BronzeTransactionDeduplicator()
    
    # Backup existing files
    print("\nStep 1: Backing up existing files...")
    backed_up = deduplicator.backup_existing_files()
    print(f"Backed up {backed_up} files")
    
    # Deduplicate
    print("\nStep 2: Deduplicating transactions...")
    result = deduplicator.deduplicate_transactions()
    
    if result:
        print("\n" + "=" * 60)
        print("DEDUPLICATION COMPLETE")
        print("=" * 60)
        print(f"Total records processed: {result['total_records']:,}")
        print(f"Unique records kept: {result['unique_records']:,}")
        print(f"Duplicates removed: {result['duplicates_removed']:,}")
        print(f"Batch ID: {result['batch_id']}")
        print("=" * 60)


if __name__ == "__main__":
    main()