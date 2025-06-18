#!/usr/bin/env python3
"""
Fix Bronze Schema Conflict Script

This script addresses the schema conflict in bronze wallet transactions by:
1. Identifying files with conflicting schemas
2. Moving old processed files to a backup location
3. Keeping only raw transactions with consistent schema

Run this before processing silver layer to avoid schema merge conflicts.
"""

import os
import sys
import logging
from datetime import datetime
import boto3
from botocore.client import Config
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MinIO configuration
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET_NAME = "solana-data"
BRONZE_PREFIX = "bronze/wallet_transactions/"
BACKUP_PREFIX = "bronze/wallet_transactions_old_schema/"

def get_minio_client():
    """Create MinIO S3 client"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4')
    )

def list_bronze_files(s3_client):
    """List all parquet files in bronze wallet_transactions"""
    logger.info("Listing bronze wallet transaction files...")
    
    files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=BRONZE_PREFIX):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                files.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified']
                })
    
    logger.info(f"Found {len(files)} parquet files")
    return files

def check_file_schema(s3_client, file_key):
    """Check the schema of a parquet file"""
    try:
        # Download file
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
        parquet_data = obj['Body'].read()
        
        # Read schema
        table = pq.read_table(BytesIO(parquet_data))
        schema = table.schema
        
        # Check for raw vs processed schema
        column_names = [field.name for field in schema]
        
        has_base_address = 'base_address' in column_names
        has_quote_address = 'quote_address' in column_names
        has_from_address = 'from_address' in column_names
        has_transaction_type = 'transaction_type' in column_names
        
        # Determine schema type
        if has_base_address and has_quote_address:
            schema_type = 'raw'
        elif has_from_address and has_transaction_type:
            schema_type = 'processed'
        else:
            schema_type = 'unknown'
        
        # Check pnl_processed_at type
        pnl_field_type = None
        for field in schema:
            if field.name == 'pnl_processed_at':
                pnl_field_type = str(field.type)
                break
        
        return {
            'schema_type': schema_type,
            'pnl_field_type': pnl_field_type,
            'columns': column_names,
            'row_count': len(table)
        }
        
    except Exception as e:
        logger.error(f"Error checking schema for {file_key}: {e}")
        return {'schema_type': 'error', 'error': str(e)}

def move_file_to_backup(s3_client, source_key):
    """Move a file to backup location"""
    # Create backup key
    backup_key = source_key.replace(BRONZE_PREFIX, BACKUP_PREFIX)
    
    try:
        # Copy to backup location
        copy_source = {'Bucket': BUCKET_NAME, 'Key': source_key}
        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=BUCKET_NAME,
            Key=backup_key
        )
        
        # Delete original
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=source_key)
        
        logger.info(f"Moved {source_key} → {backup_key}")
        return True
        
    except Exception as e:
        logger.error(f"Error moving {source_key}: {e}")
        return False

def main():
    """Main function to fix schema conflicts"""
    logger.info("Starting bronze schema conflict fix...")
    
    # Initialize MinIO client
    try:
        s3_client = get_minio_client()
        logger.info("Connected to MinIO successfully")
    except Exception as e:
        logger.error(f"Failed to connect to MinIO: {e}")
        return False
    
    # List all bronze files
    files = list_bronze_files(s3_client)
    if not files:
        logger.info("No files found in bronze layer")
        return True
    
    # Analyze schemas
    logger.info("Analyzing file schemas...")
    raw_files = []
    processed_files = []
    error_files = []
    
    for file_info in files:
        file_key = file_info['key']
        schema_info = check_file_schema(s3_client, file_key)
        
        file_info.update(schema_info)
        
        if schema_info['schema_type'] == 'raw':
            raw_files.append(file_info)
        elif schema_info['schema_type'] == 'processed':
            processed_files.append(file_info)
        else:
            error_files.append(file_info)
    
    # Report findings
    logger.info(f"Schema analysis complete:")
    logger.info(f"  Raw schema files: {len(raw_files)}")
    logger.info(f"  Processed schema files: {len(processed_files)}")
    logger.info(f"  Error/Unknown files: {len(error_files)}")
    
    # Show file details
    if raw_files:
        logger.info("Raw schema files (keeping):")
        for f in raw_files:
            logger.info(f"  {f['key']} - {f['row_count']} rows")
    
    if processed_files:
        logger.info("Processed schema files (moving to backup):")
        for f in processed_files:
            logger.info(f"  {f['key']} - {f['row_count']} rows")
    
    if error_files:
        logger.info("Error/Unknown files:")
        for f in error_files:
            logger.info(f"  {f['key']} - {f.get('error', 'unknown schema')}")
    
    # Move processed files to backup
    if processed_files:
        logger.info(f"Moving {len(processed_files)} processed schema files to backup...")
        moved_count = 0
        
        for file_info in processed_files:
            if move_file_to_backup(s3_client, file_info['key']):
                moved_count += 1
        
        logger.info(f"Successfully moved {moved_count}/{len(processed_files)} files")
    
    # Final summary
    logger.info("Schema conflict fix completed!")
    logger.info(f"Bronze layer now contains {len(raw_files)} raw schema files")
    logger.info(f"Moved {len(processed_files)} processed schema files to backup")
    
    if len(raw_files) > 0:
        logger.info("✅ Bronze layer is now clean for silver processing")
        return True
    else:
        logger.warning("⚠️ No raw schema files found - may need to run bronze tasks first")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)