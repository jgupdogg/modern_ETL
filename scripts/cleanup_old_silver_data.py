#!/usr/bin/env python3
"""
Clean up old silver layer data with incompatible schema.
Removes files with the old time_period partition structure.
"""

import boto3
from botocore.client import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MinIO configuration
MINIO_ENDPOINT = 'http://localhost:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin123'
MINIO_BUCKET = 'solana-data'

def cleanup_old_silver_data(dry_run=True):
    """Remove old silver layer files with time_period partition"""
    
    # Create MinIO client
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4')
    )
    
    # List all objects in silver/wallet_pnl
    prefix = 'silver/wallet_pnl/'
    
    logger.info(f"Scanning for old silver data in s3://{MINIO_BUCKET}/{prefix}")
    
    # Paginate through all objects
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix)
    
    files_to_delete = []
    
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            
            # Check if this is an old file with time_period partition
            if 'time_period=' in key:
                files_to_delete.append(key)
                logger.info(f"Found old file to delete: {key}")
    
    logger.info(f"\nFound {len(files_to_delete)} files with old schema")
    
    if not dry_run and files_to_delete:
        logger.info("Deleting old files...")
        
        # Delete in batches of 1000 (S3 limit)
        for i in range(0, len(files_to_delete), 1000):
            batch = files_to_delete[i:i+1000]
            
            delete_objects = [{'Key': key} for key in batch]
            
            response = s3_client.delete_objects(
                Bucket=MINIO_BUCKET,
                Delete={'Objects': delete_objects}
            )
            
            deleted = response.get('Deleted', [])
            errors = response.get('Errors', [])
            
            logger.info(f"Deleted {len(deleted)} files in batch")
            
            if errors:
                logger.error(f"Errors deleting files: {errors}")
        
        logger.info("Cleanup completed!")
    else:
        if dry_run:
            logger.info("\n*** DRY RUN MODE - No files were deleted ***")
            logger.info("Run with --execute flag to actually delete files")
        else:
            logger.info("No files to delete")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Clean up old silver layer data')
    parser.add_argument('--execute', action='store_true', 
                       help='Actually delete files (default is dry run)')
    
    args = parser.parse_args()
    
    cleanup_old_silver_data(dry_run=not args.execute)