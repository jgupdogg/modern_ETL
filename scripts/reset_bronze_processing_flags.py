#!/usr/bin/env python3
"""
Reset Bronze Transaction Processing Flags
Resets all bronze wallet transactions to unprocessed state for silver layer reprocessing.
"""

import os
import sys
import logging
import tempfile
import shutil
from datetime import datetime
import boto3
from botocore.client import Config
import pandas as pd

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

def get_minio_client():
    """Create MinIO S3 client"""
    return boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin123',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def list_bronze_transaction_files(s3_client, bucket='solana-data'):
    """List all bronze wallet transaction parquet files"""
    prefix = 'bronze/wallet_transactions/'
    
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )
    
    if 'Contents' not in response:
        return []
    
    # Filter for parquet files only
    parquet_files = [
        obj['Key'] for obj in response['Contents'] 
        if obj['Key'].endswith('.parquet')
    ]
    
    return parquet_files

def reset_processing_flags_in_file(s3_client, bucket, file_key, temp_dir):
    """Reset processing flags in a single parquet file"""
    logger = logging.getLogger(__name__)
    
    # Download file
    local_file = os.path.join(temp_dir, os.path.basename(file_key))
    s3_client.download_file(bucket, file_key, local_file)
    
    # Read parquet file
    df = pd.read_parquet(local_file)
    original_count = len(df)
    
    # Check current processing status
    processed_count = df['processed_for_pnl'].sum() if 'processed_for_pnl' in df.columns else 0
    
    logger.info(f"File {os.path.basename(file_key)}: {original_count} records, {processed_count} were processed")
    
    # Reset processing flags
    if 'processed_for_pnl' in df.columns:
        df['processed_for_pnl'] = False
    if 'pnl_processed_at' in df.columns:
        df['pnl_processed_at'] = None
    if 'pnl_processing_status' in df.columns:
        df['pnl_processing_status'] = 'pending'
    
    # Save updated file
    df.to_parquet(local_file, index=False)
    
    # Upload back to MinIO
    s3_client.upload_file(local_file, bucket, file_key)
    
    return {
        'file': os.path.basename(file_key),
        'total_records': original_count,
        'previously_processed': processed_count,
        'reset_to_pending': original_count
    }

def main():
    """Main function to reset all bronze transaction processing flags"""
    logger = setup_logging()
    
    logger.info("🔄 Starting Bronze Transaction Processing Flag Reset")
    logger.info("=" * 60)
    
    try:
        # Create MinIO client
        s3_client = get_minio_client()
        bucket = 'solana-data'
        
        # Test connection
        s3_client.head_bucket(Bucket=bucket)
        logger.info(f"✅ Connected to MinIO bucket: {bucket}")
        
        # List all bronze transaction files
        transaction_files = list_bronze_transaction_files(s3_client, bucket)
        logger.info(f"📁 Found {len(transaction_files)} bronze transaction files")
        
        if not transaction_files:
            logger.warning("⚠️ No bronze transaction files found")
            return True
        
        # Create temporary directory for processing
        with tempfile.TemporaryDirectory() as temp_dir:
            logger.info(f"📂 Using temporary directory: {temp_dir}")
            
            results = []
            total_records = 0
            total_previously_processed = 0
            
            # Process each file
            for i, file_key in enumerate(transaction_files, 1):
                logger.info(f"\n🔄 Processing file {i}/{len(transaction_files)}: {os.path.basename(file_key)}")
                
                try:
                    result = reset_processing_flags_in_file(s3_client, bucket, file_key, temp_dir)
                    results.append(result)
                    total_records += result['total_records']
                    total_previously_processed += result['previously_processed']
                    
                    logger.info(f"   ✅ Reset {result['total_records']} records")
                    
                except Exception as e:
                    logger.error(f"   ❌ Failed to process {file_key}: {e}")
                    results.append({
                        'file': os.path.basename(file_key),
                        'error': str(e)
                    })
        
        # Summary
        logger.info(f"\n{'=' * 60}")
        logger.info("📊 RESET SUMMARY:")
        logger.info(f"   Files processed: {len([r for r in results if 'error' not in r])}/{len(transaction_files)}")
        logger.info(f"   Total records: {total_records}")
        logger.info(f"   Previously processed: {total_previously_processed}")
        logger.info(f"   Now marked as pending: {total_records}")
        
        # Log any errors
        errors = [r for r in results if 'error' in r]
        if errors:
            logger.warning(f"\n⚠️ Errors occurred in {len(errors)} files:")
            for error in errors:
                logger.warning(f"   {error['file']}: {error['error']}")
        
        success_rate = len([r for r in results if 'error' not in r]) / len(transaction_files) * 100
        logger.info(f"\n🎯 Reset completed with {success_rate:.1f}% success rate")
        
        if success_rate > 90:
            logger.info("✅ Bronze transaction processing flags successfully reset!")
            logger.info("💡 Next steps:")
            logger.info("   1. Update silver wallet PnL DAG for proper wallet-level aggregation")
            logger.info("   2. Trigger silver_wallet_pnl DAG to reprocess with corrected logic")
            logger.info("   3. Verify gold layer data for proper bot filtering")
        else:
            logger.warning("⚠️ Reset completed with some errors - manual verification recommended")
        
        return success_rate > 50
        
    except Exception as e:
        logger.error(f"💥 Fatal error during reset: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)