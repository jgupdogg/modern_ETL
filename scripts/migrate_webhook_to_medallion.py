#!/usr/bin/env python3
"""
Migration script: Move webhook data from processed-webhooks to proper medallion architecture
Moves data from s3://webhook-data/processed-webhooks/ to s3://webhook-data/bronze/webhooks/
"""

import boto3
from botocore.client import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_webhook_data():
    """
    Migrate webhook data from processed-webhooks to bronze/webhooks
    """
    # MinIO configuration
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin123',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    bucket_name = 'webhook-data'
    source_prefix = 'processed-webhooks/'
    target_prefix = 'bronze/webhooks/'
    
    try:
        # List all objects in the source prefix
        logger.info(f"Listing objects in {bucket_name}/{source_prefix}")
        
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=source_prefix)
        
        objects_to_copy = []
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects_to_copy.append(obj['Key'])
        
        logger.info(f"Found {len(objects_to_copy)} objects to migrate")
        
        if len(objects_to_copy) == 0:
            logger.info("No objects to migrate")
            return
        
        # Copy objects to new location
        copied_count = 0
        for source_key in objects_to_copy:
            # Create target key by replacing prefix
            target_key = source_key.replace(source_prefix, target_prefix, 1)
            
            copy_source = {'Bucket': bucket_name, 'Key': source_key}
            
            try:
                s3_client.copy_object(
                    CopySource=copy_source,
                    Bucket=bucket_name,
                    Key=target_key
                )
                copied_count += 1
                
                if copied_count % 100 == 0:
                    logger.info(f"Copied {copied_count}/{len(objects_to_copy)} objects")
                    
            except Exception as e:
                logger.error(f"Error copying {source_key} to {target_key}: {e}")
        
        logger.info(f"Successfully copied {copied_count} objects to bronze layer")
        
        # Verify the migration
        logger.info("Verifying migration...")
        target_objects = []
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=target_prefix)
        
        for page in page_iterator:
            if 'Contents' in page:
                target_objects.extend([obj['Key'] for obj in page['Contents']])
        
        logger.info(f"Verification: Found {len(target_objects)} objects in bronze layer")
        
        if len(target_objects) == len(objects_to_copy):
            logger.info("✅ Migration completed successfully!")
            logger.info("Old processed-webhooks data is still available for safety")
            logger.info("You can delete it manually after confirming the bronze layer works")
        else:
            logger.warning(f"⚠️ Object count mismatch: source={len(objects_to_copy)}, target={len(target_objects)}")
            
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise

if __name__ == "__main__":
    migrate_webhook_data()