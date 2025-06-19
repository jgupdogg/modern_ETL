#!/usr/bin/env python3
"""
MinIO Lifecycle Policy Configuration

Sets up automatic lifecycle policies for MinIO buckets to prevent unlimited data accumulation.
Configures different retention periods for Bronze, Silver, and Gold data layers.
"""

import boto3
import json
import logging
import os
from datetime import datetime
from botocore.client import Config
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MinIO Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')

# Lifecycle Configuration (in days)
LIFECYCLE_POLICIES = {
    'webhook-data': {
        'bronze': 30,      # Bronze layer: 30 days
        'silver': 90,      # Silver layer: 90 days  
        'gold': 365        # Gold layer: 1 year
    },
    'solana-data': {
        'bronze': 30,      # Bronze layer: 30 days
        'silver': 180,     # Silver layer: 6 months (more valuable for analysis)
        'gold': 730        # Gold layer: 2 years (final analytics)
    }
}

def get_minio_client():
    """Create MinIO client"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def create_lifecycle_rules(bucket_name, layer_retention_days):
    """
    Create lifecycle rules for a bucket with different retention periods for each layer
    """
    rules = []
    
    for layer, retention_days in layer_retention_days.items():
        rule = {
            'ID': f'{bucket_name}-{layer}-cleanup',
            'Status': 'Enabled',
            'Filter': {
                'Prefix': f'{layer}/'
            },
            'Expiration': {
                'Days': retention_days
            }
        }
        rules.append(rule)
        logger.info(f"Created rule for {bucket_name}/{layer}: {retention_days} days retention")
    
    return {
        'Rules': rules
    }

def set_bucket_lifecycle(s3_client, bucket_name, lifecycle_config):
    """
    Set lifecycle policy for a MinIO bucket
    """
    try:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_config
        )
        logger.info(f"✅ Lifecycle policy set for bucket: {bucket_name}")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            logger.warning(f"⚠️ Bucket {bucket_name} does not exist, skipping lifecycle setup")
            return False
        else:
            logger.error(f"❌ Failed to set lifecycle for {bucket_name}: {e}")
            raise

def get_bucket_lifecycle(s3_client, bucket_name):
    """
    Get current lifecycle policy for a bucket
    """
    try:
        response = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        return response.get('Rules', [])
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
            return []
        else:
            raise

def list_bucket_objects_by_layer(s3_client, bucket_name):
    """
    Get object counts and sizes by layer for reporting
    """
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        
        layer_stats = {}
        
        for layer in ['bronze', 'silver', 'gold']:
            object_count = 0
            total_size = 0
            
            try:
                for page in paginator.paginate(Bucket=bucket_name, Prefix=f'{layer}/'):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            object_count += 1
                            total_size += obj['Size']
                
                layer_stats[layer] = {
                    'object_count': object_count,
                    'total_size_mb': round(total_size / 1024 / 1024, 2),
                    'total_size_bytes': total_size
                }
            except ClientError:
                layer_stats[layer] = {'object_count': 0, 'total_size_mb': 0, 'total_size_bytes': 0}
        
        return layer_stats
        
    except ClientError as e:
        logger.error(f"Failed to list objects for {bucket_name}: {e}")
        return {}

def setup_minio_lifecycle_policies(dry_run=False):
    """
    Set up all MinIO lifecycle policies
    """
    logger.info("🚀 Setting up MinIO lifecycle policies...")
    logger.info(f"Mode: {'DRY RUN' if dry_run else 'EXECUTE'}")
    logger.info(f"Endpoint: {MINIO_ENDPOINT}")
    
    try:
        s3_client = get_minio_client()
        
        # Test connection
        s3_client.list_buckets()
        logger.info("✅ MinIO connection successful")
        
        results = {}
        
        for bucket_name, layer_retention in LIFECYCLE_POLICIES.items():
            logger.info(f"\n📦 Processing bucket: {bucket_name}")
            
            # Get current stats
            stats = list_bucket_objects_by_layer(s3_client, bucket_name)
            logger.info(f"Current bucket stats: {stats}")
            
            # Create lifecycle configuration
            lifecycle_config = create_lifecycle_rules(bucket_name, layer_retention)
            logger.info(f"Lifecycle config: {json.dumps(lifecycle_config, indent=2)}")
            
            if not dry_run:
                # Set lifecycle policy
                success = set_bucket_lifecycle(s3_client, bucket_name, lifecycle_config)
                
                if success:
                    # Verify the policy was set
                    current_rules = get_bucket_lifecycle(s3_client, bucket_name)
                    logger.info(f"✅ Verified lifecycle rules: {len(current_rules)} rules active")
                    
                    results[bucket_name] = {
                        'status': 'success',
                        'rules_count': len(current_rules),
                        'layer_stats': stats,
                        'retention_days': layer_retention
                    }
                else:
                    results[bucket_name] = {
                        'status': 'skipped',
                        'reason': 'bucket_not_found',
                        'retention_days': layer_retention
                    }
            else:
                results[bucket_name] = {
                    'status': 'dry_run',
                    'would_create_rules': len(lifecycle_config['Rules']),
                    'layer_stats': stats,
                    'retention_days': layer_retention
                }
        
        logger.info(f"\n🎉 Lifecycle policy setup completed!")
        logger.info(f"Results: {json.dumps(results, indent=2)}")
        
        return results
        
    except Exception as e:
        logger.error(f"💥 Lifecycle setup failed: {e}")
        raise

def check_minio_lifecycle_status():
    """
    Check current lifecycle policy status for all buckets
    """
    logger.info("📊 Checking current MinIO lifecycle status...")
    
    try:
        s3_client = get_minio_client()
        
        # List all buckets
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        
        status_report = {}
        
        for bucket_name in buckets:
            logger.info(f"\n📦 Checking bucket: {bucket_name}")
            
            # Get lifecycle rules
            rules = get_bucket_lifecycle(s3_client, bucket_name)
            
            # Get bucket stats
            stats = list_bucket_objects_by_layer(s3_client, bucket_name)
            
            status_report[bucket_name] = {
                'lifecycle_rules': len(rules),
                'rules_detail': rules,
                'layer_statistics': stats,
                'total_objects': sum(layer.get('object_count', 0) for layer in stats.values()),
                'total_size_mb': sum(layer.get('total_size_mb', 0) for layer in stats.values())
            }
            
            logger.info(f"  Lifecycle rules: {len(rules)}")
            logger.info(f"  Total objects: {status_report[bucket_name]['total_objects']}")
            logger.info(f"  Total size: {status_report[bucket_name]['total_size_mb']:.2f} MB")
        
        logger.info(f"\n📋 Complete status report:")
        logger.info(json.dumps(status_report, indent=2, default=str))
        
        return status_report
        
    except Exception as e:
        logger.error(f"💥 Status check failed: {e}")
        raise

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Configure MinIO lifecycle policies')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without applying')
    parser.add_argument('--status', action='store_true', help='Check current lifecycle status')
    parser.add_argument('--endpoint', default=MINIO_ENDPOINT, help='MinIO endpoint URL')
    parser.add_argument('--access-key', default=MINIO_ACCESS_KEY, help='MinIO access key')
    parser.add_argument('--secret-key', default=MINIO_SECRET_KEY, help='MinIO secret key')
    
    args = parser.parse_args()
    
    # Override configuration with command line arguments
    global MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
    MINIO_ENDPOINT = args.endpoint
    MINIO_ACCESS_KEY = args.access_key
    MINIO_SECRET_KEY = args.secret_key
    
    print("🗄️ MinIO Lifecycle Policy Manager")
    print("=" * 50)
    print(f"Endpoint: {MINIO_ENDPOINT}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    try:
        if args.status:
            check_minio_lifecycle_status()
        else:
            setup_minio_lifecycle_policies(dry_run=args.dry_run)
            
            if args.dry_run:
                print(f"\n💡 To apply the lifecycle policies, run:")
                print(f"   python {__file__} --endpoint {MINIO_ENDPOINT}")
        
    except Exception as e:
        print(f"\n💥 Operation failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())