#!/usr/bin/env python3
import boto3

s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin123',
    region_name='us-east-1'
)

bucket = 'solana-data'
prefixes = [
    'bronze/wallet_trade_history_public/',
    'bronze/wallet_trade_history_bronze/',
    'bronze/wallet_trade_history_bronze_fixed/',
    'bronze/wallet_transactions/'
]

print("Checking all migration data paths...")

for prefix in prefixes:
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)
        if 'Contents' in response:
            print(f"✅ {prefix} - {len(response['Contents'])} files (showing first few)")
        else:
            print(f"❌ {prefix} - No files found")
    except Exception as e:
        print(f"❌ {prefix} - Error: {e}")