#!/usr/bin/env python3
"""
Test script for bronze layer ingestion components
"""
import os
import sys
import json
import boto3
from botocore.client import Config
import requests
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

def test_api_key():
    """Test if BirdEye API key is available"""
    api_key = os.environ.get('BIRDSEYE_API_KEY')
    if api_key:
        print(f"✓ API key found: {api_key[:10]}...")
        return True
    else:
        print("✗ API key not found in environment")
        return False

def test_birdeye_api():
    """Test BirdEye API connection"""
    api_key = os.environ.get('BIRDSEYE_API_KEY')
    if not api_key:
        print("✗ Cannot test API - no key found")
        return False
    
    headers = {
        "X-API-KEY": api_key,
        "Accept": "application/json"
    }
    
    params = {
        "sort_by": "liquidity",
        "sort_type": "desc",
        "offset": 0,
        "limit": 1,
        "min_liquidity": 200000,
        "max_liquidity": 1000000
    }
    
    try:
        response = requests.get(
            "https://public-api.birdeye.so/defi/v3/token/list",
            headers=headers,
            params=params,
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        
        if "data" in data and "items" in data["data"]:
            print(f"✓ API test successful - found {len(data['data']['items'])} tokens")
            if data['data']['items']:
                print(f"  Sample token: {data['data']['items'][0].get('symbol', 'N/A')}")
            return True
        else:
            print(f"✗ Unexpected API response structure: {data}")
            return False
            
    except Exception as e:
        print(f"✗ API test failed: {e}")
        return False

def test_minio_connection():
    """Test MinIO connection and bucket access"""
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',  # Using localhost for script test
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin123',
            config=Config(signature_version='s3v4')
        )
        
        # List buckets
        buckets = s3_client.list_buckets()
        print(f"✓ MinIO connection successful - found {len(buckets['Buckets'])} buckets")
        
        # Check if solana-data bucket exists
        bucket_names = [b['Name'] for b in buckets['Buckets']]
        if 'solana-data' in bucket_names:
            print("✓ solana-data bucket exists")
            
            # Try to list existing bronze data
            try:
                response = s3_client.list_objects_v2(
                    Bucket='solana-data',
                    Prefix='bronze/token_list_v3/',
                    MaxKeys=5
                )
                if 'Contents' in response:
                    print(f"✓ Found {len(response['Contents'])} existing files in bronze/token_list_v3/")
                    for obj in response['Contents'][:3]:
                        print(f"  - {obj['Key']} ({obj['Size']} bytes)")
                else:
                    print("✓ bronze/token_list_v3/ directory is empty (ready for new data)")
            except Exception as e:
                print(f"  Note: {e}")
                
            return True
        else:
            print(f"✗ solana-data bucket not found. Available buckets: {bucket_names}")
            return False
            
    except Exception as e:
        print(f"✗ MinIO connection failed: {e}")
        return False

def test_parquet_write():
    """Test writing a sample parquet file to MinIO"""
    try:
        # Create sample data
        sample_data = {
            'token_address': ['ABC123', 'DEF456'],
            'symbol': ['TEST1', 'TEST2'],
            'name': ['Test Token 1', 'Test Token 2'],
            'liquidity': [250000.0, 500000.0],
            'price': [0.001, 0.002],
            'ingested_at': [pd.Timestamp.utcnow(), pd.Timestamp.utcnow()],
            'batch_id': ['test_batch', 'test_batch']
        }
        
        df = pd.DataFrame(sample_data)
        
        # Create PyArrow table
        table = pa.Table.from_pandas(df)
        
        # Write to buffer
        buffer = BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)
        
        print(f"✓ Created test parquet data ({buffer.getbuffer().nbytes} bytes)")
        
        # Try to write to MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin123',
            config=Config(signature_version='s3v4')
        )
        
        test_key = f"bronze/token_list_v3/test/test_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"
        
        s3_client.put_object(
            Bucket='solana-data',
            Key=test_key,
            Body=buffer.getvalue()
        )
        
        print(f"✓ Successfully wrote test file to s3://solana-data/{test_key}")
        
        # Clean up test file
        s3_client.delete_object(Bucket='solana-data', Key=test_key)
        print("✓ Cleaned up test file")
        
        return True
        
    except Exception as e:
        print(f"✗ Parquet write test failed: {e}")
        return False

def check_recent_data():
    """Check the most recent data in the bronze layer"""
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin123',
            config=Config(signature_version='s3v4')
        )
        
        # List all objects in token_list_v3
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket='solana-data',
            Prefix='bronze/token_list_v3/'
        )
        
        all_objects = []
        for page in pages:
            if 'Contents' in page:
                all_objects.extend(page['Contents'])
        
        if not all_objects:
            print("No data found in bronze/token_list_v3/")
            return
        
        # Sort by LastModified to get the most recent
        all_objects.sort(key=lambda x: x['LastModified'], reverse=True)
        
        print(f"\nFound {len(all_objects)} files in bronze/token_list_v3/")
        print("\nMost recent files:")
        for obj in all_objects[:5]:
            print(f"  - {obj['Key']}")
            print(f"    Modified: {obj['LastModified']}")
            print(f"    Size: {obj['Size']:,} bytes")
        
        # Read the most recent parquet file
        if all_objects and all_objects[0]['Key'].endswith('.parquet'):
            print(f"\nReading most recent file: {all_objects[0]['Key']}")
            
            response = s3_client.get_object(
                Bucket='solana-data',
                Key=all_objects[0]['Key']
            )
            
            # Read parquet from bytes
            parquet_data = response['Body'].read()
            df = pd.read_parquet(BytesIO(parquet_data))
            
            print(f"✓ Successfully read parquet file with {len(df)} rows")
            print(f"\nColumns: {list(df.columns)}")
            
            if 'ingested_at' in df.columns:
                print(f"\nIngestion timestamps:")
                print(f"  - Min: {df['ingested_at'].min()}")
                print(f"  - Max: {df['ingested_at'].max()}")
                print(f"  - Current time: {pd.Timestamp.utcnow()}")
            
            print(f"\nSample data (first 3 rows):")
            print(df[['token_address', 'symbol', 'liquidity', 'price']].head(3))
            
    except Exception as e:
        print(f"Error checking recent data: {e}")

if __name__ == "__main__":
    print("Testing Bronze Layer Ingestion Components")
    print("=" * 50)
    
    # Run tests
    api_key_ok = test_api_key()
    print()
    
    if api_key_ok:
        api_ok = test_birdeye_api()
        print()
    
    minio_ok = test_minio_connection()
    print()
    
    if minio_ok:
        parquet_ok = test_parquet_write()
        print()
    
    print("\nChecking existing bronze layer data...")
    print("-" * 50)
    check_recent_data()