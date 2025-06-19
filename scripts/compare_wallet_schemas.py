#!/usr/bin/env python3
"""
Compare bronze wallet transactions schemas between current and old schema versions.
This script examines parquet files in both directories and provides a detailed comparison.
"""

import duckdb
import boto3
from botocore.client import Config
import json
from datetime import datetime

def setup_duckdb_connection():
    """Setup DuckDB connection with S3/MinIO extensions."""
    conn = duckdb.connect(':memory:')
    
    # Install and load required extensions
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    
    # Configure S3 settings for MinIO
    conn.execute("""
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = 'minioadmin';
        SET s3_secret_access_key = 'minioadmin123';
        SET s3_use_ssl = false;
        SET s3_url_style = 'path';
    """)
    
    return conn

def setup_minio_client():
    """Setup MinIO client for direct bucket access."""
    return boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin123',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def list_parquet_files(s3_client, bucket, prefix):
    """List parquet files in a given S3 prefix."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' not in response:
            return []
        
        parquet_files = []
        for obj in response['Contents']:
            if obj['Key'].endswith('.parquet'):
                parquet_files.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified']
                })
        return sorted(parquet_files, key=lambda x: x['last_modified'], reverse=True)
    except Exception as e:
        print(f"Error listing files in {prefix}: {e}")
        return []

def get_parquet_schema(conn, s3_path):
    """Get schema information from a parquet file."""
    try:
        # Get schema using DESCRIBE
        query = f"DESCRIBE SELECT * FROM read_parquet('{s3_path}') LIMIT 0;"
        schema_result = conn.execute(query).fetchall()
        
        # Get sample data
        sample_query = f"SELECT * FROM read_parquet('{s3_path}') LIMIT 3;"
        sample_result = conn.execute(sample_query).fetchall()
        sample_columns = [desc[0] for desc in conn.description]
        
        return {
            'schema': schema_result,
            'sample_data': sample_result,
            'sample_columns': sample_columns
        }
    except Exception as e:
        print(f"Error reading schema from {s3_path}: {e}")
        return None

def compare_schemas(current_schema, old_schema):
    """Compare two schemas and identify differences."""
    if not current_schema or not old_schema:
        return {"error": "One or both schemas could not be loaded"}
    
    # Handle different formats of schema results from DuckDB DESCRIBE
    current_cols = {}
    for col_info in current_schema['schema']:
        if len(col_info) >= 2:
            current_cols[col_info[0]] = col_info[1]
    
    old_cols = {}
    for col_info in old_schema['schema']:
        if len(col_info) >= 2:
            old_cols[col_info[0]] = col_info[1]
    
    # Find added columns
    added_cols = set(current_cols.keys()) - set(old_cols.keys())
    
    # Find removed columns
    removed_cols = set(old_cols.keys()) - set(current_cols.keys())
    
    # Find common columns
    common_cols = set(current_cols.keys()) & set(old_cols.keys())
    
    # Find type changes
    type_changes = {}
    for col in common_cols:
        if current_cols[col] != old_cols[col]:
            type_changes[col] = {
                'old_type': old_cols[col],
                'new_type': current_cols[col]
            }
    
    return {
        'added_columns': list(added_cols),
        'removed_columns': list(removed_cols),
        'common_columns': list(common_cols),
        'type_changes': type_changes,
        'current_total_cols': len(current_cols),
        'old_total_cols': len(old_cols)
    }

def main():
    print("=== Bronze Wallet Transactions Schema Comparison ===")
    print(f"Analysis started at: {datetime.now().isoformat()}")
    print()
    
    # Setup connections
    conn = setup_duckdb_connection()
    s3_client = setup_minio_client()
    
    # Define paths
    bucket = 'solana-data'
    current_prefix = 'bronze/wallet_transactions/'
    old_prefix = 'bronze/wallet_transactions_old_schema/'
    
    print("1. Listing parquet files in both directories...")
    
    # List files in both directories
    current_files = list_parquet_files(s3_client, bucket, current_prefix)
    old_files = list_parquet_files(s3_client, bucket, old_prefix)
    
    print(f"Current schema directory: {len(current_files)} parquet files")
    print(f"Old schema directory: {len(old_files)} parquet files")
    print()
    
    if not current_files:
        print("❌ No parquet files found in current schema directory")
        return
    
    if not old_files:
        print("❌ No parquet files found in old schema directory")
        return
    
    # Get most recent file from each directory
    current_file = current_files[0]
    old_file = old_files[0]
    
    print(f"Analyzing current schema file: {current_file['key']}")
    print(f"  Size: {current_file['size']:,} bytes")
    print(f"  Last modified: {current_file['last_modified']}")
    print()
    
    print(f"Analyzing old schema file: {old_file['key']}")
    print(f"  Size: {old_file['size']:,} bytes")
    print(f"  Last modified: {old_file['last_modified']}")
    print()
    
    # Get schemas
    print("2. Reading schemas from parquet files...")
    current_s3_path = f's3://{bucket}/{current_file["key"]}'
    old_s3_path = f's3://{bucket}/{old_file["key"]}'
    
    current_schema = get_parquet_schema(conn, current_s3_path)
    old_schema = get_parquet_schema(conn, old_s3_path)
    
    if not current_schema:
        print("❌ Failed to read current schema")
        return
    
    if not old_schema:
        print("❌ Failed to read old schema")
        return
    
    print("✅ Successfully loaded both schemas")
    print()
    
    # Display current schema
    print("3. Current Schema Structure:")
    print("-" * 50)
    for i, col_info in enumerate(current_schema['schema'], 1):
        col_name = col_info[0] if len(col_info) > 0 else "Unknown"
        col_type = col_info[1] if len(col_info) > 1 else "Unknown"
        print(f"{i:2d}. {col_name:<30} {col_type}")
    print()
    
    # Display old schema
    print("4. Old Schema Structure:")
    print("-" * 50)
    for i, col_info in enumerate(old_schema['schema'], 1):
        col_name = col_info[0] if len(col_info) > 0 else "Unknown"
        col_type = col_info[1] if len(col_info) > 1 else "Unknown"
        print(f"{i:2d}. {col_name:<30} {col_type}")
    print()
    
    # Compare schemas
    print("5. Schema Comparison Results:")
    print("=" * 50)
    comparison = compare_schemas(current_schema, old_schema)
    
    print(f"Total columns - Current: {comparison['current_total_cols']}, Old: {comparison['old_total_cols']}")
    print()
    
    if comparison['added_columns']:
        print("✅ Added Columns:")
        for col in sorted(comparison['added_columns']):
            col_type = "Unknown"
            for col_info in current_schema['schema']:
                if len(col_info) >= 2 and col_info[0] == col:
                    col_type = col_info[1]
                    break
            print(f"  + {col:<30} {col_type}")
        print()
    
    if comparison['removed_columns']:
        print("❌ Removed Columns:")
        for col in sorted(comparison['removed_columns']):
            col_type = "Unknown"
            for col_info in old_schema['schema']:
                if len(col_info) >= 2 and col_info[0] == col:
                    col_type = col_info[1]
                    break
            print(f"  - {col:<30} {col_type}")
        print()
    
    if comparison['type_changes']:
        print("🔄 Type Changes:")
        for col, changes in comparison['type_changes'].items():
            print(f"  ~ {col:<30} {changes['old_type']} → {changes['new_type']}")
        print()
    
    if not comparison['added_columns'] and not comparison['removed_columns'] and not comparison['type_changes']:
        print("✅ No schema differences found - schemas are identical")
        print()
    
    # Show sample data for comparison
    print("6. Sample Data Comparison:")
    print("=" * 50)
    
    print("Current Schema Sample (first 3 rows):")
    print("-" * 40)
    if current_schema['sample_data']:
        for i, row in enumerate(current_schema['sample_data']):
            print(f"Row {i+1}:")
            for j, (col, val) in enumerate(zip(current_schema['sample_columns'], row)):
                val_str = str(val)[:50] + "..." if len(str(val)) > 50 else str(val)
                print(f"  {col}: {val_str}")
            print()
    
    print("Old Schema Sample (first 3 rows):")
    print("-" * 40)
    if old_schema['sample_data']:
        for i, row in enumerate(old_schema['sample_data']):
            print(f"Row {i+1}:")
            for j, (col, val) in enumerate(zip(old_schema['sample_columns'], row)):
                val_str = str(val)[:50] + "..." if len(str(val)) > 50 else str(val)
                print(f"  {col}: {val_str}")
            print()
    
    # Summary
    print("7. Summary:")
    print("=" * 50)
    total_changes = len(comparison['added_columns']) + len(comparison['removed_columns']) + len(comparison['type_changes'])
    
    if total_changes == 0:
        print("✅ Schemas are identical - no changes detected")
    else:
        print(f"📊 Total changes detected: {total_changes}")
        print(f"  - Added columns: {len(comparison['added_columns'])}")
        print(f"  - Removed columns: {len(comparison['removed_columns'])}")
        print(f"  - Type changes: {len(comparison['type_changes'])}")
    
    print()
    print(f"Analysis completed at: {datetime.now().isoformat()}")
    
    # Close connection
    conn.close()

if __name__ == "__main__":
    main()