#!/usr/bin/env python3
"""
Test script to verify PostgreSQL to MinIO migration.
"""

import os
import sys
import logging
import pandas as pd
import psycopg2
import boto3
from botocore.exceptions import ClientError
import pyarrow.parquet as pq
from io import BytesIO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MigrationTester:
    """Tests the migration results."""
    
    def __init__(self):
        """Initialize connections."""
        # PostgreSQL connection (Docker container)
        self.pg_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': 5432,
            'database': 'solana_pipeline',
            'user': 'airflow',
            'password': 'airflow'
        }
        
        # MinIO connection
        self.s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://localhost:9000'),
            aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
            region_name='us-east-1'
        )
        
        self.bucket_name = "solana-data"
        self.bronze_prefix = "bronze/"
        
        self.tables = [
            'token_list_v3',
            'token_whales', 
            'wallet_trade_history',
            'token_metadata'
        ]
    
    def test_connection_postgres(self) -> bool:
        """Test PostgreSQL connection."""
        try:
            with psycopg2.connect(**self.pg_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version()")
                    version = cur.fetchone()[0]
                    logger.info(f"✅ PostgreSQL connected: {version}")
                    return True
        except Exception as e:
            logger.error(f"❌ PostgreSQL connection failed: {e}")
            return False
    
    def test_connection_minio(self) -> bool:
        """Test MinIO connection."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"✅ MinIO connected, bucket {self.bucket_name} exists")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.error(f"❌ Bucket {self.bucket_name} does not exist")
            else:
                logger.error(f"❌ MinIO connection failed: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ MinIO connection failed: {e}")
            return False
    
    def get_postgres_count(self, table_name: str) -> int:
        """Get row count from PostgreSQL."""
        try:
            with psycopg2.connect(**self.pg_config) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM bronze.{table_name}")
                    return cur.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to get PostgreSQL count for {table_name}: {e}")
            return -1
    
    def get_minio_count(self, table_name: str) -> int:
        """Get row count from MinIO Parquet files."""
        try:
            prefix = f"{self.bronze_prefix}{table_name}/"
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                logger.warning(f"No files found for {table_name} in MinIO")
                return 0
            
            total_rows = 0
            for obj in response['Contents']:
                if obj['Key'].endswith('.parquet'):
                    # Download and count rows
                    obj_response = self.s3_client.get_object(
                        Bucket=self.bucket_name,
                        Key=obj['Key']
                    )
                    parquet_data = obj_response['Body'].read()
                    table = pq.read_table(BytesIO(parquet_data))
                    total_rows += len(table)
            
            return total_rows
        except Exception as e:
            logger.error(f"Failed to get MinIO count for {table_name}: {e}")
            return -1
    
    def test_table_migration(self, table_name: str) -> bool:
        """Test migration for a single table."""
        logger.info(f"Testing migration for {table_name}...")
        
        # Get counts
        pg_count = self.get_postgres_count(table_name)
        minio_count = self.get_minio_count(table_name)
        
        if pg_count == -1 or minio_count == -1:
            logger.error(f"❌ Could not get counts for {table_name}")
            return False
        
        logger.info(f"PostgreSQL rows: {pg_count}")
        logger.info(f"MinIO rows: {minio_count}")
        
        if pg_count == minio_count:
            logger.info(f"✅ {table_name}: Row counts match ({pg_count} rows)")
            return True
        else:
            logger.error(f"❌ {table_name}: Row count mismatch - PG:{pg_count}, MinIO:{minio_count}")
            return False
    
    def test_sample_data(self, table_name: str) -> bool:
        """Test sample data integrity."""
        try:
            # Get first few rows from PostgreSQL
            with psycopg2.connect(**self.pg_config) as conn:
                pg_df = pd.read_sql_query(
                    f"SELECT * FROM bronze.{table_name} ORDER BY 1 LIMIT 5",
                    conn
                )
            
            # Get first Parquet file from MinIO
            prefix = f"{self.bronze_prefix}{table_name}/"
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=1
            )
            
            if 'Contents' not in response:
                logger.error(f"No Parquet files found for {table_name}")
                return False
            
            first_file = response['Contents'][0]['Key']
            obj_response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=first_file
            )
            parquet_data = obj_response['Body'].read()
            minio_df = pd.read_parquet(BytesIO(parquet_data)).head(5)
            
            # Compare schemas
            if list(pg_df.columns) == list(minio_df.columns):
                logger.info(f"✅ {table_name}: Column schemas match")
                return True
            else:
                logger.error(f"❌ {table_name}: Schema mismatch")
                logger.error(f"PostgreSQL columns: {list(pg_df.columns)}")
                logger.error(f"MinIO columns: {list(minio_df.columns)}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to test sample data for {table_name}: {e}")
            return False
    
    def list_minio_files(self, table_name: str):
        """List files in MinIO for a table."""
        try:
            prefix = f"{self.bronze_prefix}{table_name}/"
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                logger.info(f"No files found for {table_name}")
                return
            
            logger.info(f"Files for {table_name}:")
            for obj in response['Contents']:
                size_mb = obj['Size'] / (1024 * 1024)
                logger.info(f"  {obj['Key']} ({size_mb:.1f} MB)")
                
        except Exception as e:
            logger.error(f"Failed to list files for {table_name}: {e}")
    
    def run_all_tests(self) -> bool:
        """Run all migration tests."""
        logger.info("Starting migration validation tests...")
        
        # Test connections
        if not self.test_connection_postgres():
            return False
        
        if not self.test_connection_minio():
            return False
        
        # Test each table
        all_passed = True
        for table_name in self.tables:
            logger.info(f"\n--- Testing {table_name} ---")
            
            # Test row counts
            if not self.test_table_migration(table_name):
                all_passed = False
                continue
            
            # Test sample data
            if not self.test_sample_data(table_name):
                all_passed = False
                continue
            
            # List files
            self.list_minio_files(table_name)
        
        if all_passed:
            logger.info("\n✅ All migration tests passed!")
        else:
            logger.error("\n❌ Some migration tests failed!")
        
        return all_passed

def main():
    """Main test function."""
    tester = MigrationTester()
    
    if len(sys.argv) > 1:
        # Test specific table
        table_name = sys.argv[1]
        if table_name not in tester.tables:
            logger.error(f"Invalid table name: {table_name}")
            logger.info(f"Available tables: {tester.tables}")
            sys.exit(1)
        
        success = (
            tester.test_connection_postgres() and
            tester.test_connection_minio() and
            tester.test_table_migration(table_name) and
            tester.test_sample_data(table_name)
        )
    else:
        # Test all tables
        success = tester.run_all_tests()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()