#!/usr/bin/env python3
"""
PostgreSQL to MinIO Migration Script
Migrates bronze schema tables from PostgreSQL to MinIO in Parquet format.
"""

import os
import sys
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd
import psycopg2
import boto3
from botocore.exceptions import ClientError
import pyarrow as pa
import pyarrow.parquet as pq

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('migration.log')
    ]
)
logger = logging.getLogger(__name__)

class PostgresMinioMigrator:
    """Handles migration from PostgreSQL to MinIO."""
    
    def __init__(self):
        """Initialize connections to PostgreSQL and MinIO."""
        self.batch_size = 10000
        self.bucket_name = "solana-data"
        self.bronze_prefix = "bronze/"
        self.status_file = "migration_status.json"
        
        # PostgreSQL connection (Real database)
        self.pg_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'solana_pipeline'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'St0ck!adePG')
        }
        
        # MinIO connection
        self.s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://localhost:9000'),
            aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
            region_name='us-east-1'
        )
        
        # Tables to migrate - bronze, silver, and gold
        self.tables = [
            'token_list_v3',
            'token_whales', 
            'wallet_trade_history',
            'token_metadata',
            'silver.tracked_tokens',
            'silver.wallet_pnl',
            'gold.top_traders'
        ]
        
        # Initialize status tracking
        self.status = self.load_status()
    
    def load_status(self) -> Dict:
        """Load migration status from file."""
        if os.path.exists(self.status_file):
            try:
                with open(self.status_file, 'r') as f:
                    status = json.load(f)
                    # Ensure all current tables are in status
                    for table in self.tables:
                        if table not in status:
                            status[table] = {'completed_batches': 0, 'total_rows': 0, 'status': 'pending'}
                    return status
            except Exception as e:
                logger.warning(f"Could not load status file: {e}")
        
        return {table: {'completed_batches': 0, 'total_rows': 0, 'status': 'pending'} 
                for table in self.tables}
    
    def save_status(self):
        """Save migration status to file."""
        try:
            with open(self.status_file, 'w') as f:
                json.dump(self.status, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save status: {e}")
    
    def setup_minio_bucket(self):
        """Create MinIO bucket if it doesn't exist."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket {self.bucket_name} already exists")
        except ClientError:
            try:
                self.s3_client.create_bucket(Bucket=self.bucket_name)
                logger.info(f"Created bucket {self.bucket_name}")
            except Exception as e:
                logger.error(f"Failed to create bucket: {e}")
                raise
    
    def get_table_count(self, table_name: str) -> int:
        """Get total row count for a table."""
        try:
            with psycopg2.connect(**self.pg_config) as conn:
                with conn.cursor() as cur:
                    # Handle schema.table format
                    if '.' in table_name:
                        schema, table = table_name.split('.')
                        full_table_name = f"{schema}.{table}"
                    else:
                        full_table_name = f"bronze.{table_name}"
                    
                    cur.execute(f"SELECT COUNT(*) FROM {full_table_name}")
                    return cur.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to get count for {table_name}: {e}")
            return 0
    
    def extract_batch(self, table_name: str, offset: int, limit: int) -> Optional[pd.DataFrame]:
        """Extract a batch of data from PostgreSQL."""
        try:
            with psycopg2.connect(**self.pg_config) as conn:
                # Handle schema.table format
                if '.' in table_name:
                    schema, table = table_name.split('.')
                    full_table_name = f"{schema}.{table}"
                else:
                    full_table_name = f"bronze.{table_name}"
                
                query = f"""
                SELECT * FROM {full_table_name}
                ORDER BY 1
                LIMIT {limit} OFFSET {offset}
                """
                df = pd.read_sql_query(query, conn)
                logger.info(f"Extracted {len(df)} rows from {table_name} (offset: {offset})")
                return df if not df.empty else None
                
        except Exception as e:
            logger.error(f"Failed to extract batch from {table_name}: {e}")
            return None
    
    def upload_batch(self, df: pd.DataFrame, table_name: str, batch_num: int) -> bool:
        """Upload DataFrame batch to MinIO as Parquet."""
        try:
            # Convert DataFrame to Parquet bytes
            buffer = df.to_parquet(index=False, compression='snappy')
            
            # Upload to MinIO - handle schema in path
            if '.' in table_name:
                # For schema.table, use schema-table format in path
                clean_name = table_name.replace('.', '-')
            else:
                clean_name = table_name
            
            key = f"{self.bronze_prefix}{clean_name}/part_{batch_num:04d}.parquet"
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=buffer,
                ContentType='application/octet-stream'
            )
            
            logger.info(f"Uploaded {len(df)} rows to s3://{self.bucket_name}/{key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload batch {batch_num} for {table_name}: {e}")
            return False
    
    def migrate_table(self, table_name: str) -> bool:
        """Migrate a single table from PostgreSQL to MinIO."""
        logger.info(f"Starting migration for table: {table_name}")
        
        # Skip if already completed
        if self.status[table_name]['status'] == 'completed':
            logger.info(f"Table {table_name} already migrated")
            return True
        
        # Get total row count
        total_rows = self.get_table_count(table_name)
        if total_rows == 0:
            logger.warning(f"Table {table_name} is empty")
            self.status[table_name]['status'] = 'completed'
            return True
        
        logger.info(f"Table {table_name} has {total_rows} rows")
        self.status[table_name]['total_rows'] = total_rows
        
        # Resume from last completed batch
        start_batch = self.status[table_name]['completed_batches']
        total_batches = (total_rows + self.batch_size - 1) // self.batch_size
        
        logger.info(f"Resuming from batch {start_batch}, total batches: {total_batches}")
        
        # Process batches
        for batch_num in range(start_batch, total_batches):
            offset = batch_num * self.batch_size
            
            # Extract batch
            df = self.extract_batch(table_name, offset, self.batch_size)
            if df is None:
                logger.error(f"Failed to extract batch {batch_num}")
                return False
            
            # Upload batch
            if not self.upload_batch(df, table_name, batch_num):
                logger.error(f"Failed to upload batch {batch_num}")
                return False
            
            # Update status
            self.status[table_name]['completed_batches'] = batch_num + 1
            self.save_status()
            
            logger.info(f"Progress: {batch_num + 1}/{total_batches} batches ({((batch_num + 1) / total_batches * 100):.1f}%)")
        
        # Mark as completed
        self.status[table_name]['status'] = 'completed'
        self.save_status()
        logger.info(f"✅ Migration completed for {table_name}")
        return True
    
    def migrate_all_tables(self) -> bool:
        """Migrate all tables."""
        logger.info("Starting migration of all tables")
        
        # Setup MinIO bucket
        self.setup_minio_bucket()
        
        success = True
        for table_name in self.tables:
            try:
                if not self.migrate_table(table_name):
                    logger.error(f"Failed to migrate {table_name}")
                    success = False
            except Exception as e:
                logger.error(f"Error migrating {table_name}: {e}")
                success = False
        
        if success:
            logger.info("✅ All tables migrated successfully")
        else:
            logger.error("❌ Some tables failed to migrate")
        
        return success

def main():
    """Main migration function."""
    migrator = PostgresMinioMigrator()
    
    if len(sys.argv) > 1:
        # Migrate specific table
        table_name = sys.argv[1]
        if table_name not in migrator.tables:
            logger.error(f"Invalid table name: {table_name}")
            logger.info(f"Available tables: {migrator.tables}")
            sys.exit(1)
        
        success = migrator.migrate_table(table_name)
    else:
        # Migrate all tables
        success = migrator.migrate_all_tables()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()