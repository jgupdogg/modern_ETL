#!/usr/bin/env python3
"""
Top Traders Migration Script
Migrates the gold.top_traders table from PostgreSQL to MinIO gold layer.
"""

import os
import sys
import logging
from datetime import datetime
import pandas as pd
import psycopg2
import boto3
from botocore.exceptions import ClientError
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TopTradersMigrator:
    """Migrates top_traders table from PostgreSQL to MinIO gold layer."""
    
    def __init__(self):
        """Initialize connections to PostgreSQL and MinIO."""
        self.bucket_name = "solana-data"
        self.gold_prefix = "gold/top_traders/"
        
        # PostgreSQL connection
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
    
    def get_top_traders_count(self) -> int:
        """Get total row count for top_traders table."""
        try:
            with psycopg2.connect(**self.pg_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM gold.top_traders")
                    count = cur.fetchone()[0]
                    logger.info(f"Found {count} top traders in PostgreSQL")
                    return count
        except Exception as e:
            logger.error(f"Failed to get top_traders count: {e}")
            return 0
    
    def extract_top_traders(self) -> pd.DataFrame:
        """Extract all top traders from PostgreSQL."""
        try:
            with psycopg2.connect(**self.pg_config) as conn:
                query = """
                SELECT * FROM gold.top_traders
                ORDER BY total_pnl DESC
                """
                df = pd.read_sql_query(query, conn)
                logger.info(f"Extracted {len(df)} top traders from PostgreSQL")
                logger.info(f"Sample data columns: {list(df.columns)}")
                
                if not df.empty:
                    logger.info(f"Top performer: {df.iloc[0]['wallet_address']} with PnL: ${df.iloc[0]['total_pnl']:.2f}")
                
                return df
                
        except Exception as e:
            logger.error(f"Failed to extract top traders: {e}")
            return pd.DataFrame()
    
    def upload_to_gold_layer(self, df: pd.DataFrame) -> bool:
        """Upload DataFrame to MinIO gold layer as unpartitioned parquet."""
        if df.empty:
            logger.warning("No data to upload")
            return False
        
        try:
            # Convert to PyArrow table for better control
            table = pa.Table.from_pandas(df)
            
            # Create parquet buffer
            buffer = BytesIO()
            pq.write_table(table, buffer, compression='snappy')
            buffer.seek(0)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            key = f"{self.gold_prefix}top_traders_{timestamp}.parquet"
            
            # Upload to MinIO
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            logger.info(f"✅ Successfully uploaded {len(df)} top traders to s3://{self.bucket_name}/{key}")
            
            # Create success marker
            success_key = f"{self.gold_prefix}_SUCCESS_{timestamp}"
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=success_key,
                Body=b'',
                ContentType='text/plain'
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload to gold layer: {e}")
            return False
    
    def verify_upload(self) -> bool:
        """Verify the upload was successful by listing gold layer contents."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.gold_prefix
            )
            
            if 'Contents' not in response:
                logger.error("No objects found in gold layer")
                return False
            
            parquet_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
            success_files = [obj for obj in response['Contents'] if obj['Key'].startswith(f"{self.gold_prefix}_SUCCESS")]
            
            logger.info(f"✅ Verification: Found {len(parquet_files)} parquet files and {len(success_files)} success markers")
            
            for obj in parquet_files:
                logger.info(f"  - {obj['Key']} ({obj['Size']} bytes, {obj['LastModified']})")
            
            return len(parquet_files) > 0
            
        except Exception as e:
            logger.error(f"Failed to verify upload: {e}")
            return False
    
    def migrate_top_traders(self) -> bool:
        """Main migration function."""
        logger.info("🚀 Starting top traders migration from PostgreSQL to MinIO gold layer")
        
        # Get count first
        count = self.get_top_traders_count()
        if count == 0:
            logger.warning("No top traders found in PostgreSQL")
            return False
        
        # Extract data
        df = self.extract_top_traders()
        if df.empty:
            logger.error("Failed to extract top traders data")
            return False
        
        # Upload to MinIO
        if not self.upload_to_gold_layer(df):
            logger.error("Failed to upload to MinIO")
            return False
        
        # Verify upload
        if not self.verify_upload():
            logger.error("Upload verification failed")
            return False
        
        logger.info(f"🎉 Successfully migrated {len(df)} top traders to MinIO gold layer")
        logger.info("Top traders are now available at: s3://solana-data/gold/top_traders/")
        
        return True

def main():
    """Main function."""
    migrator = TopTradersMigrator()
    success = migrator.migrate_top_traders()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()