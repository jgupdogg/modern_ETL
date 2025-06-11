#!/usr/bin/env python3
"""
DuckDB Initialization Script
Sets up DuckDB with S3/MinIO extensions and basic configuration.
"""

import os
import sys
import logging
import duckdb

def setup_logging():
    """Configure logging for the initialization script."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def initialize_duckdb():
    """Initialize DuckDB with S3/MinIO configuration."""
    logger = setup_logging()
    
    try:
        # Get configuration from environment
        db_path = os.getenv('DUCKDB_DATABASE_PATH', '/data/analytics.duckdb')
        s3_endpoint = os.getenv('DUCKDB_S3_ENDPOINT', 'http://minio:9000')
        s3_access_key = os.getenv('DUCKDB_S3_ACCESS_KEY', 'minioadmin')
        s3_secret_key = os.getenv('DUCKDB_S3_SECRET_KEY', 'minioadmin123')
        
        logger.info(f"Initializing DuckDB at: {db_path}")
        logger.info(f"S3 Endpoint: {s3_endpoint}")
        
        # Ensure data directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Connect to DuckDB
        conn = duckdb.connect(db_path)
        
        # Install and load required extensions
        logger.info("Installing required extensions...")
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        
        # Configure S3/MinIO settings
        logger.info("Configuring S3/MinIO settings...")
        # Remove http:// prefix for DuckDB S3 endpoint
        endpoint_clean = s3_endpoint.replace('http://', '').replace('https://', '')
        conn.execute(f"SET s3_endpoint='{endpoint_clean}';")
        conn.execute(f"SET s3_access_key_id='{s3_access_key}';")
        conn.execute(f"SET s3_secret_access_key='{s3_secret_key}';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute("SET s3_url_style='path';")
        
        # Test basic functionality
        logger.info("Testing DuckDB functionality...")
        result = conn.execute("SELECT 'DuckDB initialized successfully' as status;").fetchone()
        logger.info(f"Test result: {result[0]}")
        
        # Create initial schema if needed
        logger.info("Setting up database schema...")
        conn.execute("""
            CREATE SCHEMA IF NOT EXISTS bronze;
        """)
        
        conn.execute("""
            CREATE SCHEMA IF NOT EXISTS silver;
        """)
        
        # Test S3 connectivity (this will fail if MinIO isn't ready, but that's OK)
        try:
            logger.info("Testing S3/MinIO connectivity...")
            test_query = "SELECT count(*) FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet');"
            result = conn.execute(test_query).fetchone()
            logger.info(f"Found {result[0]} records in bronze data")
        except Exception as e:
            logger.warning(f"S3 connectivity test failed (this is OK if no data exists yet): {e}")
        
        # Close connection
        conn.close()
        
        logger.info("DuckDB initialization completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error initializing DuckDB: {e}")
        sys.exit(1)

if __name__ == "__main__":
    initialize_duckdb()