#!/usr/bin/env python3
"""
Create Token Metrics Table using DuckDB
Creates the enhanced token metrics table structure using DuckDB (which we know works)
"""

import sys
import logging
from datetime import datetime, date
import tempfile
import os

def test_duckdb_table_creation():
    """Create token metrics table using DuckDB"""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        import duckdb
        import boto3
        from botocore.client import Config
        
        logger.info("✅ DuckDB and boto3 imports successful")
        
        # Setup DuckDB with S3
        conn = duckdb.connect(':memory:')
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("SET s3_endpoint='minio:9000';")
        conn.execute("SET s3_access_key_id='minioadmin';")
        conn.execute("SET s3_secret_access_key='minioadmin123';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute("SET s3_url_style='path';")
        
        logger.info("✅ DuckDB S3 configuration complete")
        
        # Create enhanced token metrics table
        conn.execute("""
        CREATE TABLE token_metrics (
            -- Core identity
            token_address VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            name VARCHAR,
            
            -- Market data (cleaned)
            liquidity DOUBLE NOT NULL,
            price DOUBLE NOT NULL,
            market_cap DOUBLE,
            fdv DOUBLE,
            volume_24h_usd DOUBLE,
            
            -- Volume metrics (cleaned from NaN)
            volume_1h_usd DOUBLE DEFAULT 0.0,
            volume_6h_usd DOUBLE DEFAULT 0.0,
            volume_12h_usd DOUBLE DEFAULT 0.0,
            
            -- Price changes
            price_change_1h_percent DOUBLE,
            price_change_6h_percent DOUBLE,
            price_change_12h_percent DOUBLE,
            price_change_24h_percent DOUBLE,
            
            -- Trading activity
            trade_1h_count BIGINT DEFAULT 0,
            trade_6h_count BIGINT DEFAULT 0,
            trade_12h_count BIGINT DEFAULT 0,
            trade_24h_count BIGINT DEFAULT 0,
            
            -- Quality flags (NEW - Enhanced)
            data_quality_score DOUBLE CHECK (data_quality_score >= 0 AND data_quality_score <= 1),
            has_complete_metrics BOOLEAN DEFAULT FALSE,
            volume_metrics_available BOOLEAN DEFAULT FALSE,
            price_change_available BOOLEAN DEFAULT FALSE,
            
            -- Processing metadata
            ingestion_date DATE NOT NULL,
            ingested_at TIMESTAMP NOT NULL,
            batch_id VARCHAR NOT NULL
        )
        """)
        
        logger.info("✅ Token metrics table schema created")
        
        # Insert sample data
        sample_data_sql = """
        INSERT INTO token_metrics VALUES
        (
            '85VBFQZC9TZkfaptBWjvUw7YbZjy52A6mjtPGjstQAmQ',  -- token_address
            'W',  -- symbol
            'Wrapped Token',  -- name
            380000.0,  -- liquidity
            0.65,  -- price
            1200000.0,  -- market_cap
            1500000.0,  -- fdv
            95000.0,  -- volume_24h_usd
            12000.0,  -- volume_1h_usd
            45000.0,  -- volume_6h_usd
            78000.0,  -- volume_12h_usd
            -2.5,  -- price_change_1h_percent
            1.8,  -- price_change_6h_percent
            4.2,  -- price_change_12h_percent
            -1.1,  -- price_change_24h_percent
            25,  -- trade_1h_count
            128,  -- trade_6h_count
            245,  -- trade_12h_count
            456,  -- trade_24h_count
            0.95,  -- data_quality_score
            true,  -- has_complete_metrics
            true,  -- volume_metrics_available
            true,  -- price_change_available
            CURRENT_DATE,  -- ingestion_date
            CURRENT_TIMESTAMP,  -- ingested_at
            'test_batch_duckdb_20250621'  -- batch_id
        ),
        (
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',  -- USDC
            'USDC',
            'USD Coin',
            2500000.0,
            1.0,
            NULL,  -- market_cap missing
            NULL,  -- fdv missing
            250000.0,
            0.0,  -- volume_1h_usd (cleaned from NULL)
            0.0,  -- volume_6h_usd (cleaned from NULL)
            180000.0,
            0.0,
            0.1,
            -0.05,
            0.02,
            45,
            0,  -- trade_6h_count (cleaned from NULL)
            289,
            612,
            0.65,  -- medium quality
            false,
            false,  -- some volume metrics missing
            true,
            CURRENT_DATE,
            CURRENT_TIMESTAMP,
            'test_batch_duckdb_20250621'
        ),
        (
            'So11111111111111111111111111111111111111112',  -- SOL
            'SOL',
            'Solana',
            5000000.0,
            145.50,
            68000000000.0,
            85000000000.0,
            1250000.0,
            89000.0,
            425000.0,
            890000.0,
            3.2,
            -1.8,
            8.5,
            5.2,
            156,
            678,
            1234,
            2567,
            1.0,  -- perfect quality
            true,
            true,
            true,
            CURRENT_DATE,
            CURRENT_TIMESTAMP,
            'test_batch_duckdb_20250621'
        )
        """
        
        conn.execute(sample_data_sql)
        logger.info("✅ Sample data inserted")
        
        # Verify data
        result = conn.execute("SELECT COUNT(*) FROM token_metrics").fetchone()
        logger.info(f"✅ Table contains {result[0]} records")
        
        # Show sample data
        logger.info("📋 Sample data from table:")
        sample = conn.execute("""
        SELECT token_address, symbol, liquidity, data_quality_score, 
               has_complete_metrics, ingestion_date
        FROM token_metrics
        ORDER BY data_quality_score DESC
        """).fetchall()
        
        for row in sample:
            logger.info(f"  {row[1]}: ${row[2]:,.0f} liquidity, {row[3]:.2f} quality, complete={row[4]}")
        
        # Export to smart-trader bucket
        output_path = "s3://smart-trader/bronze/token_metrics/test_batch_duckdb.parquet"
        
        export_sql = f"""
        COPY token_metrics TO '{output_path}' 
        (FORMAT PARQUET, OVERWRITE_OR_IGNORE)
        """
        
        conn.execute(export_sql)
        logger.info(f"✅ Table exported to: {output_path}")
        
        # Verify export by reading back
        verify_sql = f"""
        SELECT COUNT(*) FROM parquet_scan('{output_path}')
        """
        verify_result = conn.execute(verify_sql).fetchone()
        logger.info(f"✅ Export verified: {verify_result[0]} records read back")
        
        # Show enhanced schema info
        schema_info = conn.execute("DESCRIBE token_metrics").fetchall()
        logger.info(f"📊 Table schema: {len(schema_info)} columns")
        
        enhanced_features = [
            ("Data Quality Tracking", "data_quality_score, has_complete_metrics"),
            ("Volume Completeness", "volume_metrics_available"),
            ("Price Change Tracking", "price_change_available"),
            ("Clean NaN Handling", "DEFAULT 0.0 for volume metrics"),
            ("Comprehensive Metadata", "ingestion_date, batch_id, ingested_at")
        ]
        
        logger.info("🚀 Enhanced features implemented:")
        for feature, description in enhanced_features:
            logger.info(f"  ✅ {feature}: {description}")
        
        conn.close()
        logger.info("🎉 DuckDB token metrics table creation SUCCESSFUL!")
        return True
        
    except Exception as e:
        logger.error(f"❌ DuckDB table creation failed: {e}")
        return False

if __name__ == "__main__":
    success = test_duckdb_table_creation()
    sys.exit(0 if success else 1)