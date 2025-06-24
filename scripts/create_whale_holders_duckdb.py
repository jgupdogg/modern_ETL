#!/usr/bin/env python3
"""
Create Whale Holders Table using DuckDB
Creates the enhanced whale holders table structure using DuckDB
"""

import sys
import logging
from datetime import datetime, date

def create_whale_holders_table():
    """Create enhanced whale holders table using DuckDB"""
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
        
        # Create enhanced whale holders table schema (16 columns)
        conn.execute("""
        CREATE TABLE whale_holders (
            -- Core identity
            token_address VARCHAR NOT NULL,
            wallet_address VARCHAR NOT NULL,
            rank INTEGER NOT NULL,
            
            -- Holdings data
            holdings_token_amount DOUBLE NOT NULL,
            holdings_value_usd DOUBLE NOT NULL,
            percentage_of_supply DOUBLE,
            
            -- Enhanced analytics (NEW)
            holdings_tier VARCHAR CHECK (holdings_tier IN ('MEGA_WHALE', 'LARGE_WHALE', 'MEDIUM_WHALE', 'SMALL_WHALE')),
            risk_score DOUBLE CHECK (risk_score >= 0 AND risk_score <= 1),
            concentration_risk DOUBLE,
            wallet_diversity_score DOUBLE,
            
            -- Data quality tracking (NEW)
            data_completeness_score DOUBLE CHECK (data_completeness_score >= 0 AND data_completeness_score <= 1),
            has_valid_holdings BOOLEAN DEFAULT FALSE,
            
            -- Processing metadata
            rank_date DATE NOT NULL,
            fetched_at TIMESTAMP NOT NULL,
            batch_id VARCHAR NOT NULL,
            
            -- Partitioning columns
            rank_year INTEGER NOT NULL,
            rank_month INTEGER NOT NULL
        )
        """)
        
        logger.info("✅ Enhanced whale holders table schema created (16 columns)")
        
        # Insert sample whale holder data
        sample_data_sql = """
        INSERT INTO whale_holders VALUES
        (
            'So11111111111111111111111111111111111111112',  -- SOL token_address
            '9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM',  -- wallet_address 
            1,  -- rank
            15423.45,  -- holdings_token_amount
            2245672.50,  -- holdings_value_usd (15423 * $145.50)
            0.0032,  -- percentage_of_supply (0.32%)
            'MEGA_WHALE',  -- holdings_tier
            0.85,  -- risk_score (high concentration)
            0.78,  -- concentration_risk
            0.65,  -- wallet_diversity_score
            1.0,  -- data_completeness_score (perfect)
            true,  -- has_valid_holdings
            CURRENT_DATE,  -- rank_date
            CURRENT_TIMESTAMP,  -- fetched_at
            'whale_batch_20250621',  -- batch_id
            2025,  -- rank_year
            6  -- rank_month
        ),
        (
            'So11111111111111111111111111111111111111112',  -- SOL
            'HN7cABqLq46Es1jh92dQQisAq662SmxELLLsHHe4YWrH',  -- wallet_address
            2,  -- rank
            8756.23,  -- holdings_token_amount
            1274032.47,  -- holdings_value_usd
            0.0018,  -- percentage_of_supply
            'LARGE_WHALE',  -- holdings_tier
            0.72,  -- risk_score
            0.65,  -- concentration_risk
            0.80,  -- wallet_diversity_score (more diversified)
            0.95,  -- data_completeness_score
            true,  -- has_valid_holdings
            CURRENT_DATE,
            CURRENT_TIMESTAMP,
            'whale_batch_20250621',
            2025,
            6
        ),
        (
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',  -- USDC
            'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',  -- wallet_address
            1,  -- rank
            5250000.0,  -- holdings_token_amount (5.25M USDC)
            5250000.0,  -- holdings_value_usd ($1 = 1 USDC)
            0.0105,  -- percentage_of_supply (1.05%)
            'MEGA_WHALE',  -- holdings_tier
            0.92,  -- risk_score (very high concentration)
            0.88,  -- concentration_risk
            0.45,  -- wallet_diversity_score (less diversified)
            1.0,  -- data_completeness_score
            true,  -- has_valid_holdings
            CURRENT_DATE,
            CURRENT_TIMESTAMP,
            'whale_batch_20250621',
            2025,
            6
        ),
        (
            '85VBFQZC9TZkfaptBWjvUw7YbZjy52A6mjtPGjstQAmQ',  -- W token
            'Bb9bsTQa1bGEtQ5KagGkvSHyuLqDWumFUcRqFusFNqLe',  -- wallet_address
            1,  -- rank
            1750000.0,  -- holdings_token_amount
            1137500.0,  -- holdings_value_usd (1.75M * $0.65)
            0.0875,  -- percentage_of_supply (8.75% - high concentration)
            'MEGA_WHALE',  -- holdings_tier
            0.95,  -- risk_score (extremely high concentration)
            0.92,  -- concentration_risk
            0.35,  -- wallet_diversity_score (concentrated in one token)
            0.88,  -- data_completeness_score (some missing data)
            true,  -- has_valid_holdings
            CURRENT_DATE,
            CURRENT_TIMESTAMP,
            'whale_batch_20250621',
            2025,
            6
        )
        """
        
        conn.execute(sample_data_sql)
        logger.info("✅ Sample whale holder data inserted")
        
        # Verify data
        result = conn.execute("SELECT COUNT(*) FROM whale_holders").fetchone()
        logger.info(f"✅ Table contains {result[0]} whale holder records")
        
        # Show sample data with analytics
        logger.info("📋 Enhanced whale holder data:")
        sample = conn.execute("""
        SELECT 
            SUBSTR(wallet_address, 1, 8) || '...' as wallet,
            CASE 
                WHEN token_address = 'So11111111111111111111111111111111111111112' THEN 'SOL'
                WHEN token_address = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' THEN 'USDC'
                ELSE 'W'
            END as token,
            rank,
            holdings_tier,
            holdings_value_usd,
            risk_score,
            data_completeness_score
        FROM whale_holders
        ORDER BY holdings_value_usd DESC
        """).fetchall()
        
        for row in sample:
            logger.info(f"  {row[1]} #{row[2]}: {row[0]} - {row[3]} whale, ${row[4]:,.0f}, risk={row[5]:.2f}, quality={row[6]:.2f}")
        
        # Analytics summary
        logger.info("📊 Whale holder analytics:")
        analytics = conn.execute("""
        SELECT 
            holdings_tier,
            COUNT(*) as whale_count,
            AVG(holdings_value_usd) as avg_value,
            AVG(risk_score) as avg_risk,
            AVG(data_completeness_score) as avg_quality
        FROM whale_holders
        GROUP BY holdings_tier
        ORDER BY avg_value DESC
        """).fetchall()
        
        for row in analytics:
            logger.info(f"  {row[0]}: {row[1]} whales, ${row[2]:,.0f} avg, {row[3]:.2f} risk, {row[4]:.2f} quality")
        
        # Export to smart-trader bucket
        output_path = "s3://smart-trader/bronze/whale_holders/whale_batch_20250621.parquet"
        
        export_sql = f"""
        COPY whale_holders TO '{output_path}' 
        (FORMAT PARQUET, OVERWRITE_OR_IGNORE)
        """
        
        conn.execute(export_sql)
        logger.info(f"✅ Whale holders table exported to: {output_path}")
        
        # Verify export
        verify_sql = f"""
        SELECT COUNT(*) FROM parquet_scan('{output_path}')
        """
        verify_result = conn.execute(verify_sql).fetchone()
        logger.info(f"✅ Export verified: {verify_result[0]} records read back")
        
        # Show enhanced features summary
        enhanced_features = [
            ("Holdings Tiers", "MEGA_WHALE, LARGE_WHALE, MEDIUM_WHALE, SMALL_WHALE"),
            ("Risk Analytics", "risk_score, concentration_risk, wallet_diversity_score"),
            ("Data Quality", "data_completeness_score, has_valid_holdings"),
            ("Enhanced Partitioning", "rank_year, rank_month for efficient queries"),
            ("Comprehensive Metadata", "rank_date, fetched_at, batch_id tracking")
        ]
        
        logger.info("🚀 Enhanced whale holder features:")
        for feature, description in enhanced_features:
            logger.info(f"  ✅ {feature}: {description}")
        
        conn.close()
        logger.info("🎉 Enhanced whale holders table creation SUCCESSFUL!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Whale holders table creation failed: {e}")
        return False

if __name__ == "__main__":
    success = create_whale_holders_table()
    sys.exit(0 if success else 1)