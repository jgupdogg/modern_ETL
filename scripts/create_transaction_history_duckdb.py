#!/usr/bin/env python3
"""
Create Transaction History Table using DuckDB
Creates the enhanced transaction history table structure using DuckDB
"""

import sys
import logging
from datetime import datetime, date, timedelta

def create_transaction_history_table():
    """Create enhanced transaction history table using DuckDB"""
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
        
        # Create enhanced transaction history table schema (32 columns)
        conn.execute("""
        CREATE TABLE transaction_history (
            -- Core transaction identity
            wallet_address VARCHAR NOT NULL,
            transaction_hash VARCHAR NOT NULL,
            transaction_timestamp TIMESTAMP NOT NULL,
            transaction_type VARCHAR CHECK (transaction_type IN ('BUY', 'SELL', 'UNKNOWN')),
            
            -- Token details
            from_token_address VARCHAR,
            from_token_symbol VARCHAR,
            from_token_amount DOUBLE,
            from_token_price_usd DOUBLE,
            
            to_token_address VARCHAR,
            to_token_symbol VARCHAR, 
            to_token_amount DOUBLE,
            to_token_price_usd DOUBLE,
            
            -- Transaction value metrics
            total_value_usd DOUBLE,
            fee_usd DOUBLE DEFAULT 0.0,
            net_value_usd DOUBLE,
            
            -- Enhanced analytics (NEW)
            trade_size_tier VARCHAR CHECK (trade_size_tier IN ('WHALE', 'LARGE', 'MEDIUM', 'SMALL', 'MICRO')),
            price_impact_percent DOUBLE,
            slippage_percent DOUBLE,
            trade_efficiency_score DOUBLE CHECK (trade_efficiency_score >= 0 AND trade_efficiency_score <= 1),
            
            -- Market context (NEW)
            market_volatility_score DOUBLE,
            timing_score DOUBLE CHECK (timing_score >= 0 AND timing_score <= 1),
            is_arbitrage_opportunity BOOLEAN DEFAULT FALSE,
            
            -- PnL processing flags
            processed_for_pnl BOOLEAN DEFAULT FALSE,
            pnl_processed_at TIMESTAMP,
            pnl_processing_batch_id VARCHAR,
            pnl_processing_status VARCHAR CHECK (pnl_processing_status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED')),
            
            -- Data quality tracking
            data_completeness_score DOUBLE CHECK (data_completeness_score >= 0 AND data_completeness_score <= 1),
            has_valid_amounts BOOLEAN DEFAULT FALSE,
            has_valid_prices BOOLEAN DEFAULT FALSE,
            
            -- Processing metadata
            fetched_at TIMESTAMP NOT NULL,
            batch_id VARCHAR NOT NULL,
            
            -- Partitioning columns
            transaction_year INTEGER NOT NULL,
            transaction_month INTEGER NOT NULL
        )
        """)
        
        logger.info("✅ Enhanced transaction history table schema created (32 columns)")
        
        # Insert sample transaction data
        base_time = datetime.now() - timedelta(days=7)
        
        sample_data_sql = f"""
        INSERT INTO transaction_history VALUES
        (
            '9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM',  -- wallet_address (SOL whale)
            'TxHash001ABC123DEF456GHI789JKL012MNO345PQR678',  -- transaction_hash
            '{base_time.isoformat()}',  -- transaction_timestamp
            'BUY',  -- transaction_type
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',  -- from_token_address (USDC)
            'USDC',  -- from_token_symbol
            145500.0,  -- from_token_amount (145.5k USDC)
            1.0,  -- from_token_price_usd
            'So11111111111111111111111111111111111111112',  -- to_token_address (SOL)
            'SOL',  -- to_token_symbol
            1000.0,  -- to_token_amount (1k SOL)
            145.50,  -- to_token_price_usd
            145500.0,  -- total_value_usd
            72.75,  -- fee_usd (0.05% fee)
            145427.25,  -- net_value_usd
            'WHALE',  -- trade_size_tier
            0.15,  -- price_impact_percent
            0.08,  -- slippage_percent
            0.92,  -- trade_efficiency_score (good execution)
            0.35,  -- market_volatility_score (moderate volatility)
            0.88,  -- timing_score (good timing)
            false,  -- is_arbitrage_opportunity
            false,  -- processed_for_pnl
            NULL,  -- pnl_processed_at
            NULL,  -- pnl_processing_batch_id
            'PENDING',  -- pnl_processing_status
            1.0,  -- data_completeness_score (perfect data)
            true,  -- has_valid_amounts
            true,  -- has_valid_prices
            CURRENT_TIMESTAMP,  -- fetched_at
            'tx_batch_20250621',  -- batch_id
            {base_time.year},  -- transaction_year
            {base_time.month}  -- transaction_month
        ),
        (
            '9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM',  -- same whale
            'TxHash002DEF789ABC456JKL123MNO890PQR567STU',  -- transaction_hash
            '{(base_time + timedelta(days=3)).isoformat()}',  -- 3 days later
            'SELL',  -- transaction_type
            'So11111111111111111111111111111111111111112',  -- from_token_address (SOL)
            'SOL',  -- from_token_symbol
            300.0,  -- from_token_amount (300 SOL - partial sell)
            158.20,  -- to_token_price_usd (profit!)
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',  -- to_token_address (USDC)
            'USDC',  -- to_token_symbol
            47460.0,  -- to_token_amount (47.46k USDC)
            1.0,  -- to_token_price_usd
            47460.0,  -- total_value_usd
            23.73,  -- fee_usd
            47436.27,  -- net_value_usd
            'LARGE',  -- trade_size_tier
            0.08,  -- price_impact_percent (smaller trade)
            0.04,  -- slippage_percent
            0.96,  -- trade_efficiency_score (excellent execution)
            0.42,  -- market_volatility_score
            0.95,  -- timing_score (excellent timing - sold at peak)
            false,  -- is_arbitrage_opportunity
            false,  -- processed_for_pnl
            NULL,  -- pnl_processed_at
            NULL,  -- pnl_processing_batch_id
            'PENDING',  -- pnl_processing_status
            1.0,  -- data_completeness_score
            true,  -- has_valid_amounts
            true,  -- has_valid_prices
            CURRENT_TIMESTAMP,  -- fetched_at
            'tx_batch_20250621',  -- batch_id
            {(base_time + timedelta(days=3)).year},  -- transaction_year
            {(base_time + timedelta(days=3)).month}  -- transaction_month
        ),
        (
            'HN7cABqLq46Es1jh92dQQisAq662SmxELLLsHHe4YWrH',  -- different whale
            'TxHash003GHI456DEF123ABC789JKL456MNO123PQR',  -- transaction_hash
            '{(base_time + timedelta(days=1)).isoformat()}',  -- 1 day after first
            'BUY',  -- transaction_type
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',  -- from_token_address (USDC)
            'USDC',  -- from_token_symbol
            50000.0,  -- from_token_amount (50k USDC)
            1.0,  -- from_token_price_usd
            '85VBFQZC9TZkfaptBWjvUw7YbZjy52A6mjtPGjstQAmQ',  -- to_token_address (W token)
            'W',  -- to_token_symbol
            76923.08,  -- to_token_amount (50k / 0.65 = ~76.9k W)
            0.65,  -- to_token_price_usd
            50000.0,  -- total_value_usd
            25.0,  -- fee_usd
            49975.0,  -- net_value_usd
            'LARGE',  -- trade_size_tier
            0.25,  -- price_impact_percent (higher for smaller token)
            0.15,  -- slippage_percent
            0.85,  -- trade_efficiency_score (decent execution)
            0.60,  -- market_volatility_score (higher volatility)
            0.72,  -- timing_score (okay timing)
            false,  -- is_arbitrage_opportunity
            false,  -- processed_for_pnl
            NULL,  -- pnl_processed_at
            NULL,  -- pnl_processing_batch_id
            'PENDING',  -- pnl_processing_status
            0.95,  -- data_completeness_score (minor missing data)
            true,  -- has_valid_amounts
            true,  -- has_valid_prices
            CURRENT_TIMESTAMP,  -- fetched_at
            'tx_batch_20250621',  -- batch_id
            {(base_time + timedelta(days=1)).year},  -- transaction_year
            {(base_time + timedelta(days=1)).month}  -- transaction_month
        ),
        (
            'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',  -- USDC whale doing arbitrage
            'TxHash004JKL789GHI456DEF123ABC890MNO567PQR',  -- transaction_hash
            '{(base_time + timedelta(hours=6)).isoformat()}',  -- 6 hours after first
            'SELL',  -- transaction_type
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',  -- from_token_address (USDC)
            'USDC',  -- from_token_symbol
            100000.0,  -- from_token_amount (100k USDC)
            1.0,  -- from_token_price_usd
            'So11111111111111111111111111111111111111112',  -- to_token_address (SOL)
            'SOL',  -- to_token_symbol
            688.07,  -- to_token_amount (100k / 145.3 = ~688 SOL)
            145.30,  -- to_token_price_usd (slight arbitrage opportunity)
            100000.0,  -- total_value_usd
            50.0,  -- fee_usd
            99950.0,  -- net_value_usd
            'WHALE',  -- trade_size_tier
            0.02,  -- price_impact_percent (minimal impact)
            0.01,  -- slippage_percent (excellent execution)
            0.99,  -- trade_efficiency_score (nearly perfect)
            0.25,  -- market_volatility_score (low volatility)
            0.98,  -- timing_score (excellent arbitrage timing)
            true,  -- is_arbitrage_opportunity
            false,  -- processed_for_pnl
            NULL,  -- pnl_processed_at
            NULL,  -- pnl_processing_batch_id
            'PENDING',  -- pnl_processing_status
            1.0,  -- data_completeness_score
            true,  -- has_valid_amounts
            true,  -- has_valid_prices
            CURRENT_TIMESTAMP,  -- fetched_at
            'tx_batch_20250621',  -- batch_id
            {base_time.year},  -- transaction_year
            {base_time.month}  -- transaction_month
        )
        """
        
        conn.execute(sample_data_sql)
        logger.info("✅ Sample transaction history data inserted")
        
        # Verify data
        result = conn.execute("SELECT COUNT(*) FROM transaction_history").fetchone()
        logger.info(f"✅ Table contains {result[0]} transaction records")
        
        # Show sample data with enhanced analytics
        logger.info("📋 Enhanced transaction history data:")
        sample = conn.execute("""
        SELECT 
            SUBSTR(wallet_address, 1, 8) || '...' as wallet,
            transaction_type,
            from_token_symbol || '->' || to_token_symbol as trade_pair,
            total_value_usd,
            trade_size_tier,
            trade_efficiency_score,
            timing_score,
            is_arbitrage_opportunity
        FROM transaction_history
        ORDER BY total_value_usd DESC
        """).fetchall()
        
        for row in sample:
            arb_flag = " 🎯" if row[7] else ""
            logger.info(f"  {row[0]} {row[1]} {row[2]}: ${row[3]:,.0f} {row[4]} (eff={row[5]:.2f}, timing={row[6]:.2f}){arb_flag}")
        
        # Analytics summary
        logger.info("📊 Transaction analytics summary:")
        
        # Trade size distribution
        size_analysis = conn.execute("""
        SELECT 
            trade_size_tier,
            COUNT(*) as tx_count,
            AVG(total_value_usd) as avg_value,
            AVG(trade_efficiency_score) as avg_efficiency,
            AVG(timing_score) as avg_timing
        FROM transaction_history
        GROUP BY trade_size_tier
        ORDER BY avg_value DESC
        """).fetchall()
        
        for row in size_analysis:
            logger.info(f"  {row[0]}: {row[1]} txs, ${row[2]:,.0f} avg, eff={row[3]:.2f}, timing={row[4]:.2f}")
        
        # PnL processing status
        pnl_status = conn.execute("""
        SELECT 
            pnl_processing_status,
            COUNT(*) as tx_count,
            SUM(total_value_usd) as total_volume
        FROM transaction_history
        GROUP BY pnl_processing_status
        """).fetchall()
        
        logger.info("📈 PnL processing status:")
        for row in pnl_status:
            logger.info(f"  {row[0]}: {row[1]} transactions, ${row[2]:,.0f} volume")
        
        # Export to smart-trader bucket
        output_path = "s3://smart-trader/bronze/transaction_history/tx_batch_20250621.parquet"
        
        export_sql = f"""
        COPY transaction_history TO '{output_path}' 
        (FORMAT PARQUET, OVERWRITE_OR_IGNORE)
        """
        
        conn.execute(export_sql)
        logger.info(f"✅ Transaction history table exported to: {output_path}")
        
        # Verify export
        verify_sql = f"""
        SELECT COUNT(*) FROM parquet_scan('{output_path}')
        """
        verify_result = conn.execute(verify_sql).fetchone()
        logger.info(f"✅ Export verified: {verify_result[0]} records read back")
        
        # Show enhanced features summary
        enhanced_features = [
            ("Trade Analytics", "trade_size_tier, price_impact_percent, slippage_percent, trade_efficiency_score"),
            ("Market Context", "market_volatility_score, timing_score, is_arbitrage_opportunity"),
            ("PnL Processing", "processed_for_pnl, pnl_processing_batch_id, pnl_processing_status"),
            ("Data Quality", "data_completeness_score, has_valid_amounts, has_valid_prices"),
            ("Enhanced Schema", "32 columns with comprehensive transaction analysis"),
            ("Time Partitioning", "transaction_year, transaction_month for efficient queries")
        ]
        
        logger.info("🚀 Enhanced transaction history features:")
        for feature, description in enhanced_features:
            logger.info(f"  ✅ {feature}: {description}")
        
        conn.close()
        logger.info("🎉 Enhanced transaction history table creation SUCCESSFUL!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Transaction history table creation failed: {e}")
        return False

if __name__ == "__main__":
    success = create_transaction_history_table()
    sys.exit(0 if success else 1)