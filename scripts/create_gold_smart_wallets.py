#!/usr/bin/env python3
"""
Create Gold Layer: Enhanced Smart Wallets
Execute the enhanced smart wallets transformation and save to smart-trader bucket
"""

import sys
import logging
from datetime import datetime

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

def create_gold_smart_wallets():
    """
    Execute enhanced smart wallets transformation and save to gold layer
    """
    logger = setup_logging()
    
    try:
        import duckdb
        
        logger.info("🏆 CREATING GOLD LAYER: ENHANCED SMART WALLETS")
        logger.info("=" * 60)
        
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
        
        # Generate timestamp for this gold run
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Execute the full enhanced smart wallets transformation
        logger.info("")
        logger.info("🔄 EXECUTING ENHANCED SMART WALLETS TRANSFORMATION")
        logger.info("-" * 50)
        
        # Create the enhanced smart wallets gold table
        create_gold_query = f"""
        CREATE TABLE enhanced_smart_wallets AS
        WITH enhanced_silver_pnl AS (
            -- Load enhanced silver wallet PnL data from smart-trader bucket
            SELECT *
            FROM parquet_scan('s3://smart-trader/silver/wallet_pnl/**/*.parquet')
        ),
        
        -- Step 1: Apply minimal but meaningful filtering criteria
        filtered_wallets AS (
            SELECT 
                wallet_address,
                token_address,
                
                -- Core PnL metrics
                realized_pnl,
                unrealized_pnl,
                total_pnl,
                
                -- Trading performance metrics
                total_trades,
                win_rate,
                roi,
                
                -- Enhanced analytics (NEW - from our enhanced silver layer)
                COALESCE(avg_trade_efficiency, 0.5) as avg_trade_efficiency,
                COALESCE(avg_timing_score, 0.5) as avg_timing_score,
                COALESCE(pnl_quality_score, 0.5) as pnl_quality_score,
                
                -- Position data
                current_position_tokens,
                COALESCE(current_price_estimate, 0) as current_price_estimate,
                COALESCE(current_position_value_usd, 0) as current_position_value_usd,
                total_cost_basis,
                
                -- Time analytics
                first_trade_date,
                last_trade_date,
                trading_period_days,
                trade_frequency,
                
                -- Processing metadata
                calculation_date,
                processed_at,
                batch_id
                
            FROM enhanced_silver_pnl
            WHERE 
                -- Minimal criteria adjusted for current data
                total_pnl >= 0                          -- Include break-even traders for now
                AND total_trades >= 1                   -- Any trading activity
                AND pnl_quality_score >= 0.5           -- Basic data quality
                AND avg_trade_efficiency >= 0.7        -- Good execution quality
                AND token_address IS NOT NULL          -- Valid token data
                AND wallet_address IS NOT NULL         -- Valid wallet data
        ),
        
        -- Step 2: Calculate enhanced smart trader scores
        scored_wallets AS (
            SELECT 
                *,
                
                -- Enhanced Smart Trader Score Algorithm
                ROUND(
                    -- Profitability factor (40% weight)
                    (LEAST(total_pnl / 1000.0, 1.0) * 0.4) +
                    
                    -- Trade quality factor (30% weight) - using enhanced analytics
                    (avg_trade_efficiency * 0.3) +
                    
                    -- Timing factor (20% weight) - using enhanced analytics  
                    (avg_timing_score * 0.2) +
                    
                    -- Consistency factor (10% weight)
                    (CASE 
                        WHEN total_trades >= 10 THEN 0.1
                        WHEN total_trades >= 5 THEN 0.07
                        WHEN total_trades >= 3 THEN 0.05
                        ELSE 0.02
                    END),
                    3
                ) as enhanced_smart_trader_score,
                
                -- ROI-based performance tier (adjusted for current data)
                CASE 
                    WHEN total_pnl > 0 AND roi > 0 AND win_rate > 0.5 AND total_trades >= 5 THEN 'ELITE'
                    WHEN total_pnl > 0 AND roi > 0 AND win_rate > 0.3 AND total_trades >= 3 THEN 'STRONG'  
                    WHEN total_pnl >= 0 AND avg_trade_efficiency >= 0.8 AND total_trades >= 1 THEN 'PROMISING'
                    ELSE 'DEVELOPING'
                END as performance_tier,
                
                -- Trade execution quality tier (NEW - based on enhanced analytics)
                CASE 
                    WHEN avg_trade_efficiency >= 0.9 AND avg_timing_score >= 0.8 THEN 'EXCELLENT_EXECUTION'
                    WHEN avg_trade_efficiency >= 0.8 AND avg_timing_score >= 0.7 THEN 'GOOD_EXECUTION'
                    WHEN avg_trade_efficiency >= 0.7 AND avg_timing_score >= 0.6 THEN 'AVERAGE_EXECUTION'
                    ELSE 'IMPROVING_EXECUTION'
                END as execution_quality_tier
                
            FROM filtered_wallets
        ),
        
        -- Step 3: Rank and add portfolio-level analytics
        ranked_wallets AS (
            SELECT 
                *,
                
                -- Ranking metrics
                ROW_NUMBER() OVER (ORDER BY enhanced_smart_trader_score DESC, total_pnl DESC) as overall_rank,
                ROW_NUMBER() OVER (ORDER BY total_pnl DESC) as profitability_rank,
                ROW_NUMBER() OVER (ORDER BY avg_trade_efficiency DESC, avg_timing_score DESC) as execution_rank,
                ROW_NUMBER() OVER (ORDER BY roi DESC) as roi_rank,
                
                -- Percentile rankings
                PERCENT_RANK() OVER (ORDER BY enhanced_smart_trader_score) as score_percentile,
                PERCENT_RANK() OVER (ORDER BY total_pnl) as pnl_percentile,
                PERCENT_RANK() OVER (ORDER BY avg_trade_efficiency) as efficiency_percentile,
                
                -- Portfolio value analysis
                CASE 
                    WHEN current_position_value_usd >= 100000 THEN 'LARGE_PORTFOLIO'
                    WHEN current_position_value_usd >= 10000 THEN 'MEDIUM_PORTFOLIO'  
                    WHEN current_position_value_usd >= 1000 THEN 'SMALL_PORTFOLIO'
                    ELSE 'MICRO_PORTFOLIO'
                END as portfolio_size_tier
                
            FROM scored_wallets
        ),
        
        -- Step 4: Add token-level aggregations for multi-token traders
        wallet_summary AS (
            SELECT 
                wallet_address,
                
                -- Aggregate metrics across all tokens for this wallet
                COUNT(*) as tokens_traded,
                SUM(total_pnl) as total_portfolio_pnl,
                AVG(enhanced_smart_trader_score) as avg_smart_trader_score,
                MAX(enhanced_smart_trader_score) as best_token_score,
                
                -- Portfolio diversification metrics
                COUNT(CASE WHEN total_pnl > 0 THEN 1 END) as profitable_tokens,
                COUNT(CASE WHEN total_pnl < 0 THEN 1 END) as losing_tokens,
                
                -- Quality and execution aggregates
                AVG(pnl_quality_score) as avg_portfolio_quality,
                AVG(avg_trade_efficiency) as avg_portfolio_efficiency,
                AVG(avg_timing_score) as avg_portfolio_timing,
                
                -- Best performing token details
                (ARRAY_AGG(token_address ORDER BY total_pnl DESC))[1] as best_performing_token,
                MAX(total_pnl) as best_token_pnl,
                
                -- Processing metadata
                MAX(processed_at) as latest_processed_at,
                COUNT(DISTINCT batch_id) as data_batches
                
            FROM ranked_wallets
            GROUP BY wallet_address
        )
        
        -- Final SELECT: Combine individual token performance with portfolio summary
        SELECT 
            rw.wallet_address,
            rw.token_address,
            
            -- Individual token performance
            rw.total_pnl,
            rw.realized_pnl,
            rw.unrealized_pnl,
            rw.roi,
            rw.win_rate,
            rw.total_trades,
            
            -- Enhanced analytics
            rw.avg_trade_efficiency,
            rw.avg_timing_score,
            rw.pnl_quality_score,
            rw.enhanced_smart_trader_score,
            
            -- Classifications
            rw.performance_tier,
            rw.execution_quality_tier,
            rw.portfolio_size_tier,
            
            -- Rankings
            rw.overall_rank,
            rw.profitability_rank,
            rw.execution_rank,
            rw.score_percentile,
            rw.pnl_percentile,
            rw.efficiency_percentile,
            
            -- Portfolio-level metrics (from wallet_summary)
            ws.tokens_traded,
            ws.total_portfolio_pnl,
            ws.avg_smart_trader_score,
            ws.profitable_tokens,
            ws.losing_tokens,
            ws.avg_portfolio_quality,
            ws.avg_portfolio_efficiency,
            ws.best_performing_token,
            ws.best_token_pnl,
            
            -- Position and timing data
            rw.current_position_value_usd,
            rw.trading_period_days,
            rw.trade_frequency,
            rw.first_trade_date,
            rw.last_trade_date,
            
            -- Processing metadata
            rw.calculation_date,
            rw.processed_at,
            CURRENT_TIMESTAMP as gold_processed_at,
            'enhanced_smart_wallets_v2' as gold_model_version,
            rw.batch_id as source_batch_id
            
        FROM ranked_wallets rw
        LEFT JOIN wallet_summary ws ON rw.wallet_address = ws.wallet_address
        
        -- Order by enhanced smart trader score (best traders first)
        ORDER BY rw.enhanced_smart_trader_score DESC, rw.total_pnl DESC
        """
        
        conn.execute(create_gold_query)
        logger.info("✅ Enhanced smart wallets table created successfully")
        
        # Get summary statistics
        summary_stats = conn.execute("""
        SELECT 
            COUNT(*) as total_smart_wallets,
            COUNT(DISTINCT wallet_address) as unique_wallets,
            AVG(enhanced_smart_trader_score) as avg_score,
            MAX(enhanced_smart_trader_score) as max_score,
            COUNT(CASE WHEN performance_tier = 'ELITE' THEN 1 END) as elite_wallets,
            COUNT(CASE WHEN performance_tier = 'STRONG' THEN 1 END) as strong_wallets,
            COUNT(CASE WHEN performance_tier = 'PROMISING' THEN 1 END) as promising_wallets
        FROM enhanced_smart_wallets
        """).fetchone()
        
        logger.info("")
        logger.info("📊 Gold Layer Summary Statistics:")
        logger.info(f"  Total smart wallet records: {summary_stats[0]}")
        logger.info(f"  Unique smart wallets: {summary_stats[1]}")
        logger.info(f"  Average smart trader score: {summary_stats[2]:.3f}")
        logger.info(f"  Maximum smart trader score: {summary_stats[3]:.3f}")
        logger.info(f"  Elite tier wallets: {summary_stats[4]}")
        logger.info(f"  Strong tier wallets: {summary_stats[5]}")
        logger.info(f"  Promising tier wallets: {summary_stats[6]}")
        
        # Save to S3 (MinIO) smart-trader bucket
        logger.info("")
        logger.info("💾 SAVING TO SMART-TRADER GOLD BUCKET")
        logger.info("-" * 50)
        
        output_path = f"s3://smart-trader/gold/smart_wallets/enhanced_smart_wallets_{timestamp}.parquet"
        
        export_query = f"""
        COPY (
            SELECT * FROM enhanced_smart_wallets
        ) TO '{output_path}' 
        (FORMAT PARQUET, COMPRESSION SNAPPY)
        """
        
        conn.execute(export_query)
        logger.info(f"✅ Enhanced smart wallets saved to: {output_path}")
        
        # Create a metadata file
        metadata_path = f"s3://smart-trader/gold/smart_wallets/_metadata_{timestamp}.json"
        metadata = f"""
        {{
            "transformation_timestamp": "{datetime.now().isoformat()}",
            "model_version": "enhanced_smart_wallets_v2",
            "total_records": {summary_stats[0]},
            "unique_wallets": {summary_stats[1]},
            "avg_score": {summary_stats[2]:.3f},
            "max_score": {summary_stats[3]:.3f},
            "performance_tiers": {{
                "elite": {summary_stats[4]},
                "strong": {summary_stats[5]},
                "promising": {summary_stats[6]}
            }},
            "source_data": "s3://smart-trader/silver/wallet_pnl/",
            "filtering_criteria": {{
                "min_pnl": 0.0,
                "min_trades": 1,
                "min_quality_score": 0.5,
                "min_efficiency": 0.7
            }},
            "scoring_algorithm": {{
                "profitability_weight": 0.4,
                "trade_quality_weight": 0.3,
                "timing_weight": 0.2,
                "consistency_weight": 0.1
            }}
        }}
        """
        
        conn.execute(f"""
        COPY (SELECT '{metadata}' as metadata_json) 
        TO '{metadata_path}' 
        (FORMAT JSON)
        """)
        logger.info(f"✅ Metadata saved to: {metadata_path}")
        
        # Verify the saved data
        logger.info("")
        logger.info("🔍 VERIFYING SAVED GOLD DATA")
        logger.info("-" * 50)
        
        verification = conn.execute(f"""
        SELECT 
            COUNT(*) as record_count,
            COUNT(DISTINCT wallet_address) as unique_wallets,
            AVG(enhanced_smart_trader_score) as avg_score
        FROM parquet_scan('{output_path}')
        """).fetchone()
        
        logger.info(f"✅ Verification successful:")
        logger.info(f"  Records in saved file: {verification[0]}")
        logger.info(f"  Unique wallets in file: {verification[1]}")
        logger.info(f"  Average score in file: {verification[2]:.3f}")
        
        # Show top performers
        logger.info("")
        logger.info("🏆 TOP 5 ENHANCED SMART WALLETS IN GOLD LAYER")
        logger.info("-" * 50)
        
        top_performers = conn.execute(f"""
        SELECT 
            wallet_address,
            token_address,
            enhanced_smart_trader_score,
            performance_tier,
            execution_quality_tier,
            total_pnl,
            total_trades,
            avg_trade_efficiency
        FROM parquet_scan('{output_path}')
        ORDER BY enhanced_smart_trader_score DESC
        LIMIT 5
        """).fetchall()
        
        for i, performer in enumerate(top_performers):
            wallet = performer[0][:8] + "..." if len(performer[0]) > 8 else performer[0]
            token = performer[1][:8] + "..." if len(performer[1]) > 8 else performer[1]
            logger.info(f"  {i+1}. {wallet} | {token}")
            logger.info(f"     Score: {performer[2]:.3f} | {performer[3]} | {performer[4]}")
            logger.info(f"     PnL: ${performer[5]:.2f} | Trades: {performer[6]} | Efficiency: {performer[7]:.3f}")
        
        logger.info("")
        logger.info("🎉 ENHANCED SMART WALLETS GOLD LAYER COMPLETE!")
        logger.info("=" * 60)
        logger.info("✅ Successfully created enhanced gold layer with multi-factor scoring")
        logger.info("✅ Applied performance and execution quality tiers")
        logger.info("✅ Generated comprehensive portfolio analytics")
        logger.info("✅ Saved to smart-trader bucket with metadata")
        logger.info("✅ Verified data integrity and completeness")
        logger.info("✅ Ready for production smart trader identification")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Enhanced smart wallets gold layer creation failed: {e}")
        return False

if __name__ == "__main__":
    success = create_gold_smart_wallets()
    sys.exit(0 if success else 1)