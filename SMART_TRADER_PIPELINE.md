# Smart Trader Identification Pipeline

**Pipeline Status**: ✅ **PRODUCTION READY**  
**Last Updated**: June 16, 2025  
**Data Location**: `s3://solana-data/` (MinIO)  
**DAG**: `smart_trader_identification_dag`

## Executive Summary

The Smart Trader Identification Pipeline is a complete end-to-end system for identifying and monitoring profitable cryptocurrency traders on Solana. The pipeline processes token data, analyzes whale holdings, calculates comprehensive PnL metrics, and outputs elite traders for real-time monitoring via Helius webhooks.

**Current Status**: Fully operational with clean, optimized data layers and active processing pipeline.

## Pipeline Architecture

```
Bronze Layer → Silver Layer → Gold Layer → Helius Webhook Integration
```

### Data Flow
```
BirdEye API → Token List → Token Whales → Wallet Transactions → PnL Calculation → Top Traders → Helius Monitoring
     ↓            ↓           ↓              ↓                  ↓              ↓              ↓
   MinIO        MinIO       MinIO          MinIO            MinIO          MinIO         Real-time
  (Bronze)     (Bronze)    (Bronze)       (Bronze)         (Silver)       (Gold)        Alerts
```

## Data Layers Overview

### Bronze Layer (Raw Data Ingestion)
**Location**: `s3://solana-data/bronze/`  
**Status**: ✅ **OPTIMIZED & ACTIVE**

#### Active Datasets (2 total)

**1. Token Whales** (`token_whales/`)
- **Records**: 1,039 whale holders
- **Files**: 1,035 parquet files  
- **Partitioning**: ✅ Date partitioned (`date=YYYY-MM-DD`)
- **Date Range**: 2025-06-13 to 2025-06-16 (4 partitions)
- **Schema**: 19 columns including holdings, rank, processing status
- **Source**: BirdEye V3 API top holders endpoint

**2. Wallet Transactions** (`wallet_transactions/`)
- **Records**: 900 transaction records
- **Files**: 900 parquet files
- **Partitioning**: ✅ Date partitioned (`date=YYYY-MM-DD`) 
- **Date Range**: 2025-06-13 to 2025-06-16 (4 partitions)
- **Schema**: 28 columns including transaction details, PnL processing flags
- **Source**: BirdEye V3 API wallet trade history endpoint

#### Legacy Cleanup ✅ **COMPLETED**
- **Action**: Removed obsolete `wallet_trade_history/` dataset
- **Result**: Clean storage, optimized queries, production-ready state

### Silver Layer (Transformed Analytics)
**Location**: `s3://solana-data/silver/`  
**Technology**: PySpark with FIFO cost basis calculation  
**Status**: ✅ **OPTIMIZED & ACTIVE**

#### Active Datasets (2 total)

**1. Tracked Tokens** (`tracked_tokens/`)
- **Records**: 58 token records across processing dates
- **Files**: 17 parquet files
- **Partitioning**: ✅ Single-level (`processing_date=YYYY-MM-DD`)
- **Date Range**: 2025-06-14 to 2025-06-16 (3 partitions)
- **Schema**: 19 columns including performance metrics and quality scoring
- **Purpose**: Filtered high-performance tokens based on momentum criteria

**Schema (19 columns)**:
```sql
token_address             VARCHAR      -- Primary identifier
symbol                    VARCHAR      -- Token symbol
name                      VARCHAR      -- Token name  
decimals                  INTEGER      -- Token decimals
logoURI                   VARCHAR      -- Token logo URL
volume24hUSD              DOUBLE       -- 24h trading volume
volume24hChangePercent    DOUBLE       -- Volume change %
price24hChangePercent     DOUBLE       -- Price change %
marketcap                 DOUBLE       -- Market capitalization
liquidity                 DOUBLE       -- Token liquidity
price                     DOUBLE       -- Current price
fdv                       DOUBLE       -- Fully diluted valuation
rank                      INTEGER      -- Token ranking
volume_mcap_ratio         DOUBLE       -- Volume to market cap ratio
quality_score             DOUBLE       -- Calculated quality metric
bronze_id                 VARCHAR      -- Source bronze record ID
created_at                TIMESTAMP    -- Record creation time
updated_at                TIMESTAMP    -- Last update time
processing_date           DATE         -- Partition key
```

**2. Wallet PnL Metrics** (`wallet_pnl/`)
- **Records**: 11,827 PnL records across all timeframes
- **Files**: 81 parquet files
- **Partitioning**: ✅ Multi-level (`calculation_year=YYYY/calculation_month=MM/time_period={all|week|month|quarter}`)
- **Unique Wallets**: 215 tracked wallets
- **Unique Tokens**: 23 tokens + portfolio aggregations
- **Purpose**: Comprehensive FIFO profit/loss calculation with multi-timeframe analysis

**Schema (29 columns)**:
```sql
wallet_address                 VARCHAR      -- Wallet identifier
token_address                  VARCHAR      -- Token or 'ALL_TOKENS' for portfolio
calculation_date               DATE         -- PnL calculation date
realized_pnl                   DOUBLE       -- Realized profit/loss
unrealized_pnl                 DOUBLE       -- Unrealized profit/loss
total_pnl                      DOUBLE       -- Total profit/loss
trade_count                    BIGINT       -- Number of trades
win_rate                       DOUBLE       -- Winning trade percentage
total_bought                   DOUBLE       -- Total buy volume
total_sold                     DOUBLE       -- Total sell volume
roi                            DOUBLE       -- Return on investment
avg_holding_time_hours         DOUBLE       -- Average holding period
avg_transaction_amount_usd     DOUBLE       -- Average transaction size
trade_frequency_daily          DOUBLE       -- Daily trade frequency
first_transaction              TIMESTAMP    -- First trade timestamp
last_transaction               TIMESTAMP    -- Last trade timestamp
current_position_tokens        DOUBLE       -- Current token holdings
current_position_cost_basis    DOUBLE       -- Cost basis of holdings
current_position_value         DOUBLE       -- Current value of holdings
processed_at                   TIMESTAMP    -- Processing timestamp
batch_id                       VARCHAR      -- Processing batch ID
data_source                    VARCHAR      -- Data source identifier
processed_for_gold             BOOLEAN      -- Gold layer processing flag
gold_processed_at              TIMESTAMP    -- Gold processing timestamp
gold_processing_status         VARCHAR      -- Gold processing status
gold_batch_id                  VARCHAR      -- Gold processing batch ID
calculation_month              BIGINT       -- Month partition value
calculation_year               BIGINT       -- Year partition value
time_period                    VARCHAR      -- Time period partition
```

**Data Organization**:
- **Token-level records**: 6,019 records (specific token performance)
- **Portfolio-level records**: 5,808 records (`token_address = 'ALL_TOKENS'`)
- **Time Period Partitions**:
  - `all`: Complete historical performance
  - `week`: Weekly performance windows
  - `month`: Monthly performance windows  
  - `quarter`: Quarterly performance windows

**Features**:
- **FIFO Cost Basis Calculation**: Accurate PnL with proper lot tracking
- **Multi-timeframe Analysis**: Performance across different time horizons
- **Portfolio Aggregation**: Combined metrics across all tokens per wallet
- **Gold Layer Integration**: Processing flags for downstream analytics
- **Comprehensive Trading Metrics**: Win rate, ROI, holding times, trade frequency

#### Legacy Cleanup ✅ **COMPLETED**
- **Action**: Removed temporary `wallet_pnl_updated_*` folders
- **Result**: Clean storage, optimized queries, no duplicate data

### Gold Layer (Top Trader Selection)
**Location**: `s3://solana-data/gold/top_traders/`  
**Purpose**: Elite trader identification for monitoring  
**Status**: ✅ **OPTIMIZED & ACTIVE**

#### Performance Tiers
- **Elite**: $10K+ PnL, 50%+ ROI, 70%+ win rate, 20+ trades
- **Strong**: $1K+ PnL, 25%+ ROI, 60%+ win rate, 10+ trades  
- **Promising**: $100+ PnL, 10%+ ROI, 50%+ win rate, 5+ trades

#### Data Structure
- **Storage**: ✅ **Unpartitioned** (optimized for small dataset)
- **Format**: Simple parquet files (`top_traders_YYYYMMDD_HHMMSS.parquet`)
- **Current Status**: ✅ **POPULATED** with 100 top traders (85 unique wallets)
- **Data Source**: Migrated from PostgreSQL `gold.top_traders` table
- **Processing**: Incremental updates only for new profitable traders

#### Output Schema (28 fields)
- **Core Performance**: total_pnl, roi, win_rate, trade_count
- **Portfolio Metrics**: total_bought, total_sold, current_position_value
- **Risk Analytics**: consistency_score, avg_holding_time, trade_frequency
- **Classification**: performance_tier, processing metadata

#### Legacy Cleanup ✅ **COMPLETED**
- **Action**: Removed complex 3-level partitioning (`ingestion_year/ingestion_month/performance_tier_partition`)
- **Result**: Simplified structure, faster queries, better performance for small dataset

### Helius Integration
**Purpose**: Real-time transaction monitoring for identified top traders  
**Process**: Updates webhook with all profitable wallet addresses from gold layer  
**Result**: Live monitoring of smart money movements

## Key DAGs & Schedule

1. **bronze_token_list**: Token data ingestion (hourly)
2. **bronze_token_whales**: Whale holder data (every 4 hours)  
3. **bronze_wallet_transactions**: Transaction history (every 6 hours)
4. **silver_wallet_pnl**: PnL calculation (every 12 hours)
5. **gold_top_traders**: Top trader selection (every 12 hours)
6. **smart_trader_identification**: Master pipeline orchestration

## ✅ OPTIMAL QUERY PATTERNS

### DuckDB Access Patterns

**Bronze Layer Queries**:
```sql
-- Token Whales (handle mixed schemas)
SELECT * FROM parquet_scan('s3://solana-data/bronze/token_whales/**/*.parquet', union_by_name=true);

-- Wallet Transactions (consistent schema)  
SELECT * FROM parquet_scan('s3://solana-data/bronze/wallet_transactions/**/*.parquet');

-- Filter unprocessed records for PnL
SELECT * FROM parquet_scan('s3://solana-data/bronze/wallet_transactions/**/*.parquet')
WHERE processed_for_pnl = false OR processed_for_pnl IS NULL;
```

**Silver Layer Queries**:
```sql
-- Tracked Tokens (current high-performance tokens)
SELECT * FROM parquet_scan('s3://solana-data/silver/tracked_tokens/**/*.parquet');

-- Latest tracked tokens by processing date
SELECT * FROM parquet_scan('s3://solana-data/silver/tracked_tokens/processing_date=2025-06-16/*.parquet');

-- Wallet PnL metrics (all timeframes)
SELECT * FROM parquet_scan('s3://solana-data/silver/wallet_pnl/**/*.parquet');

-- Portfolio-level performance (all tokens combined per wallet)
SELECT wallet_address, total_pnl, roi, win_rate 
FROM parquet_scan('s3://solana-data/silver/wallet_pnl/**/time_period=all/*.parquet')
WHERE token_address = 'ALL_TOKENS'
ORDER BY roi DESC LIMIT 10;

-- Token-specific performance for a wallet
SELECT token_address, total_pnl, roi, win_rate, trade_count
FROM parquet_scan('s3://solana-data/silver/wallet_pnl/**/time_period=all/*.parquet')
WHERE wallet_address = 'WALLET_ADDRESS_HERE' AND token_address != 'ALL_TOKENS'
ORDER BY total_pnl DESC;

-- Weekly performance analysis
SELECT * FROM parquet_scan('s3://solana-data/silver/wallet_pnl/**/time_period=week/*.parquet')
WHERE calculation_date >= '2025-06-01';

-- Top performers by timeframe
SELECT time_period, wallet_address, total_pnl, roi 
FROM parquet_scan('s3://solana-data/silver/wallet_pnl/**/*.parquet')
WHERE token_address = 'ALL_TOKENS' AND total_pnl > 0
ORDER BY time_period, total_pnl DESC;
```

**Gold Layer Queries**:
```sql
-- All top traders (simplified unpartitioned structure)
SELECT wallet_address, performance_tier, total_pnl, roi, win_rate
FROM parquet_scan('s3://solana-data/gold/top_traders/*.parquet')
ORDER BY total_pnl DESC;

-- Elite traders only
SELECT wallet_address, performance_tier, total_pnl, roi, win_rate
FROM parquet_scan('s3://solana-data/gold/top_traders/*.parquet')
WHERE performance_tier = 'elite'
ORDER BY total_pnl DESC;

-- Performance tier summary
SELECT performance_tier, COUNT(*) as trader_count, AVG(total_pnl) as avg_pnl
FROM parquet_scan('s3://solana-data/gold/top_traders/*.parquet')
GROUP BY performance_tier
ORDER BY avg_pnl DESC;
```

## Pipeline Management Commands

### Triggering DAGs
```bash
# Run complete pipeline
docker compose run airflow-cli airflow dags trigger smart_trader_identification

# Individual components
docker compose run airflow-cli airflow dags trigger bronze_token_whales
docker compose run airflow-cli airflow dags trigger silver_wallet_pnl
docker compose run airflow-cli airflow dags trigger gold_top_traders
```

### Monitoring
```bash
# Check DAG status
docker compose run airflow-cli airflow dags list-runs --dag-id smart_trader_identification

# View task logs
docker compose logs -f airflow-worker

# MinIO data exploration
# http://localhost:9001 (minioadmin/minioadmin123)
```

### DuckDB Analytics
```bash
# Access DuckDB for analysis
docker exec -it claude_pipeline-duckdb /bin/sh

# Run analysis scripts
docker exec claude_pipeline-duckdb python3 /scripts/analyze_silver_pnl_data.py
```

## 🎯 PRODUCTION FEATURES

### Data Quality
- **100% Processing Success**: All records processed without errors
- **Proper Partitioning**: Date-based partitioning for efficient queries
- **State Management**: Processing flags prevent duplicate work
- **Audit Trail**: Complete batch tracking and timestamps

### Performance Optimizations
- **FIFO Cost Basis**: Accurate PnL calculation with lot tracking
- **Incremental Processing**: Only processes new/unprocessed records
- **Memory Management**: PySpark optimized for large datasets
- **Efficient Storage**: Parquet format with compression

### Monitoring & Reliability
- **Processing Status Tracking**: Clear flags for each processing stage
- **Error Handling**: Comprehensive error catching and logging
- **Health Checks**: Data validation at each layer
- **Recovery Capability**: Reprocessing support for failed records

## Technical Architecture

### Technologies Used
- **Apache Airflow**: Workflow orchestration and scheduling
- **PySpark**: Distributed data processing and FIFO calculations
- **MinIO**: S3-compatible object storage with partitioning
- **DuckDB**: Analytical queries and data exploration
- **BirdEye API**: Cryptocurrency market data and whale tracking
- **Helius API**: Blockchain webhook monitoring integration

### Key Libraries
- **birdeye_client**: Custom API client with retry logic and rate limiting
- **PySpark SQL**: Complex analytical transformations
- **boto3**: S3/MinIO integration
- **pandas**: Data manipulation for smaller datasets

## Business Value

### Smart Money Identification
- **Elite Trader Discovery**: Automated identification of top performing wallets
- **Real-time Monitoring**: Live tracking of profitable trader movements
- **Performance Analytics**: Comprehensive metrics for trader evaluation

### Risk Management
- **Performance Tiers**: Clear classification system for trader quality
- **Historical Analysis**: Multi-timeframe performance validation
- **Consistency Scoring**: Reliability metrics for sustained performance

### Operational Efficiency
- **Automated Pipeline**: Full automation from data ingestion to monitoring
- **Scalable Architecture**: Handles growing data volumes efficiently
- **Cost Optimization**: Efficient resource usage and storage management

## 🚀 CURRENT STATUS: PRODUCTION READY

The Smart Trader Identification Pipeline is **fully operational** with:
- ✅ Clean, optimized data layers (legacy cleanup completed)
- ✅ Active processing pipeline (bronze → silver → gold)
- ✅ Proper schema patterns and query optimization
- ✅ Elite trader identification and Helius integration
- ✅ Comprehensive monitoring and error handling

**Next Steps**: Pipeline is ready for production use. Monitor performance and scale as needed based on data volume growth.