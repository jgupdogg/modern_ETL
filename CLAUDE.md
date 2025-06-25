# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Apache Airflow project for orchestrating data pipelines, using Docker Compose for containerization. The project uses Celery Executor for distributed task execution and includes PostgreSQL for metadata storage, Redis as the Celery message broker, and MinIO for object storage.

**Smart Trader Identification Pipeline** (✅ PRODUCTION READY + INTELLIGENT STATE TRACKING)
- **Purpose**: Identifies profitable Solana traders via BirdEye API analysis with intelligent incremental processing
- **Data Location**: `s3://smart-trader/` (TRUE Delta Lake with ACID transactions)
- **Architecture**: Complete medallion architecture (Bronze → Silver → Gold) with end-to-end state tracking
- **Technology**: Delta Lake 3.1.0 + dbt + PySpark 3.5.0 + MinIO S3A integration
- **State Management**: Comprehensive tracking across all layers with automatic refresh cycles
- **Processing Mode**: Incremental processing with intelligent data freshness management
- **DAG**: `optimized_delta_smart_trader_identification`
- **Documentation**: See `SMART_TRADER_PIPELINE.md` + `TRUE_DELTA_LAKE_IMPLEMENTATION.md`
- **Status**: ✅ **TRUE Delta Lake + Intelligent State Tracking COMPLETE** with ZERO fallbacks

## Key Commands

### Starting and Managing Airflow

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# View logs
docker-compose logs -f [service-name]  # e.g., airflow-scheduler, airflow-worker

# Access Airflow CLI
docker-compose run airflow-cli airflow [command]
```

### DAG Operations

```bash
# Trigger the Smart Trader Pipeline
docker compose run airflow-cli airflow dags trigger optimized_delta_smart_trader_identification

# Test a DAG
docker-compose run airflow-cli airflow dags test [dag_id]

# List all DAGs
docker-compose run airflow-cli airflow dags list

# Monitor pipeline execution
docker compose logs -f airflow-worker
```

### Accessing Services

- **Airflow Web UI**: http://localhost:8080 (username=`airflow`, password=`airflow`)
- **MinIO Console**: http://localhost:9001 (username=`minioadmin`, password=`minioadmin123`)
- **MinIO API**: http://localhost:9000
- **Flower UI** (when enabled): http://localhost:5555

## Architecture

The project uses the official Apache Airflow Docker setup with the following services:

1. **PostgreSQL**: Metadata database
2. **Redis**: Celery message broker
3. **MinIO**: S3-compatible object storage
4. **Airflow API Server**: REST API and web UI
5. **Airflow Scheduler**: Schedules DAG runs
6. **Airflow Worker**: Executes tasks via Celery
7. **DuckDB**: Analytics engine (containerized)
8. **Flower** (optional): Celery monitoring UI

## Development Workflow

1. Place DAG files in the `dags/` directory - they will be automatically picked up
2. Custom plugins go in the `plugins/` directory
3. For custom Python dependencies:
   - Quick testing: Set `_PIP_ADDITIONAL_REQUIREMENTS` in `.env`
   - Production: Use custom `Dockerfile.airflow` (already configured)
4. Configuration changes can be made in `config/airflow.cfg`

## 📁 Project Organization

### Directory Structure
- **`dags/`** - Airflow DAG definitions
- **`dags/config/`** - Centralized configuration files
- **`dags/tasks/smart_traders/`** - Smart trader pipeline task modules
- **`scripts/`** - Utility and testing scripts
- **`notebooks/`** - Jupyter notebooks for analysis
- **`dbt/`** - dbt models for transformations

## 🔧 Pipeline Configuration

### Centralized Configuration System
All Smart Trader pipeline settings are centralized in `dags/config/smart_trader_config.py`:

```bash
# View configuration
cat dags/config/smart_trader_config.py

# Test import
cd dags && python3 -c "from config.smart_trader_config import TOKEN_LIMIT; print(f'Token Limit: {TOKEN_LIMIT}')"
```

**Key Configuration Categories**:
- **API Limits**: Rate delays, batch sizes, pagination
- **Bronze Filters**: Token criteria, whale thresholds, transaction limits
- **Silver Transformation**: PnL calculations, quality filters
- **Gold Analytics**: Performance tiers, profitability thresholds
- **Infrastructure**: MinIO endpoints, PySpark memory settings, storage paths

### Memory Safety Configuration (CRITICAL)

**Conservative PySpark Settings** (`dags/config/smart_trader_config.py`):
```python
SPARK_DRIVER_MEMORY = '1g'               # Reduced from 4g to prevent crashes
SPARK_EXECUTOR_MEMORY = '1g'             # Reduced from 4g  
SPARK_DRIVER_MAX_RESULT_SIZE = '512m'    # Reduced from 2g

# Batch processing limits
SILVER_PNL_WALLET_BATCH_SIZE = 5         # Process 5 wallets at a time
SILVER_PNL_MAX_TRANSACTIONS_PER_BATCH = 500  # Limit transactions per batch
```

## MinIO Commands

```bash
# List buckets
docker exec claude_pipeline-minio mc ls local/

# View Delta Lake structure
docker exec claude_pipeline-minio mc ls local/smart-trader/ --recursive

# View Delta transaction logs
docker exec claude_pipeline-minio mc cat local/smart-trader/bronze/token_metrics/_delta_log/00000000000000000000.json

# Access MinIO Console
# http://localhost:9001 (minioadmin / minioadmin123)
```

## 🏗️ TRUE Delta Lake Integration

### Recent Fixes (2025-06-25)

**Critical Delta Lake Stability Improvements**:
- **Schema Evolution**: Added `mergeSchema: true` for all table updates to handle column additions
- **Partition Overwrite**: Added `overwriteSchema: true` to allow partition scheme changes  
- **Data Type Consistency**: Fixed timestamp casting issues to prevent type conflicts
- **Column Reference Fix**: Resolved `_delta_operation` column errors in state tracking
- **Table Existence Detection**: Improved validation to prevent false positive table detection

**Key Changes Made**:
```bash
# Fixed in TrueDeltaLakeManager 
.option("overwriteSchema", "true")    # Allows partition changes
.option("mergeSchema", "true")        # Enables schema evolution

# Fixed in optimized_delta_tasks.py
current_timestamp().cast("string")    # Consistent timestamp handling
lit("SILVER_TOKEN_UPDATE")           # Proper default values instead of invalid column refs
```

**Table Clearing Commands**:
```bash
# Complete table reset (removes all metadata and partitions)
docker exec claude_pipeline-minio mc rm --recursive --force local/smart-trader/

# Trigger fresh pipeline 
docker compose run airflow-cli airflow dags trigger optimized_delta_smart_trader_identification
```

### Delta Lake Architecture

**Data Storage**: `s3://smart-trader/` in MinIO with TRUE Delta Lake tables

**Complete Medallion Architecture**:
```
s3://smart-trader/
├── bronze/
│   ├── token_metrics/           # BirdEye token data with Delta versioning
│   ├── whale_holders/           # Top holder analysis with ACID safety  
│   └── transaction_history/     # Wallet transaction data with consistency
├── silver/
│   ├── tracked_tokens_delta/    # Liquidity-filtered tokens (>$100K)
│   ├── tracked_whales_delta/    # Unique whale-token pairs
│   └── wallet_pnl/             # FIFO PnL calculations with isolation
└── gold/
    └── smart_traders_delta/    # Smart trader rankings with durability
```

### Delta Lake Configuration

**Main Config**: `dags/config/true_delta_config.py`
**Optimized Tasks**: `dags/tasks/smart_traders/optimized_delta_tasks.py`

```python
# Delta Lake Spark Configuration (Docker-optimized)
DELTA_SPARK_CONFIG = {
    "spark.jars.packages": "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension", 
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin123",
    "spark.hadoop.fs.s3a.path.style.access": "true"
}
```

### Delta Lake Benefits

- ✅ **ACID Transactions**: No partial writes or inconsistent states
- ✅ **Schema Evolution**: Safe column additions/modifications
- ✅ **Time Travel**: Query any historical version with `versionAsOf`
- ✅ **Performance**: Optimized file layouts and automatic compaction
- ✅ **Concurrency**: Multiple readers/writers safely with transaction isolation

## 🏗️ dbt Integration

### dbt Configuration

**Profile**: `dbt/profiles.yml` - Configured for DuckDB with MinIO S3 integration

### Smart Wallets Gold Layer Model

**Location**: `dbt/models/gold/smart_wallets.sql`

**Purpose**: Transforms silver wallet PnL data into qualified smart traders

**Filtering Criteria**:
- **Portfolio-level records**: `token_address = 'ALL_TOKENS'`
- **Minimum PnL**: `total_pnl >= 10.0`
- **Minimum ROI**: `roi >= 1.0%`
- **Minimum Win Rate**: `win_rate >= 40.0%`
- **Minimum Trades**: `trade_count >= 1`

**Performance Tiers**:
- **ELITE**: Total PnL ≥ $10K, ROI ≥ 50%, Win Rate ≥ 30%
- **STRONG**: Total PnL ≥ $1K, ROI ≥ 20%, Win Rate ≥ 20%
- **PROMISING**: Total PnL ≥ $100, ROI ≥ 10%, Win Rate ≥ 10%
- **QUALIFIED**: Any positive PnL or win rate > 0%

## Data Layers

### Bronze Layer
- **Purpose**: Raw data ingestion from BirdEye API
- **Location**: `s3://smart-trader/bronze/`
- **Technology**: Delta Lake tables with schema enforcement
- **Processing**: Incremental with status tracking

### Silver Layer  
- **Purpose**: Cleaned, transformed data with business logic
- **Location**: `s3://smart-trader/silver/`
- **Key Features**:
  - FIFO-based PnL calculations
  - Portfolio-level aggregations
  - Win rate and ROI metrics
  - Simplified schema (22 fields)

### Gold Layer
- **Purpose**: Analytics-ready smart trader identification
- **Location**: `s3://smart-trader/gold/smart_traders_delta/`
- **Features**: Performance tiers, scoring algorithm, profitability ranking

## 🔄 Intelligent State Tracking System

### **End-to-End State Management**
The pipeline implements comprehensive state tracking to ensure efficient, incremental processing across all layers with automatic refresh cycles.

### **State Tracking Flow**
```
Bronze Tokens → Silver Tokens → Bronze Whales → Silver Whales → Bronze Transactions → Silver PnL → Gold Traders
     ↓               ↓              ↓               ↓                 ↓              ↓           ↓
processed=false  whale_fetch    txns_fetch    pnl_processing   processing_status  moved_to_gold  COMPLETE
     ↓           status=pending  status=pending status=pending   =pending          =false          ↓
processed=true   status=completed status=completed status=completed =processed    =true      INCREMENTAL
```

### **Bronze Layer State Tracking**
**Bronze Tokens**:
- `processed` (boolean) - Has token been processed to silver layer?
- Silver tokens task marks `processed = true` after successful processing

**Bronze Whales**:
- No state tracking needed (always fresh API data)
- Reads from `silver_tokens` to find tokens needing whale data

**Bronze Transactions**:
- No state tracking needed (raw transaction storage)
- Reads from `silver_whales` to find wallets needing transaction data

### **Silver Layer State Tracking**
**Silver Tokens**:
- `whale_fetch_status` ("pending", "completed") - Have whales been fetched for this token?
- `whale_fetched_at` (timestamp) - When were whales last fetched?
- **72-hour refresh cycle**: Whales refetched after 3 days

**Silver Whales**:
- `txns_fetched` (boolean) - Have transactions been fetched for this wallet?
- `txns_last_fetched_at` (timestamp) - When were transactions last fetched?
- `txns_fetch_status` ("pending", "completed") - Transaction fetch status
- `pnl_processed` (boolean) - Has PnL been calculated for this wallet?
- `pnl_last_processed_at` (timestamp) - When was PnL last calculated?
- `pnl_processing_status` ("pending", "completed") - PnL processing status
- **2-week transaction refresh**: Transactions refetched after 14 days
- **2-week PnL refresh**: PnL recalculated after 14 days

**Silver PnL**:
- `moved_to_gold` (boolean) - Has wallet been processed for gold layer?
- `gold_processed_at` (timestamp) - When was wallet moved to gold?
- `gold_processing_status` ("pending", "completed") - Gold processing status

### **Gold Layer Processing**
**Gold Smart Traders**:
- **Incremental processing**: Only evaluates wallets with `moved_to_gold = false`
- **Append mode**: New qualified traders appended to existing gold table
- **Historical preservation**: Previous gold records maintained
- Marks processed wallets as `moved_to_gold = true`

### **State Tracking Benefits**
- ✅ **No Duplicate Processing**: Each record processed only once per refresh cycle
- ✅ **Efficient Resource Usage**: Only processes new/changed data
- ✅ **Automatic Refresh**: Time-based refresh ensures data freshness
- ✅ **Fast Pipeline Execution**: Skips already-processed data
- ✅ **Incremental Growth**: Tables grow over time instead of rebuilding
- ✅ **Data Integrity**: Preserves historical data while adding new records

## Pipeline Execution

```bash
# Run complete pipeline
docker compose run airflow-cli airflow dags trigger optimized_delta_smart_trader_identification

# Test individual Delta Lake components
docker compose exec airflow-worker python3 -c "
from tasks.smart_traders.optimized_delta_tasks import (
    create_bronze_tokens_delta,      # BirdEye API → Delta Lake bronze tokens
    create_bronze_whales_delta,      # Silver integration → Delta Lake whales  
    create_bronze_transactions_delta, # Whale wallets → Delta Lake transactions
    create_silver_wallet_pnl_delta,  # FIFO PnL → Delta Lake silver analytics
    create_gold_smart_traders_delta   # Performance tiers → Delta Lake gold traders
)
print('✅ TRUE Delta Lake pipeline functions loaded')
"
```

## PySpark Task Pattern

**Critical**: Use the `@task` decorator pattern with SparkSession created inside task function:

```python
@task(dag=dag)
def my_pyspark_task(**context):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("MyApp").config(...).getOrCreate()
    try:
        # PySpark logic here
        pass
    finally:
        spark.stop()
```

## Infrastructure Requirements

1. **Java Dependency**: PySpark requires Java 17 (installed via custom Dockerfile)
2. **Docker Network**: Use internal names (`minio:9000`, not `localhost:9000`)
3. **Memory Limits**: Conservative 1GB settings to prevent system crashes
4. **Custom Image**: Production uses `Dockerfile.airflow` with all dependencies

## Maintenance & Cleanup

### Disk Space Monitoring
- DAG: `disk_space_monitoring` runs every 6 hours
- Warning at 80% disk usage, critical at 90%
- Automatic cleanup of Docker images, logs, and PySpark checkpoints

### Manual Cleanup
```bash
# Docker cleanup
docker system prune -f

# Log compression
/home/jgupdogg/dev/claude_pipeline/scripts/maintenance/cleanup_logs.sh

# Check disk usage
df -h /
docker system df
```

## Important Notes

- New DAGs are paused by default (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'`)
- Ensure proper file permissions on Linux by setting `AIRFLOW_UID` in `.env`
- MinIO credentials are stored in `.env` file
- All operations use TRUE Delta Lake with real `_delta_log` directories
- State tracking prevents infinite reprocessing loops