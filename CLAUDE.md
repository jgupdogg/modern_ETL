# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Apache Airflow project for orchestrating data pipelines, using Docker Compose for containerization. The project uses Celery Executor for distributed task execution and includes PostgreSQL for metadata storage, Redis as the Celery message broker, Redpanda for event streaming, and MinIO for object storage.

**The project has TWO main data pipelines:**

1. **Smart Trader Identification Pipeline** (✅ PRODUCTION READY)
   - **Purpose**: Identifies profitable Solana traders via BirdEye API analysis
   - **Data Location**: `s3://solana-data/` bucket in MinIO
   - **DAG**: `smart_trader_identification_dag`
   - **Documentation**: See `SMART_TRADER_PIPELINE.md`

2. **Webhook Notification Pipeline** (🚧 IN DEVELOPMENT)
   - **Purpose**: Real-time blockchain event processing via Helius webhooks
   - **Data Location**: `s3://webhook-data/` bucket in MinIO  
   - **Components**: FastAPI webhook listener, PySpark streaming, DuckDB analytics
   - **Status**: Core infrastructure complete, analytics layer in development

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

# Enable Flower UI for Celery monitoring
docker-compose --profile flower up -d
```

### DAG Development

```bash
# Test a DAG
docker-compose run airflow-cli airflow dags test [dag_id]

# Trigger a DAG run
docker-compose run airflow-cli airflow dags trigger [dag_id]

# List all DAGs
docker-compose run airflow-cli airflow dags list

# Pause/unpause a DAG
docker-compose run airflow-cli airflow dags pause [dag_id]
docker-compose run airflow-cli airflow dags unpause [dag_id]
```

### Accessing Services

- **Airflow API Server**: http://localhost:8080
- **Flower UI** (when enabled): http://localhost:5555
- **Redpanda Console**: http://localhost:8090
- **MinIO API**: http://localhost:9000
- **MinIO Console**: http://localhost:9001
- **Default credentials**: username=`airflow`, password=`airflow` (Airflow), username=`minioadmin`, password=`minioadmin123` (MinIO)

## Architecture

The project uses the official Apache Airflow Docker setup with the following services:

1. **PostgreSQL**: Metadata database
2. **Redis**: Celery message broker
3. **Redpanda**: Event streaming platform (Kafka-compatible)
4. **Redpanda Console**: Web UI for Redpanda monitoring
5. **MinIO**: S3-compatible object storage
6. **Airflow API Server**: REST API and web UI
7. **Airflow Scheduler**: Schedules DAG runs
8. **Airflow DAG Processor**: Parses DAG files
9. **Airflow Worker**: Executes tasks via Celery
10. **Airflow Triggerer**: Manages deferrable operators
11. **Flower** (optional): Celery monitoring UI

## Development Workflow

1. Place DAG files in the `dags/` directory - they will be automatically picked up
2. Custom plugins go in the `plugins/` directory
3. For custom Python dependencies:
   - Quick testing: Set `_PIP_ADDITIONAL_REQUIREMENTS` in `.env`
   - Production: Create a custom Dockerfile and uncomment the `build: .` line in docker-compose.yaml
4. Configuration changes can be made in `config/airflow.cfg`

## 🔧 Pipeline Configuration

### Centralized Configuration System
All Smart Trader pipeline settings are centralized in `dags/config/smart_trader_config.py`:

```bash
# View all configuration parameters
cat dags/config/smart_trader_config.py

# Test configuration import
cd dags && python3 -c "from config.smart_trader_config import TOKEN_LIMIT; print(f'Token Limit: {TOKEN_LIMIT}')"
```

### Key Configuration Categories:
- **API Limits**: Rate delays, batch sizes, pagination
- **Bronze Filters**: Token criteria, whale thresholds, transaction limits
- **Silver Transformation**: PnL timeframes, quality filters
- **Gold Analytics**: Performance tiers, profitability thresholds
- **Helius Integration**: Webhook settings, address limits
- **Infrastructure**: MinIO, PySpark, storage paths

### Configuration Benefits:
- ✅ **67 tunable parameters** in single file
- ✅ **Environment variable support** for deployments
- ✅ **No hardcoded values** in task modules
- ✅ **Easy production tuning** for different scales

## Additional Services

### Webhook Listener
A FastAPI service for receiving and storing webhook payloads:
- **Port**: 8000
- **Endpoints**: `/webhooks` (POST), `/health` (GET), `/docs` (Swagger UI), `/webhooks/status` (GET)
- **Data Storage**: `data/webhooks/` directory with date-based organization
- **Redpanda Integration**: Publishes all webhooks to Redpanda topic
- **ngrok Integration**: Use `./scripts/start-ngrok.sh` to expose the service

### Redpanda Commands

```bash
# View topics
docker exec claude_pipeline-redpanda-1 rpk topic list

# Create a topic
docker exec claude_pipeline-redpanda-1 rpk topic create [topic-name]

# Consume messages
docker exec claude_pipeline-redpanda-1 rpk topic consume webhooks --format json

# Produce a test message
echo '{"test": "message"}' | docker exec -i claude_pipeline-redpanda-1 rpk topic produce webhooks

# Check cluster health
docker exec claude_pipeline-redpanda-1 rpk cluster health

# View topic details
docker exec claude_pipeline-redpanda-1 rpk topic describe webhooks
```

### Testing Webhook to Redpanda Flow

```bash
# Send test webhook
curl -X POST http://localhost:8000/webhooks \
  -H "Content-Type: application/json" \
  -d '{"test": "data", "value": 123}'

# Verify in Redpanda Console
# http://localhost:8090

# Or use the consumer script
python scripts/redpanda_consumer.py
```

### MinIO Commands

```bash
# Access MinIO client inside container
docker exec claude_pipeline-minio mc --help

# List buckets
docker exec claude_pipeline-minio mc ls local/

# Create a bucket
docker exec claude_pipeline-minio mc mb local/[bucket-name]

# Upload file to bucket
docker exec claude_pipeline-minio mc cp [local-file] local/[bucket-name]/

# Download file from bucket
docker exec claude_pipeline-minio mc cp local/[bucket-name]/[file] [local-destination]

# Remove object
docker exec claude_pipeline-minio mc rm local/[bucket-name]/[file]

# Get bucket stats
docker exec claude_pipeline-minio mc du local/[bucket-name]
```

### Testing MinIO Integration

```bash
# Run MinIO test script
cd scripts
source venv/bin/activate
python minio_test.py

# The test script will:
# - Test connection to MinIO
# - Create/verify test bucket
# - Upload/download/delete test objects
# - Verify all operations completed successfully
```

### Viewing MinIO Data

```bash
# MinIO Web Console (easiest)
# http://localhost:9001
# Login: minioadmin / minioadmin123

# List objects via CLI
docker exec claude_pipeline-minio mc ls local/[bucket-name]/ --recursive

# Copy file from MinIO to local
docker exec claude_pipeline-minio mc cp local/[bucket]/[file] /tmp/[filename]

# Download via Python
cd scripts && source venv/bin/activate
python -c "
import boto3
from botocore.client import Config
s3 = boto3.client('s3', endpoint_url='http://localhost:9000', 
                  aws_access_key_id='minioadmin', 
                  aws_secret_access_key='minioadmin123')
s3.download_file('bucket-name', 'object-key', 'local-file')
"
```

## PySpark Streaming Integration

The project includes PySpark streaming capabilities for real-time data processing between Redpanda and MinIO.

### PySpark Streaming DAG (`pyspark_streaming_pipeline`)

The production PySpark streaming DAG processes webhook data from Redpanda to bronze layer in MinIO:

**Schedule**: Every 5 minutes  
**Memory Optimization**: Configured with 2GB driver/executor memory  
**Batch Limiting**: Processes only the most recent 1000 messages to prevent OutOfMemoryError  

```bash
# Trigger the streaming pipeline
docker-compose run airflow-cli airflow dags trigger pyspark_streaming_pipeline

# Monitor pipeline status
docker-compose run airflow-cli airflow dags list-runs --dag-id pyspark_streaming_pipeline

# Check bronze data in MinIO
docker exec claude_pipeline-minio mc ls local/webhook-data/bronze/webhooks/ --recursive
```

### Memory Management Fix (June 2025)

**Issue**: PySpark DAG was failing with `java.lang.OutOfMemoryError` when trying to process 971K+ messages from Redpanda topic.

**Solution Applied**:
1. **Limited Batch Size**: Read only most recent 1000 messages using calculated offset range
2. **Increased Memory**: Set `spark.driver.memory=2g` and `spark.executor.memory=2g`
3. **Batch Processing**: Changed from streaming to batch mode for better memory control
4. **Offset Calculation**: Uses `startingOffsets` with calculated offset to read latest 1000 messages

### PySpark Test Scripts

```bash
# Setup environment
cd scripts
source venv/bin/activate
pip install -r requirements.txt

# Test 1: Redpanda → PySpark → Local Directory
python pyspark_redpanda_test.py

# Test 2: Local Directory → PySpark → MinIO  
python pyspark_minio_test.py
```

### PySpark Data Flow

```
Webhooks → Redpanda (971K+ messages) → PySpark Streaming (last 1000) → Bronze Layer → MinIO
                                                    ↓
                                          Silver Transformation → Analytics
```

**Features:**
- **Memory-Optimized Processing**: Handles large topic volumes without OOM errors
- **Data Transformation**: Schema parsing, metadata addition, timestamp processing
- **Partitioning**: Date/hour-based partitioning for efficient querying
- **Storage Formats**: Parquet files for optimal analytics performance
- **S3A Integration**: Direct MinIO integration using S3-compatible API
- **Offset Management**: Smart offset calculation for recent data processing

### PySpark Configuration

The streaming DAG demonstrates:
- Kafka consumer configuration for Redpanda with offset limits
- Memory-optimized Spark session configuration
- S3A filesystem setup for MinIO connectivity
- Batch processing with efficient resource usage
- Error handling and data validation

### Production Considerations

For production deployment:
- **Memory Allocation**: 2GB+ driver/executor memory for large datasets
- **Batch Size Limits**: Process 1000-5000 messages per batch to prevent OOM
- **Offset Management**: Track processed offsets to avoid reprocessing
- **Resource Monitoring**: Monitor Java heap usage and Spark metrics
- **Checkpointing**: Use for fault-tolerant processing in streaming mode

## DuckDB Analytics Integration

The project includes DuckDB for analytical processing and silver layer transformations in the medallion architecture.

### DuckDB Commands

```bash
# Access DuckDB container
docker exec -it claude_pipeline-duckdb /bin/sh

# Run DuckDB CLI
docker exec claude_pipeline-duckdb python3 -c "import duckdb; conn = duckdb.connect('/data/analytics.duckdb'); print('DuckDB connected')"

# Test DuckDB + MinIO integration
docker exec claude_pipeline-duckdb python3 /scripts/duckdb_minio_test.py

# Run bronze data analysis
docker exec claude_pipeline-duckdb python3 /scripts/duckdb_phase2_bronze_analysis.py

# Execute silver layer transformations
docker exec claude_pipeline-duckdb python3 /scripts/duckdb_phase3_silver_design.py

# Final end-to-end validation
docker exec claude_pipeline-duckdb python3 /scripts/duckdb_final_validation.py
```

### DuckDB Configuration

DuckDB is configured with S3/MinIO integration:
- **Database**: `/data/analytics.duckdb` (persistent volume)
- **S3 Endpoint**: `minio:9000` (internal Docker network)
- **Extensions**: httpfs (for S3 access), parquet (built-in)
- **Dependencies**: duckdb, boto3, pytz

### Medallion Architecture Data Flow

```
Webhooks → Redpanda → PySpark (Bronze) → MinIO → DuckDB → Silver → MinIO
Token Data → BirdEye API → Bronze Layers → Silver Transformations → Analytics
Whale Wallets → BirdEye API → Bronze Transactions → PySpark PnL → Silver Analytics
```

#### Data Layers Overview

**Bronze Layer** (`s3://solana-data/bronze/`):
- `token_whales/`: Raw whale holder data from BirdEye API
- `wallet_transactions/`: Transaction history for whale wallets
- `processed-webhooks/`: Raw webhook data with processing metadata
- All partitioned by date with processing metadata

**Silver Layer**:
- **Webhook Data** (`s3://webhook-data/silver-webhooks/`):
  - `webhook_events/`: Cleaned, deduplicated event data
  - `transaction_details/`: Solana transaction-specific fields  
  - `data_quality_metrics/`: Data quality monitoring
- **Wallet Analytics** (`s3://solana-data/silver/wallet_pnl/`):
  - Token-level PnL metrics with FIFO cost basis calculation
  - Portfolio-level aggregated performance analytics
  - Multi-timeframe analysis (all/week/month/quarter periods)

#### Bronze Wallet Transactions Layer

**Purpose**: Stores transaction history for whale wallets identified in the token analysis
**Source**: BirdEye API wallet trade history endpoint
**Schedule**: Every 6 hours via Airflow DAG

**Schema Features**:
- Complete transaction details (from/to tokens, amounts, prices)
- Transaction type classification (BUY/SELL/UNKNOWN)
- **PnL Processing Fields**: `processed_for_pnl`, `pnl_processed_at`, `pnl_processing_status`
- Comprehensive metadata and batch tracking
- Mock data fallback for API failures

**Data Flow**:
```
Bronze Token Whales → Filter Unfetched → BirdEye API → Transform → Bronze Wallet Transactions
```

**Key Files**:
- DAG: `dags/bronze_wallet_transactions_dag.py`
- Output: `s3://solana-data/bronze/wallet_transactions/date=YYYY-MM-DD/`
- Status: `status_BATCH_ID.json` with processed wallet tracking

#### Silver Wallet PnL Layer

**Purpose**: Calculates comprehensive profit/loss metrics for whale wallets using FIFO methodology
**Source**: Bronze wallet transactions layer (unprocessed records)
**Schedule**: Every 12 hours via Airflow DAG
**Technology**: PySpark with S3A integration

**PnL Features**:
- **FIFO Cost Basis**: First-in-first-out lot tracking for accurate PnL calculation
- **Multi-Timeframe Analysis**: all/week/month/quarter periods
- **Token-Level Metrics**: Individual token performance per wallet
- **Portfolio-Level Aggregation**: Combined metrics across all tokens
- **Trading Analytics**: Win rate, ROI, holding time, trade frequency

**Schema Structure** (23 comprehensive fields):
- **Core PnL**: realized_pnl, unrealized_pnl, total_pnl
- **Trading Metrics**: trade_count, win_rate, total_bought, total_sold, roi
- **Position Data**: current_position_tokens, current_position_cost_basis, current_position_value
- **Time Analytics**: avg_holding_time_hours, trade_frequency_daily
- **Processing Metadata**: calculation_date, time_period, batch_id, processed_at

**Data Flow**:
```
Bronze Wallet Transactions → Filter Unprocessed → PySpark FIFO UDF → Silver PnL Metrics
                           ↓
Update Bronze Processing Status (processed_for_pnl = true)
```

**Output Structure**:
- **Token-Level PnL**: `s3://solana-data/silver/wallet_pnl/` (partitioned by year/month/timeframe)
- **Portfolio-Level PnL**: Same location with `token_address = "ALL_TOKENS"`
- **Processing Efficiency**: Handles unprocessed transactions only
- **Success Markers**: `_SUCCESS_BATCH_ID` files for monitoring

**Key Files**:
- DAG: `dags/silver_wallet_pnl_dag.py`
- UDF Implementation: FIFO cost basis calculation with lot tracking
- Output: `s3://solana-data/silver/wallet_pnl/calculation_year=YYYY/calculation_month=MM/time_period=PERIOD/`

### DuckDB Schema Structure

**Bronze View**: `bronze.webhooks`
- Deduplicated view of bronze parquet data
- ROW_NUMBER() partitioning for unique records

**Silver Tables**:
- `silver.webhook_events`: Event categorization and standardization
- `silver.transaction_details`: Transaction-specific parsing
- `silver.data_quality_metrics`: Quality monitoring and validation

### Sample DuckDB Queries

```sql
-- Query bronze data from MinIO
SELECT COUNT(*) FROM read_parquet('s3://webhook-data/processed-webhooks/**/*.parquet');

-- Access silver layer
SELECT event_type, COUNT(*) FROM silver.webhook_events GROUP BY event_type;

-- Query silver data directly from MinIO
SELECT * FROM parquet_scan('s3://webhook-data/silver-webhooks/webhook_events/**/*.parquet') LIMIT 5;

-- Data quality analysis
SELECT * FROM silver.data_quality_metrics;
```

### Performance Characteristics

- **Query Speed**: Sub-millisecond analytical queries
- **Data Throughput**: Handles 32+ bronze records → 4 unique silver events
- **Storage Efficiency**: Parquet compression with date partitioning
- **Deduplication**: 100% data integrity preservation

## Pipeline-Specific Commands

### Smart Trader Identification Pipeline

**Data Location**: `s3://solana-data/` (see `SMART_TRADER_PIPELINE.md` for full details)

```bash
# Run complete pipeline
docker compose run airflow-cli airflow dags trigger smart_trader_identification

# Individual pipeline components
docker compose run airflow-cli airflow dags trigger bronze_token_whales
docker compose run airflow-cli airflow dags trigger silver_wallet_pnl  
docker compose run airflow-cli airflow dags trigger gold_top_traders

# Monitor pipeline execution
docker compose logs -f airflow-worker

# DuckDB analytics
docker exec claude_pipeline-duckdb python3 /scripts/analyze_silver_pnl_data.py
```

### Webhook Notification Pipeline  

**Data Location**: `s3://webhook-data/`

```bash
# PySpark streaming pipeline (processes webhooks → bronze layer)
docker compose run airflow-cli airflow dags trigger pyspark_streaming_pipeline

# Silver webhook transformations (DuckDB-based)
docker compose run airflow-cli airflow dags trigger silver_webhook_transformation

# Test webhook ingestion
curl -X POST http://localhost:8000/webhooks \
  -H "Content-Type: application/json" \
  -d '{"test": "data", "value": 123}'

# Monitor webhook processing
docker exec claude_pipeline-duckdb python3 /scripts/duckdb_minio_test.py
```

## Infrastructure Learnings & Requirements

### Critical Requirements
1. **Java Dependency for PySpark**: PySpark requires Java to be installed. We use a custom Dockerfile that installs OpenJDK 17.
2. **Custom Docker Image**: The `_PIP_ADDITIONAL_REQUIREMENTS` approach is only for testing. Production requires building a custom image with all dependencies.
3. **Network Names**: Services must use internal Docker network names (e.g., `redpanda:9092`, `http://minio:9000`) not localhost when communicating between containers.

### PySpark Integration Fix (June 2025)
**Issue**: PySpark tasks were failing with "up_for_retry" status in consolidated DAG.
**Root Cause**: Task definition pattern mismatch between working streaming DAG and consolidated DAG.

**Solution Applied**:
1. **Task Pattern**: Changed from `PythonOperator` to `@task(dag=dag)` decorator pattern
2. **SparkSession Location**: Moved SparkSession creation inside task function (not in helper modules)
3. **Docker Dependencies**: Updated `Dockerfile.airflow` with Java 17 + PySpark 3.5.0 + PyArrow + boto3
4. **JAR Configuration**: Used exact JAR packages from working streaming DAG

**Key Learning**: Working PySpark pattern in Airflow:
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

### Airflow Configuration Issues
1. **XCom Backend**: The default config may have incorrect xcom_backend settings. Use `airflow.models.xcom.BaseXCom`.
2. **API Server vs Webserver**: Airflow 2.10.x uses `webserver` command, not `api-server`.
3. **Health Check Endpoint**: Use `/health` endpoint, not `/api/v2/version` for container health checks.
4. **Execution API URL**: Comment out `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` if causing issues.

### Service Dependencies
1. **DAG Processor Issues**: May experience restart loops with certain configurations. Monitor with `docker ps`.
2. **Worker Required**: Celery Executor requires airflow-worker service to be running for task execution.
3. **Database Migrations**: Fresh installs work better than upgrades when config changes significantly.

### Variable Management
1. **Airflow Variables via CLI**: Use `docker compose run airflow-cli variables set KEY "VALUE"` for setting variables.
2. **Required Variables for PySpark DAG**:
   - REDPANDA_BROKERS (use internal name: `redpanda:9092`)
   - WEBHOOK_TOPIC
   - MINIO_ENDPOINT (use internal name: `http://minio:9000`)
   - MINIO_ACCESS_KEY/SECRET_KEY
   - MINIO_BUCKET
   - LOCAL_DATA_PATH
   - CHECKPOINT_PATH
   - PROCESSING_WINDOW_MINUTES

### Debugging Tips
1. **Check Service Logs**: Use `docker logs <container-name>` for detailed error messages.
2. **Orphan Containers**: The warning about orphan containers is normal and can be ignored.
3. **PySpark Dependencies**: First run downloads Spark JARs which takes time (spark-sql-kafka, hadoop-aws, etc.)

## Important Notes

- The project currently has example DAGs disabled (`AIRFLOW__CORE__LOAD_EXAMPLES: 'false'`)
- New DAGs are paused by default (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'`)
- Ensure proper file permissions on Linux by setting `AIRFLOW_UID` in `.env` to your user ID
- The Airflow image version is controlled via `AIRFLOW_IMAGE_NAME` in `.env`
- NGROK_TOKEN is stored in `.env` for webhook tunneling
- Redpanda configuration (brokers, topics) is stored in `.env`
- MinIO configuration (credentials) is stored in `.env`
- **PySpark Fix**: Use `@task` decorator pattern with SparkSession created inside task function