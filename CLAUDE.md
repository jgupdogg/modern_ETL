# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Apache Airflow project for orchestrating data pipelines, using Docker Compose for containerization. The project uses Celery Executor for distributed task execution and includes PostgreSQL for metadata storage, Redis as the Celery message broker, Redpanda for event streaming, and MinIO for object storage.

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
Webhooks → Redpanda → PySpark Streaming → Local Files → PySpark Batch → MinIO
```

**Features:**
- **Structured Streaming**: Real-time consumption from Redpanda (Kafka-compatible)
- **Data Transformation**: Schema parsing, metadata addition, timestamp processing
- **Partitioning**: Date/hour-based partitioning for efficient querying
- **Storage Formats**: Parquet files for optimal analytics performance
- **S3A Integration**: Direct MinIO integration using S3-compatible API
- **Checkpointing**: Fault-tolerant processing with automatic recovery

### PySpark Configuration

The test scripts demonstrate:
- Kafka consumer configuration for Redpanda
- S3A filesystem setup for MinIO connectivity
- Structured streaming with checkpointing
- Batch processing with partitioning
- Error handling and data validation

### Scaling to Production

For production deployment, consider:
- Docker service for PySpark streaming applications
- Airflow DAGs for monitoring and orchestration
- Resource allocation (memory/cores) based on data volume
- Multiple streaming applications for different data types
- Dead letter queues for error handling

## Important Notes

- The project currently has example DAGs disabled (`AIRFLOW__CORE__LOAD_EXAMPLES: 'false'`)
- New DAGs are paused by default (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'`)
- Ensure proper file permissions on Linux by setting `AIRFLOW_UID` in `.env` to your user ID
- The Airflow image version is controlled via `AIRFLOW_IMAGE_NAME` in `.env`
- NGROK_TOKEN is stored in `.env` for webhook tunneling
- Redpanda configuration (brokers, topics) is stored in `.env`
- MinIO configuration (credentials) is stored in `.env`

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
```

#### Data Layers Overview

**Bronze Layer** (`s3://solana-data/bronze/`):
- `token_whales/`: Raw whale holder data from BirdEye API
- `wallet_transactions/`: Transaction history for whale wallets
- `processed-webhooks/`: Raw webhook data with processing metadata
- All partitioned by date with processing metadata

**Silver Layer** (`s3://webhook-data/silver-webhooks/`):
- `webhook_events/`: Cleaned, deduplicated event data
- `transaction_details/`: Solana transaction-specific fields
- `data_quality_metrics/`: Data quality monitoring

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

## Infrastructure Learnings & Requirements

### Critical Requirements
1. **Java Dependency for PySpark**: PySpark requires Java to be installed. We use a custom Dockerfile that installs OpenJDK 17.
2. **Custom Docker Image**: The `_PIP_ADDITIONAL_REQUIREMENTS` approach is only for testing. Production requires building a custom image with all dependencies.
3. **Network Names**: Services must use internal Docker network names (e.g., `redpanda:9092`, `http://minio:9000`) not localhost when communicating between containers.

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
3. **PySpark Dependencies**: First run downloads Spark JARs which takes time (spark-sql-kafka, hadoop-aws, etc.).