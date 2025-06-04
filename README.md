# Modern ETL Pipeline

A modern data pipeline architecture using Apache Airflow, FastAPI, Redpanda, MinIO, and cloud-native technologies.

## 🚀 Current Features

- **Apache Airflow** orchestration with Celery executor
- **FastAPI webhook listener** for real-time data ingestion
- **Redpanda** for event streaming and real-time data processing
- **MinIO** for S3-compatible object storage
- **PySpark Streaming** for real-time data processing and analytics
- **Automatic ngrok tunneling** for webhook development
- **Helius integration** for Solana blockchain data
- **Docker Compose** based deployment

## 📋 Prerequisites

- Docker and Docker Compose
- ngrok account (for webhook tunneling)
- Helius API key (for blockchain data)

## 🛠️ Setup

1. Clone the repository:
```bash
git clone https://github.com/jgupdogg/modern_ETL.git
cd modern_ETL
```

2. Create `.env` file with your credentials:
```bash
# Copy from .env.example and fill in your values
NGROK_TOKEN=your_ngrok_token
HELIUS_API_KEY=your_helius_api_key
HELIUS_ADDRESSES=address1,address2  # Solana addresses to monitor

# Redpanda Configuration (optional - defaults shown)
REDPANDA_BROKERS=redpanda:9092
WEBHOOK_TOPIC=webhooks

# MinIO Configuration (optional - defaults shown)
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
```

3. Start the services:
```bash
docker-compose up -d
```

## 🔧 Services

- **Airflow Webserver**: http://localhost:8080 (airflow/airflow)
- **Webhook Listener**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs
- **Redpanda Console**: http://localhost:8090
- **Redpanda Broker**: localhost:19092 (external), redpanda:9092 (internal)
- **MinIO API**: http://localhost:9000
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin123)
- **ngrok Dashboard**: http://localhost:4040

## 📁 Project Structure

```
.
├── config/              # Airflow configuration
├── dags/               # Airflow DAGs
├── data/               # Local data storage
│   └── webhooks/       # Webhook payloads (organized by date)
├── logs/               # Application logs
├── plugins/            # Airflow plugins
├── scripts/            # Utility scripts
├── services/           # Microservices
│   └── webhook-listener/   # FastAPI webhook service
├── docker-compose.yaml
└── CLAUDE.md          # AI assistant documentation
```

## 📊 Redpanda Integration

The webhook listener now publishes all incoming webhooks to Redpanda for real-time stream processing:

- **Topic**: `webhooks` (configurable via `WEBHOOK_TOPIC`)
- **Message Key**: Webhook message ID
- **Message Value**: Full webhook payload with metadata
- **Consumer Script**: `scripts/redpanda_consumer.py` for testing

### Testing Redpanda

```bash
# Send a test webhook
curl -X POST http://localhost:8000/webhooks \
  -H "Content-Type: application/json" \
  -d '{"test": "data"}'

# View messages in Redpanda Console
# http://localhost:8090

# Or use the consumer script
docker run --rm --network claude_pipeline_default \
  -v $(pwd)/scripts/redpanda_consumer.py:/consumer.py \
  -e REDPANDA_BROKERS=redpanda:9092 \
  python:3.11 bash -c "pip install -q aiokafka && python /consumer.py"
```

## 💾 MinIO Object Storage

MinIO provides S3-compatible object storage for the pipeline:

- **Buckets**: Organize data by project, date, or type
- **S3 Compatibility**: Works with all S3 client libraries
- **Web Console**: Manage buckets and objects via UI
- **Test Script**: `scripts/minio_test.py` for validation

### Testing MinIO

```bash
# Run the test script
cd scripts
source venv/bin/activate
python minio_test.py

# Or test via CLI
docker exec claude_pipeline-minio mc ls local/
docker exec claude_pipeline-minio mc mb local/my-bucket
```

## ⚡ PySpark Streaming

Real-time data processing pipeline using PySpark for streaming analytics:

### Data Flow
```
Webhooks → Redpanda → PySpark Streaming → Local Files → PySpark Batch → MinIO
```

### Features
- **Structured Streaming**: Real-time consumption from Redpanda (Kafka-compatible)
- **Data Transformation**: Schema parsing, metadata addition, timestamp processing
- **Fault Tolerance**: Checkpointing for automatic recovery
- **Partitioned Storage**: Date/hour-based partitioning in Parquet format
- **S3A Integration**: Direct MinIO storage using S3-compatible API

### Testing PySpark Integration

```bash
# Setup environment
cd scripts
source venv/bin/activate
pip install -r requirements.txt

# Test streaming pipeline: Redpanda → PySpark → Local
python pyspark_redpanda_test.py

# Test batch pipeline: Local → PySpark → MinIO
python pyspark_minio_test.py

# Send test data
curl -X POST http://localhost:8000/webhooks \
  -H "Content-Type: application/json" \
  -d '{"test": "pyspark", "value": 123}'

# View results in MinIO Console
# http://localhost:9001 (minioadmin/minioadmin123)
```

### Production Considerations
- Resource allocation (memory/cores) based on data volume
- Multiple streaming applications for different data types
- Airflow DAGs for monitoring and orchestration
- Dead letter queues for error handling

## 🚧 Roadmap

- [x] RedPanda integration for event streaming
- [x] MinIO for object storage
- [x] PySpark for data processing
- [ ] DBT for data transformation
- [ ] PostgreSQL enhancements
- [ ] Production PySpark service deployment

## 📝 License

MIT