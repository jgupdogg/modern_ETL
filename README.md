# Modern ETL Pipeline

A modern data pipeline architecture using Apache Airflow, FastAPI, Redpanda, and cloud-native technologies.

## 🚀 Current Features

- **Apache Airflow** orchestration with Celery executor
- **FastAPI webhook listener** for real-time data ingestion
- **Redpanda** for event streaming and real-time data processing
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

## 🚧 Roadmap

- [x] RedPanda integration for event streaming
- [ ] MinIO for object storage
- [ ] PySpark for data processing
- [ ] DBT for data transformation
- [ ] PostgreSQL enhancements

## 📝 License

MIT