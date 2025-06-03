# Modern ETL Pipeline

A modern data pipeline architecture using Apache Airflow, FastAPI, and cloud-native technologies.

## 🚀 Current Features

- **Apache Airflow** orchestration with Celery executor
- **FastAPI webhook listener** for real-time data ingestion
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
```

3. Start the services:
```bash
docker-compose up -d
```

## 🔧 Services

- **Airflow Webserver**: http://localhost:8080 (airflow/airflow)
- **Webhook Listener**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs
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

## 🚧 Roadmap

- [ ] RedPanda integration for event streaming
- [ ] MinIO for object storage
- [ ] PySpark for data processing
- [ ] DBT for data transformation
- [ ] PostgreSQL enhancements

## 📝 License

MIT