# Modern ETL Pipeline - Dual Pipeline Architecture

A comprehensive data pipeline system with two distinct processing pipelines: Smart Trader Identification (production-ready) and Webhook Notifications (in development). Built with Apache Airflow, PySpark, MinIO, and cloud-native technologies implementing medallion architecture.

## 🚀 Pipeline Overview

### 1. Smart Trader Identification Pipeline ✅ **PRODUCTION READY**
- **Purpose**: Identifies profitable Solana cryptocurrency traders
- **Technology**: BirdEye API → PySpark → MinIO → Helius monitoring
- **Data**: `s3://solana-data/` bucket with bronze/silver/gold layers
- **Status**: Fully operational with optimized data processing

### 2. Webhook Notification Pipeline 🚧 **IN DEVELOPMENT**  
- **Purpose**: Real-time blockchain event processing and analytics
- **Technology**: FastAPI → Redpanda → PySpark → DuckDB → MinIO
- **Data**: `s3://webhook-data/` bucket with streaming architecture
- **Status**: Core infrastructure complete, analytics layer in development

## 🏗️ Key Technologies

- **Apache Airflow**: Workflow orchestration with Celery executor
- **PySpark**: Distributed data processing and FIFO cost basis calculations
- **MinIO**: S3-compatible object storage with partitioned data
- **DuckDB**: Analytical queries and data exploration  
- **BirdEye API**: Cryptocurrency market data and whale tracking
- **Redpanda**: Event streaming for real-time data processing
- **FastAPI**: Webhook listener for blockchain event ingestion
- **Docker Compose**: Containerized deployment and orchestration

## 📋 Quick Start

**Prerequisites**: Docker, Docker Compose, API keys (BirdEye, Helius, ngrok)

1. **Clone and Setup**:
```bash
git clone https://github.com/jgupdogg/modern_ETL.git
cd modern_ETL
cp .env.example .env  # Add your API keys
```

2. **Start Services**:
```bash
docker-compose up -d
```

3. **Configure Airflow Variables**:
```bash
docker compose run airflow-cli airflow variables set BIRDSEYE_API_KEY "your_api_key"
```

4. **Access Services**:
- **Airflow**: http://localhost:8080 (airflow/airflow)
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin123)
- **Webhook API**: http://localhost:8000/docs

## 📚 Documentation

### Pipeline-Specific Documentation
- **Smart Trader Pipeline**: See `SMART_TRADER_PIPELINE.md` for complete details
- **Operational Commands**: See `CLAUDE.md` for Docker commands and service management
- **Webhook Pipeline**: In development, see `CLAUDE.md` for current status

### Key Commands
```bash
# Smart Trader Pipeline
docker compose run airflow-cli airflow dags trigger smart_trader_identification

# Webhook Pipeline  
docker compose run airflow-cli airflow dags trigger pyspark_streaming_pipeline

# Service Management
docker-compose up -d        # Start all services
docker-compose logs -f      # View logs
docker-compose down         # Stop services
```

## 📁 Project Structure

```
.
├── CLAUDE.md                    # Claude Code operational documentation
├── SMART_TRADER_PIPELINE.md     # Smart trader pipeline detailed docs
├── README.md                    # Project overview (this file)
├── config/                      # Airflow configuration
├── dags/                       # Airflow DAGs
│   ├── smart_trader_identification_dag.py  # Main smart trader pipeline
│   ├── tasks/                   # Modular task implementations
│   └── birdeye_client/         # BirdEye API client library
├── dbt/                        # DBT transformations (webhook pipeline)
├── data/                       # Local data storage and checkpoints
├── scripts/                    # Utility and analysis scripts
├── services/                   # Microservices
│   └── webhook-listener/       # FastAPI webhook ingestion service
└── docker-compose.yaml        # Container orchestration
```

## 🚧 Project Status

### Smart Trader Identification ✅ **PRODUCTION READY**
- Complete medallion architecture implementation
- FIFO cost basis calculation with PySpark
- Elite trader classification and performance tiers  
- Helius webhook integration for real-time monitoring
- Optimized data storage with proper partitioning

### Webhook Notification Pipeline 🚧 **IN DEVELOPMENT**
- Core infrastructure: FastAPI → Redpanda → PySpark → MinIO ✅
- Bronze layer streaming processing ✅
- Silver layer transformations with DuckDB ✅  
- Analytics and gold layer development 🚧

## 📝 License

MIT