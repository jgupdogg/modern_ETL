# Modern ETL Pipeline - Dual Pipeline Architecture

A comprehensive data pipeline system with two production-ready processing pipelines: Smart Trader Identification and Webhook Notifications. Built with Apache Airflow, PySpark, MinIO, and cloud-native technologies implementing complete medallion architecture.

## 🚀 Pipeline Overview

### 1. Smart Trader Identification Pipeline ✅ **PRODUCTION READY**
- **Purpose**: Identifies profitable Solana cryptocurrency traders
- **Technology**: BirdEye API → PySpark → MinIO → Helius monitoring
- **Data**: `s3://solana-data/` bucket with bronze/silver/gold layers
- **Status**: Fully operational with optimized data processing

### 2. Webhook Notification Pipeline ✅ **PRODUCTION READY**  
- **Purpose**: Real-time blockchain event processing and analytics
- **Technology**: FastAPI → Redpanda → PySpark → DuckDB → MinIO
- **Data**: `s3://webhook-data/` bucket with complete medallion architecture
- **Status**: Bronze → Silver → Gold layers with centralized configuration

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
- **Webhook Pipeline**: See `CLAUDE.md` for medallion architecture commands and configuration

### Key Commands
```bash
# Smart Trader Pipeline
docker compose run airflow-cli airflow dags trigger smart_trader_identification

# Webhook Pipeline (Complete Medallion Architecture)
docker compose run airflow-cli airflow dags trigger pyspark_streaming_pipeline       # Bronze
docker compose run airflow-cli airflow dags trigger silver_webhook_transformation   # Silver
docker compose run airflow-cli airflow dags trigger gold_webhook_analytics          # Gold

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
│   ├── pyspark_streaming_dag.py             # Webhook bronze layer processing
│   ├── silver_webhook_transformation_dag.py # Webhook silver layer processing
│   ├── gold_webhook_analytics_dag.py        # Webhook gold layer analytics
│   ├── config/                  # 🆕 Centralized configuration
│   │   ├── smart_trader_config.py  # 67 configurable parameters
│   │   └── webhook_config.py       # 75+ webhook pipeline parameters
│   ├── tasks/                   # Modular task implementations
│   │   ├── bronze_tasks.py      # Data ingestion (BirdEye API)
│   │   ├── silver_tasks.py      # Data transformation (PySpark)
│   │   ├── gold_tasks.py        # Analytics (Top trader selection)
│   │   └── helius_tasks.py      # Integration (Webhook management)
│   └── birdeye_client/         # BirdEye API client library
├── dbt/                        # DBT transformations (webhook pipeline)
├── data/                       # Local data storage and checkpoints
├── scripts/                    # Utility and analysis scripts
├── services/                   # Microservices
│   └── webhook-listener/       # FastAPI webhook ingestion service
└── docker-compose.yaml        # Container orchestration
```

### 🔧 Configuration Architecture

The project uses a **centralized configuration system** with:
- **Smart Trader Pipeline**: 67 configurable parameters in `smart_trader_config.py`
- **Webhook Pipeline**: 75+ configurable parameters in `webhook_config.py`
- **Single source of truth** for each pipeline with no hardcoded values
- **Environment variable support** for deployment flexibility
- **Layer-organized settings**: Bronze, Silver, Gold layer configurations
- **Production-ready tuning**: Easy parameter adjustment for different environments

## 🚧 Project Status

### Smart Trader Identification ✅ **PRODUCTION READY**
- Complete medallion architecture implementation
- FIFO cost basis calculation with PySpark
- Elite trader classification and performance tiers  
- Helius webhook integration for real-time monitoring
- Optimized data storage with proper partitioning

### Webhook Notification Pipeline ✅ **PRODUCTION READY**
- Complete medallion architecture: Bronze → Silver → Gold ✅
- Bronze layer: PySpark streaming from Redpanda to MinIO ✅
- Silver layer: DuckDB transformations with event categorization ✅  
- Gold layer: Trending tokens and whale activity analytics ✅
- Centralized configuration with 75+ tunable parameters ✅

## 📝 License

MIT