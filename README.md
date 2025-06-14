# Modern ETL Pipeline - Medallion Architecture

A modern data pipeline architecture using Apache Airflow, PySpark, DBT, MinIO, and cloud-native technologies. Implements a complete medallion architecture (Bronze → Silver → Gold) for cryptocurrency/Solana data analytics.

## 🚀 Current Features

- **Medallion Architecture**: Bronze, Silver, and Gold data layers
- **Apache Airflow** orchestration with Celery executor
- **PySpark** for distributed data processing and complex analytics
- **DBT** for SQL-based data transformations
- **MinIO** for S3-compatible object storage with partitioned data
- **BirdEye API Integration** for cryptocurrency market data
- **DuckDB** for analytical queries and data exploration
- **FastAPI webhook listener** for real-time data ingestion
- **Redpanda** for event streaming and real-time data processing
- **Automatic ngrok tunneling** for webhook development
- **Docker Compose** based deployment

## 🏗️ Medallion Architecture

The pipeline implements a complete medallion architecture for cryptocurrency data analytics:

### Bronze Layer (Raw Data)
- **Token Whales**: Top holder data from BirdEye API
- **Wallet Transactions**: Complete transaction history for whale wallets
- **Webhook Data**: Raw blockchain events and notifications
- **Data Format**: Parquet files with processing metadata

### Silver Layer (Cleaned & Enriched)
- **Tracked Tokens**: Filtered high-performance tokens with momentum indicators
- **Wallet PnL**: Comprehensive profit/loss metrics using FIFO cost basis
- **Transaction Details**: Parsed and standardized transaction data
- **Data Quality Metrics**: Validation and monitoring data

### Gold Layer (Analytics Ready)
- **Top Traders**: Elite performers with risk-adjusted metrics
- **Performance Tiers**: Elite, Strong, and Promising trader classifications
- **Portfolio Analytics**: Aggregated performance across all positions
- **Trading Insights**: Behavioral patterns and consistency scores

### Data Flow
```
BirdEye API → Bronze → DBT/PySpark → Silver → PySpark Analytics → Gold
     ↓           ↓              ↓             ↓                    ↓
  MinIO      MinIO          MinIO         MinIO                MinIO
(Raw)    (Processed)   (Transformed)  (Enriched)           (Analytics)
```

## 📋 Prerequisites

- Docker and Docker Compose
- ngrok account (for webhook tunneling)  
- BirdEye API key (for cryptocurrency data)
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
BIRDSEYE_API_KEY=your_birdeye_api_key
HELIUS_API_KEY=your_helius_api_key
HELIUS_ADDRESSES=address1,address2  # Solana addresses to monitor

# Redpanda Configuration (optional - defaults shown)
REDPANDA_BROKERS=redpanda:9092
WEBHOOK_TOPIC=webhooks

# MinIO Configuration (optional - defaults shown)
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
```

3. Set Airflow Variables (required for cryptocurrency data):
```bash
# Set BirdEye API key in Airflow
docker compose run airflow-cli airflow variables set BIRDSEYE_API_KEY "your_api_key_here"
```

4. Start the services:
```bash
docker-compose up -d
```

## 🔧 Services

### Core Services
- **Airflow Webserver**: http://localhost:8080 (airflow/airflow)
- **DuckDB Container**: Analytical queries and data exploration
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin123)
- **MinIO API**: http://localhost:9000

### Development Services  
- **Webhook Listener**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs
- **Redpanda Console**: http://localhost:8090
- **Redpanda Broker**: localhost:19092 (external), redpanda:9092 (internal)
- **ngrok Dashboard**: http://localhost:4040

### Key DAGs
- **bronze_token_whales**: Fetches top holder data every 4 hours
- **bronze_wallet_transactions**: Gets transaction history every 6 hours  
- **silver_wallet_pnl**: Calculates PnL metrics every 12 hours
- **gold_top_traders**: Creates top trader analytics (2 hours after silver)
- **dbt_silver_transformation**: DBT transformations every 10 minutes

## 📁 Project Structure

```
.
├── config/              # Airflow configuration
├── dags/               # Airflow DAGs
│   ├── birdeye_client/     # BirdEye API client
│   ├── bronze_*.py         # Bronze layer ingestion DAGs
│   ├── silver_*.py         # Silver layer transformation DAGs  
│   ├── gold_*.py           # Gold layer analytics DAGs
│   └── dbt_*.py            # DBT orchestration DAGs
├── dbt/                # DBT project
│   ├── models/             # Data transformation models
│   │   ├── bronze/         # Bronze layer views
│   │   └── silver/         # Silver layer tables
│   ├── macros/             # Reusable SQL macros
│   └── tests/              # Data quality tests
├── data/               # Local data storage
│   ├── checkpoints/        # PySpark streaming checkpoints
│   └── webhooks/           # Webhook payloads (organized by date)
├── logs/               # Application logs
├── plugins/            # Airflow plugins
├── scripts/            # Utility and test scripts
├── services/           # Microservices
│   ├── birdeye_client/     # BirdEye API service
│   └── webhook-listener/   # FastAPI webhook service
├── docker-compose.yaml
└── CLAUDE.md          # AI assistant documentation
```

## 📈 Cryptocurrency Analytics Pipeline

### Data Sources
- **BirdEye API**: Real-time cryptocurrency market data, token metrics, and whale tracking
- **Solana Blockchain**: Transaction data and wallet analytics via Helius
- **Token Performance**: Price movements, volume, liquidity, and momentum indicators

### Key Analytics Features

#### 🐋 Whale Tracking & Analysis
- **Top Holders**: Tracks top 20 holders for high-performance tokens
- **Transaction History**: Complete trading history for identified whale wallets  
- **Portfolio Analysis**: Multi-token performance across all positions
- **FIFO PnL Calculation**: Accurate cost basis tracking using first-in-first-out methodology

#### 💰 Performance Metrics
- **Profit & Loss**: Realized vs unrealized gains/losses
- **Trading Behavior**: Win rates, holding times, trade frequency
- **Risk Assessment**: Consistency scores, volatility measures
- **ROI Analysis**: Return on investment across multiple timeframes

#### 🏆 Top Trader Classification
- **Elite Tier**: $10K+ PnL, 50%+ ROI, 70%+ win rate, 20+ trades
- **Strong Tier**: $1K+ PnL, 25%+ ROI, 60%+ win rate, 10+ trades  
- **Promising Tier**: $100+ PnL, 10%+ ROI, 50%+ win rate, 5+ trades

### Data Processing Workflow
```
BirdEye API → Bronze Whales → Silver Tracked Tokens
     ↓              ↓                ↓
Transaction API → Bronze Txns → Silver PnL → Gold Top Traders
```

### Usage Examples

```bash
# View top traders in MinIO Console
# http://localhost:9001 → Browse → solana-data → gold → top_traders

# Query analytics via DuckDB
docker exec claude_pipeline-duckdb python3 -c "
import duckdb
conn = duckdb.connect('/data/analytics.duckdb')
# Configure S3 access
conn.execute('LOAD httpfs;')
conn.execute('SET s3_endpoint=\'minio:9000\';')
conn.execute('SET s3_access_key_id=\'minioadmin\';')
conn.execute('SET s3_secret_access_key=\'minioadmin123\';')

# Query top performers
result = conn.execute('''
  SELECT wallet_address, performance_tier, total_pnl, roi, win_rate
  FROM read_parquet('s3://solana-data/gold/top_traders/**/*.parquet')
  ORDER BY total_pnl DESC LIMIT 10
''').fetchall()
print(result)
"

# Trigger DAGs manually
docker compose run airflow-cli airflow dags trigger bronze_token_whales
docker compose run airflow-cli airflow dags trigger silver_wallet_pnl  
docker compose run airflow-cli airflow dags trigger gold_top_traders
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

### Completed ✅
- [x] RedPanda integration for event streaming
- [x] MinIO for object storage with partitioned data
- [x] PySpark for distributed data processing  
- [x] DBT for SQL-based data transformation
- [x] Medallion architecture (Bronze → Silver → Gold)
- [x] BirdEye API integration for cryptocurrency data
- [x] DuckDB for analytical queries
- [x] Whale tracking and transaction analysis
- [x] FIFO PnL calculation with multi-timeframe analysis
- [x] Top trader classification and performance tiers
- [x] Incremental processing with state tracking

### In Progress 🚧
- [ ] Real-time streaming enhancements
- [ ] Advanced risk metrics and backtesting
- [ ] Machine learning models for trader prediction
- [ ] API endpoints for analytics consumption

### Future Enhancements 🔮
- [ ] Multi-blockchain support (Ethereum, Polygon, etc.)
- [ ] Real-time alerting and notifications
- [ ] Web dashboard for analytics visualization
- [ ] Advanced portfolio optimization algorithms

## 📝 License

MIT