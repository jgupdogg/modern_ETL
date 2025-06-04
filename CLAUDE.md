# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Apache Airflow project for orchestrating data pipelines, using Docker Compose for containerization. The project uses Celery Executor for distributed task execution and includes PostgreSQL for metadata storage, Redis as the Celery message broker, and Redpanda for event streaming.

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
- **Default credentials**: username=`airflow`, password=`airflow`

## Architecture

The project uses the official Apache Airflow Docker setup with the following services:

1. **PostgreSQL**: Metadata database
2. **Redis**: Celery message broker
3. **Redpanda**: Event streaming platform (Kafka-compatible)
4. **Redpanda Console**: Web UI for Redpanda monitoring
5. **Airflow API Server**: REST API and web UI
6. **Airflow Scheduler**: Schedules DAG runs
7. **Airflow DAG Processor**: Parses DAG files
8. **Airflow Worker**: Executes tasks via Celery
9. **Airflow Triggerer**: Manages deferrable operators
10. **Flower** (optional): Celery monitoring UI

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

## Important Notes

- The project currently has example DAGs disabled (`AIRFLOW__CORE__LOAD_EXAMPLES: 'false'`)
- New DAGs are paused by default (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'`)
- Ensure proper file permissions on Linux by setting `AIRFLOW_UID` in `.env` to your user ID
- The Airflow image version is controlled via `AIRFLOW_IMAGE_NAME` in `.env`
- NGROK_TOKEN is stored in `.env` for webhook tunneling
- Redpanda configuration (brokers, topics) is stored in `.env`