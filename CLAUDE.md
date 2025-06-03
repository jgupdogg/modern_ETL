# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Apache Airflow project for orchestrating data pipelines, using Docker Compose for containerization. The project uses Celery Executor for distributed task execution and includes PostgreSQL for metadata storage and Redis as the Celery message broker.

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
- **Default credentials**: username=`airflow`, password=`airflow`

## Architecture

The project uses the official Apache Airflow Docker setup with the following services:

1. **PostgreSQL**: Metadata database
2. **Redis**: Celery message broker
3. **Airflow API Server**: REST API and web UI
4. **Airflow Scheduler**: Schedules DAG runs
5. **Airflow DAG Processor**: Parses DAG files
6. **Airflow Worker**: Executes tasks via Celery
7. **Airflow Triggerer**: Manages deferrable operators
8. **Flower** (optional): Celery monitoring UI

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
- **Endpoints**: `/webhooks` (POST), `/health` (GET), `/docs` (Swagger UI)
- **Data Storage**: `data/webhooks/` directory with date-based organization
- **ngrok Integration**: Use `./scripts/start-ngrok.sh` to expose the service

## Important Notes

- The project currently has example DAGs disabled (`AIRFLOW__CORE__LOAD_EXAMPLES: 'false'`)
- New DAGs are paused by default (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'`)
- Ensure proper file permissions on Linux by setting `AIRFLOW_UID` in `.env` to your user ID
- The Airflow image version is controlled via `AIRFLOW_IMAGE_NAME` in `.env`
- NGROK_TOKEN is stored in `.env` for webhook tunneling