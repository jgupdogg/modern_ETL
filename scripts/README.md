# Scripts Directory Organization

This directory contains organized scripts and utilities for the Claude Pipeline project.

## Directory Structure

### 📊 `/analysis/`
Data analysis and exploration scripts:
- `analyze_silver_pnl_data.py` - Silver layer PnL analytics
- `analyze_webhook_data.py` - Webhook data analysis using PySpark
- `comprehensive_webhook_analysis.py` - Full directory and content analysis of webhooks
- `token_swap_analysis.py` - Trading pattern and DEX analysis

### 🗄️ `/duckdb/`
DuckDB-related analytics and integration scripts:
- `duckdb_init.py` - Database initialization and setup
- `duckdb_minio_test.py` - DuckDB + MinIO integration testing
- `duckdb_phase2_bronze_analysis.py` - Bronze layer data analysis
- `duckdb_phase3_silver_design.py` - Silver layer transformations
- `duckdb_final_validation.py` - End-to-end validation

### 🏗️ `/infrastructure/`
Infrastructure setup and configuration scripts:
- `minio_test.py` - MinIO connectivity and functionality tests
- `setup_airflow_vars.py` - Airflow variable configuration
- `start-ngrok.sh` - Webhook tunneling setup

### 🔄 `/migration/`
Data migration and transformation utilities:
- `migrate_top_traders.py` - Top trader data migration
- `migrate_webhook_to_medallion.py` - Webhook medallion architecture migration
- `postgres_to_minio_migration.py` - PostgreSQL to MinIO data transfer
- `run_migration_docker.py` - Docker-based migration execution

### ⚡ `/pyspark/`
PySpark testing and integration scripts:
- `pyspark_minio_test.py` - PySpark + MinIO integration
- `pyspark_redpanda_test.py` - PySpark + Redpanda streaming tests

### 🧪 `/tests/`
Testing, debugging, and validation scripts:
- `test_*.py` - Various component and integration tests
- `check_*.py` - Data quality and configuration checks
- `debug_*.py` - Debugging utilities
- `validate_*.py` - Validation scripts

### 🌐 `/webhook/`
Webhook processing and event streaming scripts:
- `*webhook*.py` - Webhook-related utilities and integrations
- `redpanda_consumer.py` - Redpanda message consumption
- `simple_*.py` - Simple webhook utilities

## Legacy Files

The following files remain in the root `/scripts/` directory:
- `migration.log` - Migration execution logs
- `migration_status.json` - Migration status tracking
- `requirements.txt` - Python dependencies
- `venv/` - Virtual environment (development only)

## Usage

Each subdirectory contains focused scripts for specific aspects of the pipeline. This organization makes it easier to:
- Find relevant scripts quickly
- Understand script purposes at a glance
- Maintain separation of concerns
- Organize development workflows

For specific script usage, refer to individual script documentation and the main project documentation in `CLAUDE.md`.