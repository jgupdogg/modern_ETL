# Comprehensive Cleanup System Documentation

## Overview

This document describes the comprehensive cleanup system implemented to prevent future 19GB data accumulations and ensure efficient storage management across the Claude Pipeline project.

## 🚨 Problem Solved

**Before**: 19GB data accumulation causing Claude Code performance issues
- Webhook JSON files stored indefinitely 
- Redpanda topic with 971K+ messages
- No log rotation or cleanup policies
- dbt artifacts accumulating over time

**After**: Automated cleanup system with <2GB typical footprint
- 7-day webhook retention with automated cleanup
- Redpanda topic limited to 2000 recent messages
- 30-day log retention with automated cleanup
- Comprehensive monitoring and alerting

## 🏗️ System Architecture

### Phase 1: Immediate Fixes (High Priority) ✅

#### A. Webhook Data Pipeline Cleanup
- **FastAPI Service Enhancement** (`services/webhook-listener/main.py`)
  - Added configurable retention policy (default: 7 days)
  - Automatic cleanup on startup
  - REST API endpoints for manual cleanup
  - Dry-run capability for safe testing

- **Environment Configuration** (`docker-compose.yaml`)
  ```yaml
  WEBHOOK_RETENTION_DAYS: 7
  CLEANUP_ENABLED: "true"
  CLEANUP_ON_STARTUP: "true"
  ```

- **API Endpoints Added**:
  - `POST /webhooks/cleanup?dry_run=false` - Execute cleanup
  - `GET /webhooks/cleanup/dry-run` - Preview cleanup
  - `GET /webhooks/status` - View cleanup configuration

#### B. Redpanda Topic Retention
- **Scheduled Cleanup** (`dags/cleanup_maintenance_dag.py`)
- **Existing Script Integration** (`scripts/webhook/redpanda_cleanup.py`)
- **Configurable Retention**: Keep 2000 most recent messages
- **Daily Execution**: Automated via Airflow DAG

#### C. MinIO Lifecycle Policies
- **Automated Setup** (`scripts/infrastructure/minio_lifecycle_setup.py`)
- **Tiered Retention**:
  - Bronze layer: 30 days
  - Silver layer: 90 days (webhook), 180 days (solana)
  - Gold layer: 365 days (webhook), 730 days (solana)

#### D. Airflow Log Rotation
- **Configuration Update** (`config/airflow.cfg`)
- **Automated Cleanup** (`dags/cleanup_maintenance_dag.py`)
- **Retention Period**: 30 days

#### E. dbt Artifacts Cleanup
- **Pre-compilation Cleanup** (`dags/dbt_smart_wallets_dag.py`)
- **Automated Cleanup**: Remove old compilation artifacts
- **Retention**: Keep only current compilation files

### Phase 2: Enhanced Monitoring (Medium Priority) ✅

#### A. Disk Usage Monitoring (`dags/disk_monitoring_dag.py`)
- **Schedule**: Every 6 hours
- **Thresholds**:
  - Critical: 5GB (trigger immediate cleanup)
  - Warning: 2GB (early warning)
  - Info: 1GB (information only)

- **Monitored Locations**:
  - `/opt/airflow/logs` - Airflow logs
  - `/opt/airflow/dbt/target` - dbt compilation artifacts  
  - `/tmp` - Temporary files
  - Docker volumes - All Claude Pipeline volumes

#### B. Comprehensive Cleanup DAG (`dags/cleanup_maintenance_dag.py`)
- **Schedule**: Daily at 2 AM
- **Components**:
  - Webhook file cleanup (via API)
  - Redpanda topic cleanup
  - Airflow log cleanup
  - dbt artifacts cleanup
  - Docker system cleanup
  - Disk usage reporting

### Phase 3: Data Lifecycle Management (Low Priority) ✅

#### A. Advanced Lifecycle Manager (`scripts/infrastructure/data_lifecycle_manager.py`)
- **Data Age Analysis**: Track object age across all layers
- **Storage Optimization**: Identify compression/archive candidates
- **Cost Optimization**: Estimate storage savings
- **Automated Policies**: Apply lifecycle rules to MinIO buckets

#### B. Tiered Storage Strategy
```
Bronze → Silver → Gold
  ↓        ↓        ↓
30 days  90 days  365 days
  ↓        ↓        ↓
Delete   Archive   Long-term
```

## 📋 Configuration Reference

### Webhook Service Configuration
```bash
# Environment Variables
WEBHOOK_RETENTION_DAYS=7          # File retention period
CLEANUP_ENABLED=true              # Enable automatic cleanup
CLEANUP_ON_STARTUP=true           # Clean on service start
```

### Cleanup Schedules
- **Disk Monitoring**: Every 6 hours (`0 */6 * * *`)
- **Daily Cleanup**: 2 AM daily (`0 2 * * *`)
- **Redpanda Cleanup**: Part of daily cleanup
- **dbt Cleanup**: Before each compilation

### Alert Thresholds
| Level | Directory Size | Docker Volume | Action |
|-------|---------------|---------------|---------|
| Info | >1GB | >5GB | Monitor |
| Warning | >2GB | >10GB | Schedule cleanup |
| Critical | >5GB | >20GB | Immediate action |

## 🔧 Usage Instructions

### Manual Operations

#### Test Webhook Cleanup
```bash
# Preview cleanup (dry run)
curl http://localhost:8000/webhooks/cleanup/dry-run

# Execute cleanup
curl -X POST http://localhost:8000/webhooks/cleanup
```

#### Run Redpanda Cleanup
```bash
# Preview cleanup
python scripts/webhook/redpanda_cleanup.py --keep 2000

# Execute cleanup
python scripts/webhook/redpanda_cleanup.py --keep 2000 --execute
```

#### Setup MinIO Lifecycle Policies
```bash
# Preview policies
python scripts/infrastructure/minio_lifecycle_setup.py --dry-run

# Apply policies
python scripts/infrastructure/minio_lifecycle_setup.py
```

#### Generate Lifecycle Report
```bash
python scripts/infrastructure/data_lifecycle_manager.py report
```

### Airflow DAG Operations

#### Trigger Manual Cleanup
```bash
# Full cleanup
docker-compose run airflow-cli airflow dags trigger cleanup_maintenance

# Disk monitoring
docker-compose run airflow-cli airflow dags trigger disk_usage_monitoring
```

#### Monitor DAG Status
```bash
# List cleanup runs
docker-compose run airflow-cli airflow dags list-runs --dag-id cleanup_maintenance

# Check monitoring alerts
docker-compose run airflow-cli airflow dags list-runs --dag-id disk_usage_monitoring
```

## 📊 Expected Performance Impact

### Storage Reduction
- **Before**: 19GB+ accumulation
- **After**: <2GB typical footprint
- **Savings**: 90%+ storage reduction

### Performance Improvement
- **Claude Code**: Dramatic responsiveness improvement
- **Docker**: Reduced I/O and volume overhead
- **Backup/Sync**: 10x faster operations

### Maintenance Benefits
- **Automated**: No manual intervention required
- **Predictable**: Linear growth, not exponential
- **Monitored**: Early warning system prevents issues

## ⚠️ Important Notes

### Data Retention Warnings
- **Webhook files**: 7-day retention (configurable)
- **Redpanda messages**: 2000 recent messages max
- **Airflow logs**: 30-day retention
- **Critical data in MinIO**: Protected by lifecycle policies

### Recovery Procedures
- **Webhook data**: Also stored in Redpanda and MinIO bronze layer
- **Log data**: Can be regenerated by re-running DAGs
- **dbt artifacts**: Regenerated on next compilation

### Monitoring Alerts
- Monitor `disk_usage_monitoring` DAG for alerts
- Check webhook service `/webhooks/status` endpoint
- Review `cleanup_maintenance` DAG execution logs

## 🔍 Troubleshooting

### Common Issues

#### Cleanup Not Running
1. Check DAG is unpaused: `airflow dags unpause cleanup_maintenance`
2. Verify schedule: `airflow dags list --dag-id cleanup_maintenance`
3. Check Airflow worker status: `docker-compose ps airflow-worker`

#### Webhook Service Cleanup Failing
1. Check service health: `curl http://localhost:8000/health`
2. Verify configuration: `curl http://localhost:8000/webhooks/status`
3. Test dry-run: `curl http://localhost:8000/webhooks/cleanup/dry-run`

#### High Disk Usage Alerts
1. Check monitoring report: Airflow UI → `disk_usage_monitoring` DAG
2. Run immediate cleanup: `airflow dags trigger cleanup_maintenance`
3. Check Docker volumes: `docker system df`

#### MinIO Lifecycle Issues
1. Test connection: `python scripts/infrastructure/minio_lifecycle_setup.py --status`
2. Verify policies: `python scripts/infrastructure/minio_lifecycle_setup.py --dry-run`
3. Check bucket permissions: MinIO Console at `http://localhost:9001`

### Debug Commands

#### Check Current Disk Usage
```bash
# Directory sizes
du -sh /opt/airflow/logs /opt/airflow/dbt/target /tmp

# Docker system usage
docker system df

# Detailed volume analysis
docker volume ls
```

#### Verify Cleanup Configuration
```bash
# Webhook service config
curl http://localhost:8000/webhooks/status

# Airflow DAG status
docker-compose run airflow-cli airflow dags list --dag-id cleanup_maintenance

# MinIO lifecycle status
python scripts/infrastructure/minio_lifecycle_setup.py --status
```

## 🚀 Future Enhancements

### Potential Improvements
1. **Slack/Email Alerts**: Integrate monitoring with notification systems
2. **Advanced Compression**: Implement data compression before archival
3. **Cloud Integration**: Extend lifecycle policies to cloud storage
4. **Cost Tracking**: Add storage cost monitoring and optimization
5. **Data Quality Metrics**: Monitor data quality during lifecycle transitions

### Scaling Considerations
- Adjust retention periods for production scale
- Implement cross-region replication for critical data
- Add automated backup verification
- Consider event-driven cleanup triggers

## 📚 Related Documentation
- `CLAUDE.md` - Project overview and commands
- `SMART_TRADER_PIPELINE.md` - Smart trader pipeline details
- `config/airflow.cfg` - Airflow configuration
- `docker-compose.yaml` - Service configuration