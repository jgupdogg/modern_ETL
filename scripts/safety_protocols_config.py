#!/usr/bin/env python3
"""
Safety Protocols and Batch Processing Limits for Smart Trader Pipeline Migration
Prevents system crashes and ensures stable operations across all tables
"""

import os
from typing import Dict, Any

# =============================================================================
# MEMORY SAFETY CONFIGURATION
# =============================================================================

# System Requirements (Minimum available memory before starting operations)
MIN_SYSTEM_MEMORY_GB = 10
MAX_SYSTEM_MEMORY_USAGE_PERCENT = 85
MEMORY_CHECK_INTERVAL_SECONDS = 30

# PySpark Safe Memory Settings (Applied to ALL operations)
SAFE_SPARK_CONFIG = {
    "spark.driver.memory": "512m",
    "spark.executor.memory": "512m", 
    "spark.driver.maxResultSize": "256m",
    "spark.sql.execution.arrow.pyspark.enabled": "false",  # Disabled for stability
    "spark.sql.adaptive.enabled": "false",  # Predictable memory usage
    "spark.sql.adaptive.coalescePartitions.enabled": "false",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    
    # JVM Stability Settings
    "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp",
    "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp",
    
    # Network Stability
    "spark.network.timeout": "600s",
    "spark.hadoop.fs.s3a.connection.timeout": "30000",
    "spark.hadoop.fs.s3a.retry.limit": "3"
}

# =============================================================================
# TABLE-SPECIFIC BATCH LIMITS (Conservative for Safety)
# =============================================================================

# Bronze Layer Batch Limits
BRONZE_BATCH_LIMITS = {
    "token_list": {
        "records_per_batch": 100,        # Small table - can handle larger batches
        "max_concurrent_batches": 1,     # Always single batch
        "processing_timeout_minutes": 5,
        "memory_requirement_mb": 256,
        "complexity": "LOW"
    },
    
    "token_whales": {
        "records_per_batch": 1000,       # Medium table - moderate batches
        "wallets_per_batch": 100,        # When processing whale wallets
        "max_concurrent_batches": 1,
        "processing_timeout_minutes": 15,
        "memory_requirement_mb": 512,
        "complexity": "MEDIUM"
    },
    
    "wallet_transactions": {
        "wallets_per_batch": 5,          # PROVEN SAFE - already tested
        "records_per_batch": 500,        # Maximum transactions per batch
        "max_concurrent_batches": 1,
        "processing_timeout_minutes": 30,
        "memory_requirement_mb": 512,
        "complexity": "HIGH"
    }
}

# Silver Layer Batch Limits
SILVER_BATCH_LIMITS = {
    "tracked_tokens": {
        "tokens_per_batch": 50,          # Small silver table
        "max_concurrent_batches": 1,
        "processing_timeout_minutes": 10,
        "memory_requirement_mb": 256,
        "complexity": "LOW"
    },
    
    "wallet_pnl": {
        "wallets_per_batch": 5,          # CRITICAL - Heavy computation
        "transactions_per_wallet": 100,  # Limit transaction processing depth
        "max_concurrent_batches": 1,
        "processing_timeout_minutes": 60,  # Longer timeout for PnL calculations
        "memory_requirement_mb": 512,
        "complexity": "VERY_HIGH"
    }
}

# Gold Layer Batch Limits  
GOLD_BATCH_LIMITS = {
    "top_traders": {
        "traders_per_batch": 100,        # Small gold table
        "max_concurrent_batches": 1,
        "processing_timeout_minutes": 10,
        "memory_requirement_mb": 256,
        "complexity": "LOW"
    }
}

# =============================================================================
# DUCKDB VALIDATION CONFIGURATION
# =============================================================================

# DuckDB Connection Settings (Crash-safe alternative to PySpark)
DUCKDB_CONFIG = {
    "database_path": "/data/analytics.duckdb",
    "s3_endpoint": "minio:9000",
    "s3_access_key_id": "minioadmin",
    "s3_secret_access_key": "minioadmin123",
    "s3_use_ssl": "false",
    "s3_url_style": "path",
    "query_timeout_seconds": 300,  # 5 minute timeout for large queries
    "memory_limit": "2GB"  # DuckDB memory limit
}

# Validation Query Limits (Prevent DuckDB from overwhelming system)
VALIDATION_LIMITS = {
    "max_records_to_scan": 10000,      # Limit for data quality checks
    "sample_partitions": 3,             # Number of partitions to test
    "schema_validation_timeout": 60,    # Seconds for schema checks
    "connection_retry_attempts": 3,
    "retry_delay_seconds": 5
}

# =============================================================================
# CONTAINER RESOURCE MANAGEMENT
# =============================================================================

# Essential Services (Keep running during migrations)
ESSENTIAL_SERVICES = [
    "airflow-worker",
    "airflow-scheduler", 
    "airflow-apiserver",
    "postgres",
    "redis",
    "minio",
    "duckdb"
]

# Non-Essential Services (Stop during heavy operations)
NON_ESSENTIAL_SERVICES = [
    "airflow-triggerer",
    "airflow-dag-processor",
    "redpanda-console",
    "webhook-listener"
]

# Docker Resource Limits
DOCKER_RESOURCE_LIMITS = {
    "cleanup_before_operations": True,
    "max_reclaimable_space_gb": 5,      # Run cleanup if more than 5GB reclaimable
    "container_memory_limit_gb": 4,     # Per container memory limit
    "total_container_memory_limit_gb": 16,  # Total memory for all containers
}

# =============================================================================
# MIGRATION SAFETY PROTOCOLS
# =============================================================================

def get_table_safety_config(layer: str, table_name: str) -> Dict[str, Any]:
    """Get safety configuration for specific table"""
    
    layer_configs = {
        "bronze": BRONZE_BATCH_LIMITS,
        "silver": SILVER_BATCH_LIMITS,
        "gold": GOLD_BATCH_LIMITS
    }
    
    if layer not in layer_configs:
        raise ValueError(f"Unknown layer: {layer}")
    
    config = layer_configs[layer].get(table_name)
    if not config:
        # Default safe configuration
        config = {
            "records_per_batch": 100,
            "max_concurrent_batches": 1,
            "processing_timeout_minutes": 15,
            "memory_requirement_mb": 256,
            "complexity": "MEDIUM"
        }
    
    # Add common safety settings
    config.update({
        "use_duckdb_validation": True,
        "require_memory_check": True,
        "enable_progress_tracking": True,
        "auto_cleanup_on_failure": True,
        "spark_config": SAFE_SPARK_CONFIG
    })
    
    return config

def get_pre_migration_checklist() -> Dict[str, str]:
    """Get pre-migration safety checklist"""
    return {
        "memory_check": f"Verify {MIN_SYSTEM_MEMORY_GB}GB+ available memory",
        "container_health": "Confirm essential containers running",
        "docker_cleanup": "Run docker system prune if >5GB reclaimable",
        "service_scaling": "Stop non-essential services during migration",
        "backup_validation": "Confirm original data accessible",
        "duckdb_connection": "Test DuckDB S3 connectivity",
        "monitoring_setup": "Enable resource monitoring",
        "emergency_procedures": "Review rollback procedures"
    }

def get_emergency_recovery_steps() -> Dict[str, str]:
    """Get emergency recovery procedures"""
    return {
        "system_unresponsive": "docker compose restart airflow-worker",
        "memory_exhaustion": "docker system prune -f && restart containers",
        "pyspark_crash": "Switch to DuckDB validation only",
        "container_failure": "docker compose up -d essential-services-only",
        "data_corruption": "Rollback to original tables",
        "network_timeout": "Restart minio and redis containers",
        "validation_failure": "Run individual table validators",
        "complete_reset": "Stop all containers, cleanup, restart essential only"
    }

# =============================================================================
# VALIDATION PATTERNS PER TABLE TYPE
# =============================================================================

# Table-specific validation patterns
TABLE_VALIDATION_PATTERNS = {
    "bronze_token_list": {
        "required_columns": ["token_address", "symbol", "market_cap", "liquidity"],
        "quality_checks": ["non_null_addresses", "positive_market_caps", "valid_symbols"],
        "performance_checks": ["record_count", "unique_addresses"],
        "processing_fields": ["batch_id", "created_at"]
    },
    
    "bronze_token_whales": {
        "required_columns": ["token_address", "wallet_address", "rank", "holdings_percentage"],
        "quality_checks": ["valid_rankings", "positive_holdings", "non_null_wallets"],
        "performance_checks": ["whale_distribution", "token_coverage"],
        "processing_fields": ["txns_fetched", "txns_fetch_status", "batch_id"]
    },
    
    "bronze_wallet_transactions": {
        "required_columns": ["wallet_address", "transaction_hash", "timestamp", "tx_type"],
        "quality_checks": ["unique_transactions", "valid_timestamps", "proper_amounts"],
        "performance_checks": ["transaction_volume", "wallet_diversity"],
        "processing_fields": ["processed_for_pnl", "pnl_processed_at", "pnl_processing_batch_id"]
    },
    
    "silver_tracked_tokens": {
        "required_columns": ["token_address", "quality_score", "volume_mcap_ratio"],
        "quality_checks": ["score_ranges", "ratio_validity", "processing_date_consistency"],
        "performance_checks": ["qualification_rate", "score_distribution"],
        "processing_fields": ["processing_date", "bronze_id"]
    },
    
    "silver_wallet_pnl": {
        "required_columns": ["wallet_address", "total_pnl", "realized_pnl", "unrealized_pnl"],
        "quality_checks": ["pnl_math_consistency", "positive_trade_counts", "valid_roi"],
        "performance_checks": ["profitability_distribution", "calculation_completeness"],
        "processing_fields": ["calculation_date", "batch_id"]
    },
    
    "gold_top_traders": {
        "required_columns": ["wallet_address", "performance_tier", "total_pnl", "roi"],
        "quality_checks": ["tier_consistency", "ranking_order", "performance_thresholds"],
        "performance_checks": ["tier_distribution", "ranking_completeness"],
        "processing_fields": ["silver_batch_id", "gold_batch_id", "created_at"]
    }
}

# =============================================================================
# MONITORING AND ALERTING THRESHOLDS
# =============================================================================

MONITORING_THRESHOLDS = {
    "memory_usage_warning": 80,      # % system memory usage
    "memory_usage_critical": 90,     # % system memory usage  
    "disk_usage_warning": 85,        # % disk usage
    "disk_usage_critical": 95,       # % disk usage
    "container_restart_limit": 3,    # Max restarts per hour
    "validation_failure_limit": 2,   # Max consecutive validation failures
    "processing_timeout_multiplier": 1.5,  # Timeout = expected_time * multiplier
    "batch_failure_threshold": 0.1   # Max 10% batch failure rate
}

# Health Check Intervals
HEALTH_CHECK_INTERVALS = {
    "memory_check_seconds": 30,
    "container_health_seconds": 60,
    "disk_space_seconds": 300,      # 5 minutes
    "duckdb_connectivity_seconds": 120,  # 2 minutes
    "validation_status_seconds": 600     # 10 minutes
}

# =============================================================================
# EXPORT FUNCTIONS
# =============================================================================

def get_all_safety_configs():
    """Get all safety configurations for reference"""
    return {
        "memory_safety": {
            "min_memory_gb": MIN_SYSTEM_MEMORY_GB,
            "max_usage_percent": MAX_SYSTEM_MEMORY_USAGE_PERCENT,
            "spark_config": SAFE_SPARK_CONFIG
        },
        "batch_limits": {
            "bronze": BRONZE_BATCH_LIMITS,
            "silver": SILVER_BATCH_LIMITS, 
            "gold": GOLD_BATCH_LIMITS
        },
        "duckdb_config": DUCKDB_CONFIG,
        "validation_limits": VALIDATION_LIMITS,
        "container_management": {
            "essential_services": ESSENTIAL_SERVICES,
            "non_essential_services": NON_ESSENTIAL_SERVICES,
            "resource_limits": DOCKER_RESOURCE_LIMITS
        },
        "monitoring_thresholds": MONITORING_THRESHOLDS,
        "validation_patterns": TABLE_VALIDATION_PATTERNS
    }

if __name__ == "__main__":
    """Print safety configuration summary"""
    
    print("🛡️  SMART TRADER PIPELINE SAFETY CONFIGURATION")
    print("=" * 60)
    
    print("\n📊 BATCH LIMITS SUMMARY:")
    for layer, tables in [("Bronze", BRONZE_BATCH_LIMITS), ("Silver", SILVER_BATCH_LIMITS), ("Gold", GOLD_BATCH_LIMITS)]:
        print(f"\n{layer} Layer:")
        for table, config in tables.items():
            complexity = config.get('complexity', 'UNKNOWN')
            batch_size = config.get('records_per_batch', config.get('wallets_per_batch', 'N/A'))
            print(f"  - {table}: {batch_size} per batch ({complexity} complexity)")
    
    print(f"\n🧠 MEMORY SAFETY:")
    print(f"  - Minimum available: {MIN_SYSTEM_MEMORY_GB}GB")
    print(f"  - Maximum usage: {MAX_SYSTEM_MEMORY_USAGE_PERCENT}%")
    print(f"  - PySpark driver memory: {SAFE_SPARK_CONFIG['spark.driver.memory']}")
    print(f"  - PySpark executor memory: {SAFE_SPARK_CONFIG['spark.executor.memory']}")
    
    print(f"\n🐳 CONTAINER MANAGEMENT:")
    print(f"  - Essential services: {len(ESSENTIAL_SERVICES)} containers")
    print(f"  - Non-essential services: {len(NON_ESSENTIAL_SERVICES)} containers")
    
    print(f"\n🦆 DUCKDB VALIDATION:")
    print(f"  - Memory limit: {DUCKDB_CONFIG['memory_limit']}")
    print(f"  - Query timeout: {DUCKDB_CONFIG['query_timeout_seconds']}s")
    print(f"  - Max records to scan: {VALIDATION_LIMITS['max_records_to_scan']:,}")
    
    print(f"\n✅ Configuration loaded successfully!")
    print(f"🔒 All settings designed to prevent system crashes and ensure stable operations.")