#!/bin/bash
# PySpark Checkpoint and Cache Cleanup Script
# Cleans up old checkpoint directories and temporary Spark files

set -e

CHECKPOINT_BASE_DIR="/opt/airflow/data/checkpoints"
SPARK_TEMP_DIR="/tmp/spark-*"
LOCAL_CHECKPOINT_DIR="/home/jgupdogg/dev/claude_pipeline/data/checkpoints"

echo "Starting PySpark cleanup..."

# Function to cleanup checkpoints older than specified days
cleanup_checkpoints() {
    local dir=$1
    local days=${2:-7}
    
    if [[ ! -d "$dir" ]]; then
        echo "Checkpoint directory $dir not found, creating..."
        mkdir -p "$dir"
        return
    fi
    
    echo "Cleaning checkpoints in: $dir (older than $days days)"
    
    # Find and remove checkpoint directories older than specified days
    find "$dir" -type d -name "*streaming*" -mtime +$days -exec rm -rf {} \; 2>/dev/null || true
    find "$dir" -type d -name "*bronze*" -mtime +$days -exec rm -rf {} \; 2>/dev/null || true
    find "$dir" -type d -name "*silver*" -mtime +$days -exec rm -rf {} \; 2>/dev/null || true
    
    # Remove empty checkpoint directories
    find "$dir" -type d -empty -delete 2>/dev/null || true
    
    echo "Checkpoint cleanup completed for: $dir"
}

# Function to cleanup Spark temporary files
cleanup_spark_temp() {
    echo "Cleaning up Spark temporary files..."
    
    # Remove old Spark temporary directories (older than 1 day)
    find /tmp -name "spark-*" -type d -mtime +1 -exec rm -rf {} \; 2>/dev/null || true
    
    # Remove old PySpark temporary files
    find /tmp -name "pyspark*" -type f -mtime +1 -delete 2>/dev/null || true
    
    echo "Spark temporary files cleanup completed"
}

# Check disk space before cleanup
echo "Disk space before cleanup:"
df -h / | head -2

# Cleanup checkpoints (7 days retention)
cleanup_checkpoints "$LOCAL_CHECKPOINT_DIR" 7
cleanup_checkpoints "$CHECKPOINT_BASE_DIR" 7

# Cleanup Spark temporary files
cleanup_spark_temp

# Show final usage
echo "Checkpoint directory sizes after cleanup:"
du -sh "$LOCAL_CHECKPOINT_DIR" 2>/dev/null || echo "Local checkpoint dir not found"
du -sh "$CHECKPOINT_BASE_DIR" 2>/dev/null || echo "Container checkpoint dir not found"

echo "PySpark cleanup completed successfully!"