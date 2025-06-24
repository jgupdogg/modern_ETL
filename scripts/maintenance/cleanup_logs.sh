#!/bin/bash
# Airflow Log Cleanup Script
# Compresses logs older than 1 day, deletes logs older than 30 days

set -e

LOGS_DIR="/opt/airflow/logs"
CLAUDE_LOGS_DIR="/home/jgupdogg/dev/claude_pipeline/logs"

echo "Starting Airflow log cleanup..."

# Function to cleanup logs in a directory
cleanup_logs() {
    local dir=$1
    
    if [[ ! -d "$dir" ]]; then
        echo "Directory $dir not found, skipping..."
        return
    fi
    
    echo "Cleaning up logs in: $dir"
    
    # Compress logs older than 1 day and larger than 100KB
    find "$dir" -name "*.log" -mtime +1 -size +100k -exec gzip {} \;
    
    # Delete uncompressed logs older than 7 days
    find "$dir" -name "*.log" -mtime +7 -delete
    
    # Delete compressed logs older than 30 days
    find "$dir" -name "*.log.gz" -mtime +30 -delete
    
    echo "Cleanup completed for: $dir"
}

# Check disk space before cleanup
echo "Disk space before cleanup:"
df -h "$CLAUDE_LOGS_DIR" 2>/dev/null || echo "Could not check disk space"

# Cleanup both local and container logs
cleanup_logs "$CLAUDE_LOGS_DIR"
cleanup_logs "$LOGS_DIR"

# Show final disk usage
echo "Log directory sizes after cleanup:"
du -sh "$CLAUDE_LOGS_DIR" 2>/dev/null || echo "Could not check local logs size"
du -sh "$LOGS_DIR" 2>/dev/null || echo "Could not check container logs size"

echo "Log cleanup completed successfully!"