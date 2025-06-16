#!/usr/bin/env python3
"""
Run migration script inside Docker network.
"""

import subprocess
import sys
import os

def run_migration():
    """Run migration using docker exec."""
    
    # Copy the migration script to the DuckDB container
    copy_cmd = [
        'docker', 'cp', 
        '/home/jgupdogg/dev/claude_pipeline/scripts/postgres_to_minio_migration.py',
        'claude_pipeline-duckdb:/tmp/migration.py'
    ]
    
    result = subprocess.run(copy_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Failed to copy migration script: {result.stderr}")
        return False
    
    # Install dependencies in DuckDB container
    install_cmd = [
        'docker', 'exec', 'claude_pipeline-duckdb',
        'pip', 'install', 'psycopg2-binary', 'pandas', 'pyarrow'
    ]
    
    result = subprocess.run(install_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Failed to install dependencies: {result.stderr}")
        return False
    
    # Run migration script with correct network access
    migrate_cmd = [
        'docker', 'exec', '-e', 'POSTGRES_HOST=postgres',
        '-e', 'MINIO_ENDPOINT=http://minio:9000',
        'claude_pipeline-duckdb',
        'python3', '/tmp/migration.py'
    ]
    
    result = subprocess.run(migrate_cmd, text=True)
    return result.returncode == 0

if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)