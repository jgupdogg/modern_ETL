#!/usr/bin/env python3
"""
Disk Space Monitoring DAG
Monitors disk usage and runs cleanup when thresholds are exceeded
"""

import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

# DAG Configuration
DAG_ID = "disk_space_monitoring"
SCHEDULE_INTERVAL = "0 */6 * * *"  # Every 6 hours

# Thresholds
DISK_USAGE_WARNING_PERCENT = 80
DISK_USAGE_CRITICAL_PERCENT = 90

default_args = {
    'owner': 'maintenance-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Monitor disk space and trigger cleanup when needed',
    schedule=SCHEDULE_INTERVAL,
    catchup=False,
    max_active_runs=1,
    tags=['monitoring', 'maintenance', 'disk-space']
)

@task(dag=dag)
def check_disk_usage(**context):
    """Check current disk usage and determine if cleanup is needed."""
    import shutil
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Check disk usage for root filesystem
    total, used, free = shutil.disk_usage('/')
    usage_percent = (used / total) * 100
    
    # Convert to GB for readability
    total_gb = total / (1024**3)
    used_gb = used / (1024**3)
    free_gb = free / (1024**3)
    
    logger.info(f"Disk Usage: {used_gb:.1f}GB / {total_gb:.1f}GB ({usage_percent:.1f}%)")
    logger.info(f"Free Space: {free_gb:.1f}GB")
    
    # Determine action needed
    if usage_percent >= DISK_USAGE_CRITICAL_PERCENT:
        action = "critical_cleanup"
        logger.warning(f"CRITICAL: Disk usage {usage_percent:.1f}% >= {DISK_USAGE_CRITICAL_PERCENT}%")
    elif usage_percent >= DISK_USAGE_WARNING_PERCENT:
        action = "warning_cleanup"
        logger.warning(f"WARNING: Disk usage {usage_percent:.1f}% >= {DISK_USAGE_WARNING_PERCENT}%")
    else:
        action = "none"
        logger.info(f"OK: Disk usage {usage_percent:.1f}% is below warning threshold")
    
    return {
        'usage_percent': usage_percent,
        'total_gb': total_gb,
        'used_gb': used_gb,
        'free_gb': free_gb,
        'action': action
    }

@task(dag=dag)
def check_docker_usage(**context):
    """Check Docker system usage."""
    import logging
    
    logger = logging.getLogger(__name__)
    
    try:
        # Run docker system df
        result = subprocess.run(['docker', 'system', 'df'], 
                              capture_output=True, text=True, check=True)
        
        logger.info("Docker system usage:")
        logger.info(result.stdout)
        
        # Parse output to extract reclaimable space
        lines = result.stdout.strip().split('\n')
        reclaimable_gb = 0
        
        for line in lines[1:]:  # Skip header
            if 'GB' in line and 'reclaimable' in line.lower():
                parts = line.split()
                for i, part in enumerate(parts):
                    if 'GB' in part and '(' in parts[i+1]:
                        try:
                            reclaimable_gb += float(part.replace('GB', ''))
                        except ValueError:
                            pass
        
        logger.info(f"Total Docker reclaimable space: ~{reclaimable_gb:.1f}GB")
        
        return {
            'docker_output': result.stdout,
            'reclaimable_gb': reclaimable_gb,
            'needs_cleanup': reclaimable_gb > 5.0  # Cleanup if >5GB reclaimable
        }
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to check Docker usage: {e}")
        return {
            'docker_output': str(e),
            'reclaimable_gb': 0,
            'needs_cleanup': False
        }

# Conditional cleanup tasks
docker_cleanup = BashOperator(
    task_id='docker_cleanup',
    bash_command='docker system prune -f',
    dag=dag
)

log_cleanup = BashOperator(
    task_id='log_cleanup',
    bash_command='/opt/airflow/scripts/maintenance/cleanup_logs.sh',
    dag=dag
)

pyspark_cleanup = BashOperator(
    task_id='pyspark_cleanup', 
    bash_command='/opt/airflow/scripts/maintenance/cleanup_pyspark.sh',
    dag=dag
)

@task(dag=dag)
def generate_cleanup_report(disk_result, docker_result, **context):
    """Generate cleanup summary report."""
    import logging
    
    logger = logging.getLogger(__name__)
    
    logger.info("=== DISK SPACE MONITORING REPORT ===")
    logger.info(f"Disk Usage: {disk_result['used_gb']:.1f}GB / {disk_result['total_gb']:.1f}GB ({disk_result['usage_percent']:.1f}%)")
    logger.info(f"Free Space: {disk_result['free_gb']:.1f}GB")
    logger.info(f"Action Taken: {disk_result['action']}")
    logger.info(f"Docker Reclaimable: {docker_result['reclaimable_gb']:.1f}GB")
    logger.info("=====================================")
    
    return {
        'report_generated': True,
        'timestamp': context['logical_date'].isoformat()
    }

# Define task dependencies
with dag:
    disk_check = check_disk_usage()
    docker_check = check_docker_usage()
    
    # Run cleanup tasks in parallel if needed
    cleanup_report = generate_cleanup_report(disk_check, docker_check)
    
    # Set up conditional execution based on thresholds
    [disk_check, docker_check] >> docker_cleanup >> cleanup_report
    [disk_check, docker_check] >> log_cleanup >> cleanup_report
    [disk_check, docker_check] >> pyspark_cleanup >> cleanup_report