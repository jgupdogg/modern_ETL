"""
Cleanup and Maintenance DAG

Comprehensive cleanup tasks for all data layers to prevent disk space accumulation.
Runs daily with different cleanup schedules for different components.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import subprocess
import requests
import logging
import shutil
from pathlib import Path

# Configuration
DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# DAG Definition
dag = DAG(
    'cleanup_maintenance',
    default_args=DEFAULT_ARGS,
    description='Daily cleanup and maintenance tasks',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['cleanup', 'maintenance', 'storage']
)

# Configuration from environment
WEBHOOK_RETENTION_DAYS = 7
REDPANDA_RETENTION_MESSAGES = 2000
AIRFLOW_LOG_RETENTION_DAYS = 30
DBT_TARGET_CLEANUP = True

def cleanup_webhook_files(**context):
    """
    Trigger webhook file cleanup via API
    """
    try:
        # Call webhook service cleanup endpoint
        response = requests.post('http://webhook-listener:8000/webhooks/cleanup', 
                               params={'dry_run': False}, 
                               timeout=300)
        
        if response.status_code == 200:
            result = response.json()
            logging.info(f"Webhook cleanup completed: {result}")
            return result
        else:
            logging.error(f"Webhook cleanup failed: {response.status_code} - {response.text}")
            raise Exception(f"Webhook cleanup API failed: {response.status_code}")
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to connect to webhook service: {e}")
        raise

def cleanup_redpanda_topic(**context):
    """
    Clean up old messages from Redpanda topic
    """
    try:
        # Run the existing cleanup script
        cmd = [
            'python3', '/opt/airflow/scripts/webhook/redpanda_cleanup.py',
            '--keep', str(REDPANDA_RETENTION_MESSAGES),
            '--execute'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, cwd='/opt/airflow')
        
        logging.info(f"Redpanda cleanup completed: {result.stdout}")
        return {"status": "success", "output": result.stdout}
        
    except subprocess.CalledProcessError as e:
        logging.error(f"Redpanda cleanup failed: {e.stderr}")
        raise Exception(f"Redpanda cleanup failed: {e.stderr}")

def cleanup_airflow_logs(**context):
    """
    Clean up old Airflow log files
    """
    try:
        logs_dir = Path('/opt/airflow/logs')
        cutoff_date = datetime.now() - timedelta(days=AIRFLOW_LOG_RETENTION_DAYS)
        
        files_deleted = 0
        space_freed = 0
        
        # Walk through log directories
        for log_file in logs_dir.rglob('*.log'):
            try:
                file_mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
                if file_mtime < cutoff_date:
                    file_size = log_file.stat().st_size
                    log_file.unlink()
                    files_deleted += 1
                    space_freed += file_size
                    logging.debug(f"Deleted old log: {log_file}")
            except (OSError, FileNotFoundError):
                # Log file may have been deleted by another process
                continue
        
        # Clean up empty directories
        for dir_path in logs_dir.rglob('*'):
            if dir_path.is_dir() and not any(dir_path.iterdir()):
                try:
                    dir_path.rmdir()
                    logging.debug(f"Removed empty directory: {dir_path}")
                except OSError:
                    continue
        
        space_freed_mb = space_freed / 1024 / 1024
        result = {
            "status": "success",
            "files_deleted": files_deleted,
            "space_freed_mb": round(space_freed_mb, 2),
            "retention_days": AIRFLOW_LOG_RETENTION_DAYS
        }
        
        logging.info(f"Airflow log cleanup completed: {result}")
        return result
        
    except Exception as e:
        logging.error(f"Airflow log cleanup failed: {e}")
        raise

def cleanup_dbt_artifacts(**context):
    """
    Clean up dbt target directory artifacts
    """
    try:
        dbt_target_dir = Path('/opt/airflow/dbt/target')
        
        if not dbt_target_dir.exists():
            logging.info("dbt target directory does not exist, skipping cleanup")
            return {"status": "skipped", "reason": "directory_not_found"}
        
        files_deleted = 0
        space_freed = 0
        
        # Keep only essential files, clean up old compilation artifacts
        files_to_clean = [
            'run_results.json',
            'manifest.json.old',
            'partial_parse.msgpack.old',
            'graph.gpickle.old'
        ]
        
        for file_pattern in files_to_clean:
            for file_path in dbt_target_dir.glob(file_pattern):
                try:
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    files_deleted += 1
                    space_freed += file_size
                    logging.debug(f"Deleted dbt artifact: {file_path}")
                except (OSError, FileNotFoundError):
                    continue
        
        # Clean up old run/test directories (keep only latest)
        for subdir in ['run', 'test']:
            subdir_path = dbt_target_dir / subdir
            if subdir_path.exists():
                try:
                    # Remove all but preserve structure
                    for item in subdir_path.rglob('*'):
                        if item.is_file() and item.stat().st_mtime < (datetime.now() - timedelta(days=1)).timestamp():
                            try:
                                file_size = item.stat().st_size
                                item.unlink()
                                files_deleted += 1
                                space_freed += file_size
                            except (OSError, FileNotFoundError):
                                continue
                except Exception as e:
                    logging.warning(f"Could not clean {subdir}: {e}")
        
        space_freed_mb = space_freed / 1024 / 1024
        result = {
            "status": "success",
            "files_deleted": files_deleted,
            "space_freed_mb": round(space_freed_mb, 2)
        }
        
        logging.info(f"dbt artifacts cleanup completed: {result}")
        return result
        
    except Exception as e:
        logging.error(f"dbt cleanup failed: {e}")
        raise

def get_disk_usage_stats(**context):
    """
    Get disk usage statistics before and after cleanup
    """
    try:
        # Get disk usage for key directories
        dirs_to_check = [
            '/opt/airflow/logs',
            '/opt/airflow/dbt/target',
            '/tmp'
        ]
        
        usage_stats = {}
        for dir_path in dirs_to_check:
            if Path(dir_path).exists():
                result = subprocess.run(['du', '-sh', dir_path], capture_output=True, text=True)
                if result.returncode == 0:
                    size = result.stdout.split('\t')[0]
                    usage_stats[dir_path] = size
        
        # Get overall disk usage
        result = subprocess.run(['df', '-h', '/'], capture_output=True, text=True)
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            if len(lines) > 1:
                disk_info = lines[1].split()
                usage_stats['root_filesystem'] = {
                    'size': disk_info[1],
                    'used': disk_info[2],
                    'available': disk_info[3],
                    'use_percent': disk_info[4]
                }
        
        logging.info(f"Disk usage stats: {usage_stats}")
        return usage_stats
        
    except Exception as e:
        logging.error(f"Failed to get disk usage stats: {e}")
        return {"error": str(e)}

# Task Definitions
get_initial_disk_usage = PythonOperator(
    task_id='get_initial_disk_usage',
    python_callable=get_disk_usage_stats,
    dag=dag
)

cleanup_webhooks = PythonOperator(
    task_id='cleanup_webhook_files',
    python_callable=cleanup_webhook_files,
    dag=dag
)

cleanup_redpanda = PythonOperator(
    task_id='cleanup_redpanda_topic',
    python_callable=cleanup_redpanda_topic,
    dag=dag
)

cleanup_logs = PythonOperator(
    task_id='cleanup_airflow_logs',
    python_callable=cleanup_airflow_logs,
    dag=dag
)

cleanup_dbt = PythonOperator(
    task_id='cleanup_dbt_artifacts',
    python_callable=cleanup_dbt_artifacts,
    dag=dag
)

get_final_disk_usage = PythonOperator(
    task_id='get_final_disk_usage',
    python_callable=get_disk_usage_stats,
    dag=dag
)

# Docker volume cleanup (optional, more aggressive)
cleanup_docker_volumes = BashOperator(
    task_id='cleanup_docker_system',
    bash_command="""
    echo "Cleaning up Docker system..."
    docker system prune -f --volumes --filter "until=72h"
    echo "Docker cleanup completed"
    """,
    dag=dag
)

# Task Dependencies
get_initial_disk_usage >> [cleanup_webhooks, cleanup_redpanda, cleanup_logs, cleanup_dbt]
[cleanup_webhooks, cleanup_redpanda, cleanup_logs, cleanup_dbt] >> cleanup_docker_volumes >> get_final_disk_usage