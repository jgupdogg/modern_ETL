"""
Disk Usage Monitoring and Alerting DAG

Monitors disk usage across all critical directories and Docker volumes to prevent
future 19GB accumulation issues. Provides early warning alerts and automated
response recommendations.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
import subprocess
import logging
import json
import requests
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
    'disk_usage_monitoring',
    default_args=DEFAULT_ARGS,
    description='Monitor disk usage and prevent data accumulation',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['monitoring', 'disk', 'alerts', 'storage']
)

# Alert Thresholds (in MB)
DISK_THRESHOLDS = {
    'critical': 5000,    # 5GB - trigger immediate cleanup
    'warning': 2000,     # 2GB - early warning
    'info': 1000         # 1GB - information only
}

# Docker Volume Thresholds (in GB)
VOLUME_THRESHOLDS = {
    'critical': 20,      # 20GB - critical alert
    'warning': 10,       # 10GB - warning
    'info': 5            # 5GB - information
}

def get_directory_usage(directory_path):
    """Get disk usage for a specific directory"""
    try:
        result = subprocess.run(['du', '-s', '-m', directory_path], 
                              capture_output=True, text=True, check=True)
        size_mb = int(result.stdout.split('\t')[0])
        return size_mb
    except (subprocess.CalledProcessError, ValueError, IndexError):
        return 0

def get_docker_volume_usage():
    """Get Docker volume usage statistics"""
    try:
        # Get Docker system info
        result = subprocess.run(['docker', 'system', 'df', '--format', 'json'], 
                              capture_output=True, text=True, check=True)
        
        system_info = json.loads(result.stdout)
        
        # Get volume-specific information
        volumes_result = subprocess.run(['docker', 'volume', 'ls', '--format', 'json'], 
                                      capture_output=True, text=True, check=True)
        
        volumes = []
        for line in volumes_result.stdout.strip().split('\n'):
            if line:
                volumes.append(json.loads(line))
        
        # Get volume sizes (approximate via inspect)
        volume_usage = {}
        for volume in volumes:
            volume_name = volume['Name']
            if 'claude_pipeline' in volume_name:
                try:
                    inspect_result = subprocess.run(['docker', 'volume', 'inspect', volume_name], 
                                                  capture_output=True, text=True, check=True)
                    volume_info = json.loads(inspect_result.stdout)[0]
                    mount_point = volume_info['Mountpoint']
                    
                    # Get actual size
                    du_result = subprocess.run(['sudo', 'du', '-s', '-B', '1G', mount_point], 
                                             capture_output=True, text=True)
                    if du_result.returncode == 0:
                        size_gb = float(du_result.stdout.split('\t')[0])
                    else:
                        size_gb = 0
                    
                    volume_usage[volume_name] = {
                        'size_gb': size_gb,
                        'mountpoint': mount_point
                    }
                except Exception as e:
                    logging.warning(f"Could not get size for volume {volume_name}: {e}")
                    volume_usage[volume_name] = {'size_gb': 0, 'mountpoint': 'unknown'}
        
        return {
            'system_info': system_info,
            'volumes': volume_usage
        }
        
    except Exception as e:
        logging.error(f"Failed to get Docker volume usage: {e}")
        return {'system_info': {}, 'volumes': {}}

def monitor_disk_usage(**context):
    """Main disk usage monitoring function"""
    
    monitoring_results = {
        'timestamp': datetime.now().isoformat(),
        'directories': {},
        'docker_volumes': {},
        'alerts': [],
        'recommendations': []
    }
    
    # Directories to monitor
    directories_to_check = [
        '/opt/airflow/logs',
        '/opt/airflow/dbt/target', 
        '/tmp',
        '/var/log',
        '/home'  # User directories
    ]
    
    # Check directory usage
    for directory in directories_to_check:
        if Path(directory).exists():
            size_mb = get_directory_usage(directory)
            monitoring_results['directories'][directory] = {
                'size_mb': size_mb,
                'size_gb': round(size_mb / 1024, 2),
                'status': 'healthy'
            }
            
            # Check thresholds
            if size_mb >= DISK_THRESHOLDS['critical']:
                status = 'critical'
                monitoring_results['alerts'].append({
                    'type': 'CRITICAL',
                    'location': directory,
                    'size_mb': size_mb,
                    'threshold': DISK_THRESHOLDS['critical'],
                    'message': f"Directory {directory} is {size_mb}MB (>{DISK_THRESHOLDS['critical']}MB critical threshold)"
                })
                monitoring_results['recommendations'].append(f"IMMEDIATE ACTION: Clean up {directory} directory")
                
            elif size_mb >= DISK_THRESHOLDS['warning']:
                status = 'warning'
                monitoring_results['alerts'].append({
                    'type': 'WARNING',
                    'location': directory,
                    'size_mb': size_mb,
                    'threshold': DISK_THRESHOLDS['warning'],
                    'message': f"Directory {directory} is {size_mb}MB (>{DISK_THRESHOLDS['warning']}MB warning threshold)"
                })
                monitoring_results['recommendations'].append(f"Schedule cleanup for {directory} directory")
                
            elif size_mb >= DISK_THRESHOLDS['info']:
                status = 'info'
                monitoring_results['alerts'].append({
                    'type': 'INFO',
                    'location': directory,
                    'size_mb': size_mb,
                    'threshold': DISK_THRESHOLDS['info'],
                    'message': f"Directory {directory} is {size_mb}MB (>{DISK_THRESHOLDS['info']}MB info threshold)"
                })
            else:
                status = 'healthy'
            
            monitoring_results['directories'][directory]['status'] = status
    
    # Check Docker volumes
    docker_usage = get_docker_volume_usage()
    monitoring_results['docker_volumes'] = docker_usage['volumes']
    
    for volume_name, volume_info in docker_usage['volumes'].items():
        size_gb = volume_info['size_gb']
        
        if size_gb >= VOLUME_THRESHOLDS['critical']:
            monitoring_results['alerts'].append({
                'type': 'CRITICAL',
                'location': f"Docker volume: {volume_name}",
                'size_gb': size_gb,
                'threshold': VOLUME_THRESHOLDS['critical'],
                'message': f"Docker volume {volume_name} is {size_gb}GB (>{VOLUME_THRESHOLDS['critical']}GB critical threshold)"
            })
            monitoring_results['recommendations'].append(f"IMMEDIATE ACTION: Clean up Docker volume {volume_name}")
            
        elif size_gb >= VOLUME_THRESHOLDS['warning']:
            monitoring_results['alerts'].append({
                'type': 'WARNING',
                'location': f"Docker volume: {volume_name}",
                'size_gb': size_gb,
                'threshold': VOLUME_THRESHOLDS['warning'],
                'message': f"Docker volume {volume_name} is {size_gb}GB (>{VOLUME_THRESHOLDS['warning']}GB warning threshold)"
            })
            monitoring_results['recommendations'].append(f"Schedule cleanup for Docker volume {volume_name}")
    
    # Overall system disk usage
    try:
        df_result = subprocess.run(['df', '-h', '/'], capture_output=True, text=True, check=True)
        df_lines = df_result.stdout.strip().split('\n')
        if len(df_lines) > 1:
            disk_info = df_lines[1].split()
            monitoring_results['system_disk'] = {
                'filesystem': disk_info[0],
                'size': disk_info[1],
                'used': disk_info[2],
                'available': disk_info[3],
                'use_percent': disk_info[4],
                'mounted_on': disk_info[5]
            }
            
            # Check if root filesystem is getting full
            use_percent = int(disk_info[4].replace('%', ''))
            if use_percent >= 90:
                monitoring_results['alerts'].append({
                    'type': 'CRITICAL',
                    'location': 'Root filesystem',
                    'use_percent': use_percent,
                    'message': f"Root filesystem is {use_percent}% full"
                })
                monitoring_results['recommendations'].append("IMMEDIATE ACTION: Free up disk space on root filesystem")
            elif use_percent >= 80:
                monitoring_results['alerts'].append({
                    'type': 'WARNING',
                    'location': 'Root filesystem',
                    'use_percent': use_percent,
                    'message': f"Root filesystem is {use_percent}% full"
                })
                
    except Exception as e:
        logging.error(f"Failed to get system disk usage: {e}")
    
    # Log results
    total_alerts = len(monitoring_results['alerts'])
    critical_alerts = len([a for a in monitoring_results['alerts'] if a['type'] == 'CRITICAL'])
    warning_alerts = len([a for a in monitoring_results['alerts'] if a['type'] == 'WARNING'])
    
    logging.info(f"Disk monitoring completed: {total_alerts} alerts ({critical_alerts} critical, {warning_alerts} warnings)")
    
    if critical_alerts > 0:
        logging.error(f"🚨 CRITICAL DISK ALERTS: {critical_alerts} locations require immediate attention")
        for alert in [a for a in monitoring_results['alerts'] if a['type'] == 'CRITICAL']:
            logging.error(f"   - {alert['message']}")
    
    if warning_alerts > 0:
        logging.warning(f"⚠️ DISK WARNINGS: {warning_alerts} locations approaching limits")
        for alert in [a for a in monitoring_results['alerts'] if a['type'] == 'WARNING']:
            logging.warning(f"   - {alert['message']}")
    
    if total_alerts == 0:
        logging.info("✅ All disk usage within normal limits")
    
    # Store results for downstream tasks
    return monitoring_results

def check_webhook_service_disk(**context):
    """Check webhook service disk usage via API"""
    try:
        # Check if webhook service is running and get count
        response = requests.get('http://webhook-listener:8000/webhooks/count', timeout=10)
        
        if response.status_code == 200:
            webhook_data = response.json()
            webhook_count = webhook_data.get('count', 0)
            
            # Get cleanup preview
            cleanup_response = requests.get('http://webhook-listener:8000/webhooks/cleanup/dry-run', timeout=10)
            
            if cleanup_response.status_code == 200:
                cleanup_preview = cleanup_response.json()
                
                return {
                    'webhook_files_count': webhook_count,
                    'cleanup_preview': cleanup_preview,
                    'service_status': 'healthy'
                }
            else:
                return {
                    'webhook_files_count': webhook_count,
                    'cleanup_preview': {'error': 'cleanup endpoint unavailable'},
                    'service_status': 'degraded'
                }
        else:
            return {
                'webhook_files_count': 0,
                'cleanup_preview': {'error': 'service unavailable'},
                'service_status': 'down'
            }
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to check webhook service: {e}")
        return {
            'webhook_files_count': 0,
            'cleanup_preview': {'error': str(e)},
            'service_status': 'error'
        }

def generate_monitoring_report(disk_monitoring_result, webhook_monitoring_result, **context):
    """Generate comprehensive monitoring report"""
    
    report = {
        'monitoring_date': context['ds'],
        'monitoring_timestamp': disk_monitoring_result['timestamp'],
        'overall_status': 'healthy',
        'summary': {
            'total_alerts': len(disk_monitoring_result['alerts']),
            'critical_alerts': len([a for a in disk_monitoring_result['alerts'] if a['type'] == 'CRITICAL']),
            'warning_alerts': len([a for a in disk_monitoring_result['alerts'] if a['type'] == 'WARNING']),
            'directories_monitored': len(disk_monitoring_result['directories']),
            'docker_volumes_monitored': len(disk_monitoring_result['docker_volumes'])
        },
        'disk_usage': disk_monitoring_result,
        'webhook_service': webhook_monitoring_result,
        'action_required': []
    }
    
    # Determine overall status
    if report['summary']['critical_alerts'] > 0:
        report['overall_status'] = 'critical'
        report['action_required'].append('Immediate cleanup required for critical alerts')
    elif report['summary']['warning_alerts'] > 0:
        report['overall_status'] = 'warning'
        report['action_required'].append('Schedule cleanup for warning alerts')
    
    # Add webhook-specific recommendations
    if webhook_monitoring_result['service_status'] == 'healthy':
        cleanup_preview = webhook_monitoring_result.get('cleanup_preview', {})
        if cleanup_preview.get('files_deleted', 0) > 1000:
            report['action_required'].append('Webhook cleanup recommended - many old files detected')
    
    # Log comprehensive report
    print("=" * 80)
    print("🔍 DISK USAGE MONITORING REPORT")
    print("=" * 80)
    print(f"Status: {report['overall_status'].upper()}")
    print(f"Alerts: {report['summary']['total_alerts']} total ({report['summary']['critical_alerts']} critical)")
    print(f"Webhook Files: {webhook_monitoring_result['webhook_files_count']}")
    
    if report['action_required']:
        print("\n⚠️ ACTIONS REQUIRED:")
        for action in report['action_required']:
            print(f"  - {action}")
    
    if disk_monitoring_result.get('recommendations'):
        print(f"\n💡 RECOMMENDATIONS:")
        for rec in disk_monitoring_result['recommendations']:
            print(f"  - {rec}")
    
    print("\n📊 DIRECTORY SIZES:")
    for dir_path, dir_info in disk_monitoring_result['directories'].items():
        status_icon = {"healthy": "✅", "warning": "⚠️", "critical": "🚨"}.get(dir_info['status'], "❓")
        print(f"  {status_icon} {dir_path}: {dir_info['size_mb']}MB ({dir_info['status']})")
    
    if disk_monitoring_result['docker_volumes']:
        print(f"\n🐳 DOCKER VOLUMES:")
        for vol_name, vol_info in disk_monitoring_result['docker_volumes'].items():
            print(f"  📦 {vol_name}: {vol_info['size_gb']}GB")
    
    print("=" * 80)
    
    return report

# Task Definitions
monitor_disk = PythonOperator(
    task_id='monitor_disk_usage',
    python_callable=monitor_disk_usage,
    dag=dag
)

check_webhook_disk = PythonOperator(
    task_id='check_webhook_service_disk',
    python_callable=check_webhook_service_disk,
    dag=dag
)

generate_report = PythonOperator(
    task_id='generate_monitoring_report',
    python_callable=generate_monitoring_report,
    op_args=[
        "{{ task_instance.xcom_pull(task_ids='monitor_disk_usage') }}",
        "{{ task_instance.xcom_pull(task_ids='check_webhook_service_disk') }}"
    ],
    dag=dag
)

# Emergency cleanup trigger (conditional)
emergency_cleanup = BashOperator(
    task_id='emergency_cleanup_trigger',
    bash_command="""
    echo "Checking if emergency cleanup is needed..."
    
    # This would trigger the cleanup DAG if critical alerts are present
    # For now, just log the recommendation
    echo "Emergency cleanup would be triggered here if critical alerts detected"
    echo "Run: docker-compose run airflow-cli airflow dags trigger cleanup_maintenance"
    """,
    dag=dag
)

# Task Dependencies
[monitor_disk, check_webhook_disk] >> generate_report >> emergency_cleanup