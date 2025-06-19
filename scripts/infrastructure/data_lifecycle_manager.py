#!/usr/bin/env python3
"""
Data Lifecycle Management System

Implements tiered storage, compression, and archival policies for the medallion architecture.
Manages data retention across Bronze, Silver, and Gold layers with configurable policies.
"""

import os
import json
import logging
import boto3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from botocore.client import Config
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')

class DataLifecycleManager:
    """
    Manages data lifecycle across the medallion architecture with tiered storage,
    compression, and automated archival policies.
    """
    
    def __init__(self):
        self.s3_client = self._get_s3_client()
        self.lifecycle_policies = self._load_lifecycle_policies()
        
    def _get_s3_client(self):
        """Create S3 client for MinIO"""
        return boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
    
    def _load_lifecycle_policies(self) -> Dict[str, Any]:
        """Load data lifecycle policies configuration"""
        return {
            'webhook-data': {
                'bronze': {
                    'retention_days': 30,
                    'compression_after_days': 7,
                    'archive_after_days': 14,
                    'storage_class_transitions': [
                        {'days': 7, 'storage_class': 'STANDARD_IA'},
                        {'days': 30, 'storage_class': 'GLACIER'}
                    ]
                },
                'silver': {
                    'retention_days': 90,
                    'compression_after_days': 14,
                    'archive_after_days': 30,
                    'storage_class_transitions': [
                        {'days': 14, 'storage_class': 'STANDARD_IA'},
                        {'days': 60, 'storage_class': 'GLACIER'}
                    ]
                },
                'gold': {
                    'retention_days': 365,
                    'compression_after_days': 30,
                    'archive_after_days': 180,
                    'storage_class_transitions': [
                        {'days': 30, 'storage_class': 'STANDARD_IA'},
                        {'days': 180, 'storage_class': 'GLACIER'}
                    ]
                }
            },
            'solana-data': {
                'bronze': {
                    'retention_days': 30,
                    'compression_after_days': 7,
                    'archive_after_days': 14,
                    'storage_class_transitions': [
                        {'days': 7, 'storage_class': 'STANDARD_IA'},
                        {'days': 30, 'storage_class': 'GLACIER'}
                    ]
                },
                'silver': {
                    'retention_days': 180,  # 6 months for analytics
                    'compression_after_days': 14,
                    'archive_after_days': 60,
                    'storage_class_transitions': [
                        {'days': 14, 'storage_class': 'STANDARD_IA'},
                        {'days': 90, 'storage_class': 'GLACIER'}
                    ]
                },
                'gold': {
                    'retention_days': 730,  # 2 years for final analytics
                    'compression_after_days': 60,
                    'archive_after_days': 365,
                    'storage_class_transitions': [
                        {'days': 60, 'storage_class': 'STANDARD_IA'},
                        {'days': 365, 'storage_class': 'GLACIER'}
                    ]
                }
            }
        }
    
    def analyze_bucket_data_age(self, bucket_name: str) -> Dict[str, Any]:
        """
        Analyze data age distribution across all layers in a bucket
        """
        logger.info(f"Analyzing data age for bucket: {bucket_name}")
        
        analysis = {
            'bucket_name': bucket_name,
            'analysis_timestamp': datetime.now().isoformat(),
            'layers': {},
            'total_objects': 0,
            'total_size_bytes': 0
        }
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            for layer in ['bronze', 'silver', 'gold']:
                layer_stats = {
                    'object_count': 0,
                    'total_size_bytes': 0,
                    'age_distribution': {
                        '0-7_days': 0,
                        '7-30_days': 0,
                        '30-90_days': 0,
                        '90-365_days': 0,
                        'over_1_year': 0
                    },
                    'oldest_object': None,
                    'newest_object': None,
                    'lifecycle_candidates': {
                        'compression': 0,
                        'archive': 0,
                        'deletion': 0
                    }
                }
                
                try:
                    for page in paginator.paginate(Bucket=bucket_name, Prefix=f'{layer}/'):
                        if 'Contents' in page:
                            for obj in page['Contents']:
                                layer_stats['object_count'] += 1
                                layer_stats['total_size_bytes'] += obj['Size']
                                
                                # Calculate age
                                obj_age = datetime.now(obj['LastModified'].tzinfo) - obj['LastModified']
                                age_days = obj_age.days
                                
                                # Age distribution
                                if age_days <= 7:
                                    layer_stats['age_distribution']['0-7_days'] += 1
                                elif age_days <= 30:
                                    layer_stats['age_distribution']['7-30_days'] += 1
                                elif age_days <= 90:
                                    layer_stats['age_distribution']['30-90_days'] += 1
                                elif age_days <= 365:
                                    layer_stats['age_distribution']['90-365_days'] += 1
                                else:
                                    layer_stats['age_distribution']['over_1_year'] += 1
                                
                                # Track oldest/newest
                                if layer_stats['oldest_object'] is None or obj['LastModified'] < layer_stats['oldest_object']['last_modified']:
                                    layer_stats['oldest_object'] = {
                                        'key': obj['Key'],
                                        'last_modified': obj['LastModified'],
                                        'age_days': age_days
                                    }
                                
                                if layer_stats['newest_object'] is None or obj['LastModified'] > layer_stats['newest_object']['last_modified']:
                                    layer_stats['newest_object'] = {
                                        'key': obj['Key'],
                                        'last_modified': obj['LastModified'],
                                        'age_days': age_days
                                    }
                                
                                # Lifecycle candidates
                                if bucket_name in self.lifecycle_policies:
                                    layer_policy = self.lifecycle_policies[bucket_name].get(layer, {})
                                    
                                    if age_days >= layer_policy.get('compression_after_days', 999):
                                        layer_stats['lifecycle_candidates']['compression'] += 1
                                    
                                    if age_days >= layer_policy.get('archive_after_days', 999):
                                        layer_stats['lifecycle_candidates']['archive'] += 1
                                    
                                    if age_days >= layer_policy.get('retention_days', 999):
                                        layer_stats['lifecycle_candidates']['deletion'] += 1
                
                except ClientError as e:
                    logger.warning(f"Could not analyze layer {layer} in bucket {bucket_name}: {e}")
                
                # Convert to MB for readability
                layer_stats['total_size_mb'] = round(layer_stats['total_size_bytes'] / 1024 / 1024, 2)
                
                analysis['layers'][layer] = layer_stats
                analysis['total_objects'] += layer_stats['object_count']
                analysis['total_size_bytes'] += layer_stats['total_size_bytes']
            
            analysis['total_size_mb'] = round(analysis['total_size_bytes'] / 1024 / 1024, 2)
            analysis['total_size_gb'] = round(analysis['total_size_bytes'] / 1024 / 1024 / 1024, 2)
            
            return analysis
            
        except ClientError as e:
            logger.error(f"Failed to analyze bucket {bucket_name}: {e}")
            return analysis
    
    def apply_lifecycle_policies(self, bucket_name: str, dry_run: bool = True) -> Dict[str, Any]:
        """
        Apply lifecycle policies to a bucket based on the configured rules
        """
        logger.info(f"Applying lifecycle policies to bucket: {bucket_name} (dry_run={dry_run})")
        
        if bucket_name not in self.lifecycle_policies:
            raise ValueError(f"No lifecycle policies defined for bucket: {bucket_name}")
        
        bucket_policies = self.lifecycle_policies[bucket_name]
        
        # Create lifecycle configuration
        lifecycle_rules = []
        
        for layer, policy in bucket_policies.items():
            # Basic expiration rule
            rule = {
                'ID': f'{bucket_name}-{layer}-lifecycle',
                'Status': 'Enabled',
                'Filter': {'Prefix': f'{layer}/'},
                'Expiration': {'Days': policy['retention_days']}
            }
            
            # Add storage class transitions if supported by MinIO
            if policy.get('storage_class_transitions'):
                rule['Transitions'] = []
                for transition in policy['storage_class_transitions']:
                    rule['Transitions'].append({
                        'Days': transition['days'],
                        'StorageClass': transition['storage_class']
                    })
            
            lifecycle_rules.append(rule)
        
        lifecycle_config = {'Rules': lifecycle_rules}
        
        result = {
            'bucket_name': bucket_name,
            'action': 'apply_lifecycle_policies',
            'dry_run': dry_run,
            'timestamp': datetime.now().isoformat(),
            'policies_applied': len(lifecycle_rules),
            'lifecycle_config': lifecycle_config,
            'status': 'success'
        }
        
        if not dry_run:
            try:
                self.s3_client.put_bucket_lifecycle_configuration(
                    Bucket=bucket_name,
                    LifecycleConfiguration=lifecycle_config
                )
                logger.info(f"✅ Applied {len(lifecycle_rules)} lifecycle rules to {bucket_name}")
                
                # Verify the configuration was applied
                response = self.s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
                result['verification'] = {
                    'rules_active': len(response.get('Rules', [])),
                    'configuration_confirmed': True
                }
                
            except ClientError as e:
                logger.error(f"❌ Failed to apply lifecycle policies to {bucket_name}: {e}")
                result['status'] = 'error'
                result['error'] = str(e)
        else:
            logger.info(f"🔍 DRY RUN: Would apply {len(lifecycle_rules)} lifecycle rules to {bucket_name}")
            for rule in lifecycle_rules:
                logger.info(f"  - {rule['ID']}: {rule['Filter']['Prefix']} -> expire after {rule['Expiration']['Days']} days")
        
        return result
    
    def optimize_storage_costs(self, bucket_name: str, dry_run: bool = True) -> Dict[str, Any]:
        """
        Analyze and optimize storage costs through compression and archival
        """
        logger.info(f"Optimizing storage costs for bucket: {bucket_name}")
        
        # First, analyze current data
        analysis = self.analyze_bucket_data_age(bucket_name)
        
        optimization_plan = {
            'bucket_name': bucket_name,
            'analysis_timestamp': datetime.now().isoformat(),
            'current_stats': analysis,
            'optimization_opportunities': {},
            'estimated_savings': {},
            'dry_run': dry_run
        }
        
        total_compression_candidates = 0
        total_archive_candidates = 0
        total_deletion_candidates = 0
        
        for layer, layer_stats in analysis['layers'].items():
            layer_opportunities = {
                'compression_candidates': layer_stats['lifecycle_candidates']['compression'],
                'archive_candidates': layer_stats['lifecycle_candidates']['archive'],
                'deletion_candidates': layer_stats['lifecycle_candidates']['deletion'],
                'current_size_mb': layer_stats['total_size_mb']
            }
            
            # Estimate storage savings (rough calculations)
            compression_savings_mb = layer_opportunities['compression_candidates'] * 0.3  # 30% compression
            archive_savings_mb = layer_opportunities['archive_candidates'] * 0.7  # 70% cost reduction
            deletion_savings_mb = layer_stats['total_size_mb'] * (layer_opportunities['deletion_candidates'] / max(layer_stats['object_count'], 1))
            
            layer_opportunities['estimated_savings'] = {
                'compression_mb': round(compression_savings_mb, 2),
                'archive_mb': round(archive_savings_mb, 2),
                'deletion_mb': round(deletion_savings_mb, 2),
                'total_savings_mb': round(compression_savings_mb + archive_savings_mb + deletion_savings_mb, 2)
            }
            
            optimization_plan['optimization_opportunities'][layer] = layer_opportunities
            
            total_compression_candidates += layer_opportunities['compression_candidates']
            total_archive_candidates += layer_opportunities['archive_candidates']
            total_deletion_candidates += layer_opportunities['deletion_candidates']
        
        optimization_plan['summary'] = {
            'total_compression_candidates': total_compression_candidates,
            'total_archive_candidates': total_archive_candidates,
            'total_deletion_candidates': total_deletion_candidates,
            'total_current_size_mb': analysis['total_size_mb'],
            'estimated_total_savings_mb': sum(
                layer['estimated_savings']['total_savings_mb'] 
                for layer in optimization_plan['optimization_opportunities'].values()
            )
        }
        
        # Calculate percentage savings
        if analysis['total_size_mb'] > 0:
            savings_percentage = (optimization_plan['summary']['estimated_total_savings_mb'] / analysis['total_size_mb']) * 100
            optimization_plan['summary']['estimated_savings_percentage'] = round(savings_percentage, 2)
        
        logger.info(f"📊 Storage optimization analysis completed:")
        logger.info(f"   Current size: {analysis['total_size_mb']:.2f}MB")
        logger.info(f"   Compression candidates: {total_compression_candidates} objects")
        logger.info(f"   Archive candidates: {total_archive_candidates} objects")
        logger.info(f"   Deletion candidates: {total_deletion_candidates} objects")
        logger.info(f"   Estimated savings: {optimization_plan['summary']['estimated_total_savings_mb']:.2f}MB ({optimization_plan['summary'].get('estimated_savings_percentage', 0):.1f}%)")
        
        return optimization_plan
    
    def generate_lifecycle_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive data lifecycle management report
        """
        logger.info("Generating comprehensive data lifecycle report...")
        
        report = {
            'report_timestamp': datetime.now().isoformat(),
            'lifecycle_policies': self.lifecycle_policies,
            'bucket_analyses': {},
            'summary': {
                'total_buckets': 0,
                'total_objects': 0,
                'total_size_gb': 0,
                'optimization_potential_mb': 0
            },
            'recommendations': []
        }
        
        # Analyze each bucket
        for bucket_name in self.lifecycle_policies.keys():
            try:
                logger.info(f"Analyzing bucket: {bucket_name}")
                
                # Data age analysis
                age_analysis = self.analyze_bucket_data_age(bucket_name)
                
                # Storage optimization analysis
                optimization_analysis = self.optimize_storage_costs(bucket_name, dry_run=True)
                
                report['bucket_analyses'][bucket_name] = {
                    'age_analysis': age_analysis,
                    'optimization_analysis': optimization_analysis
                }
                
                # Update summary
                report['summary']['total_buckets'] += 1
                report['summary']['total_objects'] += age_analysis['total_objects']
                report['summary']['total_size_gb'] += age_analysis['total_size_gb']
                report['summary']['optimization_potential_mb'] += optimization_analysis['summary']['estimated_total_savings_mb']
                
                # Generate bucket-specific recommendations
                if optimization_analysis['summary']['estimated_savings_percentage'] > 20:
                    report['recommendations'].append(f"High optimization potential for {bucket_name}: {optimization_analysis['summary']['estimated_savings_percentage']:.1f}% savings possible")
                
                if age_analysis['total_size_gb'] > 5:
                    report['recommendations'].append(f"Large bucket {bucket_name} ({age_analysis['total_size_gb']:.1f}GB) should have active lifecycle policies")
                
            except Exception as e:
                logger.error(f"Failed to analyze bucket {bucket_name}: {e}")
                report['bucket_analyses'][bucket_name] = {'error': str(e)}
        
        # Overall recommendations
        if report['summary']['optimization_potential_mb'] > 1000:  # >1GB potential savings
            report['recommendations'].append("Significant storage optimization potential - implement lifecycle policies immediately")
        
        if report['summary']['total_size_gb'] > 20:  # >20GB total
            report['recommendations'].append("Large data footprint - consider tiered storage strategies")
        
        logger.info(f"📋 Lifecycle report generated:")
        logger.info(f"   Buckets analyzed: {report['summary']['total_buckets']}")
        logger.info(f"   Total objects: {report['summary']['total_objects']}")
        logger.info(f"   Total size: {report['summary']['total_size_gb']:.2f}GB")
        logger.info(f"   Optimization potential: {report['summary']['optimization_potential_mb']:.2f}MB")
        logger.info(f"   Recommendations: {len(report['recommendations'])}")
        
        return report

def main():
    """Main function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Data Lifecycle Management System')
    parser.add_argument('action', choices=['analyze', 'optimize', 'apply-policies', 'report'], 
                       help='Action to perform')
    parser.add_argument('--bucket', help='Bucket name (required for analyze, optimize, apply-policies)')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without applying')
    parser.add_argument('--endpoint', default=MINIO_ENDPOINT, help='MinIO endpoint URL')
    parser.add_argument('--access-key', default=MINIO_ACCESS_KEY, help='MinIO access key')
    parser.add_argument('--secret-key', default=MINIO_SECRET_KEY, help='MinIO secret key')
    
    args = parser.parse_args()
    
    # Override configuration with command line arguments
    global MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
    MINIO_ENDPOINT = args.endpoint
    MINIO_ACCESS_KEY = args.access_key
    MINIO_SECRET_KEY = args.secret_key
    
    print("📊 Data Lifecycle Management System")
    print("=" * 60)
    print(f"Endpoint: {MINIO_ENDPOINT}")
    print(f"Action: {args.action}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    try:
        manager = DataLifecycleManager()
        
        if args.action == 'analyze':
            if not args.bucket:
                print("❌ --bucket is required for analyze action")
                return 1
            
            result = manager.analyze_bucket_data_age(args.bucket)
            print(json.dumps(result, indent=2, default=str))
            
        elif args.action == 'optimize':
            if not args.bucket:
                print("❌ --bucket is required for optimize action")
                return 1
            
            result = manager.optimize_storage_costs(args.bucket, dry_run=args.dry_run)
            print(json.dumps(result, indent=2, default=str))
            
        elif args.action == 'apply-policies':
            if not args.bucket:
                print("❌ --bucket is required for apply-policies action")
                return 1
            
            result = manager.apply_lifecycle_policies(args.bucket, dry_run=args.dry_run)
            print(json.dumps(result, indent=2, default=str))
            
        elif args.action == 'report':
            result = manager.generate_lifecycle_report()
            print(json.dumps(result, indent=2, default=str))
        
    except Exception as e:
        print(f"💥 Operation failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())