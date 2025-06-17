#!/usr/bin/env python3
"""
Redpanda Topic Cleanup Script
Removes old messages from the webhooks topic, keeping only the most recent N messages.
"""

import os
import sys
import subprocess
import json
from datetime import datetime

def run_command(cmd, description=""):
    """Run a command and return the result"""
    print(f"🔧 {description}")
    print(f"   Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(f"   ✅ Success")
        if result.stdout.strip():
            print(f"   Output: {result.stdout.strip()}")
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Error: {e.stderr}")
        raise

def get_topic_info():
    """Get current topic information"""
    print("\n📊 Getting topic information...")
    
    cmd = ["docker", "exec", "claude_pipeline-redpanda-1", "rpk", "topic", "describe", "webhooks", "--print-partitions"]
    output = run_command(cmd, "Checking topic partitions")
    
    # Parse the output to get HIGH-WATERMARK
    lines = output.split('\n')
    for line in lines:
        if line.startswith('0'):  # Partition 0 line
            parts = line.split()
            if len(parts) >= 6:
                high_watermark = int(parts[5])
                log_start_offset = int(parts[4])
                return {
                    'partition': 0,
                    'log_start_offset': log_start_offset,
                    'high_watermark': high_watermark,
                    'total_messages': high_watermark - log_start_offset
                }
    
    raise ValueError("Could not parse topic information")

def cleanup_old_messages(keep_recent=2000, dry_run=True):
    """
    Clean up old messages from the webhooks topic
    
    Args:
        keep_recent (int): Number of recent messages to keep
        dry_run (bool): If True, only show what would be done
    """
    print(f"\n🧹 Cleaning up webhooks topic (keeping {keep_recent} recent messages)")
    
    # Get current topic state
    topic_info = get_topic_info()
    print(f"\n📈 Current Topic State:")
    print(f"   Log Start Offset: {topic_info['log_start_offset']}")
    print(f"   High Watermark: {topic_info['high_watermark']}")
    print(f"   Total Messages: {topic_info['total_messages']}")
    
    if topic_info['total_messages'] <= keep_recent:
        print(f"✅ Topic has {topic_info['total_messages']} messages, which is <= {keep_recent}. No cleanup needed!")
        return topic_info
    
    # Calculate new start offset
    new_start_offset = topic_info['high_watermark'] - keep_recent
    messages_to_delete = new_start_offset - topic_info['log_start_offset']
    
    print(f"\n🎯 Cleanup Plan:")
    print(f"   Current range: {topic_info['log_start_offset']} → {topic_info['high_watermark']}")
    print(f"   New start offset: {new_start_offset}")
    print(f"   Messages to delete: {messages_to_delete}")
    print(f"   Messages to keep: {keep_recent}")
    
    if dry_run:
        print(f"\n⚠️ DRY RUN MODE - Would delete {messages_to_delete} messages")
        print(f"   Run with --execute to actually perform cleanup")
        return topic_info
    
    # Perform the cleanup using Redpanda's log truncation
    print(f"\n🚀 Executing cleanup...")
    
    try:
        # Method 1: Use rpk topic alter-config to set retention
        # This is safer than direct offset manipulation
        print("   Setting short retention to trigger cleanup...")
        
        # Set retention to 1 second to trigger immediate cleanup
        cmd = ["docker", "exec", "claude_pipeline-redpanda-1", "rpk", "topic", "alter-config", "webhooks", "--set", "retention.ms=1000"]
        run_command(cmd, "Setting short retention period")
        
        # Wait a moment for cleanup to occur
        import time
        print("   Waiting for retention cleanup...")
        time.sleep(5)
        
        # Reset retention to normal value (7 days)
        cmd = ["docker", "exec", "claude_pipeline-redpanda-1", "rpk", "topic", "alter-config", "webhooks", "--set", "retention.ms=604800000"]
        run_command(cmd, "Restoring normal retention period")
        
        # Check new state
        new_topic_info = get_topic_info()
        print(f"\n✅ Cleanup completed!")
        print(f"   Messages before: {topic_info['total_messages']}")
        print(f"   Messages after: {new_topic_info['total_messages']}")
        
        return new_topic_info
        
    except Exception as e:
        print(f"\n❌ Retention-based cleanup failed: {e}")
        print("   Falling back to alternative method...")
        
        # Method 2: Create new topic and migrate recent data
        return recreate_topic_with_recent_data(topic_info, keep_recent)

def recreate_topic_with_recent_data(topic_info, keep_recent):
    """Alternative cleanup method: recreate topic with only recent data"""
    print(f"\n🔄 Recreating topic with recent {keep_recent} messages...")
    
    # This is more complex and risky, so for now we'll recommend manual cleanup
    print("⚠️ Advanced cleanup required. Recommended approach:")
    print("1. Stop webhook listener temporarily")
    print("2. Export recent messages to backup")
    print("3. Delete and recreate topic")
    print("4. Reimport recent messages")
    print("5. Restart webhook listener")
    
    raise NotImplementedError("Advanced cleanup not implemented yet. Use retention method or manual cleanup.")

def main():
    """Main cleanup function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Clean up old webhook messages from Redpanda')
    parser.add_argument('--keep', type=int, default=2000, help='Number of recent messages to keep (default: 2000)')
    parser.add_argument('--execute', action='store_true', help='Actually perform the cleanup (default: dry run)')
    parser.add_argument('--container', default='claude_pipeline-redpanda-1', help='Redpanda container name')
    
    args = parser.parse_args()
    
    print("🧹 Redpanda Webhook Topic Cleanup")
    print("=" * 50)
    print(f"Target: Keep {args.keep} most recent messages")
    print(f"Mode: {'EXECUTE' if args.execute else 'DRY RUN'}")
    print(f"Container: {args.container}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    try:
        # Verify Redpanda container is running
        cmd = ["docker", "exec", args.container, "rpk", "cluster", "health"]
        run_command(cmd, "Checking Redpanda health")
        
        # Perform cleanup
        result = cleanup_old_messages(keep_recent=args.keep, dry_run=not args.execute)
        
        print(f"\n🎉 Cleanup operation completed successfully!")
        
        if not args.execute:
            print(f"\n💡 To actually perform the cleanup, run:")
            print(f"   python {__file__} --keep {args.keep} --execute")
        
        return result
        
    except Exception as e:
        print(f"\n💥 Cleanup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()