#!/usr/bin/env python3
"""
Example Redpanda consumer script for testing webhook data flow.
"""

import json
import asyncio
import signal
import sys
from datetime import datetime
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

# Configuration
import os
REDPANDA_BROKERS = os.getenv("REDPANDA_BROKERS", "localhost:9092")
WEBHOOK_TOPIC = os.getenv("WEBHOOK_TOPIC", "webhooks")
CONSUMER_GROUP = "webhook-consumer-group"

# Global flag for graceful shutdown
shutdown_event = asyncio.Event()

def signal_handler(sig, frame):
    """Handle shutdown signals."""
    print("\nShutting down consumer...")
    shutdown_event.set()

async def consume_webhooks():
    """Consume messages from the webhooks topic."""
    consumer = AIOKafkaConsumer(
        WEBHOOK_TOPIC,
        bootstrap_servers=REDPANDA_BROKERS,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',  # Start from beginning if no offset exists
        enable_auto_commit=True
    )
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Start the consumer
        await consumer.start()
        print(f"Connected to Redpanda broker: {REDPANDA_BROKERS}")
        print(f"Consuming from topic: {WEBHOOK_TOPIC}")
        print(f"Consumer group: {CONSUMER_GROUP}")
        print("\nWaiting for messages... (Press Ctrl+C to stop)\n")
        
        # Consume messages
        async for msg in consumer:
            if shutdown_event.is_set():
                break
                
            try:
                # Extract message details
                message_id = msg.value.get('message_id', 'unknown')
                timestamp = msg.value.get('timestamp', 'unknown')
                source_ip = msg.value.get('source_ip', 'unknown')
                file_path = msg.value.get('file_path', 'unknown')
                payload = msg.value.get('payload', {})
                
                # Display message info
                print(f"{'='*60}")
                print(f"Message ID: {message_id}")
                print(f"Timestamp: {timestamp}")
                print(f"Source IP: {source_ip}")
                print(f"File Path: {file_path}")
                print(f"Partition: {msg.partition}")
                print(f"Offset: {msg.offset}")
                print(f"Key: {msg.key.decode('utf-8') if msg.key else 'None'}")
                print(f"\nPayload Preview:")
                print(json.dumps(payload, indent=2)[:500] + "..." if len(json.dumps(payload)) > 500 else json.dumps(payload, indent=2))
                print(f"{'='*60}\n")
                
            except Exception as e:
                print(f"Error processing message: {e}")
                
    except KafkaError as e:
        print(f"Kafka error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Clean shutdown
        await consumer.stop()
        print("Consumer stopped.")

async def check_topic_exists():
    """Check if the webhooks topic exists."""
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    
    admin = AIOKafkaAdminClient(
        bootstrap_servers=REDPANDA_BROKERS
    )
    
    try:
        await admin.start()
        
        # Get topic metadata
        metadata = await admin.list_topics()
        
        if WEBHOOK_TOPIC not in metadata:
            print(f"Topic '{WEBHOOK_TOPIC}' does not exist. Creating it...")
            
            # Create the topic
            topic = NewTopic(
                name=WEBHOOK_TOPIC,
                num_partitions=3,
                replication_factor=1
            )
            
            await admin.create_topics([topic])
            print(f"Topic '{WEBHOOK_TOPIC}' created successfully.")
        else:
            print(f"Topic '{WEBHOOK_TOPIC}' exists.")
            
    except Exception as e:
        print(f"Error checking/creating topic: {e}")
    finally:
        await admin.close()

async def main():
    """Main entry point."""
    print("Redpanda Webhook Consumer")
    print("=" * 60)
    
    # Check if topic exists
    await check_topic_exists()
    
    # Start consuming
    await consume_webhooks()

if __name__ == "__main__":
    # Run the consumer
    asyncio.run(main())