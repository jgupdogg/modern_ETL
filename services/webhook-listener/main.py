import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import uuid4
import asyncio

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Webhook Listener", version="1.0.0")

# Configuration
DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Redpanda configuration
REDPANDA_BROKERS = os.getenv("REDPANDA_BROKERS", "localhost:9092")
WEBHOOK_TOPIC = os.getenv("WEBHOOK_TOPIC", "webhooks")

# Global Kafka producer
producer: Optional[AIOKafkaProducer] = None

class WebhookResponse(BaseModel):
    status: str
    message_id: str
    timestamp: str

async def get_kafka_producer():
    """Get or create Kafka producer instance."""
    global producer
    if producer is None:
        producer = AIOKafkaProducer(
            bootstrap_servers=REDPANDA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await producer.start()
        logger.info(f"Kafka producer connected to {REDPANDA_BROKERS}")
    return producer

@app.on_event("startup")
async def startup_event():
    """Initialize Kafka producer on startup."""
    try:
        await get_kafka_producer()
        logger.info("Kafka producer initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Kafka producer: {e}")
        # Continue running even if Kafka is not available

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up Kafka producer on shutdown."""
    global producer
    if producer:
        await producer.stop()
        logger.info("Kafka producer stopped")

@app.get("/")
async def root():
    return {
        "message": "Webhook Listener is running", 
        "version": "1.0.0",
        "redpanda_enabled": producer is not None
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@app.post("/webhooks")
async def webhook_listener(request: Request) -> WebhookResponse:
    """
    Receive webhook payloads and store them as JSON files.
    """
    try:
        # Get the raw payload
        payload = await request.json()
        
        # Generate unique ID and timestamp
        message_id = str(uuid4())
        timestamp = datetime.utcnow()
        
        # Create date-based directory structure
        date_dir = DATA_DIR / timestamp.strftime("%Y/%m/%d")
        date_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare the data to save
        webhook_data = {
            "message_id": message_id,
            "timestamp": timestamp.isoformat(),
            "payload": payload,
            "headers": dict(request.headers),
            "source_ip": request.client.host if request.client else None
        }
        
        # Save to file
        filename = f"{timestamp.strftime('%Y%m%d_%H%M%S')}_{message_id}.json"
        file_path = date_dir / filename
        
        with open(file_path, 'w') as f:
            json.dump(webhook_data, f, indent=2, default=str)
        
        logger.info(f"Webhook saved: {file_path}")
        
        # Publish to Redpanda if available
        if producer:
            try:
                # Create Kafka message
                kafka_message = {
                    "message_id": message_id,
                    "timestamp": timestamp.isoformat(),
                    "payload": payload,
                    "headers": dict(request.headers),
                    "source_ip": request.client.host if request.client else None,
                    "file_path": str(file_path)
                }
                
                # Send to Kafka
                await producer.send_and_wait(
                    WEBHOOK_TOPIC,
                    value=kafka_message,
                    key=message_id.encode('utf-8')
                )
                logger.info(f"Webhook published to Redpanda topic '{WEBHOOK_TOPIC}': {message_id}")
            except Exception as e:
                logger.error(f"Failed to publish to Redpanda: {e}")
                # Continue even if Kafka publish fails
        
        return WebhookResponse(
            status="success",
            message_id=message_id,
            timestamp=timestamp.isoformat()
        )
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON payload: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/webhooks/count")
async def get_webhook_count():
    """
    Get count of stored webhooks.
    """
    try:
        count = sum(1 for _ in DATA_DIR.rglob("*.json"))
        return {"count": count, "data_directory": str(DATA_DIR)}
    except Exception as e:
        logger.error(f"Error counting webhooks: {e}")
        raise HTTPException(status_code=500, detail="Error counting webhooks")

@app.get("/webhooks/status")
async def get_webhook_status():
    """
    Get current webhook registration status.
    """
    try:
        url_file = DATA_DIR / "current_webhook_url.txt"
        if url_file.exists():
            with open(url_file, 'r') as f:
                webhook_url = f.read().strip()
            return {
                "status": "registered",
                "webhook_url": webhook_url,
                "helius_configured": bool(os.getenv("HELIUS_API_KEY")),
                "redpanda_enabled": producer is not None,
                "redpanda_brokers": REDPANDA_BROKERS,
                "redpanda_topic": WEBHOOK_TOPIC
            }
        else:
            return {
                "status": "not_registered",
                "webhook_url": None,
                "helius_configured": bool(os.getenv("HELIUS_API_KEY")),
                "redpanda_enabled": producer is not None,
                "redpanda_brokers": REDPANDA_BROKERS,
                "redpanda_topic": WEBHOOK_TOPIC
            }
    except Exception as e:
        logger.error(f"Error getting webhook status: {e}")
        raise HTTPException(status_code=500, detail="Error getting webhook status")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)