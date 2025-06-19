import os
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import uuid4
import asyncio
import shutil

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

# Cleanup configuration
WEBHOOK_RETENTION_DAYS = int(os.getenv("WEBHOOK_RETENTION_DAYS", "7"))
CLEANUP_ENABLED = os.getenv("CLEANUP_ENABLED", "true").lower() == "true"
CLEANUP_ON_STARTUP = os.getenv("CLEANUP_ON_STARTUP", "true").lower() == "true"

# Redpanda configuration
REDPANDA_BROKERS = os.getenv("REDPANDA_BROKERS", "localhost:9092")
WEBHOOK_TOPIC = os.getenv("WEBHOOK_TOPIC", "webhooks")

# Global Kafka producer
producer: Optional[AIOKafkaProducer] = None

class WebhookResponse(BaseModel):
    status: str
    message_id: str
    timestamp: str

class CleanupResponse(BaseModel):
    status: str
    files_deleted: int
    directories_removed: int
    space_freed_mb: float
    retention_days: int

def cleanup_old_webhook_files(dry_run: bool = False) -> Dict[str, Any]:
    """
    Clean up webhook files older than WEBHOOK_RETENTION_DAYS.
    
    Args:
        dry_run: If True, only report what would be deleted without actually deleting
        
    Returns:
        Dictionary with cleanup statistics
    """
    if not CLEANUP_ENABLED and not dry_run:
        return {
            "status": "disabled",
            "files_deleted": 0,
            "directories_removed": 0,
            "space_freed_mb": 0.0,
            "retention_days": WEBHOOK_RETENTION_DAYS,
            "message": "Cleanup is disabled via CLEANUP_ENABLED=false"
        }
    
    cutoff_date = datetime.utcnow() - timedelta(days=WEBHOOK_RETENTION_DAYS)
    files_deleted = 0
    directories_removed = 0
    space_freed_bytes = 0
    
    logger.info(f"Starting webhook cleanup (dry_run={dry_run}), retention={WEBHOOK_RETENTION_DAYS} days")
    logger.info(f"Deleting files older than: {cutoff_date.isoformat()}")
    
    try:
        # Walk through date-based directory structure
        for year_dir in DATA_DIR.iterdir():
            if not year_dir.is_dir() or not year_dir.name.isdigit():
                continue
                
            for month_dir in year_dir.iterdir():
                if not month_dir.is_dir() or not month_dir.name.isdigit():
                    continue
                    
                for day_dir in month_dir.iterdir():
                    if not day_dir.is_dir() or not day_dir.name.isdigit():
                        continue
                    
                    # Parse directory date
                    try:
                        dir_date = datetime.strptime(f"{year_dir.name}-{month_dir.name}-{day_dir.name}", "%Y-%m-%d")
                    except ValueError:
                        logger.warning(f"Skipping invalid date directory: {day_dir}")
                        continue
                    
                    # Check if directory is older than retention period
                    if dir_date < cutoff_date:
                        # Count files and size before deletion
                        files_in_dir = list(day_dir.glob("*.json"))
                        dir_size = sum(f.stat().st_size for f in files_in_dir if f.is_file())
                        
                        logger.info(f"Cleaning directory: {day_dir} ({len(files_in_dir)} files, {dir_size/1024/1024:.2f}MB)")
                        
                        if not dry_run:
                            shutil.rmtree(day_dir)
                            directories_removed += 1
                        
                        files_deleted += len(files_in_dir)
                        space_freed_bytes += dir_size
                
                # Remove empty month directories
                if not dry_run and month_dir.exists() and not any(month_dir.iterdir()):
                    month_dir.rmdir()
                    logger.info(f"Removed empty month directory: {month_dir}")
            
            # Remove empty year directories  
            if not dry_run and year_dir.exists() and not any(year_dir.iterdir()):
                year_dir.rmdir()
                logger.info(f"Removed empty year directory: {year_dir}")
        
        space_freed_mb = space_freed_bytes / 1024 / 1024
        
        result = {
            "status": "success",
            "files_deleted": files_deleted,
            "directories_removed": directories_removed,
            "space_freed_mb": round(space_freed_mb, 2),
            "retention_days": WEBHOOK_RETENTION_DAYS,
            "cutoff_date": cutoff_date.isoformat(),
            "dry_run": dry_run
        }
        
        logger.info(f"Cleanup completed: {files_deleted} files, {directories_removed} dirs, {space_freed_mb:.2f}MB")
        return result
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        return {
            "status": "error",
            "files_deleted": files_deleted,
            "directories_removed": directories_removed,
            "space_freed_mb": space_freed_bytes / 1024 / 1024,
            "retention_days": WEBHOOK_RETENTION_DAYS,
            "error": str(e),
            "dry_run": dry_run
        }

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
    """Initialize Kafka producer and run cleanup on startup."""
    try:
        await get_kafka_producer()
        logger.info("Kafka producer initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Kafka producer: {e}")
        # Continue running even if Kafka is not available
    
    # Run cleanup on startup if enabled
    if CLEANUP_ON_STARTUP and CLEANUP_ENABLED:
        try:
            logger.info("Running startup cleanup...")
            cleanup_result = cleanup_old_webhook_files(dry_run=False)
            logger.info(f"Startup cleanup result: {cleanup_result}")
        except Exception as e:
            logger.error(f"Startup cleanup failed: {e}")

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
                "redpanda_topic": WEBHOOK_TOPIC,
                "cleanup_enabled": CLEANUP_ENABLED,
                "retention_days": WEBHOOK_RETENTION_DAYS
            }
        else:
            return {
                "status": "not_registered",
                "webhook_url": None,
                "helius_configured": bool(os.getenv("HELIUS_API_KEY")),
                "redpanda_enabled": producer is not None,
                "redpanda_brokers": REDPANDA_BROKERS,
                "redpanda_topic": WEBHOOK_TOPIC,
                "cleanup_enabled": CLEANUP_ENABLED,
                "retention_days": WEBHOOK_RETENTION_DAYS
            }
    except Exception as e:
        logger.error(f"Error getting webhook status: {e}")
        raise HTTPException(status_code=500, detail="Error getting webhook status")

@app.post("/webhooks/cleanup")
async def cleanup_webhooks(dry_run: bool = False) -> CleanupResponse:
    """
    Clean up old webhook files.
    
    Args:
        dry_run: If True, only report what would be deleted without actually deleting
    """
    try:
        result = cleanup_old_webhook_files(dry_run=dry_run)
        
        return CleanupResponse(
            status=result["status"],
            files_deleted=result["files_deleted"],
            directories_removed=result["directories_removed"],
            space_freed_mb=result["space_freed_mb"],
            retention_days=result["retention_days"]
        )
    except Exception as e:
        logger.error(f"Error during webhook cleanup: {e}")
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {str(e)}")

@app.get("/webhooks/cleanup/dry-run")
async def cleanup_webhooks_dry_run() -> CleanupResponse:
    """
    Preview what would be cleaned up without actually deleting files.
    """
    return await cleanup_webhooks(dry_run=True)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)