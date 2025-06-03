import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

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

class WebhookResponse(BaseModel):
    status: str
    message_id: str
    timestamp: str

@app.get("/")
async def root():
    return {"message": "Webhook Listener is running", "version": "1.0.0"}

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
                "helius_configured": bool(os.getenv("HELIUS_API_KEY"))
            }
        else:
            return {
                "status": "not_registered",
                "webhook_url": None,
                "helius_configured": bool(os.getenv("HELIUS_API_KEY"))
            }
    except Exception as e:
        logger.error(f"Error getting webhook status: {e}")
        raise HTTPException(status_code=500, detail="Error getting webhook status")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)