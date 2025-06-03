import os
import logging
import httpx
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class HeliusWebhookManager:
    """Manages Helius webhook registration and updates."""
    
    def __init__(self):
        self.api_key = os.getenv("HELIUS_API_KEY")
        if not self.api_key:
            raise ValueError("HELIUS_API_KEY is not set")
        
        self.base_url = "https://api.helius.xyz/v0"
        self.headers = {
            "Content-Type": "application/json"
        }
        
        # Configuration from environment
        self.transaction_types = os.getenv("HELIUS_TRANSACTION_TYPES", "SWAP").split(",")
        self.transaction_types = [t.strip() for t in self.transaction_types]
        self.webhook_type = os.getenv("HELIUS_WEBHOOK_TYPE", "enhanced")
        self.auth_header = os.getenv("HELIUS_AUTH_HEADER", "")
    
    def get_addresses(self) -> List[str]:
        """
        Get addresses to monitor. For now, returns hardcoded addresses.
        Later this can be replaced with database fetch.
        """
        # TODO: Replace with actual address fetching logic
        addresses_env = os.getenv("HELIUS_ADDRESSES", "")
        if addresses_env:
            return [addr.strip() for addr in addresses_env.split(",") if addr.strip()]
        
        # Default addresses for testing
        return []
    
    async def get_all_webhooks(self) -> List[Dict]:
        """Get all existing webhooks."""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{self.base_url}/webhooks?api-key={self.api_key}",
                    headers=self.headers
                )
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.error(f"Error fetching webhooks: {e}")
                return []
    
    async def create_webhook(self, webhook_url: str) -> Optional[Dict]:
        """Create a new webhook."""
        addresses = self.get_addresses()
        if not addresses:
            logger.warning("No addresses configured for webhook monitoring")
            return None
        
        payload = {
            "webhookURL": webhook_url,
            "transactionTypes": self.transaction_types,
            "accountAddresses": addresses,
            "webhookType": self.webhook_type
        }
        
        if self.auth_header:
            payload["authHeader"] = self.auth_header
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{self.base_url}/webhooks?api-key={self.api_key}",
                    headers=self.headers,
                    json=payload
                )
                response.raise_for_status()
                result = response.json()
                logger.info(f"Webhook created successfully: {result}")
                return result
            except Exception as e:
                logger.error(f"Error creating webhook: {e}")
                return None
    
    async def edit_webhook(self, webhook_id: str, webhook_url: str) -> Optional[Dict]:
        """Update an existing webhook."""
        addresses = self.get_addresses()
        if not addresses:
            logger.warning("No addresses configured for webhook monitoring")
            return None
        
        payload = {
            "webhookURL": webhook_url,
            "transactionTypes": self.transaction_types,
            "accountAddresses": addresses,
            "webhookType": self.webhook_type
        }
        
        if self.auth_header:
            payload["authHeader"] = self.auth_header
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.put(
                    f"{self.base_url}/webhooks/{webhook_id}?api-key={self.api_key}",
                    headers=self.headers,
                    json=payload
                )
                response.raise_for_status()
                result = response.json()
                logger.info(f"Webhook updated successfully: {result}")
                return result
            except Exception as e:
                logger.error(f"Error updating webhook: {e}")
                return None
    
    async def update_or_create_webhook(self, webhook_url: str) -> bool:
        """Update existing webhook or create new one."""
        try:
            # Get existing webhooks
            existing_webhooks = await self.get_all_webhooks()
            
            if existing_webhooks and len(existing_webhooks) > 0:
                # Update the first webhook
                webhook_id = existing_webhooks[0].get("webhookID")
                if webhook_id:
                    logger.info(f"Updating existing webhook {webhook_id}")
                    result = await self.edit_webhook(webhook_id, webhook_url)
                    return result is not None
                else:
                    logger.warning("Webhook found but no ID, creating new one")
                    result = await self.create_webhook(webhook_url)
                    return result is not None
            else:
                # Create new webhook
                logger.info("No existing webhooks, creating new one")
                result = await self.create_webhook(webhook_url)
                return result is not None
                
        except Exception as e:
            logger.error(f"Error in update_or_create_webhook: {e}")
            return False