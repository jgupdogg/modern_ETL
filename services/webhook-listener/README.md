# Webhook Listener Service

A FastAPI service for receiving and storing webhook payloads as JSON files.

## Features

- Receives POST requests at `/webhooks` endpoint
- Stores payloads as JSON files with metadata
- Organizes files by date (YYYY/MM/DD structure)
- Health check endpoint at `/health`
- Automatic API documentation at `/docs`

## API Endpoints

- `GET /` - Service info
- `GET /health` - Health check
- `POST /webhooks` - Receive webhook payload
- `GET /webhooks/count` - Get count of stored webhooks
- `GET /docs` - Swagger UI documentation

## File Storage Format

Webhooks are stored in `/data/webhooks/YYYY/MM/DD/` with filenames:
`YYYYMMDD_HHMMSS_<uuid>.json`

Each file contains:
- `message_id`: Unique identifier
- `timestamp`: ISO format timestamp
- `payload`: The actual webhook payload
- `headers`: Request headers
- `source_ip`: Source IP address

## Configuration

Add to your `.env` file:
```bash
HELIUS_API_KEY=your_api_key_here
HELIUS_ADDRESSES=address1,address2,address3  # Solana addresses to monitor
HELIUS_TRANSACTION_TYPES=SWAP  # Or multiple: SWAP,TRANSFER
HELIUS_WEBHOOK_TYPE=enhanced
HELIUS_AUTH_HEADER=  # Optional auth header for webhook
```

## Usage with Docker Compose

```bash
# Build and start all services (webhook listener + ngrok)
docker-compose up -d

# View logs
docker-compose logs -f webhook-listener
docker-compose logs -f ngrok

# Check ngrok URL
curl http://localhost:4040/api/tunnels

# Test the service
curl http://localhost:8000/health
```

## Automatic Helius Registration

The service automatically:
1. Waits for ngrok to start and get a public URL
2. Registers or updates the webhook URL with Helius
3. Starts accepting webhook requests

The current webhook URL is saved to `data/webhooks/current_webhook_url.txt`

## Manual ngrok (alternative)

If you prefer to run ngrok separately:
```bash
# Start ngrok tunnel
./scripts/start-ngrok.sh

# The ngrok URL will be displayed - use this for your webhook endpoint
```