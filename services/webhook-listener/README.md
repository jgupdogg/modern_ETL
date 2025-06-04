# Webhook Listener Service

A FastAPI service for receiving and storing webhook payloads as JSON files, with real-time streaming to Redpanda.

## Features

- Receives POST requests at `/webhooks` endpoint
- Stores payloads as JSON files with metadata
- Publishes payloads to Redpanda for real-time processing
- Organizes files by date (YYYY/MM/DD structure)
- Health check endpoint at `/health`
- Automatic API documentation at `/docs`

## API Endpoints

- `GET /` - Service info (includes Redpanda status)
- `GET /health` - Health check
- `POST /webhooks` - Receive webhook payload (saves to file and publishes to Redpanda)
- `GET /webhooks/count` - Get count of stored webhooks
- `GET /webhooks/status` - Get webhook service status including Redpanda info
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
# Helius Configuration
HELIUS_API_KEY=your_api_key_here
HELIUS_ADDRESSES=address1,address2,address3  # Solana addresses to monitor
HELIUS_TRANSACTION_TYPES=SWAP  # Or multiple: SWAP,TRANSFER
HELIUS_WEBHOOK_TYPE=enhanced
HELIUS_AUTH_HEADER=  # Optional auth header for webhook

# Redpanda Configuration
REDPANDA_BROKERS=redpanda:9092  # Kafka brokers
WEBHOOK_TOPIC=webhooks  # Topic name for webhook messages
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

## Redpanda Integration

All webhook payloads are automatically published to Redpanda for real-time processing:

- **Topic**: Configurable via `WEBHOOK_TOPIC` (default: `webhooks`)
- **Message Key**: Webhook message ID (UUID)
- **Message Format**: JSON with full webhook metadata

### Testing Redpanda Integration

```bash
# Check webhook status including Redpanda connection
curl http://localhost:8000/webhooks/status | jq

# Send a test webhook
curl -X POST http://localhost:8000/webhooks \
  -H "Content-Type: application/json" \
  -d '{"test": "data", "timestamp": "2024-01-01T12:00:00Z"}'

# View in Redpanda Console
# Open http://localhost:8090 in your browser

# Or consume messages via CLI
docker exec claude_pipeline-redpanda-1 rpk topic consume webhooks --format json
```

## Manual ngrok (alternative)

If you prefer to run ngrok separately:
```bash
# Start ngrok tunnel
./scripts/start-ngrok.sh

# The ngrok URL will be displayed - use this for your webhook endpoint
```