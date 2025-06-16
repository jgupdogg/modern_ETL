#!/bin/bash

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Check if NGROK_TOKEN is set
if [ -z "$NGROK_TOKEN" ]; then
    echo "Error: NGROK_TOKEN is not set in .env file"
    exit 1
fi

# Start ngrok
echo "Starting ngrok tunnel for webhook listener on port 8000..."
ngrok config add-authtoken $NGROK_TOKEN
ngrok http 8000