#!/bin/bash

cd /opt/openai-data-hub

# Pull latest code
git pull origin main

# Restart FastAPI service
sudo systemctl restart openai-data-hub

echo "Deployment complete."
