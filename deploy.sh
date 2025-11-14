#!/bin/bash
set -e

cd /opt/openai-data-hub

# Create backup of current version
if [ -d backup ]; then
    rm -rf backup
fi
cp -r app backup/

# Pull latest code
if git pull origin main; then
    echo "Code pulled successfully."
else
    echo "Git pull failed — rolling back..."
    rm -rf app
    mv backup app
    sudo systemctl restart openai-data-hub
    exit 1
fi

# Restart service
if sudo systemctl restart openai-data-hub; then
    echo "Service restarted successfully."
else
    echo "Service failed to restart — rolling back..."
    rm -rf app
    mv backup app
    sudo systemctl restart openai-data-hub
    exit 1
fi

echo "Deployment completed with rollback safety."

