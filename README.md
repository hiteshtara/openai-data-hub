# Production openai-data-hub
A production-ready ultra-cheap AI data lake.
openai-data-hub is a production-grade, ultra-cheap AI data service deployed on AWS.
It runs a FastAPI backend on an Amazon Linux EC2 instance with:

Automatic deployment from GitHub to EC2

Rollback-safe deployment script on the server

Systemd-managed FastAPI service that auto-restarts

Uvicorn running on port 80

Secure SSH authentication (Mac + GitHub → EC2)

Integrated OpenAI API support

A growing set of ETL + AI endpoints

This project costs approximately $20–$30/month on AWS.

🧱 Architecture
Your Mac (local dev)
      │
      │ git push (SSH)
      ▼
GitHub Repository ────────────────▶ GitHub Actions Workflow
      │                                  │
      │                                  │ SSH to EC2
      ▼                                  ▼
Continuous Integration (CI)          EC2 Server (CD)
                                     /opt/openai-data-hub
                                     ├── deploy.sh  (rollback)
                                     ├── app/
                                     └── systemd service

🖥️ AWS Components Used
Component	Purpose
EC2 t3.micro (Amazon Linux 2023)	Runs FastAPI + Uvicorn
Systemd service	Keeps API always running
SSH Deploy Keys	Secure GitHub → EC2 access
GitHub Actions	Automatic deployments
Rollback script	Ensures server never breaks
OpenAI API	AI features (summaries, RAG, etc.)
⚙️ Installation (EC2 Setup Summary)
1. Install Python & requirements
sudo dnf update -y
sudo dnf install python3.11 python3-pip git -y
pip3.11 install -r app/requirements.txt

2. Add OpenAI API key
echo 'export OPENAI_API_KEY="your_key_here"' >> ~/.bashrc
source ~/.bashrc

3. Start FastAPI manually (for testing)
sudo /usr/local/bin/uvicorn main:app --host 0.0.0.0 --port 80

🔥 Production Service (systemd)

Location:

/etc/systemd/system/openai-data-hub.service


Contents:

[Unit]
Description=OpenAI Data Hub FastAPI Service
After=network.target

[Service]
User=ec2-user
WorkingDirectory=/opt/openai-data-hub/app
ExecStart=/usr/local/bin/uvicorn main:app --host 0.0.0.0 --port 80
Restart=always
Environment=OPENAI_API_KEY=your_key_here

[Install]
WantedBy=multi-user.target


Enable:

sudo systemctl daemon-reload
sudo systemctl enable openai-data-hub
sudo systemctl start openai-data-hub

🔐 SSH Keys Overview

Two keys are used:

1. Your personal login key

Used with openai-key.pem to SSH manually.

2. GitHub deploy key (ED25519)

Used by GitHub Actions to SSH into EC2 for deployments.

Both keys live in:

~/.ssh/authorized_keys

🤖 GitHub Actions CI/CD Pipeline

File:

.github/workflows/deploy.yml


Triggers:

On every push to main

Pipeline steps:

Checkout repo

SCP updated source code to EC2

SSH into EC2

Execute deploy.sh

If deploy fails → rollback automatically

🛡️ Rollback-Safe Deployment Script

Location:

/opt/openai-data-hub/deploy.sh


Key features:

Creates backup of working code

Pulls latest from GitHub

If pull fails → restore backup

Restarts FastAPI service

If restart fails → restore backup

This guarantees zero downtime.

🌐 Endpoints Available Now
Base
GET /

Test Endpoints
GET /ingest/csv
GET /query/data
GET /ai/rag


These confirm:

Server is alive

Routing is working

OpenAI is integrated

Deployment updated correctly

🧪 How To Deploy Code (Local Development)

On your Mac:

git add .
git commit -m "update"
git push


Automatically:

GitHub Actions runs

Code is SCP’d to EC2

deploy.sh executes

FastAPI restarts

Rollback if needed

Completely automated.