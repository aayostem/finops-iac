#!/bin/bash

echo "Starting Voice Feature Store Services..."

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "docker-compose could not be found, please install it first"
    exit 1
fi

# Create .env from example if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env file from .env.example..."
    cp .env.example .env
    echo "Please update .env with your actual configuration"
fi

# Start services
echo "Starting services with Docker Compose..."
docker-compose -f docker/docker-compose.yml up -d

echo "Services started! Check the following endpoints:"
echo "API: http://localhost:8000"
echo "API Docs: http://localhost:8000/docs"
echo "Flink Dashboard: http://localhost:8081"
echo "Grafana: http://localhost:3000 (admin/admin)"