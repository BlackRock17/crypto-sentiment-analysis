#!/bin/bash

# Start script for the entire Crypto Sentiment Analysis infrastructure
echo "Starting Crypto Sentiment Analysis Infrastructure"

# Navigate to project root
cd $(pwd)

# Start Kafka
echo "Starting Kafka..."
docker-compose -f deployment/docker/docker-compose.kafka.yml up -d

# Wait for Kafka to start
echo "Waiting for Kafka to start..."
sleep 10

# Check Kafka connection
echo "Checking Kafka connection..."
python scripts/check_kafka_connection.py --timeout 30 --test-produce-consume

# Create Kafka topics
echo "Creating Kafka topics..."
python scripts/create_kafka_topics.py

# Start Airflow
echo "Starting Airflow..."
docker-compose -f deployment/docker/docker-compose.airflow.yml up -d

# Start main application
echo "Starting main application..."
docker-compose -f deployment/docker/docker-compose.yml up -d

echo "All services started!"
echo "Web UI for Airflow: http://localhost:8080 (admin/admin)"
echo "API for the application: http://localhost:8000"
echo "Kafka UI: http://localhost:9000"
