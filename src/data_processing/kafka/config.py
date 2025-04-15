"""
Simplified Kafka configuration.
"""
import os
import logging

# Set up logging
logger = logging.getLogger(__name__)

# Kafka broker configuration - adjust for Docker environment
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')

# Topic names - only the ones we need for now
TOPICS = {
    "RAW_TWEETS": "raw-tweets",
    "MARKET_SENTIMENT": "market-sentiment-analysis"
}

# Default Kafka producer configuration
PRODUCER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'twitter-sentiment-producer',
    'security.protocol': 'PLAINTEXT'
}

# Default Kafka consumer configuration
CONSUMER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'tweet-ingestion-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,  # Manual commit for better control
    'security.protocol': 'PLAINTEXT',
    'api.version.request': True
}
