"""
Kafka configuration module for centralized Kafka settings.
"""
from typing import Dict, Any, Optional

# Default Kafka broker configuration
DEFAULT_BOOTSTRAP_SERVERS = "localhost:29092"

# Topic names
TOPICS = {
    "RAW_TWEETS": "twitter-raw-tweets",
    "TOKEN_MENTIONS": "token-mentions",
    "SENTIMENT_RESULTS": "sentiment-results",
    "TOKEN_CATEGORIZATION": "token-categorization-tasks",
    "SYSTEM_NOTIFICATIONS": "system-notifications"
}

# Default producer configuration
DEFAULT_PRODUCER_CONFIG = {
    'bootstrap.servers': DEFAULT_BOOTSTRAP_SERVERS,
    'client.id': 'crypto-sentiment-producer',
    'acks': 'all',  # Wait for all replicas to acknowledge
    'retries': 3,  # Retry on temporary failures
    'retry.backoff.ms': 500,  # Time between retries
    'compression.type': 'snappy',  # Use compression for efficiency
    'batch.size': 16384,  # Batch size in bytes
    'linger.ms': 10,  # Wait to accumulate messages before sending
}

# Default consumer configuration
DEFAULT_CONSUMER_CONFIG = {
    'bootstrap.servers': DEFAULT_BOOTSTRAP_SERVERS,
    'group.id': 'crypto-sentiment-consumer',
    'auto.offset.reset': 'earliest',  # Start from earliest offset if no committed offset is found
    'enable.auto.commit': False,  # Manual commit for better control
    'max.poll.interval.ms': 300000,  # 5 minutes max processing time
    'session.timeout.ms': 30000,  # 30 seconds timeout
    'fetch.min.bytes': 1,  # Fetch at least 1 byte (immediately)
    'heartbeat.interval.ms': 10000,  # 10 seconds heartbeat
}


def get_producer_config(client_id: Optional[str] = None, **overrides) -> Dict[str, Any]:
    """
    Get Kafka producer configuration with optional overrides.

    Args:
        client_id: Unique client ID for the producer
        **overrides: Additional configuration options to override defaults

    Returns:
        Configuration dictionary for Kafka producer
    """
    config = DEFAULT_PRODUCER_CONFIG.copy()

    if client_id:
        config['client.id'] = client_id

    # Apply overrides
    for key, value in overrides.items():
        config[key] = value

    return config


def get_consumer_config(group_id: Optional[str] = None, **overrides) -> Dict[str, Any]:
    """
    Get Kafka consumer configuration with optional overrides.

    Args:
        group_id: Consumer group ID
        **overrides: Additional configuration options to override defaults

    Returns:
        Configuration dictionary for Kafka consumer
    """
    config = DEFAULT_CONSUMER_CONFIG.copy()

    if group_id:
        config['group.id'] = f"crypto-sentiment-{group_id}"

    # Apply overrides
    for key, value in overrides.items():
        config[key] = value

    return config
