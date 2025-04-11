"""
Simple Kafka producer for sending messages to Kafka topics.
"""
import json
import logging
from typing import Dict, Any
from confluent_kafka import Producer

from src.data_processing.kafka.config import PRODUCER_CONFIG, TOPICS

# Set up logging
logger = logging.getLogger(__name__)

# Global producer instance
_producer = None


def get_producer() -> Producer:
    """
    Returns a singleton Kafka producer instance.

    Returns:
        Confluent Kafka Producer instance
    """
    global _producer
    if _producer is None:
        _producer = Producer(PRODUCER_CONFIG)
    return _producer


def send_message(topic: str, message: Dict[str, Any]) -> bool:
    """
    Sends a message to a Kafka topic.

    Args:
        topic: Kafka topic name
        message: Message to send (dictionary will be converted to JSON)

    Returns:
        True if message was sent, False otherwise
    """
    try:
        # Get producer instance
        producer = get_producer()

        # Convert message to JSON string
        value = json.dumps(message).encode('utf-8')

        # Send message
        producer.produce(
            topic=topic,
            value=value,
            callback=lambda err, msg: logger.error(f"Message delivery failed: {err}") if err else None
        )

        # Flush to ensure message is sent immediately
        producer.flush(timeout=5)  # 5 seconds timeout

        logger.info(f"Message sent to topic {topic}")
        return True

    except Exception as e:
        logger.error(f"Failed to send message to Kafka: {e}")
        return False


def send_tweet(tweet_data: Dict[str, Any]) -> bool:
    """
    Sends a tweet to the raw-tweets Kafka topic.

    Args:
        tweet_data: Tweet data dictionary

    Returns:
        True if message was sent, False otherwise
    """
    return send_message(TOPICS["RAW_TWEETS"], tweet_data)
