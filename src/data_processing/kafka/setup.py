"""
Utilities for setting up Kafka topics and consumers.
"""
import logging
from typing import List
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

from src.data_processing.kafka.config import PRODUCER_CONFIG, TOPICS

# Set up logging
logger = logging.getLogger(__name__)


def create_topics(topic_names: List[str], num_partitions: int = 1, replication_factor: int = 1) -> bool:
    """
    Create Kafka topics if they don't exist.

    Args:
        topic_names: List of topic names to create
        num_partitions: Number of partitions for each topic (default: 1)
        replication_factor: Replication factor for each topic (default: 1)

    Returns:
        True if all topics were created successfully, False otherwise
    """
    try:
        # Create admin client
        admin_config = PRODUCER_CONFIG.copy()
        admin_client = AdminClient(admin_config)

        # Check which topics already exist
        existing_topics = admin_client.list_topics(timeout=10)

        # Filter out topics that already exist
        topics_to_create = [
            topic for topic in topic_names
            if topic not in existing_topics.topics
        ]

        if not topics_to_create:
            logger.info("All topics already exist")
            return True

        # Create list of NewTopic objects
        new_topics = [
            NewTopic(
                topic,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
            for topic in topics_to_create
        ]

        # Create topics
        result = admin_client.create_topics(new_topics)

        # Wait for results
        for topic, future in result.items():
            try:
                future.result()  # Raises exception if topic creation failed
                logger.info(f"Topic {topic} created")
            except KafkaException as e:
                if "already exists" in str(e):
                    logger.info(f"Topic {topic} already exists")
                else:
                    logger.error(f"Failed to create topic {topic}: {e}")
                    return False

        return True

    except Exception as e:
        logger.error(f"Error creating Kafka topics: {e}")
        return False


def ensure_topics_exist():
    """
    Ensure all required Kafka topics exist.
    """
    try:
        # Create all topics defined in config
        topic_names = list(TOPICS.values())
        create_topics(topic_names)
    except Exception as e:
        logger.error(f"Failed to ensure topics exist: {e}")


def check_kafka_connection() -> bool:
    """
    Check if Kafka is reachable.

    Returns:
        True if Kafka is reachable, False otherwise
    """
    try:
        admin_config = PRODUCER_CONFIG.copy()
        admin_client = AdminClient(admin_config)
        cluster_metadata = admin_client.list_topics(timeout=5)
        if cluster_metadata:
            logger.info("Kafka connection successful")
            return True
        return False
    except Exception as e:
        logger.error(f"Kafka connection failed: {e}")
        return False
