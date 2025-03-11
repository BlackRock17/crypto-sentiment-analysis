"""
Kafka setup utilities for creating topics and configuring kafka environment.
"""
import logging
import time
from typing import List, Dict, Any
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)

# Default Kafka broker address
DEFAULT_BOOTSTRAP_SERVERS = "localhost:29092"

# Define topics with their configurations
DEFAULT_TOPICS = [
    {
        "name": "twitter-raw-tweets",
        "partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": 604800000  # 7 days in milliseconds
        }
    },
    {
        "name": "token-mentions",
        "partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": 604800000  # 7 days
        }
    },
    {
        "name": "sentiment-results",
        "partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": 604800000  # 7 days
        }
    },
    {
        "name": "token-categorization-tasks",
        "partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": 1209600000  # 14 days
        }
    },
    {
        "name": "system-notifications",
        "partitions": 1,
        "replication_factor": 1,
        "config": {
            "retention.ms": 259200000  # 3 days
        }
    }
]


def create_topics(
        bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
        topics: List[Dict[str, Any]] = DEFAULT_TOPICS,
        max_retries: int = 5,
        retry_interval: int = 5
) -> bool:
    """
    Create Kafka topics if they don't exist.

    Args:
        bootstrap_servers: Kafka broker address(es)
        topics: List of topic configurations
        max_retries: Maximum number of connection retries
        retry_interval: Seconds between retries

    Returns:
        bool: True if all topics were created successfully, False otherwise
    """
    retries = 0

    while retries < max_retries:
        try:
            logger.info(f"Attempting to connect to Kafka brokers at {bootstrap_servers}")

            # Create admin client
            admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

            # Check if broker is available
            metadata = admin_client.list_topics(timeout=10)
            logger.info(f"Connected to Kafka cluster with {len(metadata.brokers)} brokers")

            # Check existing topics
            existing_topics = metadata.topics
            logger.info(f"Found {len(existing_topics)} existing topics")

            # Prepare new topics
            new_topics = []
            for topic_config in topics:
                topic_name = topic_config["name"]
                if topic_name not in existing_topics:
                    logger.info(f"Preparing to create topic: {topic_name}")
                    new_topics.append(
                        NewTopic(
                            topic_name,
                            num_partitions=topic_config["partitions"],
                            replication_factor=topic_config["replication_factor"],
                            config=topic_config.get("config", {})
                        )
                    )
                else:
                    logger.info(f"Topic {topic_name} already exists")

            # Create new topics if any
            if new_topics:
                futures = admin_client.create_topics(new_topics)

                # Wait for each topic to be created
                for topic, future in futures.items():
                    try:
                        future.result()  # The result itself is None
                        logger.info(f"Topic {topic} created successfully")
                    except Exception as e:
                        logger.error(f"Failed to create topic {topic}: {e}")
                        return False

            logger.info("Kafka topic setup completed successfully")
            return True

        except Exception as e:
            logger.warning(f"Failed to connect to Kafka (attempt {retries + 1}/{max_retries}): {e}")
            retries += 1
            if retries < max_retries:
                logger.info(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)

    logger.error(f"Failed to set up Kafka topics after {max_retries} attempts")
    return False


def ensure_all_topics_exist():
    """
    Check if all required topics exist and create any missing ones.
    """
    from src.data_processing.kafka.config import TOPICS, DEFAULT_BOOTSTRAP_SERVERS

    # Create admin client
    admin_client = AdminClient({'bootstrap.servers': DEFAULT_BOOTSTRAP_SERVERS})

    # Get existing topics
    existing_topics = admin_client.list_topics(timeout=10).topics
    logger.info(f"Existing topics: {list(existing_topics.keys())}")

    # Check each topic and create if missing
    new_topics = []
    for topic_name in TOPICS.values():
        if topic_name not in existing_topics:
            logger.info(f"Topic '{topic_name}' does not exist, will create it")
            new_topics.append(
                NewTopic(
                    topic_name,
                    num_partitions=3,
                    replication_factor=1,
                    config={
                        "retention.ms": 604800000  # 7 days
                    }
                )
            )

    # Create missing topics
    if new_topics:
        logger.info(f"Creating {len(new_topics)} missing topics")
        futures = admin_client.create_topics(new_topics)

        # Wait for each topic to be created
        for topic, future in futures.items():
            try:
                future.result()  # The result itself is None
                logger.info(f"Topic {topic} created successfully")
            except Exception as e:
                logger.error(f"Failed to create topic {topic}: {e}")
                return False
    else:
        logger.info("All required topics already exist")

    return True


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Create topics
    success = create_topics()
    if success:
        print("Kafka topics created successfully")
    else:
        print("Failed to create Kafka topics")
