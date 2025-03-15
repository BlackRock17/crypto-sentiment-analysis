"""
Kafka sensors for Apache Airflow to detect messages in Kafka topics.
"""

import json
import logging
from typing import Dict, Any, Callable, Optional

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient

# Configure logging
logger = logging.getLogger(__name__)


class KafkaMessageSensor(BaseSensorOperator):
    """
    Sensor that waits for messages in a Kafka topic.

    Args:
        topic: Kafka topic to monitor
        group_id: Consumer group ID
        bootstrap_servers: Kafka bootstrap servers
        message_filter: Optional function to filter messages
        consumer_config: Additional consumer configuration
        consumer_timeout: Consumer timeout in seconds
    """

    @apply_defaults
    def __init__(
            self,
            topic: str,
            group_id: str,
            bootstrap_servers: str = 'kafka:9092',
            message_filter: Optional[Callable[[Dict[str, Any]], bool]] = None,
            consumer_config: Optional[Dict[str, Any]] = None,
            consumer_timeout: int = 5,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.message_filter = message_filter
        self.consumer_config = consumer_config or {}
        self.consumer_timeout = consumer_timeout

        # Log initialization
        logger.info(f"Initialized KafkaMessageSensor for topic: {topic}")

    def poke(self, context):
        """
        Function called by the sensor to check if it should move forward.

        Args:
            context: Airflow task context

        Returns:
            True if a message is available and matches the filter, False otherwise
        """
        # Create consumer
        consumer = self._create_consumer()

        try:
            # Subscribe to topic
            consumer.subscribe([self.topic])

            # Poll for messages
            message = consumer.poll(timeout=self.consumer_timeout)

            if message is None:
                logger.info(f"No message received from topic {self.topic}")
                return False

            if message.error():
                logger.error(f"Error reading message from topic {self.topic}: {message.error()}")
                return False

            # Process message
            try:
                value = message.value()
                if value is None:
                    logger.warning(f"Received message with NULL value from topic {self.topic}")
                    return False

                # Try to decode as JSON
                try:
                    data = json.loads(value.decode('utf-8'))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    # Not a valid JSON or UTF-8 string
                    logger.warning(f"Received non-JSON message from topic {self.topic}")
                    return False

                # Apply filter if provided
                if self.message_filter is not None:
                    if not self.message_filter(data):
                        logger.info(f"Message from topic {self.topic} did not match filter")
                        return False

                # Store the message in XCom for downstream tasks
                context['ti'].xcom_push(key='kafka_message', value=data)
                logger.info(f"Received matching message from topic {self.topic}")
                return True

            except Exception as e:
                logger.error(f"Error processing message from topic {self.topic}: {e}")
                return False

        finally:
            # Close consumer
            consumer.close()

    def _create_consumer(self):
        """
        Create a Kafka consumer with the provided configuration.

        Returns:
            Configured Kafka consumer
        """
        # Default configuration
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'max.poll.interval.ms': 300000  # 5 minutes
        }

        # Apply additional configuration
        config.update(self.consumer_config)

        return Consumer(config)


class KafkaTopicSensor(BaseSensorOperator):
    """
    Sensor that checks if a Kafka topic exists.

    Args:
        topic: Kafka topic to check
        bootstrap_servers: Kafka bootstrap servers
    """

    @apply_defaults
    def __init__(
            self,
            topic: str,
            bootstrap_servers: str = 'kafka:9092',
            **kwargs
    ):
        super().__init__(**kwargs)
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers

        # Log initialization
        logger.info(f"Initialized KafkaTopicSensor for topic: {topic}")

    def poke(self, context):
        """
        Function called by the sensor to check if the topic exists.

        Args:
            context: Airflow task context

        Returns:
            True if the topic exists, False otherwise
        """

        try:
            # Create admin client
            admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})

            # Get cluster metadata
            metadata = admin_client.list_topics(timeout=10)

            # Check if topic exists
            exists = self.topic in metadata.topics

            if exists:
                logger.info(f"Topic {self.topic} exists")
            else:
                logger.info(f"Topic {self.topic} does not exist yet")

            return exists

        except Exception as e:
            logger.error(f"Error checking Kafka topic {self.topic}: {e}")
            return False


class KafkaMessageCountSensor(BaseSensorOperator):
    """
    Sensor that checks if a Kafka topic has a minimum number of messages.

    Args:
        topic: Kafka topic to check
        min_count: Minimum number of messages required
        group_id: Consumer group ID
        bootstrap_servers: Kafka bootstrap servers
        consumer_config: Additional consumer configuration
        consumer_timeout: Consumer timeout in seconds
    """

    @apply_defaults
    def __init__(
            self,
            topic: str,
            min_count: int,
            group_id: str,
            bootstrap_servers: str = 'kafka:9092',
            consumer_config: Optional[Dict[str, Any]] = None,
            consumer_timeout: int = 5,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.topic = topic
        self.min_count = min_count
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.consumer_config = consumer_config or {}
        self.consumer_timeout = consumer_timeout

        # Log initialization
        logger.info(f"Initialized KafkaMessageCountSensor for topic: {topic}, min_count: {min_count}")

    def poke(self, context):
        """
        Function called by the sensor to check if the minimum message count is reached.

        Args:
            context: Airflow task context

        Returns:
            True if the minimum message count is reached, False otherwise
        """
        # Create consumer
        consumer = self._create_consumer()

        try:
            # Subscribe to topic
            consumer.subscribe([self.topic])

            # Counter for messages
            message_count = 0

            # Poll for messages up to min_count
            while message_count < self.min_count:
                message = consumer.poll(timeout=self.consumer_timeout)

                if message is None:
                    # No more messages available
                    break

                if message.error():
                    logger.error(f"Error reading message from topic {self.topic}: {message.error()}")
                    break

                # Increment message count
                message_count += 1

            # Check if minimum count reached
            reached = message_count >= self.min_count

            if reached:
                logger.info(f"Topic {self.topic} has at least {self.min_count} messages")
            else:
                logger.info(f"Topic {self.topic} has only {message_count} messages, requires {self.min_count}")

            return reached

        finally:
            # Close consumer
            consumer.close()

    def _create_consumer(self):
        """
        Create a Kafka consumer with the provided configuration.

        Returns:
            Configured Kafka consumer
        """
        # Default configuration
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000  # 5 minutes
        }

        # Apply additional configuration
        config.update(self.consumer_config)

        return Consumer(config)
