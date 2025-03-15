"""
Kafka operators for Apache Airflow to interact with Kafka topics.
"""

import json
import logging
from typing import Dict, Any, List, Optional, Union, Callable

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

# Configure logging
logger = logging.getLogger(__name__)


class KafkaProduceOperator(BaseOperator):
    """
    Operator that sends a message to a Kafka topic.

    Args:
        topic: Kafka topic to send to
        message: Message to send (will be serialized to JSON)
        bootstrap_servers: Kafka bootstrap servers
        producer_config: Additional producer configuration
        message_key: Optional message key
    """

    @apply_defaults
    def __init__(
            self,
            topic: str,
            message: Union[Dict[str, Any], List[Dict[str, Any]], Callable],
            bootstrap_servers: str = 'kafka:9092',
            producer_config: Optional[Dict[str, Any]] = None,
            message_key: Optional[str] = None,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.topic = topic
        self.message = message
        self.bootstrap_servers = bootstrap_servers
        self.producer_config = producer_config or {}
        self.message_key = message_key

        # Log initialization
        logger.info(f"Initialized KafkaProduceOperator for topic: {topic}")

    def execute(self, context):
        """
        Execute the operator.

        Args:
            context: Airflow task context

        Returns:
            Producer delivery report information
        """
        # Get message from callable if provided
        if callable(self.message):
            message_data = self.message(context)
        else:
            message_data = self.message

        # Create producer
        producer = self._create_producer()

        try:
            # Handle both single message and list of messages
            messages = message_data if isinstance(message_data, list) else [message_data]

            results = []
            for msg in messages:
                # Serialize message to JSON
                try:
                    value = json.dumps(msg).encode('utf-8')
                except (TypeError, ValueError) as e:
                    logger.error(f"Error serializing message: {e}")
                    raise

                # Get message key
                key = None
                if self.message_key:
                    if self.message_key in msg:
                        key = str(msg[self.message_key]).encode('utf-8')
                    else:
                        logger.warning(f"Message key '{self.message_key}' not found in message")

                # Produce message
                delivery_report = {}

                def delivery_callback(err, msg):
                    if err is not None:
                        delivery_report['error'] = str(err)
                        logger.error(f"Failed to deliver message: {err}")
                    else:
                        delivery_report['topic'] = msg.topic()
                        delivery_report['partition'] = msg.partition()
                        delivery_report['offset'] = msg.offset()
                        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

                # Send message
                producer.produce(
                    topic=self.topic,
                    value=value,
                    key=key,
                    callback=delivery_callback
                )

                # Serve delivery callbacks from previous produce calls
                producer.poll(0)

                results.append(delivery_report)

            # Wait for any outstanding messages to be delivered
            remaining = producer.flush(10)

            if remaining > 0:
                logger.warning(f"{remaining} messages were not delivered within timeout")

            return results

        except KafkaException as e:
            logger.error(f"Kafka error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error producing message: {e}")
            raise

    def _create_producer(self):
        """
        Create a Kafka producer with the provided configuration.

        Returns:
            Configured Kafka producer
        """
        # Default configuration
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': f'airflow-{self.topic}-producer',
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 500
        }

        # Apply additional configuration
        config.update(self.producer_config)

        return Producer(config)


class KafkaCreateTopicOperator(BaseOperator):
    """
    Operator that creates a Kafka topic if it doesn't exist.

    Args:
        topic: Kafka topic to create
        num_partitions: Number of partitions for the topic
        replication_factor: Replication factor for the topic
        config: Topic configuration
        bootstrap_servers: Kafka bootstrap servers
    """

    @apply_defaults
    def __init__(
            self,
            topic: str,
            num_partitions: int = 3,
            replication_factor: int = 1,
            config: Optional[Dict[str, str]] = None,
            bootstrap_servers: str = 'kafka:9092',
            **kwargs
    ):
        super().__init__(**kwargs)
        self.topic = topic
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.config = config or {}
        self.bootstrap_servers = bootstrap_servers

        # Log initialization
        logger.info(f"Initialized KafkaCreateTopicOperator for topic: {topic}")

    def execute(self, context):
        """
        Execute the operator.

        Args:
            context: Airflow task context

        Returns:
            True if the topic was created, False if it already existed
        """

        try:
            # Create admin client
            admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})

            # Check if topic exists
            metadata = admin_client.list_topics(timeout=10)

            if self.topic in metadata.topics:
                logger.info(f"Topic {self.topic} already exists")
                return False

            # Create topic
            new_topic = NewTopic(
                topic=self.topic,
                num_partitions=self.num_partitions,
                replication_factor=self.replication_factor,
                config=self.config
            )

            # Create the topic
            futures = admin_client.create_topics([new_topic])

            # Wait for topic creation to complete
            for topic, future in futures.items():
                try:
                    future.result()  # The result itself is None
                    logger.info(f"Topic {topic} created successfully")
                except Exception as e:
                    logger.error(f"Failed to create topic {topic}: {e}")
                    raise

            return True

        except Exception as e:
            logger.error(f"Error creating Kafka topic {self.topic}: {e}")
            raise


class KafkaConsumeOperator(BaseOperator):
    """
    Operator that consumes messages from a Kafka topic and processes them.

    Args:
        topic: Kafka topic to consume from
        group_id: Consumer group ID
        max_messages: Maximum number of messages to consume
        message_processor: Function to process each message
        bootstrap_servers: Kafka bootstrap servers
        consumer_config: Additional consumer configuration
        timeout: Consumer timeout in seconds
    """

    @apply_defaults
    def __init__(
            self,
            topic: str,
            group_id: str,
            max_messages: int,
            message_processor: Callable[[Dict[str, Any]], Any],
            bootstrap_servers: str = 'kafka:9092',
            consumer_config: Optional[Dict[str, Any]] = None,
            timeout: int = 10,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.topic = topic
        self.group_id = group_id
        self.max_messages = max_messages
        self.message_processor = message_processor
        self.bootstrap_servers = bootstrap_servers
        self.consumer_config = consumer_config or {}
        self.timeout = timeout

        # Log initialization
        logger.info(f"Initialized KafkaConsumeOperator for topic: {topic}, max_messages: {max_messages}")

    def execute(self, context):
        """
        Execute the operator.

        Args:
            context: Airflow task context

        Returns:
            List of processed message results
        """
        # Create consumer
        consumer = self._create_consumer()

        try:
            # Subscribe to topic
            consumer.subscribe([self.topic])

            # Process messages
            messages_processed = 0
            results = []

            while messages_processed < self.max_messages:
                message = consumer.poll(timeout=self.timeout)

                if message is None:
                    # No more messages available
                    break

                if message.error():
                    logger.error(f"Error reading message from topic {self.topic}: {message.error()}")
                    continue

                # Process message
                try:
                    value = message.value()
                    if value is None:
                        logger.warning(f"Received message with NULL value from topic {self.topic}")
                        continue

                    # Try to decode as JSON
                    try:
                        data = json.loads(value.decode('utf-8'))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        # Not a valid JSON or UTF-8 string
                        logger.warning(f"Received non-JSON message from topic {self.topic}")
                        continue

                    # Process message
                    result = self.message_processor(data)
                    results.append(result)

                    # Commit offset
                    consumer.commit(message)

                    # Increment counter
                    messages_processed += 1

                except Exception as e:
                    logger.error(f"Error processing message from topic {self.topic}: {e}")

            logger.info(f"Processed {messages_processed} messages from topic {self.topic}")

            return results

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
