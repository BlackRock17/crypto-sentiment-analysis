"""
Kafka consumer implementation for receiving messages from Kafka topics.
"""
import json
import logging
import signal
import time
from typing import Dict, Any, List, Callable, Optional, Union
from threading import Thread, Event
from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from src.data_processing.kafka.config import get_consumer_config, TOPICS

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Base Kafka consumer for receiving messages from Kafka topics."""

    def __init__(
            self,
            topics: Union[str, List[str]],
            group_id: str,
            config_overrides: Dict[str, Any] = None,
            auto_commit: bool = False
    ):
        """
        Initialize Kafka consumer.

        Args:
            topics: Topic or list of topics to subscribe to
            group_id: Consumer group ID
            config_overrides: Configuration overrides for the consumer
            auto_commit: Whether to automatically commit offsets
        """
        # Ensure topics is a list
        self.topics = [topics] if isinstance(topics, str) else topics

        # Consumer configuration
        config = get_consumer_config(group_id, **(config_overrides or {}))
        config['enable.auto.commit'] = auto_commit
        self.auto_commit = auto_commit

        # Create consumer
        self.consumer = Consumer(config)
        self.consumer.subscribe(self.topics)

        # Control flags
        self._stop_event = Event()
        self._consumer_thread = None

        logger.info(f"Initialized Kafka consumer for topics {self.topics}, group_id: {group_id}")

    def deserialize_message(self, message: Message) -> Any:
        """
        Deserialize message value from bytes.

        Args:
            message: Kafka message

        Returns:
            Deserialized message value
        """
        try:
            value = message.value()
            if value is None:
                return None

            # Try to decode as JSON
            try:
                return json.loads(value.decode('utf-8'))
            except json.JSONDecodeError:
                # Return as string if not valid JSON
                return value.decode('utf-8')
            except UnicodeDecodeError:
                # Return raw bytes if not valid UTF-8
                return value

        except Exception as e:
            logger.error(f"Error deserializing message: {e}")
            return None

    def handle_message(self, message: Message) -> bool:
        """
        Process a message from Kafka.
        Subclasses should override this method.

        Args:
            message: Kafka message

        Returns:
            True if the message was processed successfully, False otherwise
        """
        # Default implementation just logs the message
        try:
            # Get message details
            topic = message.topic()
            partition = message.partition()
            offset = message.offset()
            timestamp = message.timestamp()

            # Deserialize value
            value = self.deserialize_message(message)

            logger.debug(f"Received message from {topic} [{partition}] at offset {offset}")

            # This method should be overridden by subclasses
            return True

        except Exception as e:
            logger.error(f"Error handling message: {e}")
            return False

    def _consume_loop(self):
        """Main consumer loop that runs in a separate thread."""
        logger.info(f"Starting consumer loop for topics {self.topics}")

        # Set up signal handling for graceful shutdown
        self._setup_signal_handling()

        try:
            while not self._stop_event.is_set():
                try:
                    # Poll for messages
                    message = self.consumer.poll(timeout=1.0)

                    if message is None:
                        continue

                    if message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            # End of partition, not an error
                            continue
                        else:
                            logger.error(f"Consumer error: {message.error()}")
                            continue

                    # Process the message
                    success = self.handle_message(message)

                    # Commit offset if auto_commit is False and processing was successful
                    if not self.auto_commit and success:
                        self.consumer.commit(message)

                except KafkaException as e:
                    logger.error(f"Kafka error in consumer loop: {e}")
                    time.sleep(1)  # Avoid tight error loop

                except Exception as e:
                    logger.error(f"Unexpected error in consumer loop: {e}")
                    time.sleep(1)  # Avoid tight error loop

        finally:
            # Close the consumer
            try:
                self.consumer.close()
                logger.info("Consumer closed")
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")

    def _setup_signal_handling(self):
        """Set up signal handling for graceful shutdown."""

        def signal_handler(sig, frame):
            logger.info(f"Received signal {sig}, stopping consumer...")
            self.stop()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def start(self):
        """Start the consumer in a background thread."""
        if self._consumer_thread is not None and self._consumer_thread.is_alive():
            logger.warning("Consumer is already running")
            return

        self._stop_event.clear()
        self._consumer_thread = Thread(target=self._consume_loop)
        self._consumer_thread.daemon = True
        self._consumer_thread.start()
        logger.info(f"Consumer started for topics {self.topics}")

        return self

    def stop(self):
        """Stop the consumer."""
        logger.info("Stopping consumer...")
        self._stop_event.set()

        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=30)
            logger.info("Consumer thread joined")

        if self._consumer_thread and self._consumer_thread.is_alive():
            logger.warning("Consumer thread did not terminate gracefully")

        self._consumer_thread = None

    def is_running(self) -> bool:
        """Check if the consumer is running."""
        return self._consumer_thread is not None and self._consumer_thread.is_alive()


class BatchKafkaConsumer(KafkaConsumer):
    """Kafka consumer that processes messages in batches."""

    def __init__(
            self,
            topics: Union[str, List[str]],
            group_id: str,
            batch_size: int = 100,
            batch_timeout: float = 5.0,
            config_overrides: Dict[str, Any] = None,
            auto_commit: bool = False
    ):
        """
        Initialize batch Kafka consumer.

        Args:
            topics: Topic or list of topics to subscribe to
            group_id: Consumer group ID
            batch_size: Maximum number of messages to include in a batch
            batch_timeout: Maximum time to wait for a batch to fill in seconds
            config_overrides: Configuration overrides for the consumer
            auto_commit: Whether to automatically commit offsets
        """
        super().__init__(topics, group_id, config_overrides, auto_commit)
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout

    def handle_batch(self, messages: List[Message]) -> bool:
        """
        Process a batch of messages from Kafka.
        Subclasses should override this method.

        Args:
            messages: List of Kafka messages

        Returns:
            True if all messages were processed successfully, False otherwise
        """
        # Default implementation just processes each message individually
        success = True
        for message in messages:
            if not self.handle_message(message):
                success = False

        return success

    def _consume_loop(self):
        """Main consumer loop that processes messages in batches."""
        logger.info(f"Starting batch consumer loop for topics {self.topics}")

        # Set up signal handling for graceful shutdown
        self._setup_signal_handling()

        try:
            while not self._stop_event.is_set():
                try:
                    # Collect a batch of messages
                    messages = []
                    start_time = time.time()

                    while len(messages) < self.batch_size and (time.time() - start_time) < self.batch_timeout:
                        if self._stop_event.is_set():
                            break

                        message = self.consumer.poll(timeout=0.1)

                        if message is None:
                            continue

                        if message.error():
                            if message.error().code() == KafkaError._PARTITION_EOF:
                                # End of partition, not an error
                                continue
                            else:
                                logger.error(f"Consumer error: {message.error()}")
                                continue

                        messages.append(message)

                    # Process the batch if we have any messages
                    if messages:
                        logger.debug(f"Processing batch of {len(messages)} messages")
                        success = self.handle_batch(messages)

                        # Commit the last message offset if processing was successful and auto_commit is False
                        if not self.auto_commit and success:
                            self.consumer.commit(messages[-1])

                except KafkaException as e:
                    logger.error(f"Kafka error in batch consumer loop: {e}")
                    time.sleep(1)  # Avoid tight error loop

                except Exception as e:
                    logger.error(f"Unexpected error in batch consumer loop: {e}")
                    time.sleep(1)  # Avoid tight error loop

        finally:
            # Close the consumer
            try:
                self.consumer.close()
                logger.info("Batch consumer closed")
            except Exception as e:
                logger.error(f"Error closing batch consumer: {e}")
