"""
Base Kafka consumer implementation.
"""
import json
import logging
import signal
from typing import List, Optional, Callable
from confluent_kafka import Consumer, Message

from src.data_processing.kafka.config import CONSUMER_CONFIG

# Set up logging
logger = logging.getLogger(__name__)


class KafkaConsumerBase:
    """Base class for Kafka consumers."""

    def __init__(self, topics: List[str], message_processor: Callable, group_id: Optional[str] = None):
        """
        Initialize the Kafka consumer.

        Args:
            topics: List of topics to subscribe to
            message_processor: Function to process messages
            group_id: Consumer group ID (optional)
        """
        self.topics = topics
        self.message_processor = message_processor
        self.consumer = None
        self.running = False

        # Set up config
        self.config = CONSUMER_CONFIG.copy()
        if group_id:
            self.config['group.id'] = group_id

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, sig, frame):
        """Handle termination signals."""
        logger.info(f"Received signal {sig}, shutting down consumer...")
        self.running = False

    def _setup_consumer(self):
        """Set up the Kafka consumer."""
        self.consumer = Consumer(self.config)
        self.consumer.subscribe(self.topics)
        logger.info(f"Consumer subscribed to topics: {self.topics}")

    def _process_message(self, message: Message) -> bool:
        """
        Process a Kafka message using the message processor.

        Args:
            message: Kafka message

        Returns:
            True if processing was successful, False otherwise
        """
        if message.error():
            logger.error(f"Consumer error: {message.error()}")
            return False

        try:
            # Decode message value
            if message.value() is None:
                return False

            value = json.loads(message.value().decode('utf-8'))

            # Use the message processor function
            return self.message_processor(value)

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in message: {e}")
            return False
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False

    def start(self, timeout: float = 1.0):
        """
        Start consuming messages.

        Args:
            timeout: Poll timeout in seconds
        """
        if self.consumer is None:
            self._setup_consumer()

        self.running = True
        logger.info(f"Starting consumer for topics: {self.topics}")

        try:
            while self.running:
                # Poll for messages
                message = self.consumer.poll(timeout)

                if message is None:
                    continue

                # Process the message
                success = self._process_message(message)

                # Commit offset if processing was successful
                if success:
                    self.consumer.commit(message)

        except Exception as e:
            logger.error(f"Error in consumer: {e}")
        finally:
            self.stop()

    def stop(self):
        """Stop the consumer."""
        if self.consumer:
            logger.info("Closing consumer")
            self.consumer.close()
            self.consumer = None
        self.running = False
