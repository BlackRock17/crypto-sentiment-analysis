"""
Kafka producer implementation for sending messages to Kafka topics.
"""
import json
import logging
import time
# from datetime import time
from typing import Dict, Any, Optional, Callable
from confluent_kafka import Producer, KafkaException

from src.data_processing.kafka.config import get_producer_config, TOPICS
from src.monitoring import KafkaLogger

logger = logging.getLogger(__name__)


class KafkaProducer:
    """Base Kafka producer for sending messages to Kafka topics."""

    def __init__(self, client_id: Optional[str] = None, config_overrides: Dict[str, Any] = None):
        """
        Initialize Kafka producer.

        Args:
            client_id: Unique identifier for this producer
            config_overrides: Configuration overrides for the producer
        """
        config = get_producer_config(client_id, **(config_overrides or {}))
        self.producer = Producer(config)
        self.kafka_logger = KafkaLogger(client_id or "default-producer")
        logger.info(f"Initialized Kafka producer with client_id: {client_id or 'default'}")

    def delivery_callback(self, err, msg):
        """
        Callback function for delivery reports.

        Args:
            err: Error object (None if successful)
            msg: Message object that was delivered
        """
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def serialize_message(self, value: Any) -> bytes:
        """
        Serialize message value to bytes.

        Args:
            value: Message value to serialize

        Returns:
            Serialized message as bytes
        """
        try:
            if isinstance(value, bytes):
                return value
            elif isinstance(value, str):
                return value.encode('utf-8')
            else:
                return json.dumps(value).encode('utf-8')
        except Exception as e:
            logger.error(f"Error serializing message: {e}")
            raise ValueError(f"Cannot serialize message: {e}")

    def send(
            self,
            topic: str,
            value: Any,
            key: Optional[str] = None,
            headers: Optional[Dict[str, str]] = None,
            callback: Optional[Callable] = None
    ) -> bool:
        """
        Send a message to a Kafka topic.

        Args:
            topic: Topic name
            value: Message value
            key: Message key (used for partitioning)
            headers: Optional message headers
            callback: Optional callback function to override default callback

        Returns:
            True if the message was queued successfully, False otherwise
        """
        start_time = time.time()
        message_size = None

        try:
            # Serialize the message
            serialized_value = self.serialize_message(value)

            # Convert key to bytes if provided
            key_bytes = key.encode('utf-8') if key is not None else None

            # Convert headers to list of tuples if provided
            header_list = [(k, v.encode('utf-8')) for k, v in headers.items()] if headers else None

            # Produce the message
            self.producer.produce(
                topic=topic,
                value=serialized_value,
                key=key_bytes,
                headers=header_list,
                callback=callback or self.delivery_callback
            )

            # Serve delivery callbacks from previous produce calls
            self.producer.poll(0)

            # Calculate message size if possible
            message_size = len(serialized_value) if isinstance(serialized_value, bytes) else None

            # Log success
            duration_ms = (time.time() - start_time) * 1000
            self.kafka_logger.log_producer_event(
                topic=topic,
                event_type="send",
                message_key=key,
                message_size=message_size,
                duration_ms=duration_ms
            )

            return True

        except (ValueError, KafkaException) as e:
            logger.error(f"Error sending message to topic {topic}: {e}")
            duration_ms = (time.time() - start_time) * 1000
            self.kafka_logger.log_producer_event(
                topic=topic,
                event_type="error",
                message_key=key,
                message_size=message_size,
                duration_ms=duration_ms,
                error=e
            )
            return False

    def flush(self, timeout: float = 10.0) -> int:
        """
        Wait for all messages in the producer queue to be delivered.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            Number of messages still in the queue
        """
        return self.producer.flush(timeout)


class TwitterProducer(KafkaProducer):
    """Producer specialized for Twitter data."""

    def __init__(self):
        """Initialize Twitter-specific producer."""
        super().__init__(client_id="twitter-producer")

    def send_tweet(self, tweet_data: Dict[str, Any]) -> bool:
        """
        Send a tweet to the raw tweets topic.

        Args:
            tweet_data: Tweet data

        Returns:
            True if successful, False otherwise
        """
        # Add timestamp if not present
        if 'timestamp' not in tweet_data:
            import datetime
            tweet_data['timestamp'] = datetime.datetime.now().isoformat()

        # Use tweet_id as the message key if available for partitioning
        key = str(tweet_data.get('tweet_id', '')) or None

        return self.send(
            topic=TOPICS['RAW_TWEETS'],
            value=tweet_data,
            key=key
        )

    def send_token_mention(self, token_mention: Dict[str, Any]) -> bool:
        """
        Send a token mention to the token mentions topic.

        Args:
            token_mention: Token mention data

        Returns:
            True if successful, False otherwise
        """
        # Use token_id as key for partitioning
        key = str(token_mention.get('token_id', '')) or None

        return self.send(
            topic=TOPICS['TOKEN_MENTIONS'],
            value=token_mention,
            key=key
        )


class SentimentProducer(KafkaProducer):
    """Producer specialized for sentiment analysis results."""

    def __init__(self):
        """Initialize sentiment-specific producer."""
        super().__init__(client_id="sentiment-producer")

    def send_sentiment_result(self, sentiment_data: Dict[str, Any]) -> bool:
        """
        Send sentiment analysis result.

        Args:
            sentiment_data: Sentiment analysis data

        Returns:
            True if successful, False otherwise
        """
        # Use tweet_id as key for partitioning
        key = str(sentiment_data.get('tweet_id', '')) or None

        return self.send(
            topic=TOPICS['SENTIMENT_RESULTS'],
            value=sentiment_data,
            key=key
        )


class TokenCategoryProducer(KafkaProducer):
    """Producer specialized for token categorization tasks."""

    def __init__(self):
        """Initialize token categorization producer."""
        super().__init__(client_id="token-category-producer")

    def send_categorization_task(self, token_id: int, priority: int = 0) -> bool:
        """
        Send a token categorization task.

        Args:
            token_id: ID of the token to categorize
            priority: Priority level (higher = more important)

        Returns:
            True if successful, False otherwise
        """
        import datetime

        task_data = {
            'token_id': token_id,
            'priority': priority,
            'timestamp': datetime.datetime.now().isoformat()
        }

        return self.send(
            topic=TOPICS['TOKEN_CATEGORIZATION'],
            value=task_data,
            key=str(token_id)
        )


class NotificationProducer(KafkaProducer):
    """Producer specialized for system notifications."""

    def __init__(self):
        """Initialize notification producer."""
        super().__init__(client_id="notification-producer")

    def send_notification(self, notification_data: Dict[str, Any]) -> bool:
        """
        Send a system notification.

        Args:
            notification_data: Notification data

        Returns:
            True if successful, False otherwise
        """
        # Ensure required fields
        if 'type' not in notification_data or 'message' not in notification_data:
            logger.error("Notification must include 'type' and 'message' fields")
            return False

        return self.send(
            topic=TOPICS['SYSTEM_NOTIFICATIONS'],
            value=notification_data
        )


class TokenMentionProducer(KafkaProducer):
    """Producer specialized for token mentions."""

    def __init__(self):
        """Initialize token mention producer."""
        super().__init__(client_id="token-mention-producer")

    def send_token_mention(self, token_mention: Dict[str, Any]) -> bool:
        """
        Send a token mention to the token mentions topic.

        Args:
            token_mention: Token mention data

        Returns:
            True if successful, False otherwise
        """
        # Use token_id as key for partitioning if available
        token_id = token_mention.get('token_data', {}).get('token_id', None)
        key = str(token_id) if token_id else None

        return self.send(
            topic=TOPICS['TOKEN_MENTIONS'],
            value=token_mention,
            key=key
        )
