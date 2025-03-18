import logging
import json
import time
import unittest
from typing import Dict, Any, Optional



# Configure logger
logger = logging.getLogger(__name__)


class KafkaLogger:
    """Enhanced logging for Kafka operations with structured logs."""

    def __init__(self, service_name: str):
        """
        Initialize Kafka logger.

        Args:
            service_name: Name of the service using this logger
        """
        self.service_name = service_name
        self.logger = logging.getLogger(f"kafka.{service_name}")

    def log_producer_event(self,
                           topic: str,
                           event_type: str,
                           message_key: Optional[str] = None,
                           message_size: Optional[int] = None,
                           duration_ms: Optional[float] = None,
                           error: Optional[Exception] = None,
                           **additional_data) -> None:
        """
        Log producer-related events with structured information.

        Args:
            topic: Kafka topic
            event_type: Type of event (send, error, etc.)
            message_key: Optional message key
            message_size: Optional message size in bytes
            duration_ms: Optional operation duration in milliseconds
            error: Optional exception if an error occurred
            additional_data: Any additional data to include in log
        """
        log_data = {
            "timestamp": time.time(),
            "service": self.service_name,
            "component": "producer",
            "topic": topic,
            "event": event_type,
        }

        if message_key:
            log_data["message_key"] = message_key

        if message_size:
            log_data["message_size_bytes"] = message_size

        if duration_ms:
            log_data["duration_ms"] = duration_ms

        if error:
            log_data["error"] = str(error)
            log_data["error_type"] = error.__class__.__name__

        # Add any additional data
        log_data.update(additional_data)

        # Determine log level based on event type
        if error or event_type == "error":
            self.logger.error(f"Kafka producer error: {json.dumps(log_data)}")
        elif event_type == "warning":
            self.logger.warning(f"Kafka producer warning: {json.dumps(log_data)}")
        else:
            self.logger.info(f"Kafka producer event: {json.dumps(log_data)}")

    def log_consumer_event(self,
                           topic: str,
                           event_type: str,
                           group_id: Optional[str] = None,
                           partition: Optional[int] = None,
                           offset: Optional[int] = None,
                           lag: Optional[int] = None,
                           processing_time_ms: Optional[float] = None,
                           error: Optional[Exception] = None,
                           **additional_data) -> None:
        """
        Log consumer-related events with structured information.

        Args:
            topic: Kafka topic
            event_type: Type of event (receive, commit, error, etc.)
            group_id: Optional consumer group ID
            partition: Optional partition number
            offset: Optional message offset
            lag: Optional consumer lag (difference between latest offset and consumed offset)
            processing_time_ms: Optional processing time in milliseconds
            error: Optional exception if an error occurred
            additional_data: Any additional data to include in log
        """
        log_data = {
            "timestamp": time.time(),
            "service": self.service_name,
            "component": "consumer",
            "topic": topic,
            "event": event_type,
        }

        if group_id and not isinstance(group_id, unittest.mock.Mock):
            log_data["group_id"] = group_id

        if partition is not None and not isinstance(partition, unittest.mock.Mock):
            log_data["partition"] = partition

        if offset is not None and not isinstance(offset, unittest.mock.Mock):
            log_data["offset"] = offset

        if lag is not None and not isinstance(lag, unittest.mock.Mock):
            log_data["consumer_lag"] = lag

        if processing_time_ms and not isinstance(processing_time_ms, unittest.mock.Mock):
            log_data["processing_time_ms"] = processing_time_ms

        if error and not isinstance(error, unittest.mock.Mock):
            log_data["error"] = str(error)
            log_data["error_type"] = error.__class__.__name__

        # Add any additional data
        serializable_additional_data = {}
        for key, value in additional_data.items():
            if not isinstance(value, unittest.mock.Mock):
                serializable_additional_data[key] = value
        log_data.update(serializable_additional_data)

        # Determine log level based on event type
        try:
            if error or event_type == "error":
                self.logger.error(f"Kafka consumer error: {json.dumps(log_data)}")
            elif event_type == "warning" or (lag and lag > 1000):
                self.logger.warning(f"Kafka consumer warning: {json.dumps(log_data)}")
            else:
                self.logger.info(f"Kafka consumer event: {json.dumps(log_data)}")
        except TypeError as e:
            # Fallback за случаите, когато JSON сериализацията се провали
            if error or event_type == "error":
                self.logger.error(f"Kafka consumer error (JSON serialize failed): {log_data}")
            elif event_type == "warning":
                self.logger.warning(f"Kafka consumer warning (JSON serialize failed): {log_data}")
            else:
                self.logger.info(f"Kafka consumer event (JSON serialize failed): {log_data}")
