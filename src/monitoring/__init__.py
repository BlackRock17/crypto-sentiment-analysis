"""
Monitoring module for the Crypto Sentiment project.
"""

import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class KafkaLogger:
    """Logger for Kafka-related events."""

    def __init__(self, component_name: str):
        """
        Initialize the Kafka logger.

        Args:
            component_name: Name of the component (for identification)
        """
        self.component_name = component_name
        self.start_time = datetime.now()

    def log_producer_event(
            self,
            topic: str,
            event_type: str,
            message_key: Optional[str] = None,
            message_size: Optional[int] = None,
            duration_ms: Optional[float] = None,
            error: Optional[Exception] = None
    ):
        """
        Log a Kafka producer event.

        Args:
            topic: Kafka topic
            event_type: Type of event (send, error)
            message_key: Message key (if available)
            message_size: Size of the message in bytes
            duration_ms: Duration of the operation in milliseconds
            error: Exception if an error occurred
        """
        event_data = {
            'timestamp': datetime.now().isoformat(),
            'component': self.component_name,
            'topic': topic,
            'event_type': event_type,
            'message_key': message_key,
            'message_size': message_size,
            'duration_ms': duration_ms
        }

        if error:
            event_data['error'] = str(error)

        logger.info(f"Kafka producer event: {event_data}")

    def log_consumer_event(
            self,
            topic: str,
            event_type: str,
            group_id: str,
            message_count: int = 0,
            duration_ms: Optional[float] = None,
            error: Optional[Exception] = None
    ):
        """
        Log a Kafka consumer event.

        Args:
            topic: Kafka topic
            event_type: Type of event (poll, process, error)
            group_id: Consumer group ID
            message_count: Number of messages processed
            duration_ms: Duration of the operation in milliseconds
            error: Exception if an error occurred
        """
        event_data = {
            'timestamp': datetime.now().isoformat(),
            'component': self.component_name,
            'topic': topic,
            'event_type': event_type,
            'group_id': group_id,
            'message_count': message_count,
            'duration_ms': duration_ms
        }

        if error:
            event_data['error'] = str(error)

        logger.info(f"Kafka consumer event: {event_data}")


class PerformanceMonitor:
    """Monitor for tracking performance metrics."""

    def __init__(self, component_name: str):
        """
        Initialize the performance monitor.

        Args:
            component_name: Name of the component (for identification)
        """
        self.component_name = component_name
        self.start_time = None
        self.metrics = {}

    def start_timer(self, operation_name: str):
        """
        Start a timer for an operation.

        Args:
            operation_name: Name of the operation to time
        """
        if operation_name not in self.metrics:
            self.metrics[operation_name] = {
                'count': 0,
                'total_duration': 0,
                'min_duration': float('inf'),
                'max_duration': 0
            }

        self.start_time = time.time()

    def stop_timer(self, operation_name: str) -> float:
        """
        Stop a timer and record the duration.

        Args:
            operation_name: Name of the operation

        Returns:
            Duration of the operation in milliseconds
        """
        if self.start_time is None:
            logger.warning(f"Timer for {operation_name} was not started")
            return 0

        duration = (time.time() - self.start_time) * 1000  # Convert to milliseconds

        # Update metrics
        self.metrics[operation_name]['count'] += 1
        self.metrics[operation_name]['total_duration'] += duration
        self.metrics[operation_name]['min_duration'] = min(self.metrics[operation_name]['min_duration'], duration)
        self.metrics[operation_name]['max_duration'] = max(self.metrics[operation_name]['max_duration'], duration)

        self.start_time = None

        return duration

    def get_metrics(self) -> Dict[str, Dict[str, float]]:
        """
        Get the recorded metrics.

        Returns:
            Dictionary with metrics for each operation
        """
        result = {}

        for operation, metrics in self.metrics.items():
            count = metrics['count']

            result[operation] = {
                'count': count,
                'total_duration_ms': metrics['total_duration'],
                'avg_duration_ms': metrics['total_duration'] / count if count > 0 else 0,
                'min_duration_ms': metrics['min_duration'] if count > 0 else 0,
                'max_duration_ms': metrics['max_duration']
            }

        return result

    def reset_metrics(self):
        """Reset all metrics."""
        self.metrics = {}
        self.start_time = None


# Function to initialize monitoring for a component
def init_monitoring(component_name: str) -> Dict[str, Any]:
    """
    Initialize monitoring for a component.

    Args:
        component_name: Name of the component

    Returns:
        Dictionary with monitoring objects
    """
    return {
        'kafka_logger': KafkaLogger(component_name),
        'performance_monitor': PerformanceMonitor(component_name)
    }


