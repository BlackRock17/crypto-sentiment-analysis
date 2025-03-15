import time
import threading
from typing import Dict, Any, Optional
import logging
from prometheus_client import start_http_server, Gauge, Counter, Histogram

from src.data_processing.kafka.config import DEFAULT_BOOTSTRAP_SERVERS
from src.monitoring import KafkaMonitor

# Configure logger
logger = logging.getLogger(__name__)


class KafkaPrometheusExporter:
    """Export Kafka metrics to Prometheus."""

    def __init__(self, bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS, port: int = 8000):
        """
        Initialize Prometheus exporter for Kafka metrics.

        Args:
            bootstrap_servers: Kafka broker address(es)
            port: Port to expose Prometheus metrics on
        """
        self.bootstrap_servers = bootstrap_servers
        self.port = port
        self.monitor = None
        self.running = False

        # Define Prometheus metrics
        self.broker_gauge = Gauge('kafka_broker_count', 'Number of active Kafka brokers')
        self.topic_gauge = Gauge('kafka_topic_count', 'Number of Kafka topics')
        self.topic_partition_gauge = Gauge('kafka_topic_partition_count', 'Number of partitions per topic', ['topic'])
        self.consumer_group_gauge = Gauge('kafka_consumer_group_count', 'Number of consumer groups')
        self.consumer_group_members_gauge = Gauge('kafka_consumer_group_members', 'Number of members in consumer group',
                                                  ['group'])
        self.consumer_group_lag_gauge = Gauge('kafka_consumer_group_lag', 'Consumer group lag by topic and partition',
                                              ['group', 'topic', 'partition'])

        # Producer metrics
        self.producer_message_counter = Counter('kafka_producer_messages_total', 'Total number of messages produced',
                                                ['topic'])
        self.producer_error_counter = Counter('kafka_producer_errors_total', 'Total number of producer errors',
                                              ['topic'])
        self.producer_message_size = Histogram('kafka_producer_message_size_bytes',
                                               'Size of produced messages in bytes',
                                               ['topic'])

        # Consumer metrics
        self.consumer_message_counter = Counter('kafka_consumer_messages_total', 'Total number of messages consumed',
                                                ['topic', 'group'])
        self.consumer_error_counter = Counter('kafka_consumer_errors_total', 'Total number of consumer errors',
                                              ['topic', 'group'])
        self.consumer_processing_time = Histogram('kafka_consumer_processing_time_seconds',
                                                  'Time to process a message in seconds', ['topic', 'group'])

    def start(self):
        """Start the Prometheus exporter."""
        if self.running:
            logger.warning("Prometheus exporter is already running")
            return self

        # Start Prometheus HTTP server
        start_http_server(self.port)
        logger.info(f"Started Prometheus exporter on port {self.port}")

        # Start Kafka monitor
        self.monitor = KafkaMonitor(self.bootstrap_servers)
        self.monitor.add_metrics_callback(self._update_prometheus_metrics)
        self.monitor.start()

        self.running = True
        logger.info("Kafka Prometheus exporter started")
        return self

    def stop(self):
        """Stop the Prometheus exporter."""
        if not self.running:
            return

        # Stop Kafka monitor
        if self.monitor:
            self.monitor.stop()

        self.running = False
        logger.info("Kafka Prometheus exporter stopped")

    def _update_prometheus_metrics(self, metrics: Dict[str, Any]):
        """
        Update Prometheus metrics from Kafka metrics.

        Args:
            metrics: Kafka metrics dictionary
        """
        try:
            # Update broker metrics
            self.broker_gauge.set(metrics["broker_count"])

            # Update topic metrics
            topic_count = len(metrics["topics"])
            self.topic_gauge.set(topic_count)

            for topic_name, topic_info in metrics["topics"].items():
                self.topic_partition_gauge.labels(topic=topic_name).set(topic_info["partition_count"])

            # Update consumer group metrics
            group_count = len(metrics["consumer_groups"])
            self.consumer_group_gauge.set(group_count)

            for group_id, group_info in metrics["consumer_groups"].items():
                member_count = len(group_info.get("members", []))
                self.consumer_group_members_gauge.labels(group=group_id).set(member_count)

                # In a real implementation, we would update lag metrics here

        except Exception as e:
            logger.error(f"Error updating Prometheus metrics: {e}")

    def record_producer_message(self, topic: str, message_size: int):
        """
        Record a produced message in Prometheus metrics.

        Args:
            topic: Kafka topic
            message_size: Size of message in bytes
        """
        self.producer_message_counter.labels(topic=topic).inc()
        self.producer_message_size.labels(topic=topic).observe(message_size)

    def record_producer_error(self, topic: str):
        """
        Record a producer error in Prometheus metrics.

        Args:
            topic: Kafka topic
        """
        self.producer_error_counter.labels(topic=topic).inc()

    def record_consumer_message(self, topic: str, group: str, processing_time: float):
        """
        Record a consumed message in Prometheus metrics.

        Args:
            topic: Kafka topic
            group: Consumer group
            processing_time: Time to process message in seconds
        """
        self.consumer_message_counter.labels(topic=topic, group=group).inc()
        self.consumer_processing_time.labels(topic=topic, group=group).observe(processing_time)

    def record_consumer_error(self, topic: str, group: str):
        """
        Record a consumer error in Prometheus metrics.

        Args:
            topic: Kafka topic
            group: Consumer group
        """
        self.consumer_error_counter.labels(topic=topic, group=group).inc()
