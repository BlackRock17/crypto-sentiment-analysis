import time
import logging
import threading
from typing import Dict, List, Any, Optional, Set, Callable
from confluent_kafka.admin import AdminClient, ClusterMetadata, GroupMetadata
import json
import os

# Configure logger
logger = logging.getLogger(__name__)


class KafkaMonitor:
    """
    Monitor for Kafka cluster health and performance metrics.
    Collects metrics about brokers, topics, consumer groups, and more.
    """

    def __init__(self, bootstrap_servers: str, metrics_interval: int = 60):
        """
        Initialize Kafka monitor.

        Args:
            bootstrap_servers: Kafka broker address(es)
            metrics_interval: Interval for collecting metrics in seconds
        """
        self.bootstrap_servers = bootstrap_servers
        self.metrics_interval = metrics_interval
        self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
        self.running = False
        self.monitor_thread = None
        self.metrics_callbacks: List[Callable[[Dict[str, Any]], None]] = []

    def start(self):
        """Start the monitoring thread."""
        if self.running:
            logger.warning("Kafka monitor is already running")
            return

        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitoring_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        logger.info(f"Kafka monitor started with interval {self.metrics_interval}s")

    def stop(self):
        """Stop the monitoring thread."""
        logger.info("Stopping Kafka monitor...")
        self.running = False

        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=30)
            logger.info("Kafka monitor stopped")

    def add_metrics_callback(self, callback: Callable[[Dict[str, Any]], None]):
        """
        Add a callback function to be called when metrics are collected.

        Args:
            callback: Function that takes a metrics dictionary
        """
        self.metrics_callbacks.append(callback)

    def _monitoring_loop(self):
        """Main monitoring loop that collects metrics at regular intervals."""
        while self.running:
            try:
                # Collect metrics
                metrics = self._collect_metrics()

                # Save metrics to file
                self._save_metrics_to_file(metrics)

                # Call any registered callbacks
                for callback in self.metrics_callbacks:
                    try:
                        callback(metrics)
                    except Exception as e:
                        logger.error(f"Error in metrics callback: {e}")

                # Sleep until next collection
                for _ in range(self.metrics_interval):
                    if not self.running:
                        break
                    time.sleep(1)

            except Exception as e:
                logger.error(f"Error in Kafka monitoring loop: {e}")
                time.sleep(10)  # Sleep for a bit before trying again

    def _collect_metrics(self) -> Dict[str, Any]:
        """
        Collect metrics about Kafka cluster.

        Returns:
            Dictionary with Kafka metrics
        """
        # Get timestamp for the metrics
        timestamp = time.time()

        # Get cluster metadata
        metadata = None
        try:
            metadata = self.admin_client.list_topics(timeout=10)
        except Exception as e:
            logger.error(f"Error getting Kafka metadata: {e}")

        # Initialize metrics dictionary
        metrics = {
            "timestamp": timestamp,
            "broker_count": 0,
            "topics": {},
            "consumer_groups": {},
            "broker_info": {},
            "status": "error" if metadata is None else "ok"
        }

        # If we couldn't get metadata, return limited metrics
        if metadata is None:
            return metrics

        # Extract broker information
        metrics["broker_count"] = len(metadata.brokers)
        for broker_id, broker in metadata.brokers.items():
            metrics["broker_info"][broker_id] = {
                "host": broker.host,
                "port": broker.port
            }

        # Extract topic information
        for topic_name, topic in metadata.topics.items():
            if topic_name == "__consumer_offsets":
                continue  # Skip internal topic

            topic_info = {
                "partition_count": len(topic.partitions),
                "partitions": {}
            }

            # Extract partition information
            for partition_id, partition in topic.partitions.items():
                topic_info["partitions"][partition_id] = {
                    "leader": partition.leader,
                    "replicas": partition.replicas,
                    "isrs": partition.isrs  # In-sync replicas
                }

            metrics["topics"][topic_name] = topic_info

        # Get consumer group information
        try:
            consumer_groups = self.admin_client.list_consumer_groups()
            if consumer_groups:
                for group_id, group_info in consumer_groups.items():
                    metrics["consumer_groups"][group_id] = {
                        "state": group_info.state,
                        "members": []
                    }

                    # Get detailed group information
                    group_details = self.admin_client.describe_consumer_groups([group_id])
                    if group_details and group_id in group_details:
                        group = group_details[group_id]
                        for member in group.members:
                            member_info = {
                                "id": member.id,
                                "client_id": member.client_id,
                                "host": member.host,
                                "assignments": []
                            }

                            if member.assignment:
                                for topic, partitions in member.assignment.items():
                                    member_info["assignments"].append({
                                        "topic": topic,
                                        "partitions": partitions
                                    })

                            metrics["consumer_groups"][group_id]["members"].append(member_info)

        except Exception as e:
            logger.error(f"Error getting consumer group information: {e}")

        return metrics

    def _save_metrics_to_file(self, metrics: Dict[str, Any], metrics_dir: str = "logs/kafka_metrics"):
        """
        Save metrics to a JSON file for historical tracking.

        Args:
            metrics: Metrics dictionary
            metrics_dir: Directory to save metrics files
        """
        try:
            # Ensure metrics directory exists
            os.makedirs(metrics_dir, exist_ok=True)

            # Create filename based on timestamp
            timestamp = metrics["timestamp"]
            date_str = time.strftime("%Y-%m-%d", time.localtime(timestamp))
            time_str = time.strftime("%H-%M-%S", time.localtime(timestamp))
            filename = f"{metrics_dir}/kafka_metrics_{date_str}_{time_str}.json"

            # Write metrics to file
            with open(filename, 'w') as f:
                json.dump(metrics, f, indent=2)

            logger.debug(f"Saved Kafka metrics to {filename}")
        except Exception as e:
            logger.error(f"Error saving metrics to file: {e}")
