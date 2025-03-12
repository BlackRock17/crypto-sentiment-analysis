import logging
import os
from typing import Dict, Any, Optional


from src.data_processing.kafka.config import DEFAULT_BOOTSTRAP_SERVERS
from src.monitoring import KafkaAlertManager, KafkaMonitor

# Configure logger
logger = logging.getLogger(__name__)

# Global instances for singletons
kafka_monitor = None
kafka_alert_manager = None


def setup_kafka_monitoring(bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
                           metrics_interval: int = 60,
                           email_config: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Set up Kafka monitoring and alerting.

    Args:
        bootstrap_servers: Kafka broker address(es)
        metrics_interval: Interval for collecting metrics in seconds
        email_config: Optional email configuration for alerting

    Returns:
        Dictionary with monitoring components
    """
    global kafka_monitor, kafka_alert_manager

    # Initialize components
    logger.info("Setting up Kafka monitoring...")

    # Set up alert manager
    kafka_alert_manager = KafkaAlertManager(email_config)

    # Set up Kafka monitor
    kafka_monitor = KafkaMonitor(bootstrap_servers, metrics_interval)

    # Register alerts callback
    kafka_monitor.add_metrics_callback(process_metrics_for_alerts)

    # Start the monitor
    kafka_monitor.start()

    logger.info("Kafka monitoring setup complete")

    return {
        "monitor": kafka_monitor,
        "alert_manager": kafka_alert_manager
    }


def shutdown_kafka_monitoring():
    """Shutdown Kafka monitoring components."""
    global kafka_monitor, kafka_alert_manager

    logger.info("Shutting down Kafka monitoring...")

    if kafka_monitor:
        kafka_monitor.stop()

    logger.info("Kafka monitoring shutdown complete")


def process_metrics_for_alerts(metrics: Dict[str, Any]):
    """
    Process Kafka metrics and trigger alerts if needed.

    Args:
        metrics: Kafka metrics dictionary
    """
    global kafka_alert_manager

    if not kafka_alert_manager:
        return

    # Check broker count
    expected_broker_count = 1  # For single-node setup, adjust as needed
    if metrics["broker_count"] < expected_broker_count:
        kafka_alert_manager.trigger_alert(
            alert_type="broker_down",
            level=KafkaAlertManager.LEVEL_CRITICAL,
            message=f"Kafka broker count ({metrics['broker_count']}) is less than expected ({expected_broker_count})",
            details={"broker_info": metrics["broker_info"]}
        )

    # Check consumer groups for high lag
    for group_id, group_info in metrics.get("consumer_groups", {}).items():
        # In a real implementation, we would check consumer lag here
        pass
