import argparse
import logging
import time
import signal
import sys

from src.monitoring.prometheus_exporter import KafkaPrometheusExporter
from src.data_processing.kafka.config import DEFAULT_BOOTSTRAP_SERVERS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global variable for exporter
exporter = None


def signal_handler(sig, frame):
    """Handle signals for graceful shutdown."""
    global exporter
    logger.info(f"Received signal {sig}, stopping exporter...")
    if exporter:
        exporter.stop()
    sys.exit(0)


def main():
    """Start the Prometheus exporter for Kafka metrics."""
    global exporter

    parser = argparse.ArgumentParser(description='Start Kafka Prometheus exporter')
    parser.add_argument('--bootstrap-servers', default=DEFAULT_BOOTSTRAP_SERVERS,
                        help='Kafka bootstrap servers (comma-separated)')
    parser.add_argument('--port', type=int, default=8000,
                        help='Port to expose Prometheus metrics on')
    args = parser.parse_args()

    # Set up signal handling
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Start the exporter
        logger.info(f"Starting Kafka Prometheus exporter on port {args.port}")
        exporter = KafkaPrometheusExporter(
            bootstrap_servers=args.bootstrap_servers,
            port=args.port
        )
        exporter.start()

        # Keep the script running
        logger.info("Exporter running. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        if exporter:
            exporter.stop()
    except Exception as e:
        logger.error(f"Error starting Prometheus exporter: {e}")
        if exporter:
            exporter.stop()


if __name__ == "__main__":
    main()
