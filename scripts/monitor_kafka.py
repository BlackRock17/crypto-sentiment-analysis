import argparse
import logging
import time
import json
from datetime import datetime

from pip._internal.utils.misc import tabulate

from src.data_processing.kafka.config import DEFAULT_BOOTSTRAP_SERVERS
from src.monitoring import KafkaMonitor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def print_metrics(metrics):
    """Pretty-print Kafka metrics."""
    # Print header
    print("\n" + "=" * 80)
    print(f"KAFKA METRICS - {datetime.fromtimestamp(metrics['timestamp']).strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # Print broker info
    print(f"\nBroker Count: {metrics['broker_count']}")
    if metrics["broker_info"]:
        broker_table = []
        for broker_id, broker in metrics["broker_info"].items():
            broker_table.append([broker_id, f"{broker['host']}:{broker['port']}"])
        print(tabulate(broker_table, headers=["Broker ID", "Host:Port"]))

    # Print topic info
    print(f"\nTopics: {len(metrics['topics'])}")
    if metrics["topics"]:
        topic_table = []
        for topic_name, topic_info in metrics["topics"].items():
            topic_table.append([topic_name, topic_info["partition_count"]])
        print(tabulate(topic_table, headers=["Topic", "Partitions"]))

    # Print consumer group info
    print(f"\nConsumer Groups: {len(metrics['consumer_groups'])}")
    if metrics["consumer_groups"]:
        group_table = []
        for group_id, group_info in metrics["consumer_groups"].items():
            group_table.append([
                group_id,
                group_info["state"],
                len(group_info.get("members", []))
            ])
        print(tabulate(group_table, headers=["Group ID", "State", "Members"]))

    print("\n" + "=" * 80 + "\n")


def save_metrics_to_file(metrics, output_file):
    """Save metrics to a JSON file."""
    with open(output_file, 'w') as f:
        json.dump(metrics, f, indent=2)
    logger.info(f"Saved metrics to {output_file}")


def main():
    """Run Kafka monitoring."""
    parser = argparse.ArgumentParser(description='Monitor Kafka cluster')
    parser.add_argument('--bootstrap-servers', default=DEFAULT_BOOTSTRAP_SERVERS,
                        help='Kafka bootstrap servers (comma-separated)')
    parser.add_argument('--interval', type=int, default=10,
                        help='Monitoring interval in seconds')
    parser.add_argument('--count', type=int, default=0,
                        help='Number of iterations (0 for infinite)')
    parser.add_argument('--output-file', type=str, default=None, help='Output file for metrics (JSON)')
    args = parser.parse_args()

    try:
        # Create Kafka monitor
        monitor = KafkaMonitor(args.bootstrap_servers, args.interval)

        # Set up callback to print metrics
        def metrics_callback(metrics):
            print_metrics(metrics)

            # Save to file if requested
            if args.output_file:
                save_metrics_to_file(metrics, args.output_file)

        monitor.add_metrics_callback(metrics_callback)

        # Start monitoring
        logger.info(f"Starting Kafka monitoring with interval {args.interval}s")
        monitor.start()

        # Run for specified number of iterations or indefinitely
        iteration = 0
        while args.count == 0 or iteration < args.count:
            time.sleep(args.interval)
            iteration += 1

            if args.count > 0:
                logger.info(f"Completed {iteration}/{args.count} iterations")

        # Stop monitoring
        monitor.stop()
        logger.info("Kafka monitoring stopped")

    except KeyboardInterrupt:
        logger.info("Monitoring interrupted by user")
        if 'monitor' in locals():
            monitor.stop()
    except Exception as e:
        logger.error(f"Error during Kafka monitoring: {e}")

if __name__ == "__main__":
    main()
