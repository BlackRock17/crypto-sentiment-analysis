"""
DAG for monitoring Kafka topics and processing.
"""

import sys
import logging
import json
from datetime import datetime, timedelta

from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Add the project root to the Python path
sys.path.append("/opt/airflow")

# Import project modules
from airflow.plugins.crypto_utils import DEFAULT_ARGS, task_failure_callback, send_notification

# Configure logging
logger = logging.getLogger(__name__)

# Define the DAG
dag = DAG(
    'kafka_monitoring',
    default_args={
        **DEFAULT_ARGS,
        'on_failure_callback': task_failure_callback
    },
    description='Monitors Kafka topics and processing',
    schedule_interval='*/30 * * * *',  # Run every 30 minutes
    catchup=False,
    tags=['kafka', 'monitoring'],
)


def check_kafka_topics(**kwargs):
    """
    Task to check Kafka topics and their status.
    """

    logger.info("Checking Kafka topics status")

    # Kafka bootstrap servers
    bootstrap_servers = "kafka:9092"

    # Topics to check
    expected_topics = [
        "twitter-raw-tweets",
        "token-mentions",
        "sentiment-results",
        "token-categorization-tasks",
        "system-notifications"
    ]

    # Create admin client
    try:
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

        # Get cluster metadata with timeout
        metadata = admin_client.list_topics(timeout=10)

        if not metadata:
            logger.error("Could not get Kafka metadata")
            raise Exception("Failed to get Kafka metadata")

        # Check which topics exist
        existing_topics = metadata.topics
        missing_topics = [topic for topic in expected_topics if topic not in existing_topics]

        # Log results
        logger.info(f"Found {len(existing_topics)} topics in Kafka")
        logger.info(f"Missing topics: {missing_topics}")

        # Create missing topics if needed
        if missing_topics:
            logger.warning(f"The following topics are missing: {missing_topics}")

            # Create NewTopic objects for missing topics
            new_topics = [
                NewTopic(
                    topic,
                    num_partitions=3,
                    replication_factor=1,
                    config={
                        "retention.ms": "604800000"  # 7 days
                    }
                )
                for topic in missing_topics
            ]

            # Create the topics
            futures = admin_client.create_topics(new_topics)

            # Wait for each topic to be created
            for topic, future in futures.items():
                try:
                    future.result()  # The result itself is None
                    logger.info(f"Topic {topic} created successfully")
                except Exception as e:
                    logger.error(f"Failed to create topic {topic}: {e}")

            # Notify about the missing topics
            if len(missing_topics) > 0:
                send_notification(
                    title="Kafka Topics Missing",
                    message=f"Created {len(missing_topics)} missing Kafka topics: {', '.join(missing_topics)}",
                    priority="medium"
                )

        return {
            'existing_topics': list(existing_topics.keys()),
            'missing_topics': missing_topics,
            'broker_count': len(metadata.brokers)
        }

    except Exception as e:
        logger.error(f"Error checking Kafka topics: {e}")

        # Send notification about Kafka connection issue
        send_notification(
            title="Kafka Connection Error",
            message=f"Failed to connect to Kafka: {str(e)}",
            priority="high"
        )

        raise


def check_kafka_consumers(**kwargs):
    """
    Task to check Kafka consumer groups and their status.
    """

    logger.info("Checking Kafka consumer groups")

    # Kafka bootstrap servers
    bootstrap_servers = "kafka:9092"

    # Expected consumer groups
    expected_groups = [
        "tweet-processor",
        "token-processor",
        "sentiment-analyzer",
        "token-categorizer"
    ]

    # Create admin client
    try:
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

        # List consumer groups
        consumer_groups = admin_client.list_consumer_groups(timeout=10)

        # Extract group IDs
        groups = [group.group_id for group in consumer_groups.valid]

        # Check which expected groups are missing
        missing_groups = [group for group in expected_groups if group not in groups]

        # Log results
        logger.info(f"Found {len(groups)} consumer groups")
        for group in groups:
            logger.info(f"Consumer group: {group}")

        if missing_groups:
            logger.warning(f"Missing consumer groups: {missing_groups}")

            # Send notification about missing consumer groups
            send_notification(
                title="Kafka Consumer Groups Missing",
                message=f"The following consumer groups are missing: {', '.join(missing_groups)}",
                priority="high" if len(missing_groups) > 2 else "medium",
                missing_groups=missing_groups
            )

        return {
            'existing_groups': groups,
            'missing_groups': missing_groups
        }

    except Exception as e:
        logger.error(f"Error checking Kafka consumer groups: {e}")
        raise


def monitor_message_processing(**kwargs):
    """
    Task to monitor message processing rates across Kafka topics.
    """

    logger.info("Monitoring Kafka message processing rates")

    # Kafka bootstrap servers
    bootstrap_servers = "kafka:9092"

    # Topics to monitor
    topics_to_monitor = [
        "twitter-raw-tweets",
        "token-mentions",
        "sentiment-results",
        "token-categorization-tasks"
    ]

    # Create admin client
    try:
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

        # Get metadata for topics
        metadata = admin_client.list_topics(timeout=10)

        # Check message counts for each topic
        topic_stats = {}

        for topic_name in topics_to_monitor:
            if topic_name in metadata.topics:
                topic = metadata.topics[topic_name]

                # Get total number of messages across all partitions
                # Note: This is not a direct Kafka API, it would require additional code in a real implementation
                # For now, we'll simulate this
                message_count = sum([p.high_watermark - p.low_watermark for p in topic.partitions.values()])

                topic_stats[topic_name] = {
                    'message_count': message_count,
                    'partition_count': len(topic.partitions)
                }

                logger.info(f"Topic {topic_name}: {message_count} messages across {len(topic.partitions)} partitions")

        # Check for processing bottlenecks
        # In a real implementation, we would compare the counts between related topics
        # For now, we'll use a simple heuristic

        # If raw tweets are much higher than processed mentions, that's a bottleneck
        raw_tweets_count = topic_stats.get('twitter-raw-tweets', {}).get('message_count', 0)
        token_mentions_count = topic_stats.get('token-mentions', {}).get('message_count', 0)

        if raw_tweets_count > 0 and token_mentions_count > 0:
            processing_ratio = token_mentions_count / raw_tweets_count

            if processing_ratio < 0.5:  # Less than 50% processed
                logger.warning(f"Potential bottleneck: Only {processing_ratio:.1%} of tweets processed")

                # Send notification about processing bottleneck
                send_notification(
                    title="Kafka Processing Bottleneck Detected",
                    message=f"Only {processing_ratio:.1%} of tweets are being processed. Raw tweets: {raw_tweets_count}, Processed mentions: {token_mentions_count}",
                    priority="high" if processing_ratio < 0.2 else "medium",
                    raw_tweets=raw_tweets_count,
                    token_mentions=token_mentions_count
                )

        return topic_stats

    except Exception as e:
        logger.error(f"Error monitoring Kafka message processing: {e}")
        raise


def check_kafka_cluster_health(**kwargs):
    """
    Task to check Kafka cluster health.
    """

    logger.info("Checking Kafka cluster health")

    # Kafka bootstrap servers
    bootstrap_servers = "kafka:9092"

    # Create admin client
    try:
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

        # Get cluster metadata
        metadata = admin_client.list_topics(timeout=10)

        # Extract broker information
        brokers = metadata.brokers

        # Check broker count (for a production cluster, would check for minimum required brokers)
        broker_count = len(brokers)

        logger.info(f"Kafka cluster has {broker_count} brokers")

        # Log broker details
        for broker_id, broker in brokers.items():
            logger.info(f"Broker {broker_id}: {broker.host}:{broker.port}")

        # In a production environment, we would check for under-replicated partitions,
        # offline partitions, disk usage, etc.
        # For this example, we'll just check the broker count

        if broker_count < 1:
            logger.error("No Kafka brokers available")

            # Send notification about no brokers
            send_notification(
                title="Kafka Cluster Critical",
                message="No Kafka brokers available. The system cannot process messages.",
                priority="high"
            )

        return {
            'broker_count': broker_count,
            'brokers': [f"{broker.host}:{broker.port}" for broker_id, broker in brokers.items()]
        }

    except Exception as e:
        logger.error(f"Error checking Kafka cluster health: {e}")

        # Send notification about Kafka connection issue
        send_notification(
            title="Kafka Cluster Health Check Failed",
            message=f"Failed to check Kafka cluster health: {str(e)}",
            priority="high"
        )

        raise


# Define tasks
check_topics_task = PythonOperator(
    task_id='check_kafka_topics',
    python_callable=check_kafka_topics,
    dag=dag,
)

check_consumers_task = PythonOperator(
    task_id='check_kafka_consumers',
    python_callable=check_kafka_consumers,
    dag=dag,
)

monitor_processing_task = PythonOperator(
    task_id='monitor_message_processing',
    python_callable=monitor_message_processing,
    dag=dag,
)

check_cluster_task = PythonOperator(
    task_id='check_kafka_cluster_health',
    python_callable=check_kafka_cluster_health,
    dag=dag,
)

# Define task dependencies
check_cluster_task >> check_topics_task >> check_consumers_task >> monitor_processing_task
