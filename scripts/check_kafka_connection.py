import logging
import argparse
import time
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_kafka_connection(bootstrap_servers, timeout=10):
    """
    Check if Kafka is accessible.

    Args:
        bootstrap_servers: Kafka broker address(es)
        timeout: Timeout in seconds

    Returns:
        bool: True if Kafka is accessible, False otherwise
    """
    logger.info(f"Checking Kafka connection to: {bootstrap_servers}")

    # Create admin client
    try:
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

        # Try to get cluster metadata with timeout
        start_time = time.time()
        metadata = None

        while time.time() - start_time < timeout:
            try:
                metadata = admin_client.list_topics(timeout=1)
                break
            except KafkaException as e:
                logger.debug(f"Waiting for Kafka: {e}")
                time.sleep(1)

        if metadata:
            broker_count = len(metadata.brokers)
            topic_count = len(metadata.topics)
            logger.info(f"Kafka connection successful! Found {broker_count} brokers and {topic_count} topics")
            return True
        else:
            logger.error(f"Could not connect to Kafka within {timeout} seconds")
            return False

    except Exception as e:
        logger.error(f"Error connecting to Kafka: {e}")
        return False


def test_produce_consume(bootstrap_servers, test_topic="kafka-test-topic"):
    """
    Test producing and consuming a message.

    Args:
        bootstrap_servers: Kafka broker address(es)
        test_topic: Topic to use for testing

    Returns:
        bool: True if test passed, False otherwise
    """
    logger.info(f"Testing produce/consume on topic: {test_topic}")

    # Create the test topic if it doesn't exist
    try:
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

        # Check if topic exists
        topics = admin_client.list_topics(timeout=10).topics
        if test_topic not in topics:
            logger.info(f"Creating test topic: {test_topic}")
            topic_list = [NewTopic(test_topic, num_partitions=1, replication_factor=1)]
            admin_client.create_topics(topic_list)
            # Wait for topic creation
            time.sleep(2)
    except Exception as e:
        logger.error(f"Error creating test topic: {e}")
        return False

    # Test message
    test_message = f"Test message {time.time()}"
    message_received = False

    # Create producer
    producer_config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'kafka-test-producer'
    }

    # Create consumer
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'kafka-test-consumer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }

    try:
        # Create producer and send message
        producer = Producer(producer_config)

        def delivery_callback(err, msg):
            if err:
                logger.error(f"Message delivery failed: {err}")
            else:
                logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

        # Produce message
        producer.produce(test_topic, test_message.encode('utf-8'), callback=delivery_callback)
        producer.flush(10)

        # Create consumer and receive message
        consumer = Consumer(consumer_config)
        consumer.subscribe([test_topic])

        # Try to consume the message with timeout
        start_time = time.time()
        while time.time() - start_time < 10 and not message_received:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached end of partition")
                else:
                    logger.error(f"Consumer error: {msg.error()}")
            else:
                received_message = msg.value().decode('utf-8')
                logger.info(f"Received message: {received_message}")
                if received_message == test_message:
                    message_received = True
                    break

        # Clean up
        consumer.close()

        if message_received:
            logger.info("Produce/consume test passed!")
            return True
        else:
            logger.error("Produce/consume test failed: Message not received")
            return False

    except Exception as e:
        logger.error(f"Error in produce/consume test: {e}")
        return False


def main():
    """Main function to check Kafka connection and run tests."""
    parser = argparse.ArgumentParser(description='Check Kafka connection')
    parser.add_argument('--bootstrap-servers', default='localhost:29092',
                        help='Kafka bootstrap servers (comma-separated)')
    parser.add_argument('--timeout', type=int, default=10,
                        help='Connection timeout in seconds')
    parser.add_argument('--test-produce-consume', action='store_true',
                        help='Test producing and consuming a message')
    args = parser.parse_args()

    # Check connection
    if check_kafka_connection(args.bootstrap_servers, args.timeout):
        logger.info("Kafka connection check: PASSED")

        # Run produce/consume test if requested
        if args.test_produce_consume:
            if test_produce_consume(args.bootstrap_servers):
                logger.info("Produce/consume test: PASSED")
            else:
                logger.error("Produce/consume test: FAILED")
                return 1

        return 0
    else:
        logger.error("Kafka connection check: FAILED")
        return 1


if __name__ == "__main__":
    exit(main())
