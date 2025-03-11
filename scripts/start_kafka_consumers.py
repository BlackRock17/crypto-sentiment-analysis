import time
import logging
import argparse
import signal
import sys

from src.data_processing.kafka.consumers.tweet_consumer import TweetConsumer
from src.data_processing.kafka.consumers.token_mention_consumer import TokenMentionConsumer
from src.data_processing.kafka.consumers.sentiment_consumer import SentimentConsumer
from src.data_processing.kafka.consumers.token_categorization_consumer import TokenCategorizationConsumer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global variables to store consumers
consumers = []


def signal_handler(sig, frame):
    """Handle signals for graceful shutdown."""
    logger.info(f"Received signal {sig}, stopping consumers...")
    stop_consumers()
    sys.exit(0)


def start_consumers(consumer_types):
    """Start specified Kafka consumers."""
    global consumers

    if 'all' in consumer_types or 'tweet' in consumer_types:
        logger.info("Starting Tweet Consumer...")
        tweet_consumer = TweetConsumer()
        tweet_consumer.start()
        consumers.append(tweet_consumer)

    if 'all' in consumer_types or 'mention' in consumer_types:
        logger.info("Starting Token Mention Consumer...")
        mention_consumer = TokenMentionConsumer()
        mention_consumer.start()
        consumers.append(mention_consumer)

    if 'all' in consumer_types or 'sentiment' in consumer_types:
        logger.info("Starting Sentiment Consumer...")
        sentiment_consumer = SentimentConsumer()
        sentiment_consumer.start()
        consumers.append(sentiment_consumer)

    if 'all' in consumer_types or 'categorization' in consumer_types:
        logger.info("Starting Token Categorization Consumer...")
        categorization_consumer = TokenCategorizationConsumer()
        categorization_consumer.start()
        consumers.append(categorization_consumer)

    logger.info(f"Started {len(consumers)} consumers")


def stop_consumers():
    """Stop all running Kafka consumers."""
    global consumers

    logger.info(f"Stopping {len(consumers)} consumers...")
    for i, consumer in enumerate(consumers):
        try:
            logger.info(f"Stopping consumer {i + 1}...")
            consumer.stop()
            timeout = 5.0
            start_time = time.time()
            while consumer.is_running() and (time.time() - start_time) < timeout:
                time.sleep(0.1)
            logger.info(f"Consumer {i + 1} stopped" if not consumer.is_running() else f"Consumer {i + 1} stop timeout")
        except Exception as e:
            logger.error(f"Error stopping consumer {i + 1}: {e}")

    consumers = []
    logger.info("All consumers stopped")


def main():
    """Run the Kafka consumers test."""
    parser = argparse.ArgumentParser(description='Start Kafka consumers for testing')
    parser.add_argument('--consumers', nargs='+', default=['all'],
                        choices=['all', 'tweet', 'mention', 'sentiment', 'categorization'],
                        help='Which consumers to start')
    parser.add_argument('--run-time', type=int, default=0,
                        help='How long to run consumers (in seconds, 0 for indefinite)')
    args = parser.parse_args()

    # Set up signal handling
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Start consumers
        start_consumers(args.consumers)

        # Run for specified time or indefinitely
        if args.run_time > 0:
            logger.info(f"Consumers will run for {args.run_time} seconds")
            time.sleep(args.run_time)
            stop_consumers()
            logger.info("Test completed")
        else:
            logger.info("Consumers running indefinitely. Press Ctrl+C to stop.")
            while True:
                time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        stop_consumers()
    except Exception as e:
        logger.error(f"Error: {e}")
        stop_consumers()


if __name__ == "__main__":
    main()
