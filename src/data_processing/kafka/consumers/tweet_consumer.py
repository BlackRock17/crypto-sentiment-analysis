"""
Consumer for processing raw tweets from Kafka.
"""
import logging
import asyncio

from typing import Dict, Any
from src.data_processing.kafka.consumer import KafkaConsumerBase
from src.data_processing.kafka.config import TOPICS
from src.services.tweet_service import TweetService
from src.repositories.tweet_repository import TweetRepository
from src.api.twitter import notify_tweet_processed
from fastapi import FastAPI
from src.main import app as fastapi_app

# Set up logging
logger = logging.getLogger(__name__)


class TweetConsumer(KafkaConsumerBase):
    """Consumer for tweet processing."""

    def __init__(self):
        """Initialize the tweet consumer."""
        # Create service instances
        self.tweet_repository = TweetRepository()
        self.tweet_service = TweetService(self.tweet_repository)

        # Initialize the base class
        super().__init__(
            topics=[TOPICS["RAW_TWEETS"]],
            message_processor=self._process_tweet
        )

    def _process_tweet(self, message: Dict[str, Any]) -> bool:
        """
        Process a tweet message from Kafka.

        Args:
            message: Kafka message payload as dictionary

        Returns:
            True if processing was successful, False otherwise
        """
        logger.info(f"Processing tweet: {message.get('tweet_id', 'unknown')}")

        try:
            # Process the tweet using the service
            result = self.tweet_service.process_tweet(message)

            if result is not None:
                tweet_id = result["id"]
                status = result["status"]

                if status == "created":
                    logger.info(f"Successfully created new tweet, DB ID: {tweet_id}")
                else:
                    logger.info(f"Found existing tweet, DB ID: {tweet_id}")

                processing_id = message.get("processing_id")
                if processing_id:

                    result = {
                        "status": "success",
                        "message": "Tweet processed successfully",
                        "tweet_id": message.get("tweet_id"),
                        "db_id": tweet_id
                    }

                    loop = asyncio.get_event_loop()
                    loop.create_task(notify_tweet_processed(fastapi_app, processing_id, result))

                return True
            else:
                logger.error("Failed to process tweet")
                return False

        except Exception as e:
            logger.error(f"Error in tweet processing: {e}")
            return False


# This section could be moved to a common runner file
def run_consumer():
    """Run the tweet consumer."""
    consumer = TweetConsumer()
    try:
        consumer.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, stopping consumer")
    finally:
        consumer.stop()


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    # Run the consumer
    run_consumer()
