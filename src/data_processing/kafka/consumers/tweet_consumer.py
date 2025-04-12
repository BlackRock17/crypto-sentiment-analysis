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
        """
        logger.info(f"Processing tweet: {message.get('tweet_id', 'unknown')}")
        logger.info(f"Message content: {message}")  # Добавено за дебъг

        try:
            # Process the tweet using the service
            result = self.tweet_service.process_tweet(message)
            logger.info(f"Process result: {result}")  # Добавено за дебъг

            if result is not None:
                # Извличаме информацията от върнатия резултат
                tweet_id = result["id"]
                status = result["status"]

                if status == "created":
                    logger.info(f"Successfully created new tweet, DB ID: {tweet_id}")
                else:
                    logger.info(f"Found existing tweet, DB ID: {tweet_id}")

                # Проверяваме за processing_id за SSE известия
                processing_id = message.get("processing_id")
                logger.info(f"Processing ID from message: {processing_id}")  # Добавено за дебъг

                if processing_id:
                    logger.info(f"Will try to notify about processing_id: {processing_id}")
                    try:
                        # Импортираме функцията, за да избегнем циклични зависимости
                        from src.api.twitter import notify_tweet_processed
                        from src.main import app as fastapi_app

                        # Създаваме резултата за клиента
                        notification_result = {
                            "status": "success",
                            "message": f"Tweet {'processed' if status == 'created' else 'found'} successfully",
                            "tweet_id": message.get("tweet_id"),
                            "db_id": tweet_id,
                            "operation": status  # Добавяме информация за типа операция
                        }

                        logger.info(f"Notification result: {notification_result}")

                        # Извикваме известяването асинхронно
                        import asyncio

                        loop = asyncio.get_event_loop()
                        loop.create_task(notify_tweet_processed(fastapi_app, processing_id, notification_result))
                        logger.info(f"Notification task created for {processing_id}")
                    except Exception as e:
                        logger.error(f"Error creating notification task: {e}")
                        import traceback
                        logger.error(traceback.format_exc())
                else:
                    logger.warning("No processing_id in message, skipping notification")

                return True
            else:
                logger.error("Failed to process tweet")
                return False

        except Exception as e:
            logger.error(f"Error in tweet processing: {e}")
            import traceback
            logger.error(traceback.format_exc())
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
