"""
Consumer for processing market sentiment analysis.
"""
import logging
from typing import Dict, Any
from datetime import datetime

from src.data_processing.kafka.consumer import KafkaConsumerBase
from src.data_processing.kafka.config import TOPICS
from src.ml.sentiment_analyzer import SentimentAnalyzer
from src.data_processing.models.database import SentimentEnum
from src.data_processing.crud.create import create_market_sentiment
from src.data_processing.database import get_db_context

# Set up logging
logger = logging.getLogger(__name__)


class MarketSentimentConsumer(KafkaConsumerBase):
    """Consumer for market sentiment analysis processing."""

    def __init__(self):
        """Initialize the market sentiment consumer."""
        self.sentiment_analyzer = SentimentAnalyzer()

        super().__init__(
            topics=[TOPICS["MARKET_SENTIMENT"]],
            message_processor=self._process_market_sentiment,
            group_id="market-sentiment-group"
        )

    def _process_market_sentiment(self, message: Dict[str, Any]) -> bool:
        """
        Process a market sentiment message from Kafka.

        Args:
            message: Kafka message payload as dictionary

        Returns:
            True if processing was successful, False otherwise
        """
        logger.info(f"Processing market sentiment for tweet ID: {message.get('tweet_id', 'unknown')}")

        try:
            # Extract data from the message
            tweet_id = message.get("tweet_id")
            text = message.get("text", "")

            if not tweet_id or not text:
                logger.error("Missing required fields in message")
                return False

            # Analyze sentiment using the ML model
            sentiment, confidence_score = self.sentiment_analyzer.analyze_text(text)

            # Log the result
            logger.info(f"Sentiment analysis result: {sentiment.value} (confidence: {confidence_score:.4f})")

            # Save to database
            with get_db_context() as db:
                db_sentiment = create_market_sentiment(
                    db=db,
                    tweet_id=tweet_id,
                    sentiment=sentiment,
                    confidence_score=confidence_score
                )

                if db_sentiment:
                    logger.info(f"Saved market sentiment for tweet ID {tweet_id}")
                    return True
                else:
                    logger.error(f"Failed to save market sentiment for tweet ID {tweet_id}")
                    return False

        except Exception as e:
            logger.error(f"Error in market sentiment processing: {e}")
            return False


# This section could be moved to a common runner file
def run_consumer():
    """Run the market sentiment consumer."""
    consumer = MarketSentimentConsumer()
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
