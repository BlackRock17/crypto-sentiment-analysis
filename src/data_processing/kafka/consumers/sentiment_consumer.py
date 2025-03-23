"""
Kafka consumer for sentiment analysis of tweets.
"""
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional

from src.data_processing.kafka.consumer import KafkaConsumer
from src.data_processing.kafka.config import TOPICS
from src.data_processing.database import get_db
from src.data_processing.models.database import SentimentEnum
from src.data_processing.crud.read import get_sentiment_analysis_by_tweet_id

logger = logging.getLogger(__name__)


class SentimentConsumer(KafkaConsumer):
    """Consumer for performing sentiment analysis on tweets and storing results."""

    def __init__(self):
        """Initialize sentiment analysis consumer."""
        super().__init__(
            topics=TOPICS["SENTIMENT_RESULTS"],
            group_id="sentiment-analyzer",
            auto_commit=False
        )

    def handle_message(self, message):
        """
        Process a sentiment analysis message from Kafka.

        Args:
            message: Kafka message containing data for sentiment analysis

        Returns:
            True if processing was successful, False otherwise
        """
        try:
            # Get database session
            db = next(get_db())

            try:
                # Deserialize the message
                sentiment_data = self.deserialize_message(message)
                if not sentiment_data:
                    logger.warning(f"Empty or invalid sentiment data received")
                    return False

                logger.info(f"Analyzing sentiment for tweet: {sentiment_data.get('tweet_id')}")

                # Get required data
                tweet_id = sentiment_data.get('tweet_id')

                if not tweet_id:
                    logger.error("Missing tweet_id in sentiment analysis message")
                    return False

                # Get the tweet text from the database
                from src.data_processing.crud.read import get_tweet_by_id
                tweet = get_tweet_by_id(db, tweet_id)

                if not tweet:
                    logger.error(f"Tweet not found: {tweet_id}")
                    return False

                existing_analysis = get_sentiment_analysis_by_tweet_id(db, tweet.id)
                if existing_analysis:
                    logger.info(f"Sentiment analysis already exists for tweet {tweet_id}, skipping")
                    return True

                # Perform sentiment analysis
                # This is a placeholder - in a real application, you would call your sentiment analysis model
                # For now, we'll just use a simple example
                sentiment, confidence = self._analyze_sentiment(tweet.text)

                # Store sentiment analysis result
                from src.data_processing.crud.create import create_sentiment_analysis
                sentiment_result = create_sentiment_analysis(
                    db=db,
                    tweet_id=tweet_id,
                    sentiment=sentiment,
                    confidence_score=confidence
                )

                if not sentiment_result:
                    logger.error(f"Failed to store sentiment analysis for tweet {tweet_id}")
                    return False

                logger.info(f"Stored sentiment analysis for tweet {tweet_id}: {sentiment.value}")
                return True

            except Exception as e:
                logger.error(f"Error processing sentiment analysis: {e}")
                return False

            finally:
                # Close database session
                db.close()

        except Exception as e:
            logger.error(f"Error in sentiment consumer: {e}")
            return False

    def _analyze_sentiment(self, text: str) -> tuple:
        """
        Simple placeholder for sentiment analysis.
        In a real application, this would call a ML model or API.

        Args:
            text: Text to analyze

        Returns:
            Tuple of (sentiment_enum, confidence_score)
        """
        # This is just a very basic placeholder implementation
        # In a real application, use a proper sentiment analysis model

        positive_words = ['good', 'great', 'excellent', 'up', 'bullish', 'moon', 'gain']
        negative_words = ['bad', 'terrible', 'down', 'bearish', 'crash', 'loss']

        text_lower = text.lower()

        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)

        if positive_count > negative_count:
            return SentimentEnum.POSITIVE, 0.6 + (0.1 * min(positive_count, 4))
        elif negative_count > positive_count:
            return SentimentEnum.NEGATIVE, 0.6 + (0.1 * min(negative_count, 4))
        else:
            return SentimentEnum.NEUTRAL, 0.7
