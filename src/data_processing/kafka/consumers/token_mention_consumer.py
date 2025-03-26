"""
Kafka consumer for processing token mentions.
"""
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional

from src.data_processing.kafka.consumer import KafkaConsumer
from src.data_processing.kafka.config import TOPICS
from src.data_collection.twitter.repository import TwitterRepository
from src.data_processing.database import get_db
from src.data_processing.kafka.producer import SentimentProducer, TokenCategoryProducer
from src.data_processing.crud.read import get_tweet_by_id
from src.data_processing.models import BlockchainToken

logger = logging.getLogger(__name__)


class TokenMentionConsumer(KafkaConsumer):
    """Consumer for processing token mentions and storing them in the database."""

    def __init__(self):
        """Initialize token mention consumer."""
        super().__init__(
            topics=TOPICS["TOKEN_MENTIONS"],
            group_id="token-processor",
            auto_commit=False
        )
        self.sentiment_producer = SentimentProducer()
        self.token_category_producer = TokenCategoryProducer()

    def handle_message(self, message):
        """
        Process a token mention message from Kafka.

        Args:
            message: Kafka message containing token mention data

        Returns:
            True if processing was successful, False otherwise
        """
        try:
            # Get database session
            db = next(get_db())

            try:
                # Deserialize the message
                token_mention_data = self.deserialize_message(message)
                if not token_mention_data:
                    logger.warning(f"Empty or invalid token mention data received")
                    return False

                logger.info(f"Processing token mention for tweet: {token_mention_data.get('tweet_id')}")

                # Initialize repository
                repository = TwitterRepository(db)

                # Get tweet and token data
                tweet_id = token_mention_data.get('tweet_id')
                token_data = token_mention_data.get('token_data', {})

                if not tweet_id or not token_data:
                    logger.error("Missing required data in token mention message")
                    return False

                tweet = get_tweet_by_id(db, tweet_id)

                if not tweet:
                    logger.error(f"Tweet with ID {tweet_id} not found in database")
                    return False

                token_mentions = repository.store_token_mentions(
                    tweet=tweet,
                    token_data=[token_data]
                )

                if not token_mentions:
                    logger.error(f"Failed to store token mention for tweet {tweet_id}")
                    return False

                for mention in token_mentions:
                    token = repository.db.query(BlockchainToken).filter(BlockchainToken.id == mention.token_id).first()

                    # Проверяваме дали токенът се нуждае от категоризация
                    needs_categorization = (
                            token.blockchain_network is None or
                            (
                                        token.network_confidence is not None and token.network_confidence < 0.7 and not token.manually_verified)
                    )

                    if needs_categorization:
                        logger.info(f"Sending token {token.symbol} (ID: {token.id}) for categorization")
                        try:
                            self.token_category_producer.send_categorization_task(token_id=token.id)
                        except Exception as e:
                            logger.error(f"Error sending token for categorization: {e}")

                # For each stored mention, send for sentiment analysis
                for mention in token_mentions:
                    sentiment_data = {
                        "tweet_id": tweet_id,
                        "token_id": mention.token_id,
                        "mention_id": mention.id,
                        "created_at": datetime.utcnow().isoformat()
                    }

                    # Send for sentiment analysis
                    self.sentiment_producer.send_sentiment_result(sentiment_data)

                logger.info(f"Processed {len(token_mentions)} token mentions for tweet {tweet_id}")
                return True

            except Exception as e:
                logger.error(f"Error processing token mention: {e}")
                return False

            finally:
                # Close database session
                db.close()

        except Exception as e:
            logger.error(f"Error in token mention consumer: {e}")
            return False
