"""
Kafka consumer for processing tweets.
"""
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional

from src.data_processing.kafka.consumer import KafkaConsumer
from src.data_processing.kafka.config import TOPICS
from src.data_collection.twitter.processor import TwitterDataProcessor
from src.data_collection.twitter.repository import TwitterRepository
from src.data_processing.database import get_db
from src.data_processing.kafka.producer import TokenMentionProducer

logger = logging.getLogger(__name__)


class TweetConsumer(KafkaConsumer):
    """Consumer for processing tweets from Kafka and extracting token mentions."""

    def __init__(self):
        """Initialize tweet consumer."""
        super().__init__(
            topics=TOPICS["RAW_TWEETS"],
            group_id="tweet-processor",
            auto_commit=False
        )
        self.token_mention_producer = TokenMentionProducer()

    def handle_message(self, message):
        """
        Process a tweet message from Kafka.

        Args:
            message: Kafka message containing tweet data

        Returns:
            True if processing was successful, False otherwise
        """
        try:
            # Get database session
            db = next(get_db())

            try:
                # Deserialize the message
                tweet_data = self.deserialize_message(message)
                if not tweet_data:
                    logger.warning(f"Empty or invalid tweet data received")
                    return False

                logger.info(f"Processing tweet: {tweet_data.get('tweet_id')}")

                # Initialize processor and repository
                processor = TwitterDataProcessor()
                repository = TwitterRepository(db)

                # Get blockchain networks and known tokens for better identification
                blockchain_networks = repository.get_blockchain_networks()
                known_tokens = repository.get_known_tokens()

                # Process tweet data for storage
                processed_tweet = processor.prepare_tweet_for_storage(tweet_data)

                # Store tweet in database
                stored_tweet = repository.store_tweet(processed_tweet)
                if not stored_tweet:
                    logger.error(f"Failed to store tweet: {tweet_data.get('tweet_id')}")
                    return False

                # Create link between influencer and tweet if necessary
                influencer_id = tweet_data.get('influencer_id')
                is_manually_added = tweet_data.get('is_manually_added', False)

                if influencer_id:
                    from src.data_processing.crud.twitter import create_influencer_tweet
                    create_influencer_tweet(
                        db=db,
                        influencer_id=influencer_id,
                        tweet_id=stored_tweet.id,
                        is_manually_added=is_manually_added
                    )

                # Extract token mentions
                token_mentions = processor.extract_blockchain_tokens(
                    processed_tweet['text'],
                    known_tokens,
                    blockchain_networks
                )

                if isinstance(token_mentions, dict):
                    token_mentions = [{"symbol": k, "blockchain_network": v} for k, v in token_mentions.items()]
                elif isinstance(token_mentions, set):
                    token_mentions = [dict(token_tuple) for token_tuple in token_mentions]

                # Process each token mention
                mentions_count = 0

                for token_data in token_mentions:
                    # Add tweet ID and send to Kafka for further processing
                    token_mention_data = {
                        "tweet_id": stored_tweet.id,
                        "tweet_text": processed_tweet['text'],
                        "token_data": token_data,
                        "created_at": datetime.utcnow().isoformat()
                    }

                    # Send token mention to Kafka
                    success = self.token_mention_producer.send_token_mention(token_mention_data)
                    if success:
                        mentions_count += 1

                logger.info(f"Processed tweet {stored_tweet.id} with {mentions_count} token mentions")
                return True

            except Exception as e:
                logger.error(f"Error processing tweet: {e}")
                return False

            finally:
                # Close database session
                db.close()

        except Exception as e:
            logger.error(f"Error in tweet consumer: {e}")
            return False
