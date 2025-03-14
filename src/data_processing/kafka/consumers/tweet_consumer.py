"""
Kafka consumer for processing tweets.
"""
import logging
import json
import traceback
from datetime import datetime
from typing import Dict, Any, Optional

from src.data_processing.kafka.consumer import KafkaConsumer
from src.data_processing.kafka.config import TOPICS
from src.data_collection.twitter.processor import TwitterDataProcessor
from src.data_collection.twitter.repository import TwitterRepository
from src.data_processing.database import get_db
from src.data_processing.kafka.producer import TokenMentionProducer

# Setting up a logger with a higher level of detail
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
        logger.info("TweetConsumer initialized")

    def handle_message(self, message):
        """
        Process a tweet message from Kafka.

        Args:
            message: Kafka message containing tweet data

        Returns:
            True if processing was successful, False otherwise
        """
        db = None
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

                # Log important fields for debugging
                created_at = tweet_data.get('created_at')

                # Safe conversion of created_at
                if 'created_at' in tweet_data and isinstance(tweet_data['created_at'], str):
                    try:
                        tweet_data['created_at'] = datetime.fromisoformat(
                            tweet_data['created_at'].replace('Z', '+00:00'))
                    except ValueError as e:
                        tweet_data['created_at'] = datetime.utcnow()
                elif 'created_at' not in tweet_data:
                    tweet_data['created_at'] = datetime.utcnow()

                # Initialize processor and repository
                processor = TwitterDataProcessor()
                repository = TwitterRepository(db)

                # Get blockchain networks and known tokens for better identification
                blockchain_networks = repository.get_blockchain_networks()

                known_tokens = repository.get_known_tokens()

                # Process tweet data for storage
                try:
                    processed_tweet = processor.prepare_tweet_for_storage(tweet_data)
                except Exception as e:
                    logger.error(f"Error preparing tweet for storage: {e}")
                    logger.error(traceback.format_exc())
                    return False

                # Store tweet in database
                try:
                    stored_tweet = repository.store_tweet(processed_tweet)
                    if not stored_tweet:
                        logger.error(f"Failed to store tweet: {tweet_data.get('tweet_id')}")
                        return False
                except Exception as e:
                    logger.error(f"Error storing tweet: {e}")
                    logger.error(traceback.format_exc())
                    return False

                # Create link between influencer and tweet if necessary
                influencer_id = tweet_data.get('influencer_id')
                is_manually_added = tweet_data.get('is_manually_added', False)

                if influencer_id:
                    try:
                        from src.data_processing.crud.twitter import create_influencer_tweet
                        create_influencer_tweet(
                            db=db,
                            influencer_id=influencer_id,
                            tweet_id=stored_tweet.id,
                            is_manually_added=is_manually_added
                        )
                    except Exception as e:
                        logger.error(f"Error creating influencer-tweet link: {e}")
                        logger.error(traceback.format_exc())
                        # Continue processing even if this fails

                # Extract token mentions
                try:
                    token_mentions = processor.extract_blockchain_tokens(
                        processed_tweet['text'],
                        known_tokens,
                        blockchain_networks
                    )
                except Exception as e:
                    logger.error(f"Error extracting token mentions: {e}")
                    logger.error(traceback.format_exc())
                    return False

                # Normalization of extraction results
                if token_mentions is None:
                    token_mentions = []
                elif isinstance(token_mentions, dict):
                    token_mentions = [token_mentions]
                elif isinstance(token_mentions, set):
                    token_mentions = [dict(token_tuple) for token_tuple in token_mentions]
                elif not isinstance(token_mentions, list):
                    token_mentions = [token_mentions]

                # Ensure each item is a dictionary
                try:
                    token_mentions = [dict(token) if not isinstance(token, dict) else token for token in token_mentions
                                      if token]
                except Exception as e:
                    logger.error(f"Error normalizing token mentions: {e}")
                    logger.error(traceback.format_exc())
                    token_mentions = []

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
                    try:
                        success = self.token_mention_producer.send_token_mention(token_mention_data)
                        if success:
                            mentions_count += 1
                        else:
                            logger.warning(f"Failed to send token mention to Kafka")
                    except Exception as e:
                        logger.error(f"Error sending token mention: {e}")
                        logger.error(traceback.format_exc())

                logger.info(f"Processed tweet {stored_tweet.id} with {mentions_count} token mentions")
                return True

            except Exception as e:
                logger.error(f"Error processing tweet: {e}")
                logger.error(traceback.format_exc())
                return False

            finally:
                # Close database session
                if db:
                    db.close()

        except Exception as e:
            logger.error(f"Error in tweet consumer: {e}")
            logger.error(traceback.format_exc())
            # Ensure we close the database session even on error
            if db:
                try:
                    db.close()
                except:
                    pass
            return False