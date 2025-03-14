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

# Настройка на logger с по-високо ниво на детайлност
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
            logger.debug("Got database session")

            try:
                # Deserialize the message
                tweet_data = self.deserialize_message(message)
                logger.debug(f"Deserialized message: {json.dumps(tweet_data, default=str)}")

                if not tweet_data:
                    logger.warning(f"Empty or invalid tweet data received")
                    return False

                logger.info(f"Processing tweet: {tweet_data.get('tweet_id')}")
                logger.debug(f"Tweet data type: {type(tweet_data)}")

                # Log important fields for debugging
                created_at = tweet_data.get('created_at')
                logger.debug(f"Created_at original: {created_at} (type: {type(created_at)})")

                # Безопасно преобразуване на created_at
                if 'created_at' in tweet_data and isinstance(tweet_data['created_at'], str):
                    try:
                        logger.debug(f"Converting created_at from string: {tweet_data['created_at']}")
                        tweet_data['created_at'] = datetime.fromisoformat(
                            tweet_data['created_at'].replace('Z', '+00:00'))
                        logger.debug(
                            f"Converted created_at to: {tweet_data['created_at']} (type: {type(tweet_data['created_at'])})")
                    except ValueError as e:
                        logger.warning(f"Error converting created_at: {e}")
                        tweet_data['created_at'] = datetime.utcnow()
                        logger.debug(f"Using fallback created_at: {tweet_data['created_at']}")
                elif 'created_at' not in tweet_data:
                    logger.warning("No created_at in tweet data, adding current time")
                    tweet_data['created_at'] = datetime.utcnow()

                # Initialize processor and repository
                logger.debug("Initializing TwitterDataProcessor")
                processor = TwitterDataProcessor()
                logger.debug("Initializing TwitterRepository")
                repository = TwitterRepository(db)

                # Get blockchain networks and known tokens for better identification
                logger.debug("Getting blockchain networks")
                blockchain_networks = repository.get_blockchain_networks()
                logger.debug(f"Got {len(blockchain_networks) if blockchain_networks else 0} blockchain networks")

                logger.debug("Getting known tokens")
                known_tokens = repository.get_known_tokens()
                logger.debug(f"Got {len(known_tokens) if known_tokens else 0} known tokens")

                # Process tweet data for storage
                logger.debug("Preparing tweet for storage")
                try:
                    processed_tweet = processor.prepare_tweet_for_storage(tweet_data)
                    logger.debug(f"Processed tweet: {json.dumps(processed_tweet, default=str)}")
                except Exception as e:
                    logger.error(f"Error preparing tweet for storage: {e}")
                    logger.error(traceback.format_exc())
                    return False

                # Store tweet in database
                logger.debug("Storing tweet in database")
                try:
                    stored_tweet = repository.store_tweet(processed_tweet)
                    if not stored_tweet:
                        logger.error(f"Failed to store tweet: {tweet_data.get('tweet_id')}")
                        return False
                    logger.debug(f"Stored tweet ID: {stored_tweet.id}")
                except Exception as e:
                    logger.error(f"Error storing tweet: {e}")
                    logger.error(traceback.format_exc())
                    return False

                # Create link between influencer and tweet if necessary
                influencer_id = tweet_data.get('influencer_id')
                is_manually_added = tweet_data.get('is_manually_added', False)

                if influencer_id:
                    logger.debug(f"Creating influencer-tweet link for influencer ID: {influencer_id}")
                    try:
                        from src.data_processing.crud.twitter import create_influencer_tweet
                        create_influencer_tweet(
                            db=db,
                            influencer_id=influencer_id,
                            tweet_id=stored_tweet.id,
                            is_manually_added=is_manually_added
                        )
                        logger.debug("Influencer-tweet link created")
                    except Exception as e:
                        logger.error(f"Error creating influencer-tweet link: {e}")
                        logger.error(traceback.format_exc())
                        # Continue processing even if this fails

                # Extract token mentions
                logger.debug("Extracting token mentions from tweet text")
                try:
                    token_mentions = processor.extract_blockchain_tokens(
                        processed_tweet['text'],
                        known_tokens,
                        blockchain_networks
                    )
                    logger.debug(f"Extracted token mentions type: {type(token_mentions)}")
                    logger.debug(f"Extracted token mentions: {token_mentions}")
                except Exception as e:
                    logger.error(f"Error extracting token mentions: {e}")
                    logger.error(traceback.format_exc())
                    return False

                # Нормализация на резултатите от извличането
                if token_mentions is None:
                    logger.debug("No token mentions found, using empty list")
                    token_mentions = []
                elif isinstance(token_mentions, dict):
                    logger.debug("Converting dict token mentions to list")
                    token_mentions = [token_mentions]
                elif isinstance(token_mentions, set):
                    logger.debug("Converting set token mentions to list of dicts")
                    token_mentions = [dict(token_tuple) for token_tuple in token_mentions]
                elif not isinstance(token_mentions, list):
                    logger.debug(f"Converting {type(token_mentions)} to list")
                    token_mentions = [token_mentions]

                # Ensure each item is a dictionary
                try:
                    token_mentions = [dict(token) if not isinstance(token, dict) else token for token in token_mentions
                                      if token]
                    logger.debug(f"Normalized token mentions: {token_mentions}")
                except Exception as e:
                    logger.error(f"Error normalizing token mentions: {e}")
                    logger.error(traceback.format_exc())
                    token_mentions = []

                # Process each token mention
                mentions_count = 0

                for token_data in token_mentions:
                    logger.debug(f"Processing token mention: {token_data}")
                    # Add tweet ID and send to Kafka for further processing
                    token_mention_data = {
                        "tweet_id": stored_tweet.id,
                        "tweet_text": processed_tweet['text'],
                        "token_data": token_data,
                        "created_at": datetime.utcnow().isoformat()
                    }

                    # Send token mention to Kafka
                    logger.debug(f"Sending token mention to Kafka: {token_mention_data}")
                    try:
                        success = self.token_mention_producer.send_token_mention(token_mention_data)
                        if success:
                            mentions_count += 1
                            logger.debug(f"Token mention sent successfully")
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
                    logger.debug("Closing database session")
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