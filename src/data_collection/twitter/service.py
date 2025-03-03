"""
Service for Twitter data collection.
Coordinates data collection, processing, and storage.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from sqlalchemy.orm import Session

from src.data_collection.twitter.client import TwitterAPIClient
from src.data_collection.twitter.processor import TwitterDataProcessor
from src.data_collection.twitter.repository import TwitterRepository
from src.data_processing.models.database import Tweet, TokenMention

# Configure logger
logger = logging.getLogger(__name__)


class TwitterCollectionService:
    """
    Service for Twitter data collection.
    Coordinates the collection, processing, and storage of Twitter data.
    """

    def __init__(self, db: Session):
        """
        Initialize the Twitter collection service.

        Args:
            db: Database session
        """
        self.db = db
        self.client = TwitterAPIClient()
        self.processor = TwitterDataProcessor(self.client)
        self.repository = TwitterRepository(db)

    def collect_and_store_influencer_tweets(self, limit_per_user: int = None) -> Tuple[int, int]:
        """
        Collect tweets from crypto influencers and store them in the database.

        Args:
            limit_per_user: Maximum number of tweets to collect per user

        Returns:
            Tuple of (tweets_collected, token_mentions_found)
        """
        logger.info(f"Starting collection of tweets from influencers")

        # Collect tweets from influencers
        tweets_data = self.processor.collect_influencer_tweets(limit_per_user=limit_per_user)

        if not tweets_data:
            logger.warning("No influencer tweets collected")
            return 0, 0

        # Get known tokens for mention detection
        known_tokens = self.repository.get_known_tokens()

        tweets_stored = 0
        mentions_found = 0

        # Process and store each tweet
        for tweet_data in tweets_data:
            # Prepare tweet for storage
            prepared_tweet = self.processor.prepare_tweet_for_storage(tweet_data)

            # Store tweet
            stored_tweet = self.repository.store_tweet(prepared_tweet)
            if not stored_tweet:
                continue

            tweets_stored += 1

            # Extract token mentions
            token_symbols = self.processor.extract_solana_tokens(prepared_tweet['text'], known_tokens)

            # Add symbols from cashtags if available
            if 'cashtags' in tweet_data:
                token_symbols.update(set(tweet_data['cashtags']))

            # Store token mentions
            if token_symbols:
                mentions = self.repository.store_token_mentions(stored_tweet, token_symbols)
                mentions_found += len(mentions)

        logger.info(
            f"Collection complete: stored {tweets_stored} influencer tweets with {mentions_found} token mentions")
        return tweets_stored, mentions_found

    def test_twitter_connection(self) -> bool:
        """
        Test connection to Twitter API.

        Returns:
            True if connection successful, False otherwise
        """
        return self.client.test_connection()
