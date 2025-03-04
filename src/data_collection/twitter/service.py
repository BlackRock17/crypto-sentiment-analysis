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
from src.data_collection.twitter.config import twitter_config
from src.data_processing.models.database import Tweet, TokenMention
from src.data_processing.models.twitter import TwitterInfluencer, TwitterInfluencerTweet
from src.data_processing.crud.twitter import (
    get_automated_influencers, create_influencer_tweet, get_influencer_by_username
)

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
        self.client = TwitterAPIClient(db=db)
        self.processor = TwitterDataProcessor(self.client)
        self.repository = TwitterRepository(db)

    def collect_and_store_automated_tweets(self) -> Tuple[int, int]:
        """
        Collect tweets from automated influencers and store them in the database.

        Returns:
            Tuple of (tweets_collected, token_mentions_found)
        """
        logger.info(f"Starting collection of tweets from automated influencers")

        # Get automated influencers
        max_influencers = twitter_config.max_automated_influencers
        influencers = get_automated_influencers(self.db, max_count=max_influencers)

        if not influencers:
            logger.warning("No automated influencers configured")
            return 0, 0

        logger.info(f"Found {len(influencers)} automated influencers")

        tweets_stored = 0
        mentions_found = 0

        # Process each influencer
        for influencer in influencers:
            # Check if we're within API limits
            if not self.client.check_api_limits(influencer.id):
                logger.warning(f"API limit reached, skipping influencer {influencer.username}")
                continue

            # Collect tweets
            tweets_data = self.client.get_user_tweets(
                username=influencer.username,
                influencer_id=influencer.id,
                max_results=twitter_config.max_tweets_per_user
            )

            if not tweets_data:
                logger.warning(f"No tweets found for {influencer.username}")
                continue

            # Process each tweet
            for tweet_data in tweets_data:
                # Store the tweet and create links
                result = self._process_and_store_tweet(tweet_data, influencer.id, is_manually_added=False)

                if result:
                    tweet_stored, mentions = result
                    tweets_stored += 1 if tweet_stored else 0
                    mentions_found += mentions

        logger.info(
            f"Collection complete: stored {tweets_stored} influencer tweets with {mentions_found} token mentions"
        )
        return tweets_stored, mentions_found

    def add_manual_tweet(
            self,
            influencer_username: str,
            tweet_text: str,
            created_at: Optional[datetime] = None,
            tweet_id: Optional[str] = None,
            retweet_count: int = 0,
            like_count: int = 0
    ) -> Tuple[Optional[Tweet], int]:
        """
        Manually add a tweet for an influencer.

        Args:
            influencer_username: Twitter username of the influencer
            tweet_text: Text content of the tweet
            created_at: Creation timestamp (defaults to now)
            tweet_id: Original Twitter ID (optional)
            retweet_count: Number of retweets
            like_count: Number of likes

        Returns:
            Tuple of (stored_tweet, mentions_count)
        """
        # Get or create influencer
        influencer = get_influencer_by_username(self.db, influencer_username)

        if not influencer:
            logger.info(f"Influencer {influencer_username} not found, creating new record")
            influencer = self._create_influencer(influencer_username)

        if not influencer:
            logger.error(f"Failed to get or create influencer {influencer_username}")
            return None, 0

        # Prepare tweet data
        if not tweet_id:
            import uuid
            tweet_id = f"manual_{uuid.uuid4().hex}"

        tweet_data = {
            "tweet_id": tweet_id,
            "text": tweet_text,
            "created_at": created_at or datetime.utcnow(),
            "author_id": f"user_{influencer_username}",
            "author_username": influencer_username,
            "retweet_count": retweet_count,
            "like_count": like_count
        }

        # Process and store tweet
        result = self._process_and_store_tweet(tweet_data, influencer.id, is_manually_added=True)

        if not result:
            return None, 0

        stored_tweet, mentions_count = result
        return stored_tweet, mentions_count

    def _process_and_store_tweet(
            self,
            tweet_data: Dict[str, Any],
            influencer_id: int,
            is_manually_added: bool = False
    ) -> Optional[Tuple[Tweet, int]]:
        """
        Process and store a tweet with token mentions.

        Args:
            tweet_data: Tweet data dictionary
            influencer_id: ID of the influencer
            is_manually_added: Whether the tweet was manually added

        Returns:
            Tuple of (stored_tweet, mentions_count) or None if failed
        """
        try:
            # Prepare tweet for storage
            prepared_tweet = self.processor.prepare_tweet_for_storage(tweet_data)

            # Store tweet
            stored_tweet = self.repository.store_tweet(prepared_tweet)
            if not stored_tweet:
                return None

            # Create link between influencer and tweet
            create_influencer_tweet(
                db=self.db,
                influencer_id=influencer_id,
                tweet_id=stored_tweet.id,
                is_manually_added=is_manually_added
            )

            # Get known tokens for mention detection
            known_tokens = self.repository.get_known_tokens()

            # Extract token mentions
            token_symbols = self.processor.extract_solana_tokens(prepared_tweet['text'], known_tokens)

            # Add symbols from cashtags if available
            if 'cashtags' in tweet_data:
                token_symbols.update(set(tweet_data['cashtags']))

            # Store token mentions
            mentions_count = 0
            if token_symbols:
                mentions = self.repository.store_token_mentions(stored_tweet, token_symbols)
                mentions_count = len(mentions)

            return stored_tweet, mentions_count

        except Exception as e:
            logger.error(f"Error processing tweet: {e}")
            self.db.rollback()
            return None

    def _create_influencer(self, username: str) -> Optional[TwitterInfluencer]:
        """
        Create a new influencer record.

        Args:
            username: Twitter username

        Returns:
            Created TwitterInfluencer instance or None if failed
        """
        from src.data_processing.crud.twitter import create_influencer

        try:
            # Try to get user information from Twitter
            user_response = self.client._execute_with_retry(
                lambda: self.client.client.get_user(username=username)
            )

            if not user_response or not user_response.data:
                logger.warning(f"User not found on Twitter: {username}")

            # Create influencer with available information
            if user_response and user_response.data:
                user = user_response.data
                description = getattr(user, 'description', None)
                name = getattr(user, 'name', None)
                followers = getattr(user, 'public_metrics', {}).get('followers_count', 0)

                return create_influencer(
                    db=self.db,
                    username=username,
                    name=name,
                    description=description,
                    follower_count=followers,
                    is_active=True,
                    is_automated=False,  # Manual by default
                    priority=0  # Default priority
                )
            else:
                # Create with minimal information
                return create_influencer(
                    db=self.db,
                    username=username,
                    is_active=True,
                    is_automated=False
                )

        except Exception as e:
            logger.error(f"Error creating influencer {username}: {e}")
            self.db.rollback()
            return None

    def test_twitter_connection(self) -> bool:
        """
        Test connection to Twitter API.

        Returns:
            True if connection successful, False otherwise
        """
        return self.client.test_connection()
