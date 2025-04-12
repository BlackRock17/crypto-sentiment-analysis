"""
Service layer for tweet processing business logic.
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from src.repositories.tweet_repository import TweetRepository

# Set up logging
logger = logging.getLogger(__name__)


class TweetService:
    """Service for handling tweet business logic."""

    def __init__(self, tweet_repository: TweetRepository):
        """
        Initialize the tweet service.

        Args:
            tweet_repository: Repository for tweet data access
        """
        self.tweet_repository = tweet_repository

    def validate_tweet(self, tweet_data: Dict[str, Any]) -> bool:
        """
        Validate tweet data based on business rules.

        Args:
            tweet_data: Tweet data to validate

        Returns:
            True if tweet is valid, False otherwise
        """
        # Check required fields
        required_fields = ['tweet_id', 'text', 'created_at']
        for field in required_fields:
            if field not in tweet_data:
                logger.error(f"Missing required field: {field}")
                return False

        # Check tweet_id is not empty
        if not tweet_data['tweet_id']:
            logger.error("Empty tweet_id is not allowed")
            return False

        # Check text is not empty
        if not tweet_data['text'].strip():
            logger.error("Empty tweet text is not allowed")
            return False

        # Additional validations could be added here

        return True

    def process_tweet(self, tweet_data: Dict[str, Any]) -> None | dict[str, int | str] | dict[str, int | str]:
        """
        Process a tweet - validate and store in database.

        Args:
            tweet_data: Tweet data to process

        Returns:
            Database ID of the stored tweet or None if processing failed
        """
        try:
            # Validate the tweet
            if not self.validate_tweet(tweet_data):
                logger.error("Tweet validation failed")
                return None

            # Check if tweet already exists
            existing_tweet = self.tweet_repository.get_tweet_by_id(tweet_data['tweet_id'])
            if existing_tweet:
                logger.info(f"Tweet already exists with ID: {existing_tweet.id}")
                return {"id": existing_tweet.id, "status": "existing"}

            # Parse created_at if it's a string
            created_at = tweet_data['created_at']
            if isinstance(created_at, str):
                try:
                    created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                except ValueError:
                    logger.warning(f"Failed to parse created_at: {created_at}, using current time")
                    created_at = datetime.utcnow()

            # Store tweet in database
            db_tweet = self.tweet_repository.create_tweet(
                tweet_id=tweet_data['tweet_id'],
                text=tweet_data['text'],
                created_at=created_at
            )

            if db_tweet:
                logger.info(f"Tweet processed and stored with ID: {db_tweet.id}")
                return {"id": db_tweet.id, "status": "created"}

            return None

        except Exception as e:
            logger.error(f"Error processing tweet: {e}")
            return None
