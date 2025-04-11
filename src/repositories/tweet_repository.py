"""
Repository layer for tweet data access.
"""
import logging
from typing import Optional
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError

from src.data_processing.database import get_db_context
from src.data_processing.models.database import Tweet
from src.data_processing.crud.create import create_tweet as crud_create_tweet
from src.data_processing.crud.read import get_tweet_by_twitter_id

# Set up logging
logger = logging.getLogger(__name__)


class TweetRepository:
    """Repository for tweet data access operations."""

    def create_tweet(self, tweet_id: str, text: str, created_at: datetime) -> Optional[Tweet]:
        """
        Create a tweet in the database.

        Args:
            tweet_id: Twitter ID of the tweet
            text: Text content of the tweet
            created_at: Creation timestamp

        Returns:
            Created Tweet object or None if creation failed
        """
        try:
            with get_db_context() as db:
                db_tweet = crud_create_tweet(
                    db=db,
                    tweet_id=tweet_id,
                    text=text,
                    created_at=created_at
                )
                return db_tweet
        except SQLAlchemyError as e:
            logger.error(f"Database error creating tweet: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error creating tweet: {e}")
            return None

    def get_tweet_by_id(self, tweet_id: str) -> Optional[Tweet]:
        """
        Get a tweet by its Twitter ID.

        Args:
            tweet_id: Twitter ID of the tweet

        Returns:
            Tweet object or None if not found
        """
        try:
            with get_db_context() as db:
                return get_tweet_by_twitter_id(db, tweet_id)
        except Exception as e:
            logger.error(f"Error getting tweet by ID: {e}")
            return None
