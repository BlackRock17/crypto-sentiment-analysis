"""
Tasks for Twitter data collection.
Defines tasks that can be run on a schedule or on-demand.
"""

import logging
import asyncio
from sqlalchemy.orm import Session

from src.data_processing.database import get_db
from src.data_collection.twitter.service import TwitterCollectionService
from src.data_collection.twitter.config import validate_twitter_credentials

# Configure logger
logger = logging.getLogger(__name__)


async def _async_collect_automated_tweets() -> bool:
    """
    Asynchronously collect tweets from configured automated influencers.

    Returns:
        True if collection was successful, False otherwise
    """
    # This runs in a thread via ThreadPoolExecutor, so we can use blocking calls
    try:
        # Get database session
        db = next(get_db())

        # Create service
        service = TwitterCollectionService(db)

        # Test connection
        if not service.test_twitter_connection():
            logger.error("Failed to connect to Twitter API")
            return False

        # Collect and store tweets
        tweets_stored, mentions_found = service.collect_and_store_automated_tweets()

        # Log results
        logger.info(
            f"Automated collection task completed: {tweets_stored} tweets stored, {mentions_found} token mentions found"
        )

        return True

    except Exception as e:
        logger.error(f"Error in automated collection task: {e}")
        return False
    finally:
        # Close database session
        if 'db' in locals():
            db.close()


def collect_automated_tweets() -> bool:
    """
    Collect tweets from automated influencers.
    Acts as a sync wrapper around the async function for compatibility with the scheduler.

    Returns:
        True if collection was successful, False otherwise
    """
    # Validate Twitter credentials
    if not validate_twitter_credentials():
        logger.error("Twitter credentials are missing or invalid")
        return False

    # Create a new event loop for this task
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(_async_collect_automated_tweets())
    finally:
        loop.close()


async def _async_add_manual_tweet(
        influencer_username: str,
        tweet_text: str,
        **kwargs
) -> bool:
    """
    Asynchronously add a manual tweet for an influencer.

    Args:
        influencer_username: Twitter username of the influencer
        tweet_text: Text content of the tweet
        **kwargs: Additional tweet parameters

    Returns:
        True if successful, False otherwise
    """
    try:
        # Get database session
        db = next(get_db())

        # Create service
        service = TwitterCollectionService(db)

        # Add manual tweet
        stored_tweet, mentions_count = service.add_manual_tweet(
            influencer_username=influencer_username,
            tweet_text=tweet_text,
            **kwargs
        )

        if not stored_tweet:
            logger.error(f"Failed to add manual tweet for {influencer_username}")
            return False

        logger.info(f"Manual tweet added with {mentions_count} token mentions")
        return True

    except Exception as e:
        logger.error(f"Error adding manual tweet: {e}")
        return False
    finally:
        # Close database session
        if 'db' in locals():
            db.close()


def add_manual_tweet(
        influencer_username: str,
        tweet_text: str,
        **kwargs
) -> bool:
    """
    Add a manual tweet for an influencer.

    Args:
        influencer_username: Twitter username of the influencer
        tweet_text: Text content of the tweet
        **kwargs: Additional tweet parameters

    Returns:
        True if successful, False otherwise
    """
    # Create a new event loop for this task
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(_async_add_manual_tweet(
            influencer_username=influencer_username,
            tweet_text=tweet_text,
            **kwargs
        ))
    finally:
        loop.close()
