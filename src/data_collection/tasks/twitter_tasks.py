"""
Tasks for Twitter data collection.
Defines tasks that can be run on a schedule or on-demand.
"""

import logging
from sqlalchemy.orm import Session

from src.data_processing.database import get_db
from src.data_collection.twitter.service import TwitterCollectionService
from src.data_collection.twitter.config import validate_twitter_credentials

# Configure logger
logger = logging.getLogger(__name__)


def collect_influencer_tweets(limit_per_user: int = 10) -> bool:
    """
    Collect tweets from configured crypto influencers.

    Args:
        limit_per_user: Maximum number of tweets to collect per influencer

    Returns:
        True if collection was successful, False otherwise
    """
    # Validate Twitter credentials
    if not validate_twitter_credentials():
        logger.error("Twitter credentials are missing or invalid")
        return False

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
        tweets_stored, mentions_found = service.collect_and_store_influencer_tweets(limit_per_user=limit_per_user)

        # Log results
        logger.info(
            f"Influencer collection task completed: {tweets_stored} tweets stored, {mentions_found} token mentions found")

        return True

    except Exception as e:
        logger.error(f"Error in influencer collection task: {e}")
        return False
    finally:
        # Close database session
        if 'db' in locals():
            db.close()
