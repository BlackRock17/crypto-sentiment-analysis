"""
Twitter API endpoints for Solana Sentiment Analysis.
"""

import logging
from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import Dict, Any, List

from src.data_processing.database import get_db
from src.data_collection.tasks.twitter_tasks import collect_influencer_tweets
from src.security.auth import get_current_superuser
from src.data_processing.models.auth import User

# Configure logger
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/twitter", tags=["Twitter"])


@router.post("/collect", status_code=202)
async def run_twitter_collection(
        background_tasks: BackgroundTasks,
        limit_per_user: int = Query(10, gt=0, le=100, description="Maximum tweets to collect per influencer"),
        current_user: User = Depends(get_current_superuser)
):
    """
    Manually trigger collection of tweets from configured influencers.
    Requires superuser (admin) privileges.

    Args:
        background_tasks: FastAPI background tasks
        limit_per_user: Maximum tweets to collect per influencer
        current_user: Current authenticated user (must be admin)

    Returns:
        Status message
    """
    logger.info(f"Manual tweet collection triggered by user {current_user.username}")

    # Run collection in background
    background_tasks.add_task(collect_influencer_tweets, limit_per_user)

    return {
        "status": "accepted",
        "message": "Tweet collection started in background",
        "limit_per_user": limit_per_user
    }


@router.get("/status")
async def get_twitter_status(
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Get status of Twitter integration.
    Requires superuser (admin) privileges.

    Args:
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        Status information
    """
    from src.data_collection.twitter.client import TwitterAPIClient
    from src.data_processing.models.database import Tweet, TokenMention

    # Create client to test connection
    client = TwitterAPIClient()
    connection_ok = client.test_connection()

    # Get statistics from database
    tweet_count = db.query(Tweet).count()
    mention_count = db.query(TokenMention).count()
    newest_tweet = db.query(Tweet).order_by(Tweet.created_at.desc()).first()

    # Get timestamp of newest tweet
    newest_tweet_time = newest_tweet.created_at if newest_tweet else None

    return {
        "twitter_connection": "ok" if connection_ok else "error",
        "stored_tweets": tweet_count,
        "token_mentions": mention_count,
        "newest_tweet": newest_tweet_time,
        "collection_frequency": "every 30 minutes"
    }
