"""
Twitter API endpoints for Solana Sentiment Analysis.
"""

import logging
from typing import List, Optional
from datetime import datetime, date

from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks, Path, Body, status
from sqlalchemy.orm import Session

from src.data_processing.database import get_db
from src.data_collection.tasks.twitter_tasks import collect_automated_tweets, add_manual_tweet
from src.security.auth import get_current_superuser, get_current_active_user
from src.data_processing.models.auth import User
from src.data_processing.models.twitter import TwitterInfluencer, TwitterApiUsage
from src.data_collection.twitter.config import (
    twitter_config, CollectionFrequency, get_collection_frequency_hours
)
from src.data_collection.twitter.repository import TwitterRepository
from src.schemas.twitter import (
    InfluencerCreate, InfluencerUpdate, InfluencerResponse,
    ManualTweetCreate, TweetResponse, ApiUsageResponse,
    TwitterSettingsUpdate, TwitterSettingsResponse
)
from src.data_processing.crud.twitter import (
    create_influencer, get_influencer, get_all_influencers,
    update_influencer, delete_influencer, toggle_influencer_automation,
    get_api_usage_stats, get_api_usage_history, get_influencer_by_username
)
from src.exceptions import (
    BadRequestException, NotFoundException, ServerErrorException
)

# Configure logger
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/twitter", tags=["Twitter"])


@router.post("/collect", status_code=202)
async def run_twitter_collection(
        background_tasks: BackgroundTasks,
        current_user: User = Depends(get_current_superuser)
):
    """
    Manually trigger collection of tweets from automated influencers.
    Requires superuser (admin) privileges.

    Args:
        background_tasks: FastAPI background tasks
        current_user: Current authenticated user (must be admin)

    Returns:
        Status message
    """
    logger.info(f"Manual tweet collection triggered by user {current_user.username}")

    # Run collection in background
    background_tasks.add_task(collect_automated_tweets)

    return {
        "status": "accepted",
        "message": "Tweet collection started in background"
    }


@router.get("/status")
async def get_twitter_status(
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Get status of Twitter integration.

    Args:
        current_user: Current authenticated user
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

    # Get automated influencers count
    automated_count = db.query(TwitterInfluencer).filter(
        TwitterInfluencer.is_active == True,
        TwitterInfluencer.is_automated == True
    ).count()

    # Get influencer stats
    total_influencers = db.query(TwitterInfluencer).count()

    # Get timestamp of newest tweet
    newest_tweet_time = newest_tweet.created_at if newest_tweet else None

    # Get collection frequency
    hours = get_collection_frequency_hours(twitter_config.collection_frequency)

    return {
        "twitter_connection": "ok" if connection_ok else "error",
        "stored_tweets": tweet_count,
        "token_mentions": mention_count,
        "newest_tweet": newest_tweet_time,
        "collection_frequency": f"every {hours} hours",
        "influencers": {
            "total": total_influencers,
            "automated": automated_count,
            "max_automated": twitter_config.max_automated_influencers
        }
    }


@router.get("/api-usage", response_model=ApiUsageResponse)
async def get_api_usage(
        date: Optional[date] = None,
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Get Twitter API usage statistics.

    Args:
        date: Date to get usage for (defaults to today)
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        API usage statistics
    """
    stats = get_api_usage_stats(db, date)

    # Calculate remaining requests
    daily_limit = twitter_config.daily_request_limit
    monthly_limit = twitter_config.monthly_request_limit

    remaining_daily = max(0, daily_limit - stats["daily_usage"])
    remaining_monthly = max(0, monthly_limit - stats["monthly_usage"])

    return {
        "date": stats["date"],
        "daily_usage": stats["daily_usage"],
        "monthly_usage": stats["monthly_usage"],
        "monthly_limit": monthly_limit,
        "daily_limit": daily_limit,
        "remaining_daily": remaining_daily,
        "remaining_monthly": remaining_monthly,
        "influencer_usage": stats["influencer_usage"]
    }


@router.get("/api-usage/history")
async def get_api_usage_history_endpoint(
        days: int = Query(30, gt=0, lt=366),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Get historical Twitter API usage data.

    Args:
        days: Number of days to look back
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        Historical API usage data
    """
    return get_api_usage_history(db, days)


@router.get("/settings", response_model=TwitterSettingsResponse)
async def get_twitter_settings(
        current_user: User = Depends(get_current_superuser)
):
    """
    Get Twitter API settings.

    Args:
        current_user: Current authenticated user (must be admin)

    Returns:
        Twitter settings
    """
    return {
        "max_automated_influencers": twitter_config.max_automated_influencers,
        "collection_frequency": twitter_config.collection_frequency.value,
        "collection_interval_hours": get_collection_frequency_hours(twitter_config.collection_frequency),
        "max_tweets_per_user": twitter_config.max_tweets_per_user,
        "monthly_request_limit": twitter_config.monthly_request_limit,
        "daily_request_limit": twitter_config.daily_request_limit
    }


@router.put("/settings", response_model=TwitterSettingsResponse)
async def update_twitter_settings(
        settings: TwitterSettingsUpdate,
        current_user: User = Depends(get_current_superuser)
):
    """
    Update Twitter API settings.

    Args:
        settings: Settings to update
        current_user: Current authenticated user (must be admin)

    Returns:
        Updated Twitter settings
    """
    # Update settings
    if settings.max_automated_influencers is not None:
        twitter_config.max_automated_influencers = settings.max_automated_influencers

    if settings.collection_frequency is not None:
        twitter_config.collection_frequency = settings.collection_frequency

    if settings.max_tweets_per_user is not None:
        twitter_config.max_tweets_per_user = settings.max_tweets_per_user

    if settings.daily_request_limit is not None:
        twitter_config.daily_request_limit = settings.daily_request_limit

    # Return updated settings
    return {
        "max_automated_influencers": twitter_config.max_automated_influencers,
        "collection_frequency": twitter_config.collection_frequency.value,
        "collection_interval_hours": get_collection_frequency_hours(twitter_config.collection_frequency),
        "max_tweets_per_user": twitter_config.max_tweets_per_user,
        "monthly_request_limit": twitter_config.monthly_request_limit,
        "daily_request_limit": twitter_config.daily_request_limit
    }


@router.get("/influencers", response_model=List[InfluencerResponse])
async def get_influencers_list(
        skip: int = 0,
        limit: int = 100,
        active_only: bool = False,
        automated_only: bool = False,
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Get list of Twitter influencers.

    Args:
        skip: Number of records to skip
        limit: Maximum number of records to return
        active_only: Only return active influencers
        automated_only: Only return automated influencers
        current_user: Current authenticated user
        db: Database session

    Returns:
        List of influencers
    """
    return get_all_influencers(db, skip, limit, active_only, automated_only)


@router.post("/influencers", response_model=InfluencerResponse, status_code=status.HTTP_201_CREATED)
async def create_influencer_endpoint(
        influencer: InfluencerCreate,
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Create a new Twitter influencer.

    Args:
        influencer: Influencer data
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        Created influencer
    """
    # Check if influencer with this username already exists
    existing = get_influencer_by_username(db, influencer.username)
    if existing:
        raise BadRequestException(f"Influencer with username '{influencer.username}' already exists")

    return create_influencer(
        db=db,
        username=influencer.username,
        name=influencer.name,
        description=influencer.description,
        follower_count=influencer.follower_count,
        is_active=influencer.is_active,
        is_automated=influencer.is_automated,
        priority=influencer.priority
    )


@router.get("/influencers/{influencer_id}", response_model=InfluencerResponse)
async def get_influencer_endpoint(
        influencer_id: int = Path(..., gt=0),
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Get a Twitter influencer by ID.

    Args:
        influencer_id: Influencer ID
        current_user: Current authenticated user
        db: Database session

    Returns:
        Influencer data
    """
    influencer = get_influencer(db, influencer_id)
    if not influencer:
        raise NotFoundException(f"Influencer with ID {influencer_id} not found")

    return influencer


@router.put("/influencers/{influencer_id}", response_model=InfluencerResponse)
async def update_influencer_endpoint(
        influencer_id: int = Path(..., gt=0),
        influencer_data: InfluencerUpdate = Body(...),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Update a Twitter influencer.

    Args:
        influencer_id: Influencer ID
        influencer_data: Updated influencer data
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        Updated influencer
    """
    # Check if influencer exists
    existing = get_influencer(db, influencer_id)
    if not existing:
        raise NotFoundException(f"Influencer with ID {influencer_id} not found")

    # If username is being updated, check for uniqueness
    if influencer_data.username and influencer_data.username != existing.username:
        username_exists = get_influencer_by_username(db, influencer_data.username)
        if username_exists:
            raise BadRequestException(f"Influencer with username '{influencer_data.username}' already exists")

    # Update influencer
    updated = update_influencer(db, influencer_id, **influencer_data.dict(exclude_unset=True))
    if not updated:
        raise ServerErrorException("Failed to update influencer")

    return updated


@router.delete("/influencers/{influencer_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_influencer_endpoint(
        influencer_id: int = Path(..., gt=0),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Delete a Twitter influencer.

    Args:
        influencer_id: Influencer ID
        current_user: Current authenticated user (must be admin)
        db: Database session
    """
    # Check if influencer exists
    existing = get_influencer(db, influencer_id)
    if not existing:
        raise NotFoundException(f"Influencer with ID {influencer_id} not found")

    # Delete influencer
    success = delete_influencer(db, influencer_id)
    if not success:
        raise ServerErrorException("Failed to delete influencer")

    return None


@router.post("/influencers/{influencer_id}/toggle-automation", response_model=InfluencerResponse)
async def toggle_influencer_automation_endpoint(
        influencer_id: int = Path(..., gt=0),
        current_user: User = Depends(get_current_superuser),
        db: Session = Depends(get_db)
):
    """
    Toggle whether an influencer is automatically tracked.

    Args:
        influencer_id: Influencer ID
        current_user: Current authenticated user (must be admin)
        db: Database session

    Returns:
        Updated influencer
    """
    # Check if influencer exists
    existing = get_influencer(db, influencer_id)
    if not existing:
        raise NotFoundException(f"Influencer with ID {influencer_id} not found")

    # Toggle automation
    updated = toggle_influencer_automation(db, influencer_id)
    if not updated:
        raise ServerErrorException("Failed to toggle influencer automation")

    return updated


@router.post("/manual-tweets", response_model=TweetResponse)
async def add_manual_tweet_endpoint(
        tweet: ManualTweetCreate,
        background_tasks: BackgroundTasks,
        current_user: User = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    """
    Manually add a tweet for analysis.

    Args:
        tweet: Tweet data
        background_tasks: FastAPI background tasks
        current_user: Current authenticated user
        db: Database session

    Returns:
        Status of the operation
    """
    logger.info(f"Manual tweet addition by {current_user.username} for influencer {tweet.influencer_username}")

    # Run in foreground for API response
    from src.data_collection.twitter.service import TwitterCollectionService

    service = TwitterCollectionService(db)
    stored_tweet, mentions_count = service.add_manual_tweet(
        influencer_username=tweet.influencer_username,
        tweet_text=tweet.text,
        created_at=tweet.created_at,
        tweet_id=tweet.tweet_id,
        retweet_count=tweet.retweet_count,
        like_count=tweet.like_count
    )

    if not stored_tweet:
        raise ServerErrorException("Failed to add manual tweet")

    # Get tweet with mentions
    repository = TwitterRepository(db)
    tweet_data = repository.get_tweet_with_mentions(stored_tweet.id)

    if not tweet_data:
        raise ServerErrorException("Failed to retrieve stored tweet")

    return tweet_data
