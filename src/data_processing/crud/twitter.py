"""
CRUD operations for Twitter-related models.
"""
from typing import List, Optional, Dict, Any, Type
from datetime import datetime, date
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, and_, or_

from src.data_processing.models import TwitterInfluencer
from src.data_processing.models.twitter import (
    TwitterInfluencer,
    TwitterInfluencerTweet,
    TwitterApiUsage
)


# --- TwitterInfluencer CRUD operations ---

def create_influencer(
        db: Session,
        username: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        follower_count: int = 0,
        is_active: bool = True,
        is_automated: bool = False,
        priority: int = 0
) -> TwitterInfluencer:
    """
    Create a new Twitter influencer record.

    Args:
        db: Database session
        username: Twitter username
        name: Display name (optional)
        description: Bio/description (optional)
        follower_count: Number of followers
        is_active: Whether the influencer is active
        is_automated: Whether tweets are collected automatically
        priority: Priority level (higher = higher priority)

    Returns:
        Created TwitterInfluencer instance
    """
    influencer = TwitterInfluencer(
        username=username,
        name=name,
        description=description,
        follower_count=follower_count,
        is_active=is_active,
        is_automated=is_automated,
        priority=priority
    )

    db.add(influencer)
    db.commit()
    db.refresh(influencer)

    return influencer


def get_influencer(db: Session, influencer_id: int) -> Optional[TwitterInfluencer]:
    """
    Get a Twitter influencer by ID.

    Args:
        db: Database session
        influencer_id: Influencer ID

    Returns:
        TwitterInfluencer instance or None if not found
    """
    return db.query(TwitterInfluencer).filter(TwitterInfluencer.id == influencer_id).first()


def get_influencer_by_username(db: Session, username: str) -> Optional[TwitterInfluencer]:
    """
    Get a Twitter influencer by username.

    Args:
        db: Database session
        username: Twitter username

    Returns:
        TwitterInfluencer instance or None if not found
    """
    return db.query(TwitterInfluencer).filter(TwitterInfluencer.username == username).first()


def get_all_influencers(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        active_only: bool = False,
        automated_only: bool = False
) -> list[Type[TwitterInfluencer]]:
    """
    Get all Twitter influencers with optional filtering.

    Args:
        db: Database session
        skip: Number of records to skip
        limit: Maximum number of records to return
        active_only: Only return active influencers
        automated_only: Only return automated influencers

    Returns:
        List of TwitterInfluencer instances
    """
    query = db.query(TwitterInfluencer)

    if active_only:
        query = query.filter(TwitterInfluencer.is_active == True)

    if automated_only:
        query = query.filter(TwitterInfluencer.is_automated == True)

    return query.order_by(desc(TwitterInfluencer.priority)).offset(skip).limit(limit).all()


def get_automated_influencers(db: Session, max_count: int = 3) -> list[TwitterInfluencer]:
    """
    Get the top N influencers for automated collection.

    Args:
        db: Database session
        max_count: Maximum number of influencers to return

    Returns:
        List of TwitterInfluencer instances
    """
    return db.query(TwitterInfluencer).filter(
        TwitterInfluencer.is_active == True,
        TwitterInfluencer.is_automated == True
    ).order_by(desc(TwitterInfluencer.priority)).limit(max_count).all()


def update_influencer(
        db: Session,
        influencer_id: int,
        **kwargs
) -> Optional[TwitterInfluencer]:
    """
    Update a Twitter influencer record.

    Args:
        db: Database session
        influencer_id: ID of the influencer to update
        **kwargs: Fields to update

    Returns:
        Updated TwitterInfluencer instance or None if not found
    """
    influencer = get_influencer(db, influencer_id)
    if not influencer:
        return None

    # Update provided fields
    for key, value in kwargs.items():
        if hasattr(influencer, key):
            setattr(influencer, key, value)

    db.commit()
    db.refresh(influencer)

    return influencer


def delete_influencer(db: Session, influencer_id: int) -> bool:
    """
    Delete a Twitter influencer.

    Args:
        db: Database session
        influencer_id: ID of the influencer to delete

    Returns:
        True if successful, False if influencer not found
    """
    influencer = get_influencer(db, influencer_id)
    if not influencer:
        return False

    db.delete(influencer)
    db.commit()

    return True


def toggle_influencer_automation(db: Session, influencer_id: int) -> Optional[TwitterInfluencer]:
    """
    Toggle the automated status of an influencer.

    Args:
        db: Database session
        influencer_id: ID of the influencer to update

    Returns:
        Updated TwitterInfluencer instance or None if not found
    """
    influencer = get_influencer(db, influencer_id)
    if not influencer:
        return None

    influencer.is_automated = not influencer.is_automated
    db.commit()
    db.refresh(influencer)

    return influencer


# --- TwitterApiUsage CRUD operations ---

def create_api_usage(
        db: Session,
        influencer_id: int,
        endpoint: str,
        requests_used: int = 1,
        date: Optional[datetime] = None,
        reset_time: Optional[datetime] = None
) -> TwitterApiUsage:
    """
    Create a new API usage record.

    Args:
        db: Database session
        influencer_id: ID of the related influencer
        endpoint: Twitter API endpoint used
        requests_used: Number of requests used
        date: Date of usage (defaults to now)
        reset_time: When the rate limit resets

    Returns:
        Created TwitterApiUsage instance
    """
    usage = TwitterApiUsage(
        date=date or datetime.utcnow(),
        influencer_id=influencer_id,
        endpoint=endpoint,
        requests_used=requests_used,
        reset_time=reset_time
    )

    db.add(usage)
    db.commit()
    db.refresh(usage)

    return usage


def get_api_usage_stats(db: Session, day: Optional[date] = None) -> Dict[str, Any]:
    """
    Get API usage statistics for a specific day.

    Args:
        db: Database session
        day: Date to get statistics for (defaults to today)

    Returns:
        Dictionary with usage statistics
    """
    if day is None:
        day = datetime.utcnow().date()

    # Calculate daily usage
    daily_usage = TwitterApiUsage.get_daily_usage(db, day)

    # Calculate monthly usage
    month = day.month
    year = day.year
    monthly_usage = TwitterApiUsage.get_monthly_usage(db, year, month)

    # Get usage by influencer
    influencer_usage = db.query(
        TwitterInfluencer.username,
        func.sum(TwitterApiUsage.requests_used).label("total_requests")
    ).join(
        TwitterApiUsage, TwitterApiUsage.influencer_id == TwitterInfluencer.id
    ).filter(
        func.date(TwitterApiUsage.date) == day
    ).group_by(
        TwitterInfluencer.username
    ).all()

    return {
        "date": day.isoformat(),
        "daily_usage": daily_usage,
        "monthly_usage": monthly_usage,
        "influencer_usage": [
            {"username": username, "requests": total} for username, total in influencer_usage
        ]
    }


def get_api_usage_history(
        db: Session,
        days: int = 30,
        skip: int = 0,
        limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Get historical API usage data.

    Args:
        db: Database session
        days: Number of days to look back
        skip: Number of records to skip
        limit: Maximum number of records to return

    Returns:
        List of daily usage data
    """
    from datetime import timedelta

    today = datetime.utcnow().date()
    start_date = today - timedelta(days=days)

    # Get daily usage totals
    daily_totals = db.query(
        func.date(TwitterApiUsage.date).label("day"),
        func.sum(TwitterApiUsage.requests_used).label("total_requests")
    ).filter(
        func.date(TwitterApiUsage.date) >= start_date
    ).group_by(
        "day"
    ).order_by(
        desc("day")
    ).offset(skip).limit(limit).all()

    return [
        {"date": day.isoformat(), "total_requests": total}
        for day, total in daily_totals
    ]


# --- TwitterInfluencerTweet CRUD operations ---

def create_influencer_tweet(
        db: Session,
        influencer_id: int,
        tweet_id: int,
        is_manually_added: bool = False
) -> TwitterInfluencerTweet:
    """
    Create a link between an influencer and a tweet.

    Args:
        db: Database session
        influencer_id: ID of the influencer
        tweet_id: ID of the tweet
        is_manually_added: Whether the tweet was manually added

    Returns:
        Created TwitterInfluencerTweet instance
    """
    influencer_tweet = TwitterInfluencerTweet(
        influencer_id=influencer_id,
        tweet_id=tweet_id,
        is_manually_added=is_manually_added
    )

    db.add(influencer_tweet)
    db.commit()
    db.refresh(influencer_tweet)

    return influencer_tweet


def get_influencer_tweets(
        db: Session,
        influencer_id: int,
        skip: int = 0,
        limit: int = 100,
        manually_added_only: bool = False
) -> list[Type[TwitterInfluencerTweet]]:
    """
    Get tweets from a specific influencer.

    Args:
        db: Database session
        influencer_id: ID of the influencer
        skip: Number of records to skip
        limit: Maximum number of records to return
        manually_added_only: Only return manually added tweets

    Returns:
        List of TwitterInfluencerTweet instances
    """
    query = db.query(TwitterInfluencerTweet).filter(
        TwitterInfluencerTweet.influencer_id == influencer_id
    )

    if manually_added_only:
        query = query.filter(TwitterInfluencerTweet.is_manually_added == True)

    return query.order_by(desc(TwitterInfluencerTweet.created_at)).offset(skip).limit(limit).all()
