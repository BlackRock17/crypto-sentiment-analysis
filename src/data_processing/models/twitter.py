"""
Twitter-related database models.
"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Text, Float
from sqlalchemy.orm import relationship

from src.data_processing.models.database import Base


class TwitterInfluencer(Base):
    """
    Model for tracking Twitter influencers.
    """
    __tablename__ = "twitter_influencers"

    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(100))
    description = Column(Text)
    follower_count = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)
    is_automated = Column(Boolean, default=False)  # Whether tweets are collected automatically
    priority = Column(Integer, default=0)  # Higher number = higher priority
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    tweets = relationship("TwitterInfluencerTweet", back_populates="influencer", cascade="all, delete-orphan")
    api_usages = relationship("TwitterApiUsage", back_populates="influencer", cascade="all, delete-orphan")


class TwitterInfluencerTweet(Base):
    """
    Model for tracking which tweets belong to which influencer.
    Links the Tweet model with the TwitterInfluencer model.
    """
    __tablename__ = "twitter_influencer_tweets"

    id = Column(Integer, primary_key=True)
    influencer_id = Column(Integer, ForeignKey("twitter_influencers.id", ondelete="CASCADE"), nullable=False)
    tweet_id = Column(Integer, ForeignKey("tweets.id", ondelete="CASCADE"), nullable=False)
    is_manually_added = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    influencer = relationship("TwitterInfluencer", back_populates="tweets")
    tweet = relationship("Tweet")


class TwitterApiUsage(Base):
    """
    Model for tracking Twitter API usage.
    """
    __tablename__ = "twitter_api_usage"

    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False, index=True)
    influencer_id = Column(Integer, ForeignKey("twitter_influencers.id", ondelete="CASCADE"), nullable=False)
    endpoint = Column(String(50), nullable=False)  # Which Twitter API endpoint was used
    requests_used = Column(Integer, default=1)
    reset_time = Column(DateTime)  # When the rate limit resets
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    influencer = relationship("TwitterInfluencer", back_populates="api_usages")

    @classmethod
    def get_daily_usage(cls, db, date=None):
        """Get the total API usage for a specific day"""
        if date is None:
            date = datetime.utcnow().date()

        from sqlalchemy import func
        return db.query(func.sum(cls.requests_used)).filter(
            func.date(cls.date) == date
        ).scalar() or 0

    @classmethod
    def get_monthly_usage(cls, db, year=None, month=None):
        """Get the total API usage for a specific month"""
        if year is None or month is None:
            now = datetime.utcnow()
            year = now.year
            month = now.month

        from sqlalchemy import func, extract
        return db.query(func.sum(cls.requests_used)).filter(
            extract('year', cls.date) == year,
            extract('month', cls.date) == month
        ).scalar() or 0
