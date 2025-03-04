"""
Pydantic models for Twitter-related API endpoints.
"""
from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, constr, validator

from src.data_collection.twitter.config import CollectionFrequency


class InfluencerBase(BaseModel):
    """Base schema for Twitter influencer data"""
    username: constr(min_length=1, max_length=50)
    name: Optional[str] = None
    description: Optional[str] = None
    follower_count: Optional[int] = 0
    is_active: bool = True
    is_automated: bool = False
    priority: int = Field(default=0, ge=0, le=100)


class InfluencerCreate(InfluencerBase):
    """Schema for creating a new influencer"""
    pass


class InfluencerUpdate(BaseModel):
    """Schema for updating an influencer"""
    username: Optional[constr(min_length=1, max_length=50)] = None
    name: Optional[str] = None
    description: Optional[str] = None
    follower_count: Optional[int] = None
    is_active: Optional[bool] = None
    is_automated: Optional[bool] = None
    priority: Optional[int] = Field(default=None, ge=0, le=100)


class InfluencerResponse(InfluencerBase):
    """Schema for influencer response"""
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ManualTweetCreate(BaseModel):
    """Schema for manually adding a tweet"""
    influencer_username: constr(min_length=1, max_length=50)
    text: constr(min_length=1, max_length=280)
    created_at: Optional[datetime] = None
    tweet_id: Optional[str] = None
    retweet_count: int = 0
    like_count: int = 0

    @validator('created_at')
    def validate_created_at(cls, v):
        if v and v > datetime.utcnow():
            raise ValueError("Created at date cannot be in the future")
        return v


class TweetResponse(BaseModel):
    """Schema for tweet response"""
    id: int
    tweet_id: str
    text: str
    created_at: datetime
    author_username: str
    retweet_count: int
    like_count: int
    token_mentions: List[str] = []
    sentiment: Optional[str] = None

    class Config:
        from_attributes = True


class ApiUsageResponse(BaseModel):
    """Schema for API usage statistics"""
    date: str
    daily_usage: int
    monthly_usage: int
    monthly_limit: int
    daily_limit: int
    remaining_daily: int
    remaining_monthly: int
    influencer_usage: List[Dict[str, Any]] = []


class TwitterSettingsUpdate(BaseModel):
    """Schema for updating Twitter settings"""
    max_automated_influencers: Optional[int] = Field(default=None, ge=1, le=10)
    collection_frequency: Optional[CollectionFrequency] = None
    max_tweets_per_user: Optional[int] = Field(default=None, ge=1, le=100)
    daily_request_limit: Optional[int] = Field(default=None, ge=1)


class TwitterSettingsResponse(BaseModel):
    """Schema for Twitter settings response"""
    max_automated_influencers: int
    collection_frequency: str
    collection_interval_hours: int
    max_tweets_per_user: int
    monthly_request_limit: int
    daily_request_limit: int
