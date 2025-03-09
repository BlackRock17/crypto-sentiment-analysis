"""
API schemas for Twitter-related API endpoints with blockchain network support.
"""
from typing import Optional, List, Dict, Any, Literal, Union
from datetime import datetime
from pydantic import BaseModel, Field, constr, validator, field_validator

from src.data_collection.twitter.config import CollectionFrequency


# ---- Existing schemas ----

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

    model_config = {
        "from_attributes": True
    }


class ManualTweetCreate(BaseModel):
    """Schema for manually adding a tweet"""
    influencer_username: constr(min_length=1, max_length=50)
    text: constr(min_length=1, max_length=280)
    created_at: Optional[datetime] = None
    tweet_id: Optional[str] = None
    retweet_count: int = 0
    like_count: int = 0

    @field_validator('created_at')
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

    model_config = {
        "from_attributes": True
    }


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


# ---- New schemas for blockchain networks ----

class BlockchainNetworkBase(BaseModel):
    """Base schema for blockchain network data"""
    name: constr(min_length=1, max_length=50)
    display_name: Optional[str] = None
    description: Optional[str] = None
    hashtags: List[str] = []
    keywords: List[str] = []
    icon_url: Optional[str] = None
    is_active: bool = True
    website_url: Optional[str] = None
    explorer_url: Optional[str] = None
    launch_date: Optional[datetime] = None


class BlockchainNetworkCreate(BlockchainNetworkBase):
    """Schema for creating a new blockchain network"""
    pass


class BlockchainNetworkUpdate(BaseModel):
    """Schema for updating a blockchain network"""
    name: Optional[constr(min_length=1, max_length=50)] = None
    display_name: Optional[str] = None
    description: Optional[str] = None
    hashtags: Optional[List[str]] = None
    keywords: Optional[List[str]] = None
    icon_url: Optional[str] = None
    is_active: Optional[bool] = None
    website_url: Optional[str] = None
    explorer_url: Optional[str] = None
    launch_date: Optional[datetime] = None


class BlockchainNetworkResponse(BlockchainNetworkBase):
    """Schema for blockchain network response"""
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = {
        "from_attributes": True
    }


# ---- New schemas for blockchain tokens ----

class BlockchainTokenBase(BaseModel):
    """Base schema for blockchain token data"""
    token_address: constr(min_length=1, max_length=44)
    symbol: constr(min_length=1, max_length=20)
    name: Optional[str] = None
    blockchain_network: Optional[str] = None
    network_confidence: Optional[float] = Field(default=None, ge=0, le=1)


class BlockchainTokenCreate(BlockchainTokenBase):
    """Schema for creating a new blockchain token"""
    pass


class BlockchainTokenUpdate(BaseModel):
    """Schema for updating a blockchain token"""
    symbol: Optional[constr(min_length=1, max_length=20)] = None
    name: Optional[str] = None
    blockchain_network: Optional[str] = None
    blockchain_network_id: Optional[int] = None
    network_confidence: Optional[float] = Field(default=None, ge=0, le=1)
    manually_verified: Optional[bool] = None
    needs_review: Optional[bool] = None


class BlockchainTokenResponse(BlockchainTokenBase):
    """Schema for blockchain token response"""
    id: int
    blockchain_network_id: Optional[int] = None
    manually_verified: bool
    needs_review: bool
    created_at: datetime
    updated_at: Optional[datetime] = None

    # Add network information if available
    network_name: Optional[str] = None
    network_display_name: Optional[str] = None

    model_config = {
        "from_attributes": True
    }


class TokenReviewAction(BaseModel):
    """Schema for token review action"""
    action: Literal["approve_network", "set_network", "merge", "reject"]
    network_id: Optional[int] = None  # Required for set_network action
    merge_with_id: Optional[int] = None  # Required for merge action
    notes: Optional[str] = None  # Optional notes about the decision

    @field_validator('network_id')
    def validate_network_id(cls, v, values):
        if values.data.get('action') == 'set_network' and v is None:
            raise ValueError("network_id is required for set_network action")
        return v

    @field_validator('merge_with_id')
    def validate_merge_with_id(cls, v, values):
        if values.data.get('action') == 'merge' and v is None:
            raise ValueError("merge_with_id is required for merge action")
        return v


class TokenMentionStats(BaseModel):
    """Statistics about token mentions"""
    token_id: int
    token_symbol: str
    token_name: Optional[str] = None
    blockchain_network: Optional[str] = None
    mention_count: int
    sentiment_score: float  # -1 to 1 scale
    first_seen: datetime
    last_seen: datetime
    sentiment_distribution: Dict[str, Dict[str, Union[int, float]]]


class TokenSimilarity(BaseModel):
    """Token similarity information"""
    id: int
    symbol: str
    name: Optional[str] = None
    blockchain_network: Optional[str] = None
    similarity: float


class TokenMergeRequest(BaseModel):
    """Request for merging tokens"""
    primary_token_id: int
    duplicate_token_ids: List[int]


class TokenCategorizationRequest(BaseModel):
    """Request for categorizing a token"""
    network_id: int
    confidence: float = Field(1.0, ge=0, le=1)
    notes: Optional[str] = None


class DuplicateTokenGroup(BaseModel):
    """Group of potential duplicate tokens"""
    symbol: str
    tokens: List[BlockchainTokenResponse]
    total_tokens: int
