"""
Configuration module for Twitter API integration.
Loads Twitter API credentials and settings from environment variables.
"""
from typing import List, Optional
from pydantic import BaseSettings, Field, validator
from enum import Enum

from config.settings import (
    TWITTER_API_KEY,
    TWITTER_API_SECRET,
    TWITTER_ACCESS_TOKEN,
    TWITTER_ACCESS_TOKEN_SECRET
)

DEFAULT_CRYPTO_INFLUENCERS = [
    "SBF_FTX",  # Sam Bankman-Fried
    "cz_binance",  # Changpeng Zhao (Binance)
    "VitalikButerin",  # Vitalik Buterin (Ethereum)
]


class CollectionFrequency(str, Enum):
    """Collection frequency options"""
    DAILY = "daily"
    HOURLY_12 = "12_hours"
    HOURLY_6 = "6_hours"
    HOURLY_3 = "3_hours"
    HOURLY_1 = "hourly"


class TwitterConfig(BaseSettings):
    """Twitter API configuration settings."""

    api_key: str = Field(default=TWITTER_API_KEY, env="TWITTER_API_KEY")
    api_secret: str = Field(default=TWITTER_API_SECRET, env="TWITTER_API_SECRET")
    access_token: str = Field(default=TWITTER_ACCESS_TOKEN, env="TWITTER_ACCESS_TOKEN")
    access_token_secret: str = Field(default=TWITTER_ACCESS_TOKEN_SECRET, env="TWITTER_ACCESS_TOKEN_SECRET")

    # API Usage Limits
    monthly_request_limit: int = 100  # Free tier limit
    daily_request_limit: int = 10  # Self-imposed daily limit

    # Accounts to follow
    max_automated_influencers: int = 3  # Maximum number of influencers to track automatically
    influencer_accounts: List[str] = DEFAULT_CRYPTO_INFLUENCERS

    # Collection frequency
    collection_frequency: CollectionFrequency = CollectionFrequency.DAILY

    # Tweet retrieval settings
    search_languages: List[str] = ["en"]  # Default to English tweets
    max_tweets_per_user: int = 10  # Maximum number of tweets from one user

    # Rate limiting settings
    rate_limit_window: int = 15  # minutes
    search_rate_limit: int = 450  # requests per 15-min window

    # Retry settings
    max_retries: int = 3
    retry_delay: int = 5  # seconds between retries

    @validator("api_key", "api_secret", "access_token", "access_token_secret")
    def check_credentials(cls, v, values, **kwargs):
        """Validate that Twitter API credentials are provided."""
        if not v:
            field_name = kwargs.get("field").name
            raise ValueError(f"Twitter {field_name} is required")
        return v

    @validator("max_automated_influencers")
    def check_max_influencers(cls, v):
        """Validate maximum number of automated influencers"""
        if v < 0:
            raise ValueError("Maximum number of automated influencers cannot be negative")
        return v

    class Config:
        env_file = ".env"
        case_sensitive = False


# Create a global configuration instance
twitter_config = TwitterConfig()


def validate_twitter_credentials() -> bool:
    """
    Validate that all required Twitter API credentials are set.

    Returns:
        bool: True if all credentials are set, False otherwise
    """
    try:
        # This will raise an exception if any validation fails
        TwitterConfig()
        return True
    except ValueError:
        return False


def get_collection_frequency_hours(frequency: CollectionFrequency) -> int:
    """
    Get collection interval in hours based on frequency setting.

    Args:
        frequency: Collection frequency enum value

    Returns:
        int: Hours between collections
    """
    frequencies = {
        CollectionFrequency.DAILY: 24,
        CollectionFrequency.HOURLY_12: 12,
        CollectionFrequency.HOURLY_6: 6,
        CollectionFrequency.HOURLY_3: 3,
        CollectionFrequency.HOURLY_1: 1,
    }
    return frequencies.get(frequency, 24)
