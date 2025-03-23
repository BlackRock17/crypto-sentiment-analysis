from typing import List, Optional
import os
from enum import Enum

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator

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

    # API credentials - default to empty strings for testing
    api_key: str = ""
    api_secret: str = ""
    access_token: str = ""
    access_token_secret: str = ""

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

    # Check if we're in test mode
    is_test_mode: bool = False

    @field_validator("max_automated_influencers")
    def check_max_influencers(cls, v):
        """Validate maximum number of automated influencers"""
        if v < 0:
            raise ValueError("Maximum number of automated influencers cannot be negative")
        return v

    # Define configuration
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore"  # Ignore extra values to avoid the "Extra inputs are not permitted" error
    )

    def __init__(self, **data):
        if "is_test_mode" not in data:
            data["is_test_mode"] = False

        # Get values from environment variables if they exist
        if TWITTER_API_KEY and TWITTER_API_KEY != "your_api_key":
            data["api_key"] = TWITTER_API_KEY
        if TWITTER_API_SECRET and TWITTER_API_SECRET != "your_api_secret":
            data["api_secret"] = TWITTER_API_SECRET
        if TWITTER_ACCESS_TOKEN and TWITTER_ACCESS_TOKEN != "your_access_token":
            data["access_token"] = TWITTER_ACCESS_TOKEN
        if TWITTER_ACCESS_TOKEN_SECRET and TWITTER_ACCESS_TOKEN_SECRET != "your_access_token_secret":
            data["access_token_secret"] = TWITTER_ACCESS_TOKEN_SECRET

        # Set test mode if environment variable is set
        if os.environ.get("TESTING") == "true":
            data["is_test_mode"] = True

        super().__init__(**data)


# Create a global configuration instance
try:
    twitter_config = TwitterConfig()
except Exception as e:
    # Fallback to a minimal configuration for testing
    print(f"Warning: Could not initialize TwitterConfig with provided settings: {e}")
    twitter_config = TwitterConfig(is_test_mode=True)


def validate_twitter_credentials() -> bool:
    """
    Validate that all required Twitter API credentials are set.

    Returns:
        bool: True if all credentials are set, False otherwise
    """
    # Skip validation in test mode
    if twitter_config.is_test_mode:
        return True

    # Check if credentials are set and not placeholders
    return (
            twitter_config.api_key and
            twitter_config.api_secret and
            twitter_config.access_token and
            twitter_config.access_token_secret and
            twitter_config.api_key != "your_api_key" and
            twitter_config.api_secret != "your_api_secret" and
            twitter_config.access_token != "your_access_token" and
            twitter_config.access_token_secret != "your_access_token_secret"
    )


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
