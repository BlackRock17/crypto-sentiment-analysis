"""
Configuration module for Twitter API integration.
Loads Twitter API credentials and settings from environment variables.
"""
from typing import List, Optional
from pydantic import BaseSettings, Field, field_validator

from config.settings import (
    TWITTER_API_KEY,
    TWITTER_API_SECRET,
    TWITTER_ACCESS_TOKEN,
    TWITTER_ACCESS_TOKEN_SECRET,
    SOLANA_HASHTAGS
)


class TwitterConfig(BaseSettings):
    """Twitter API configuration settings."""

    api_key: str = Field(default=TWITTER_API_KEY, env="TWITTER_API_KEY")
    api_secret: str = Field(default=TWITTER_API_SECRET, env="TWITTER_API_SECRET")
    access_token: str = Field(default=TWITTER_ACCESS_TOKEN, env="TWITTER_ACCESS_TOKEN")
    access_token_secret: str = Field(default=TWITTER_ACCESS_TOKEN_SECRET, env="TWITTER_ACCESS_TOKEN_SECRET")

    # Search settings
    search_hashtags: List[str] = SOLANA_HASHTAGS
    search_languages: List[str] = ["en"]  # Default to English tweets
    max_tweets_per_query: int = 100

    # Rate limiting settings
    rate_limit_window: int = 15  # minutes
    search_rate_limit: int = 450  # requests per 15-min window

    # Retry settings
    max_retries: int = 3
    retry_delay: int = 5  # seconds between retries

    @field_validator("api_key", "api_secret", "access_token", "access_token_secret")
    def check_credentials(cls, v, values, **kwargs):
        """Validate that Twitter API credentials are provided."""
        if not v:
            field_name = kwargs.get("field").name
            raise ValueError(f"Twitter {field_name} is required")
        return v

    class Config:
        env_file = ".env"
        case_sensitive = False


# Create a global configuration instance
twitter_config = TwitterConfig()