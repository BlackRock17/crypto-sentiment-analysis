"""
Configuration module for Twitter API integration.
Loads Twitter API credentials and settings from environment variables.
"""
from typing import List, Optional
from pydantic import BaseSettings, Field, validator

from config.settings import (
    TWITTER_API_KEY,
    TWITTER_API_SECRET,
    TWITTER_ACCESS_TOKEN,
    TWITTER_ACCESS_TOKEN_SECRET,
    SOLANA_HASHTAGS
)