"""
Twitter API integration module for Solana Sentiment Analysis.
"""

from src.data_collection.twitter.client import TwitterAPIClient
from src.data_collection.twitter.config import twitter_config, validate_twitter_credentials

__all__ = ['TwitterAPIClient', 'twitter_config', 'validate_twitter_credentials']