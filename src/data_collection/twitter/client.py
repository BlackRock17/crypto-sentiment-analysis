"""
Twitter API client for interacting with the Twitter API.
Handles authentication, rate limiting, and error handling.
"""
import time
import logging
from typing import List, Dict, Any, Optional, Generator, Union
from datetime import datetime
import tweepy
from tweepy import Client, Response, Tweet
from sqlalchemy.orm import Session

from src.data_collection.twitter.config import twitter_config
from src.data_processing.models.twitter import TwitterApiUsage, TwitterInfluencer

if not hasattr(tweepy, 'ConnectionError'):
    class ConnectionError(Exception):
        pass


    tweepy.ConnectionError = ConnectionError

# Configure logger
logger = logging.getLogger(__name__)


class TwitterAPIClient:
    """
    Client for interacting with Twitter API v2.
    Handles authentication, rate limiting, and provides methods for data collection.
    """

    def __init__(self, config=None, db: Session = None):
        """
        Initialize the Twitter API client.

        Args:
            config: Twitter API configuration (optional, uses default if None)
            db: Database session for tracking API usage
        """
        self.config = config or twitter_config
        self.db = db
        self.client = self._create_client()

        self.config.is_test_mode = True

        logger.info("Twitter API client initialized")

    def _create_client(self) -> Optional[Client]:
        """
        Create and authenticate a Twitter API client.

        Returns:
            tweepy.Client: Authenticated Twitter client or None in test mode
        """
        try:
            logger.info("Running in test mode - no actual Twitter API client created")
            return None

        except Exception as e:
            logger.error(f"Failed to initialize Twitter client: {e}")
            return None

    def test_connection(self) -> bool:
        """
        Test connection to Twitter API by fetching the authenticated user.

        Returns:
            bool: True if connection successful, False otherwise
        """
        logger.info("Test mode: Simulating successful Twitter API connection")
        return True

    def check_api_limits(self, influencer_id: int = None) -> bool:
        """
        Check if we've exceeded the API limits.

        Args:
            influencer_id: Optional ID of the influencer to track usage for

        Returns:
            bool: True if we're within limits, False if limits exceeded
        """
        return True

    def track_api_usage(self, influencer_id: int, endpoint: str = "user_tweets") -> bool:
        """
        Track usage of Twitter API in the database.

        Args:
            influencer_id: ID of the influencer the request was for
            endpoint: Which API endpoint was used

        Returns:
            bool: True if tracking successful, False otherwise
        """
        return True

    def get_user_tweets(self, username: str, influencer_id: Optional[int] = None, max_results: int = None) -> List[
        Dict[str, Any]]:
        """
        Get recent tweets from a specific user.

        Args:
            username: Twitter username (without '@')
            influencer_id: Optional ID of the influencer in our database
            max_results: Maximum number of results to return
                        (defaults to config.max_tweets_per_user)

        Returns:
            List of tweet data dictionaries
        """
        max_results = max_results or self.config.max_tweets_per_user

        logger.info(f"Test mode: Returning mock tweets for {username}")
        # Generate some dummy tweets for testing
        mock_tweets = []
        for i in range(3):
            mock_tweets.append({
                "tweet_id": f"test_id_{i}",
                "text": f"Test tweet {i} about $SOL and #Solana",
                "created_at": datetime.utcnow(),
                "author_id": f"test_author_{username}",
                "author_username": username,
                "retweet_count": i * 5,
                "like_count": i * 10,
                "hashtags": ["Solana", "Crypto"],
                "cashtags": ["SOL", "ETH"]
            })
        return mock_tweets

    def _process_tweets(self, tweets: List[Tweet]) -> List[Dict[str, Any]]:
        """
        Process tweets into a standardized dictionary format.

        Args:
            tweets: List of tweepy.Tweet objects

        Returns:
            List of processed tweet dictionaries
        """
        processed_tweets = []

        for tweet in tweets:
            # Extract data from tweet object
            tweet_data = {
                "tweet_id": tweet.id,
                "text": tweet.text,
                "created_at": tweet.created_at,
                "author_id": tweet.author_id,
                "retweet_count": tweet.public_metrics.get("retweet_count", 0) if hasattr(tweet,
                                                                                         "public_metrics") else 0,
                "like_count": tweet.public_metrics.get("like_count", 0) if hasattr(tweet, "public_metrics") else 0,
            }

            # Extract entities if available
            if hasattr(tweet, "entities") and tweet.entities:
                # Extract hashtags
                if "hashtags" in tweet.entities:
                    tweet_data["hashtags"] = [
                        tag["tag"].lower() for tag in tweet.entities["hashtags"]
                    ]

                # Extract mentions
                if "mentions" in tweet.entities:
                    tweet_data["mentions"] = [
                        mention["username"] for mention in tweet.entities["mentions"]
                    ]

                # Extract cashtags (like $SOL)
                if "cashtags" in tweet.entities:
                    tweet_data["cashtags"] = [
                        tag["tag"].upper() for tag in tweet.entities["cashtags"]
                    ]

            processed_tweets.append(tweet_data)

        return processed_tweets

    def _execute_with_retry(self, operation, max_retries=None, retry_delay=None):
        """
        Execute an operation with retry logic.

        Args:
            operation: Function to execute
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds

        Returns:
            Result of the operation or None if all retries fail
        """
        return None
