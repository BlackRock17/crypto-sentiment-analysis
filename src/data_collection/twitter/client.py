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
        logger.info("Twitter API client initialized")

    def _create_client(self) -> Optional[Client]:
        """
        Create and authenticate a Twitter API client.

        Returns:
            tweepy.Client: Authenticated Twitter client or None in test mode
        """
        try:
            # If in test mode, return a mock client or None
            if self.config.is_test_mode:
                logger.info("Running in test mode - no actual Twitter API client created")
                return None

            # Initialize real client with credentials
            client = tweepy.Client(
                consumer_key=self.config.api_key,
                consumer_secret=self.config.api_secret,
                access_token=self.config.access_token,
                access_token_secret=self.config.access_token_secret,
                wait_on_rate_limit=True  # Auto-wait when rate limited
            )
            return client
        except Exception as e:
            logger.error(f"Failed to initialize Twitter client: {e}")
            if self.config.is_test_mode:
                return None
            raise

    def test_connection(self) -> bool:
        """
        Test connection to Twitter API by fetching the authenticated user.

        Returns:
            bool: True if connection successful, False otherwise
        """
        # In test mode, return True to allow tests to proceed
        if self.config.is_test_mode:
            logger.info("Test mode: Simulating successful Twitter API connection")
            return True

        try:
            # Try to get user information to test connection
            if not self.client:
                return False

            me = self.client.get_me()
            logger.info(f"Twitter API connection successful. User ID: {me.data.id}")
            return True
        except Exception as e:
            logger.error(f"Twitter API connection test failed: {e}")
            return False

    def check_api_limits(self, influencer_id: int = None) -> bool:
        """
        Check if we've exceeded the API limits.

        Args:
            influencer_id: Optional ID of the influencer to track usage for

        Returns:
            bool: True if we're within limits, False if limits exceeded
        """
        # In test mode, always return True
        if self.config.is_test_mode:
            return True

        if not self.db:
            logger.warning("Database session not provided, can't check API limits")
            return True

        # Get today's usage
        today = datetime.utcnow().date()
        today_usage = TwitterApiUsage.get_daily_usage(self.db, today)

        # Get current month's usage
        now = datetime.utcnow()
        monthly_usage = TwitterApiUsage.get_monthly_usage(self.db, now.year, now.month)

        # Check against limits
        if monthly_usage >= self.config.monthly_request_limit:
            logger.warning(f"Monthly API limit reached: {monthly_usage}/{self.config.monthly_request_limit}")
            return False

        if today_usage >= self.config.daily_request_limit:
            logger.warning(f"Daily API limit reached: {today_usage}/{self.config.daily_request_limit}")
            return False

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
        # In test mode, skip tracking
        if self.config.is_test_mode:
            return True

        if not self.db:
            logger.warning("Database session not provided, can't track API usage")
            return False

        try:
            # Create usage record
            usage = TwitterApiUsage(
                date=datetime.utcnow(),
                influencer_id=influencer_id,
                endpoint=endpoint,
                requests_used=1,
                reset_time=datetime.utcnow()  # We don't have exact reset time from API
            )

            self.db.add(usage)
            self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Error tracking API usage: {e}")
            self.db.rollback()
            return False

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

        # In test mode, return mock data
        if self.config.is_test_mode:
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

        # Check if we're within API limits
        if self.db and not self.check_api_limits(influencer_id):
            logger.warning(f"API limit reached, skipping retrieval for {username}")
            return []

        try:
            # Get user ID by username
            user_response = self._execute_with_retry(
                lambda: self.client.get_user(username=username)
            )

            if not user_response or not user_response.data:
                logger.warning(f"User not found: {username}")
                return []

            user_id = user_response.data.id

            # Define tweet fields to retrieve
            tweet_fields = [
                "created_at", "author_id", "public_metrics",
                "entities", "context_annotations"
            ]

            # Get user's tweets
            response = self._execute_with_retry(
                lambda: self.client.get_users_tweets(
                    id=user_id,
                    max_results=min(max_results, 100),  # API limitation: max 100 per request
                    tweet_fields=tweet_fields,
                    exclude=["retweets", "replies"]  # Exclude retweets and replies
                )
            )

            if not response or not response.data:
                logger.info(f"No tweets found for user: {username}")
                return []

            # Process tweets into a standardized format
            tweets = self._process_tweets(response.data)

            # Add username to each tweet
            for tweet in tweets:
                tweet["author_username"] = username

            # Track API usage if we have a database session and influencer ID
            if self.db and influencer_id:
                self.track_api_usage(influencer_id, "user_tweets")

            logger.info(f"Retrieved {len(tweets)} tweets from user: {username}")
            return tweets

        except Exception as e:
            logger.error(f"Error getting tweets from user '{username}': {e}")
            return []

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
        # In test mode, return None
        if self.config.is_test_mode:
            return None

        max_retries = max_retries or self.config.max_retries
        retry_delay = retry_delay or self.config.retry_delay

        retries = 0
        last_error = None

        while retries <= max_retries:
            try:
                if retries > 0:
                    logger.info(f"Retry attempt {retries}/{max_retries}")

                return operation()

            except tweepy.TooManyRequests as e:
                logger.warning(f"Rate limit reached: {e}")
                # Get reset time from response if available
                reset_time = getattr(e, "reset", None)
                if reset_time:
                    sleep_time = max(int(reset_time) - time.time(), 1)
                    logger.info(f"Waiting {sleep_time} seconds for rate limit reset")
                    time.sleep(sleep_time)
                else:
                    time.sleep(retry_delay * (2 ** retries))  # Exponential backoff

            except (tweepy.TwitterServerError, tweepy.ConnectionError) as e:
                logger.warning(f"Twitter server error: {e}")
                time.sleep(retry_delay * (2 ** retries))  # Exponential backoff

            except Exception as e:
                logger.error(f"Error during API call: {e}")
                last_error = e
                time.sleep(retry_delay)

            retries += 1

        if last_error:
            logger.error(f"Operation failed after {max_retries} retries. Last error: {last_error}")
        return None
