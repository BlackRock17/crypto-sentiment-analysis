"""
Twitter API client for interacting with the Twitter API.
Handles authentication, rate limiting, and error handling.
"""
import time
import logging
from typing import List, Dict, Any, Optional, Generator, Union
import tweepy
from tweepy import Client, Response, Tweet

from src.data_collection.twitter.config import twitter_config

# Configure logger
logger = logging.getLogger(__name__)


class TwitterAPIClient:
    """
    Client for interacting with Twitter API v2.
    Handles authentication, rate limiting, and provides methods for data collection.
    """

    def __init__(self, config=None):
        """
        Initialize the Twitter API client.

        Args:
            config: Twitter API configuration (optional, uses default if None)
        """
        self.config = config or twitter_config
        self.client = self._create_client()
        logger.info("Twitter API client initialized")

    def _create_client(self) -> Client:
        """
        Create and authenticate a Twitter API client.

        Returns:
            tweepy.Client: Authenticated Twitter client
        """
        try:
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
            raise

    def test_connection(self) -> bool:
        """
        Test connection to Twitter API by fetching the authenticated user.

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            # Try to get user information to test connection
            me = self.client.get_me()
            logger.info(f"Twitter API connection successful. User ID: {me.data.id}")
            return True
        except Exception as e:
            logger.error(f"Twitter API connection test failed: {e}")
            return False

    def search_tweets(self, query: str, max_results: int = None) -> List[Dict[str, Any]]:
        """
        Search for tweets matching a query.

        Args:
            query: Search query string
            max_results: Maximum number of results to return
                        (defaults to config.max_tweets_per_query)

        Returns:
            List of tweet data dictionaries
        """
        max_results = max_results or self.config.max_tweets_per_query

        try:
            # Define tweet fields to retrieve
            tweet_fields = [
                "created_at", "author_id", "public_metrics",
                "entities", "context_annotations"
            ]

            # Execute the search with retry logic
            response = self._execute_with_retry(
                lambda: self.client.search_recent_tweets(
                    query=query,
                    max_results=min(max_results, 100),  # API limitation: max 100 per request
                    tweet_fields=tweet_fields,
                )
            )

            if not response or not response.data:
                logger.info(f"No tweets found for query: {query}")
                return []

            # Process tweets into a standardized format
            tweets = self._process_tweets(response.data)
            logger.info(f"Retrieved {len(tweets)} tweets for query: {query}")

            return tweets

        except Exception as e:
            logger.error(f"Error searching tweets: {e}")
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