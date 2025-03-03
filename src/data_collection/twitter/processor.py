"""
Data processing module for Twitter data.
Processes raw Twitter data and prepares it for storage in the database.
"""

import re
import logging
from typing import List, Dict, Any, Optional, Set
from datetime import datetime

from src.data_processing.models.database import Tweet, SolanaToken
from src.data_collection.twitter.client import TwitterAPIClient
from src.data_collection.twitter.config import twitter_config

# Configure logger
logger = logging.getLogger(__name__)


class TwitterDataProcessor:
    """
    Processes Twitter data and prepares it for storage in the database.
    Identifies token mentions and extracts relevant information.
    """

    def __init__(self, client: Optional[TwitterAPIClient] = None):
        """
        Initialize the Twitter data processor.

        Args:
            client: TwitterAPIClient instance (optional)
        """
        self.client = client or TwitterAPIClient()
        self.token_cache = {}  # Cache of known tokens

    def collect_solana_tweets(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Collect tweets related to Solana based on configured hashtags.

        Args:
            limit: Maximum number of tweets to collect

        Returns:
            List of processed tweet data
        """
        results = []

        # Build query from configured hashtags
        hashtags = self.build_search_query()

        # Collect tweets
        tweets = self.client.search_tweets(hashtags, max_results=limit)

        if tweets:
            logger.info(f"Collected {len(tweets)} tweets related to Solana")
            results.extend(tweets)
        else:
            logger.warning("No tweets found for the specified hashtags")

        return results

    def build_search_query(self) -> str:
        """
        Build a search query string from configured hashtags.

        Returns:
            Search query string
        """
        # Format each hashtag with # and join with OR
        hashtag_terms = ' OR '.join([f'#{tag}' for tag in twitter_config.search_hashtags])

        # Also include $SOL cashtag
        query = f"({hashtag_terms}) OR $SOL"

        # Add language filter if specified
        if twitter_config.search_languages:
            lang_filter = ' OR '.join([f'lang:{lang}' for lang in twitter_config.search_languages])
            query = f"({query}) ({lang_filter})"

        logger.info(f"Built search query: {query}")
        return query

    def extract_solana_tokens(self, tweet_text: str, known_tokens: List[SolanaToken]) -> Set[str]:
        """
        Extract mentions of Solana tokens from tweet text.

        Args:
            tweet_text: The text of the tweet
            known_tokens: List of known Solana tokens from database

        Returns:
            Set of token symbols mentioned in the tweet
        """
        mentioned_tokens = set()

        # Cache tokens if not already cached
        if not self.token_cache and known_tokens:
            self.token_cache = {token.symbol.lower(): token for token in known_tokens}

        # Extract cashtags (e.g., $SOL, $RAY)
        cashtag_pattern = r'\$([A-Za-z0-9]+)'
        cashtags = re.findall(cashtag_pattern, tweet_text)

        # Check each cashtag against known tokens
        for symbol in cashtags:
            symbol_lower = symbol.lower()
            if symbol_lower in self.token_cache:
                mentioned_tokens.add(symbol.upper())  # Store as uppercase

        return mentioned_tokens

    def prepare_tweet_for_storage(self, tweet_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare tweet data for storage in the database.

        Args:
            tweet_data: Raw tweet data

        Returns:
            Dictionary with formatted tweet data
        """
        # Format created_at if it's a string
        created_at = tweet_data.get('created_at')
        if isinstance(created_at, str):
            try:
                created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            except ValueError:
                created_at = datetime.utcnow()

        # Format data for database schema
        prepared_data = {
            'tweet_id': str(tweet_data.get('tweet_id')),
            'text': tweet_data.get('text', ''),
            'created_at': created_at or datetime.utcnow(),
            'author_id': str(tweet_data.get('author_id', '')),
            'author_username': tweet_data.get('author_username', ''),
            'retweet_count': tweet_data.get('retweet_count', 0),
            'like_count': tweet_data.get('like_count', 0),
            'collected_at': datetime.utcnow(),
            # Add cashtags if available
            'cashtags': tweet_data.get('cashtags', [])
        }

        return prepared_data