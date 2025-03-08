"""
Data processing module for Twitter data.
Processes raw Twitter data and prepares it for storage in the database.
"""

import re
import logging
from typing import List, Dict, Any, Optional, Set, Tuple
from datetime import datetime

from src.data_processing.models.database import Tweet, BlockchainToken, BlockchainNetwork
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

    def collect_influencer_tweets(self, limit_per_user: int = None) -> List[Dict[str, Any]]:
        """
        Collect tweets from configured influencer accounts.

        Args:
            limit_per_user: Maximum number of tweets to collect per user

        Returns:
            List of processed tweet data
        """
        limit_per_user = limit_per_user or twitter_config.max_tweets_per_user
        results = []

        # Get tweets from each influencer
        for username in twitter_config.influencer_accounts:
            try:
                logger.info(f"Collecting tweets from influencer: {username}")
                user_tweets = self.client.get_user_tweets(username, max_results=limit_per_user)

                if user_tweets:
                    logger.info(f"Collected {len(user_tweets)} tweets from {username}")
                    results.extend(user_tweets)
                else:
                    logger.warning(f"No tweets found for influencer: {username}")

            except Exception as e:
                logger.error(f"Error collecting tweets from {username}: {e}")

        logger.info(f"Total influencer tweets collected: {len(results)}")
        return results

    def extract_blockchain_tokens(self, tweet_text: str, known_tokens: List[BlockchainToken],
                                  blockchain_networks: List[BlockchainNetwork] = None) -> Set[Dict[str, Any]]:
        """
        Extract mentions of blockchain tokens from tweet text.

        Args:
            tweet_text: The text of the tweet
            known_tokens: List of known blockchain tokens from database
            blockchain_networks: List of known blockchain networks (optional)

        Returns:
            Set of dictionaries containing token information:
            {
                "symbol": str,                   # Token symbol (e.g., "SOL")
                "blockchain_network": str,       # Identified blockchain network or None
                "network_confidence": float,     # Confidence in network identification (0-1)
                "context": dict                  # Additional context information
            }
        """
        mentioned_tokens = set()

        # Cache tokens if not already cached
        if not self.token_cache and known_tokens:
            self.token_cache = {token.symbol.lower(): token for token in known_tokens}

        # Cache networks if provided and not already cached
        if blockchain_networks and not hasattr(self, 'network_cache'):
            self.network_cache = {network.name.lower(): network for network in blockchain_networks}

            # Also create hashtag and keyword mappings for networks
            self.network_hashtags = {}
            self.network_keywords = {}

            for network in blockchain_networks:
                # Map each hashtag to its network
                if network.hashtags:
                    for hashtag in network.hashtags:
                        self.network_hashtags[hashtag.lower()] = network.name

                # Map each keyword to its network
                if network.keywords:
                    for keyword in network.keywords:
                        self.network_keywords[keyword.lower()] = network.name

        # Extract cashtags (e.g., $SOL, $ETH)
        cashtag_pattern = r'\$([A-Za-z0-9]+)'
        cashtags = re.findall(cashtag_pattern, tweet_text)

        # Extract hashtags for context
        hashtag_pattern = r'#([A-Za-z0-9_]+)'
        hashtags = [tag.lower() for tag in re.findall(hashtag_pattern, tweet_text)]

        # Analyze text for blockchain network mentions
        text_lower = tweet_text.lower()
        detected_networks = self._detect_blockchain_networks(text_lower, hashtags)

        # Process each found cashtag
        for symbol in cashtags:
            token_info = self._process_token_symbol(symbol, detected_networks, text_lower)

            if token_info:
                mentioned_tokens.add(tuple(sorted(token_info.items())))

        # Ensure we return a set of dictionaries with a consistent format
        result_set = set()
        for token_info in mentioned_tokens:
            # Convert to a dictionary if it's a tuple of items
            if isinstance(token_info, tuple):
                token_dict = dict(token_info)
            else:
                token_dict = token_info

            # Ensure all required fields are present
            if "symbol" not in token_dict:
                continue

            if "blockchain_network" not in token_dict:
                token_dict["blockchain_network"] = None

            if "network_confidence" not in token_dict:
                token_dict["network_confidence"] = 0.0

            if "context" not in token_dict:
                token_dict["context"] = {}

            # Convert dictionary to a frozenset of items to make it hashable for a set
            result_set.add(frozenset(token_dict.items()))

        # Convert back to a set of dictionaries when returning
        return {dict(token_tuple) for token_tuple in result_set}

    def _process_token_symbol(self, symbol: str, detected_networks: Dict[str, float], text_context: str) -> Dict[
        str, Any]:
        """
        Process a token symbol to determine its blockchain network and other information.

        Args:
            symbol: Token symbol
            detected_networks: Dictionary of detected networks with confidence scores
            text_context: Lower-cased tweet text for context analysis

        Returns:
            Dictionary with token information or None if token should be ignored
        """
        symbol_upper = symbol.upper()
        symbol_lower = symbol.lower()

        # Check if it's a known token
        if symbol_lower in self.token_cache:
            known_token = self.token_cache[symbol_lower]

            # If token already has a verified network, use that
            if known_token.manually_verified and known_token.blockchain_network:
                return {
                    "symbol": symbol_upper,
                    "blockchain_network": known_token.blockchain_network,
                    "network_confidence": 1.0,  # Maximum confidence for verified tokens
                    "context": {"source": "verified_token_db"}
                }

            # For known but unverified tokens, use network with confidence level
            if known_token.blockchain_network:
                return {
                    "symbol": symbol_upper,
                    "blockchain_network": known_token.blockchain_network,
                    "network_confidence": known_token.network_confidence or 0.7,  # Default confidence for known tokens
                    "context": {"source": "token_db"}
                }

        # For unknown tokens, try to determine network from context
        network, confidence = self._determine_network_from_context(symbol, detected_networks, text_context)

        if network:
            return {
                "symbol": symbol_upper,
                "blockchain_network": network,
                "network_confidence": confidence,
                "context": {"source": "context_analysis"},
                "needs_review": confidence < 0.7  # Mark for review if confidence is low
            }

        # If no network could be determined with confidence
        return {
            "symbol": symbol_upper,
            "blockchain_network": None,
            "network_confidence": 0.0,
            "context": {"source": "unknown"},
            "needs_review": True  # Always mark unknown tokens for review
        }

    def _detect_blockchain_networks(self, text: str, hashtags: List[str]) -> Dict[str, float]:
        """
        Detect blockchain networks mentioned in the text or hashtags.

        Args:
            text: Lower-cased tweet text
            hashtags: List of hashtags in the tweet

        Returns:
            Dictionary mapping network names to confidence scores
        """
        networks_confidence = {}

        # Check if we have network information
        if not hasattr(self, 'network_cache') or not self.network_cache:
            return networks_confidence

        # Check hashtags first (strongest signal)
        for hashtag in hashtags:
            if hashtag in self.network_hashtags:
                network = self.network_hashtags[hashtag]
                networks_confidence[network] = networks_confidence.get(network, 0) + 0.4

        # Check for direct network mentions in text
        for network_name, network in self.network_cache.items():
            # Look for the network name in text
            if f" {network_name} " in f" {text} ":
                networks_confidence[network_name] = networks_confidence.get(network_name, 0) + 0.5

            # Look for display name if different from name
            if network.display_name and network.display_name.lower() != network_name:
                if f" {network.display_name.lower()} " in f" {text} ":
                    networks_confidence[network_name] = networks_confidence.get(network_name, 0) + 0.5

        # Check for keywords
        for keyword, network in self.network_keywords.items():
            if f" {keyword} " in f" {text} ":
                networks_confidence[network] = networks_confidence.get(network, 0) + 0.2

        # Normalize confidence scores to a maximum of 1.0
        for network in networks_confidence:
            networks_confidence[network] = min(networks_confidence[network], 1.0)

        return networks_confidence

    def _determine_network_from_context(self, symbol: str, detected_networks: Dict[str, float], text_context: str) -> \
    Tuple[Optional[str], float]:
        """
        Determine the most likely blockchain network for a token based on context.

        Args:
            symbol: Token symbol
            detected_networks: Dictionary of detected networks with confidence scores
            text_context: The tweet text in lowercase for additional analysis

        Returns:
            Tuple of (network_name, confidence) or (None, 0) if no network could be determined
        """
        # If we have detected networks with high confidence, use those
        high_confidence_networks = {net: conf for net, conf in detected_networks.items() if conf >= 0.6}
        if high_confidence_networks:
            # Use the network with the highest confidence
            network = max(high_confidence_networks.items(), key=lambda x: x[1])
            return network

        # For tokens with specific prefixes or properties that strongly indicate a network
        if len(symbol) >= 3:
            # Check for Solana token patterns (often have uppercase names)
            if symbol.isupper() and len(symbol) <= 5:
                return "solana", 0.3

            # Check for Ethereum token patterns (often start with "0x")
            if symbol.startswith("0x"):
                return "ethereum", 0.4

            # Check for Binance Smart Chain patterns (often have "BNB" or "BSC" in context)
            if "bnb" in text_context or "bsc" in text_context or "binance smart chain" in text_context:
                return "binance", 0.4

            # Check for Polygon (MATIC) tokens
            if "polygon" in text_context or "matic" in text_context:
                return "polygon", 0.3

            # Check for Ethereum token patterns (often have "ETH" in context)
            eth_indicators = ["ethereum", "eth ", "on eth", "eth blockchain"]
            if any(indicator in text_context for indicator in eth_indicators):
                return "ethereum", 0.3

        # If we have any detected networks, use the one with highest confidence
        if detected_networks:
            network = max(detected_networks.items(), key=lambda x: x[1])
            # Only return if there's at least some confidence
            if network[1] >= 0.2:
                return network

        # Default to no determined network
        return None, 0.0

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
            'cashtags': tweet_data.get('cashtags', []),
            # Add detected blockchain networks if available
            'detected_networks': tweet_data.get('detected_networks', {}),
            # Add identified tokens with their networks if available
            'identified_tokens': tweet_data.get('identified_tokens', [])
        }

        return prepared_data

