"""
Repository for storing Twitter data in the database.
Handles database operations for Twitter data collection.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from src.data_processing.models.database import Tweet, BlockchainToken, TokenMention, SentimentAnalysis, SentimentEnum, \
    BlockchainNetwork
from src.data_processing.models.twitter import TwitterInfluencer, TwitterInfluencerTweet
from src.data_processing.crud.create import create_tweet, create_token_mention, create_blockchain_token
from src.data_processing.crud.read import (
    get_tweet_by_twitter_id,
    get_blockchain_token_by_symbol,
    get_blockchain_token_by_symbol_and_network,
    get_token_mentions_by_tweet_id
)

# Configure logger
logger = logging.getLogger(__name__)


class TwitterRepository:
    """
    Repository for storing Twitter data in the database.
    Handles database operations for tweets and token mentions.
    """

    def __init__(self, db: Session):
        """
        Initialize the Twitter repository.

        Args:
            db: Database session
        """
        self.db = db

    def store_tweet(self, tweet_data: Dict[str, Any]) -> Optional[Tweet]:
        """
        Store a tweet in the database.

        Args:
            tweet_data: Prepared tweet data

        Returns:
            Stored Tweet instance or None if failed
        """
        try:
            # Check if tweet already exists
            existing_tweet = get_tweet_by_twitter_id(self.db, tweet_data['tweet_id'])
            if existing_tweet:
                logger.debug(f"Tweet {tweet_data['tweet_id']} already exists, skipping")
                return existing_tweet

            # Create new tweet
            tweet = create_tweet(
                db=self.db,
                tweet_id=tweet_data['tweet_id'],
                text=tweet_data['text'],
                created_at=tweet_data['created_at'],
                author_id=tweet_data['author_id'],
                author_username=tweet_data.get('author_username'),
                retweet_count=tweet_data.get('retweet_count', 0),
                like_count=tweet_data.get('like_count', 0)
            )

            logger.info(f"Stored tweet {tweet.tweet_id}")
            return tweet

        except IntegrityError as e:
            logger.error(f"IntegrityError storing tweet {tweet_data['tweet_id']}: {e}")
            self.db.rollback()
            return None

        except Exception as e:
            logger.error(f"Error storing tweet {tweet_data['tweet_id']}: {e}")
            self.db.rollback()
            return None

    def store_token_mentions(self, tweet: Tweet, token_data: List[Dict[str, Any]]) -> List[TokenMention]:
        """
        Store token mentions for a tweet.

        Args:
            tweet: Tweet instance
            token_data: List of token data dictionaries with format:
                       {
                           "symbol": str,                  # Token symbol (e.g., "SOL")
                           "blockchain_network": str,      # Identified blockchain network or None
                           "network_confidence": float,    # Confidence in network identification (0-1)
                           "context": dict                 # Additional context information
                       }

        Returns:
            List of created TokenMention instances
        """
        mentions = []

        for token_info in token_data:
            try:
                symbol = token_info["symbol"]
                blockchain_network = token_info.get("blockchain_network")
                network_confidence = token_info.get("network_confidence", 0.0)
                needs_review = token_info.get("needs_review", False)

                # Find or create token
                token = self._find_or_create_token(
                    symbol,
                    blockchain_network,
                    network_confidence,
                    needs_review=needs_review or network_confidence < 0.5  # Mark for review if confidence is low
                )

                if not token:
                    logger.warning(f"Could not find or create token with symbol {symbol}")
                    continue

                # Create token mention
                mention = create_token_mention(
                    db=self.db,
                    tweet_id=tweet.id,
                    token_id=token.id
                )

                mentions.append(mention)
                logger.info(f"Stored mention of token {symbol} in tweet {tweet.tweet_id}")

            except IntegrityError as e:
                logger.error(f"IntegrityError storing mention of {symbol} in tweet {tweet.tweet_id}: {e}")
                self.db.rollback()

            except Exception as e:
                logger.error(f"Error storing mention of {symbol} in tweet {tweet.tweet_id}: {e}")
                self.db.rollback()

        return mentions

    def _find_or_create_token(
            self,
            symbol: str,
            blockchain_network: Optional[str] = None,
            network_confidence: float = 0.0,
            needs_review: bool = False
    ) -> Optional[BlockchainToken]:
        """
        Find an existing token or create a new one if it doesn't exist.

        Args:
            symbol: Token symbol (e.g., "SOL")
            blockchain_network: Blockchain network name or None if unknown
            network_confidence: Confidence level in network identification (0-1)
            needs_review: Whether this token needs manual review

        Returns:
            BlockchainToken instance or None if failed
        """
        try:
            token = None
            needs_review = needs_review  # This might be set based on confidence or explicitly

            # First try to find token by symbol and network
            if blockchain_network:
                # Check for manually verified tokens first (they take precedence)
                tokens = self.db.query(BlockchainToken).filter(
                    BlockchainToken.symbol == symbol,
                    BlockchainToken.blockchain_network == blockchain_network,
                    BlockchainToken.manually_verified == True
                ).all()

                if tokens:
                    # If there's a manually verified token, use it
                    return tokens[0]

                # Then check for any token with this symbol and network
                token = get_blockchain_token_by_symbol(self.db, symbol, blockchain_network)
            else:
                # If no network provided, just look for the symbol
                # Note: This might return any network, so it's less reliable
                token = get_blockchain_token_by_symbol(self.db, symbol)
                needs_review = True  # Flag for review since network is uncertain

            # If token exists, update confidence if higher
            if token:
                if blockchain_network and token.blockchain_network == blockchain_network:
                    # Only update if the current confidence is higher
                    if network_confidence > token.network_confidence:
                        token.network_confidence = network_confidence
                        token.needs_review = needs_review
                        self.db.commit()
                    return token
                elif not token.manually_verified and not blockchain_network:
                    # If we found a token but without a verified network, and we don't have a network,
                    # still use it but update the review flag if needed
                    if needs_review and not token.needs_review:
                        token.needs_review = True
                        self.db.commit()
                    return token

            # Create new token if it doesn't exist
            # For new tokens without a network or low confidence, mark them for review
            if not blockchain_network or network_confidence < 0.5:
                needs_review = True

            # Placeholder address if we don't have the real one
            token_address = f"unknown_address_{symbol.lower()}"

            return create_blockchain_token(
                db=self.db,
                token_address=token_address,
                symbol=symbol,
                name=symbol,  # Use symbol as name until we have better data
                blockchain_network=blockchain_network,
                network_confidence=network_confidence,
                manually_verified=False,
                needs_review=needs_review
            )

        except Exception as e:
            logger.error(f"Error finding or creating token {symbol}: {e}")
            self.db.rollback()
            return None

    def get_tweet_with_mentions(self, tweet_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a tweet with its token mentions and sentiment analysis.

        Args:
            tweet_id: Database ID of the tweet

        Returns:
            Dictionary with tweet data, token mentions and sentiment, or None if not found
        """
        try:
            # Get the tweet
            tweet = self.db.query(Tweet).filter(Tweet.id == tweet_id).first()
            if not tweet:
                return None

            # Get token mentions
            mentions = get_token_mentions_by_tweet_id(self.db, tweet_id)
            token_symbols = []

            if mentions:
                # Get token symbols and networks
                token_ids = [mention.token_id for mention in mentions]
                tokens = self.db.query(BlockchainToken).filter(BlockchainToken.id.in_(token_ids)).all()
                token_symbols = [
                    f"{token.symbol}" + (f" ({token.blockchain_network})" if token.blockchain_network else "")
                    for token in tokens]

            # Get sentiment analysis
            sentiment = self.db.query(SentimentAnalysis).filter(SentimentAnalysis.tweet_id == tweet_id).first()
            sentiment_value = None
            if sentiment:
                sentiment_value = sentiment.sentiment.value

            # Format response
            return {
                "id": tweet.id,
                "tweet_id": tweet.tweet_id,
                "text": tweet.text,
                "created_at": tweet.created_at,
                "author_username": tweet.author_username,
                "retweet_count": tweet.retweet_count,
                "like_count": tweet.like_count,
                "token_mentions": token_symbols,
                "sentiment": sentiment_value
            }

        except Exception as e:
            logger.error(f"Error getting tweet with mentions {tweet_id}: {e}")
            return None

    def get_known_tokens(self) -> List[BlockchainToken]:
        """
        Get all known blockchain tokens from the database.

        Returns:
            List of BlockchainToken instances
        """
        try:
            return self.db.query(BlockchainToken).all()
        except Exception as e:
            logger.error(f"Error retrieving known tokens: {e}")
            return []

    def get_blockchain_networks(self) -> List[BlockchainNetwork]:
        """
        Get all blockchain networks from the database.

        Returns:
            List of BlockchainNetwork instances
        """
        try:
            return self.db.query(BlockchainNetwork).filter(BlockchainNetwork.is_active == True).all()
        except Exception as e:
            logger.error(f"Error retrieving blockchain networks: {e}")
            return []
