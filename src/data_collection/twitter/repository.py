"""
Repository for storing Twitter data in the database.
Handles database operations for Twitter data collection.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from src.data_processing.models.database import Tweet, SolanaToken, TokenMention
from src.data_processing.crud.create import create_tweet, create_token_mention
from src.data_processing.crud.read import (
    get_tweet_by_twitter_id,
    get_solana_token_by_symbol,
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

    def store_token_mentions(self, tweet: Tweet, token_symbols: Set[str]) -> List[TokenMention]:
        """
        Store token mentions for a tweet.

        Args:
            tweet: Tweet instance
            token_symbols: Set of token symbols mentioned in the tweet

        Returns:
            List of created TokenMention instances
        """
        mentions = []

        for symbol in token_symbols:
            try:
                # Find token by symbol
                token = get_solana_token_by_symbol(self.db, symbol)
                if not token:
                    logger.warning(f"Token with symbol {symbol} not found in database")
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

    def get_known_tokens(self) -> List[SolanaToken]:
        """
        Get all known Solana tokens from the database.

        Returns:
            List of SolanaToken instances
        """
        try:
            return self.db.query(SolanaToken).all()
        except Exception as e:
            logger.error(f"Error retrieving known tokens: {e}")
            return []
