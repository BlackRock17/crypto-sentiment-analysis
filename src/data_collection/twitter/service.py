"""
Service for Twitter data collection.
Coordinates data collection, processing, and storage.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from src.data_collection.twitter.client import TwitterAPIClient
from src.data_collection.twitter.processor import TwitterDataProcessor
from src.data_collection.twitter.repository import TwitterRepository
from src.data_collection.twitter.config import twitter_config
from src.data_processing.models.database import Tweet, TokenMention, BlockchainNetwork, BlockchainToken
from src.data_processing.models.twitter import TwitterInfluencer, TwitterInfluencerTweet
from src.data_processing.crud.twitter import (
    get_automated_influencers, create_influencer_tweet, get_influencer_by_username
)
from src.services.notification_service import NotificationService
# Add Kafka producer import
from src.data_processing.kafka.producer import TwitterProducer

# Configure logger
logger = logging.getLogger(__name__)


class TwitterCollectionService:
    """
    Service for Twitter data collection.
    Coordinates the collection, processing, and storage of Twitter data.
    """

    def __init__(self, db: Session):
        """
        Initialize the Twitter collection service.

        Args:
            db: Database session
        """
        self.db = db
        self.client = TwitterAPIClient(db=db)
        self.processor = TwitterDataProcessor(self.client)
        self.repository = TwitterRepository(db)
        # Initialize Twitter Kafka producer
        self.kafka_producer = TwitterProducer()

    def collect_and_store_automated_tweets(self) -> Tuple[int, int]:
        """
        Collect tweets from automated influencers and send them to Kafka for processing.

        Returns:
            Tuple of (tweets_collected, token_mentions_found)
        """
        logger.info(f"Starting collection of tweets from automated influencers")

        # Get automated influencers
        max_influencers = twitter_config.max_automated_influencers
        influencers = get_automated_influencers(self.db, max_count=max_influencers)

        if not influencers:
            logger.warning("No automated influencers configured")
            return 0, 0

        logger.info(f"Found {len(influencers)} automated influencers")

        tweets_collected = 0
        mentions_found = 0

        # Process each influencer
        for influencer in influencers:
            # Check if we're within API limits
            if not self.client.check_api_limits(influencer.id):
                logger.warning(f"API limit reached, skipping influencer {influencer.username}")
                continue

            # Collect tweets
            tweets_data = self.client.get_user_tweets(
                username=influencer.username,
                influencer_id=influencer.id,
                max_results=twitter_config.max_tweets_per_user
            )

            if not tweets_data:
                logger.warning(f"No tweets found for {influencer.username}")
                continue

            # Process each tweet
            for tweet_data in tweets_data:
                # Add influencer metadata to the tweet data
                tweet_data['influencer_id'] = influencer.id
                tweet_data['influencer_username'] = influencer.username

                # Send to Kafka instead of storing directly
                success = self.kafka_producer.send_tweet(tweet_data)

                if success:
                    tweets_collected += 1
                else:
                    logger.error(f"Failed to send tweet to Kafka: {tweet_data['tweet_id']}")

        logger.info(
            f"Collection complete: collected {tweets_collected} influencer tweets"
        )

        # Return approximate counts (actual processing will happen in Kafka consumers)
        return tweets_collected, 0  # We don't know mentions count yet

    def add_manual_tweet(
            self,
            influencer_username: str,
            tweet_text: str,
            created_at: Optional[datetime] = None,
            tweet_id: Optional[str] = None,
            retweet_count: int = 0,
            like_count: int = 0
    ) -> Tuple[Optional[Tweet], int]:
        """
        Manually add a tweet for an influencer and send to Kafka for processing.

        Args:
            influencer_username: Twitter username of the influencer
            tweet_text: Text content of the tweet
            created_at: Creation timestamp (defaults to now)
            tweet_id: Original Twitter ID (optional)
            retweet_count: Number of retweets
            like_count: Number of likes

        Returns:
            Tuple of (stored_tweet, mentions_count)
        """
        # Get or create influencer
        influencer = get_influencer_by_username(self.db, influencer_username)

        if not influencer:
            logger.info(f"Influencer {influencer_username} not found, creating new record")
            influencer = self._create_influencer(influencer_username)

        if not influencer:
            logger.error(f"Failed to get or create influencer {influencer_username}")
            return None, 0

        # Prepare tweet data
        if not tweet_id:
            import uuid
            tweet_id = f"manual_{uuid.uuid4().hex}"

        tweet_data = {
            "tweet_id": tweet_id,
            "text": tweet_text,
            "created_at": created_at.isoformat() if created_at else datetime.utcnow().isoformat(),
            "author_id": f"user_{influencer_username}",
            "author_username": influencer_username,
            "retweet_count": retweet_count,
            "like_count": like_count,
            "influencer_id": influencer.id,
            "influencer_username": influencer_username,
            "is_manually_added": True  # Flag to indicate this is a manually added tweet
        }

        # Send to Kafka for processing
        success = self.kafka_producer.send_tweet(tweet_data)

        if not success:
            logger.error(f"Failed to send manual tweet to Kafka")
            return None, 0

        # For backward compatibility, still return a Tweet object and placeholder mention count
        # The actual processing will happen asynchronously through Kafka

        # Create a placeholder tweet object to return
        tweet = Tweet(
            tweet_id=tweet_id,
            text=tweet_text,
            created_at=created_at or datetime.utcnow(),
            author_id=f"user_{influencer_username}",
            author_username=influencer_username,
            retweet_count=retweet_count,
            like_count=like_count
        )

        # Add a record for influencer-tweet association
        create_influencer_tweet(
            db=self.db,
            influencer_id=influencer.id,
            tweet_id=tweet.id if tweet.id else 0,  # Use placeholder ID if needed
            is_manually_added=True
        )

        logger.info(f"Manual tweet sent to Kafka for processing: {tweet_id}")

        return tweet, 0  # Return 0 mentions since actual processing happens via Kafka

    def _process_and_store_tweet(
            self,
            tweet_data: Dict[str, Any],
            influencer_id: int,
            is_manually_added: bool = False,
            known_tokens: List[BlockchainToken] = None,
            blockchain_networks: List[BlockchainNetwork] = None
    ) -> Optional[Tuple[Tweet, int]]:
        """
        Process and store a tweet with token mentions.

        Args:
            tweet_data: Tweet data dictionary
            influencer_id: ID of the influencer
            is_manually_added: Whether the tweet was manually added
            known_tokens: List of known tokens (for optimization)
            blockchain_networks: List of known blockchain networks (for optimization)

        Returns:
            Tuple of (stored_tweet, mentions_count) or None if failed
        """
        try:
            # Prepare tweet for storage
            prepared_tweet = self.processor.prepare_tweet_for_storage(tweet_data)

            # Store tweet
            stored_tweet = self.repository.store_tweet(prepared_tweet)
            if not stored_tweet:
                return None

            # Create link between influencer and tweet
            create_influencer_tweet(
                db=self.db,
                influencer_id=influencer_id,
                tweet_id=stored_tweet.id,
                is_manually_added=is_manually_added
            )

            # Get known tokens and networks if not provided
            if known_tokens is None:
                known_tokens = self.repository.get_known_tokens()

            if blockchain_networks is None:
                blockchain_networks = self.repository.get_blockchain_networks()

            # Extract token mentions with blockchain network information
            token_mentions = self.processor.extract_blockchain_tokens(
                prepared_tweet['text'],
                known_tokens,
                blockchain_networks
            )

            # Convert set of tuples to list of dictionaries
            token_data = []
            if isinstance(token_mentions, dict):
                # If processor returns a dict with token symbols as keys
                for symbol, network_info in token_mentions.items():
                    if isinstance(network_info, list):
                        for network, confidence in network_info:
                            token_data.append({
                                "symbol": symbol,
                                "blockchain_network": network,
                                "network_confidence": confidence
                            })
                    else:
                        # Handle case where network_info is a single value
                        token_data.append({
                            "symbol": symbol,
                            "blockchain_network": network_info if not isinstance(network_info, tuple) else network_info[
                                0],
                            "network_confidence": 0.5 if not isinstance(network_info, tuple) else network_info[1]
                        })
            elif isinstance(token_mentions, set):
                # Handle set of token dictionaries
                token_data = [dict(t) for t in token_mentions]
            else:
                logger.warning(f"Unexpected format from token extraction: {type(token_mentions)}")
                token_data = []

            # Add cashtags if available in tweet_data
            if 'cashtags' in tweet_data:
                for cashtag in tweet_data['cashtags']:
                    # Check if cashtag is already in token_data
                    if not any(t.get('symbol') == cashtag.upper() for t in token_data):
                        # Determine blockchain networks from detected_networks if available
                        network = None
                        confidence = 0.0

                        if 'detected_networks' in prepared_tweet:
                            networks = prepared_tweet['detected_networks']
                            if networks:
                                # Get the network with highest confidence
                                network, confidence = max(networks.items(), key=lambda x: x[1])

                        token_data.append({
                            "symbol": cashtag.upper(),
                            "blockchain_network": network,
                            "network_confidence": confidence
                        })

            # Store token mentions
            mentions_count = 0
            created_tokens = []

            if token_data:
                mentions = self.repository.store_token_mentions(stored_tweet, token_data)
                mentions_count = len(mentions)

                # Collect newly created tokens for notifications
                for token_info in token_data:
                    symbol = token_info.get("symbol")
                    network = token_info.get("blockchain_network")
                    confidence = token_info.get("network_confidence", 0.0)

                    token = self.repository._find_or_create_token(
                        symbol=symbol,
                        blockchain_network=network,
                        network_confidence=confidence
                    )

                    if token and token not in created_tokens:
                        created_tokens.append(token)

            # Generate notifications for newly created or uncategorized tokens
            notification_service = NotificationService(self.db)

            for token in created_tokens:
                # Check if token was newly created
                if token.created_at >= datetime.utcnow() - timedelta(minutes=1):
                    # New token detected
                    notification_service.notify_new_token(
                        token_id=token.id,
                        confidence=token.network_confidence or 0.0
                    )

                # Check if token needs review
                if token.needs_review:
                    # Get mention count for better prioritization
                    mention_count = self.db.query(TokenMention).filter(
                        TokenMention.token_id == token.id
                    ).count()

                    # Token needs categorization
                    notification_service.notify_uncategorized_token(
                        token_id=token.id,
                        mention_count=mention_count
                    )

            return stored_tweet, mentions_count

        except Exception as e:
            logger.error(f"Error processing tweet: {e}")
            self.db.rollback()
            return None

    def _create_influencer(self, username: str) -> Optional[TwitterInfluencer]:
        """
        Create a new influencer record.

        Args:
            username: Twitter username

        Returns:
            Created TwitterInfluencer instance or None if failed
        """
        from src.data_processing.crud.twitter import create_influencer

        try:
            # Try to get user information from Twitter
            user_response = self.client._execute_with_retry(
                lambda: self.client.client.get_user(username=username)
            )

            if not user_response or not user_response.data:
                logger.warning(f"User not found on Twitter: {username}")

            # Create influencer with available information
            if user_response and user_response.data:
                user = user_response.data
                description = getattr(user, 'description', None)
                name = getattr(user, 'name', None)
                followers = getattr(user, 'public_metrics', {}).get('followers_count', 0)

                return create_influencer(
                    db=self.db,
                    username=username,
                    name=name,
                    description=description,
                    follower_count=followers,
                    is_active=True,
                    is_automated=False,  # Manual by default
                    priority=0  # Default priority
                )
            else:
                # Create with minimal information
                return create_influencer(
                    db=self.db,
                    username=username,
                    is_active=True,
                    is_automated=False
                )

        except Exception as e:
            logger.error(f"Error creating influencer {username}: {e}")
            self.db.rollback()
            return None

    def test_twitter_connection(self) -> bool:
        """
        Test connection to Twitter API.

        Returns:
            True if connection successful, False otherwise
        """
        return self.client.test_connection()
