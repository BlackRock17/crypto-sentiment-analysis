import unittest
import time
import os
from uuid import uuid4
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.data_processing.models.database import Base, Tweet, TokenMention, BlockchainToken, SentimentAnalysis
from src.data_processing.kafka.producer import TwitterProducer
from src.data_processing.kafka.consumers.tweet_consumer import TweetConsumer
from src.data_processing.kafka.consumers.token_mention_consumer import TokenMentionConsumer
from src.data_processing.kafka.consumers.sentiment_consumer import SentimentConsumer

# Skip these tests if Kafka is not available
kafka_available = os.environ.get('KAFKA_AVAILABLE', 'false').lower() == 'true'
skip_message = "Skipping Kafka end-to-end tests. Set KAFKA_AVAILABLE=true to run."


@unittest.skipIf(not kafka_available, skip_message)
class EndToEndTest(unittest.TestCase):
    """End-to-end tests for the complete Kafka processing pipeline."""

    @classmethod
    def setUpClass(cls):
        """Set up test database."""
        # Set up test database
        cls.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(cls.engine)
        cls.SessionLocal = sessionmaker(bind=cls.engine)

        # Create some test data
        cls._create_test_data()

    @classmethod
    def tearDownClass(cls):
        """Clean up resources."""
        Base.metadata.drop_all(cls.engine)

    @classmethod
    def _create_test_data(cls):
        """Create test data in the database."""
        session = cls.SessionLocal()

        # Create test blockchain tokens
        token1 = BlockchainToken(
            token_address="addr_sol_123",
            symbol="SOL",
            name="Solana",
            blockchain_network="solana",
            network_confidence=0.9,
            manually_verified=True
        )
        token2 = BlockchainToken(
            token_address="addr_eth_123",
            symbol="ETH",
            name="Ethereum",
            blockchain_network="ethereum",
            network_confidence=0.9,
            manually_verified=True
        )

        session.add(token1)
        session.add(token2)
        session.commit()
        session.close()

    def setUp(self):
        """Set up test environment."""
        # Generate unique group IDs for each consumer to avoid conflicts
        tweet_group = f"test-tweet-{uuid4()}"
        mention_group = f"test-mention-{uuid4()}"
        sentiment_group = f"test-sentiment-{uuid4()}"

        # Create producer
        self.producer = TwitterProducer()

        # Create consumers with unique group IDs
        self.tweet_consumer = TweetConsumer()
        self.mention_consumer = TokenMentionConsumer()
        self.sentiment_consumer = SentimentConsumer()

        # Start consumers
        self.tweet_consumer.start()
        self.mention_consumer.start()
        self.sentiment_consumer.start()

        # Create DB session
        self.db = self.SessionLocal()

    def tearDown(self):
        """Clean up resources after each test."""
        # Stop all consumers
        self.tweet_consumer.stop()
        self.mention_consumer.stop()
        self.sentiment_consumer.stop()

        # Close DB session
        if self.db:
            self.db.close()

    def test_full_pipeline(self):
        """Test the full pipeline from tweet to sentiment analysis."""
        # Create unique test data
        test_id = str(uuid4())
        tweet_data = {
            'tweet_id': f"test_{test_id}",
            'text': f"I really love $SOL, it's going to the moon! Great investment. #{test_id}",
            'author_username': 'crypto_fan',
            'created_at': datetime.utcnow().isoformat()
        }

        # Allow consumers to start up
        time.sleep(2)

        # Send a tweet
        self.producer.send_tweet(tweet_data)

        # Flush producer to ensure message is sent
        self.producer.flush()

        # Allow time for full processing through the pipeline
        time.sleep(10)

        # Check that the tweet was stored
        tweet = self.db.query(Tweet).filter(Tweet.tweet_id == tweet_data['tweet_id']).first()
        self.assertIsNotNone(tweet, "Tweet should be stored in the database")

        # Check that token mentions were created
        mentions = self.db.query(TokenMention).filter(TokenMention.tweet_id == tweet.id).all()
        self.assertTrue(len(mentions) > 0, "Token mentions should be created")

        # Check that sentiment analysis was performed
        sentiment = self.db.query(SentimentAnalysis).filter(SentimentAnalysis.tweet_id == tweet.id).first()
        self.assertIsNotNone(sentiment, "Sentiment analysis should be stored")

        # This tweet should be positive
        self.assertEqual(sentiment.sentiment.value, "positive", "Sentiment should be positive")


if __name__ == '__main__':
    unittest.main()
