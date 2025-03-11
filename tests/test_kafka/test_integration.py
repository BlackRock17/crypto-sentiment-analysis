# tests/test_kafka/test_integration.py

import unittest
import json
import time
import os
from uuid import uuid4
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.data_processing.kafka.producer import TwitterProducer
from src.data_processing.kafka.consumers.tweet_consumer import TweetConsumer
from src.data_processing.models.database import Base, Tweet, TokenMention, BlockchainToken
from src.data_processing.kafka.config import DEFAULT_BOOTSTRAP_SERVERS

# Skip these tests if Kafka is not available
kafka_available = os.environ.get('KAFKA_AVAILABLE', 'false').lower() == 'true'
skip_message = "Skipping Kafka integration tests. Set KAFKA_AVAILABLE=true to run."


@unittest.skipIf(not kafka_available, skip_message)
class KafkaIntegrationTest(unittest.TestCase):
    """Integration tests for Kafka components."""

    @classmethod
    def setUpClass(cls):
        """Set up test database and Kafka connections."""
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
        # Create a unique consumer group for each test to avoid conflicts
        self.group_id = f"test-group-{uuid4()}"

        # Create producer
        self.producer = TwitterProducer()

        # Collection for clean-up
        self.consumers_to_stop = []

        # Create DB session
        self.db = self.SessionLocal()

    def tearDown(self):
        """Clean up resources after each test."""
        # Stop all consumers
        for consumer in self.consumers_to_stop:
            consumer.stop()

        # Close DB session
        if self.db:
            self.db.close()

    def test_tweet_flow(self):
        """Test the flow from tweet producer to tweet consumer."""
        # Create unique test data
        test_id = str(uuid4())
        tweet_data = {
            'tweet_id': f"test_{test_id}",
            'text': f"Test tweet about $SOL and $ETH #{test_id}",
            'author_username': 'test_user',
            'created_at': datetime.utcnow().isoformat()
        }

        # Create and start consumer
        consumer = TweetConsumer()
        consumer.start()
        self.consumers_to_stop.append(consumer)

        # Allow consumer to start up
        time.sleep(2)

        # Send a tweet
        self.producer.send_tweet(tweet_data)

        # Flush producer to ensure message is sent
        self.producer.flush()

        # Allow time for message to be processed
        time.sleep(5)

        # Check that the tweet was stored in the database
        tweet = self.db.query(Tweet).filter(Tweet.tweet_id == tweet_data['tweet_id']).first()
        self.assertIsNotNone(tweet, "Tweet should be stored in the database")
        self.assertEqual(tweet.text, tweet_data['text'])

        # Check that token mentions were created
        mentions = self.db.query(TokenMention).filter(TokenMention.tweet_id == tweet.id).all()
        self.assertTrue(len(mentions) > 0, "Token mentions should be created")


if __name__ == '__main__':
    unittest.main()
