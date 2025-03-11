# tests/test_kafka/test_tweet_consumer.py

import unittest
from unittest.mock import patch, MagicMock, call
import json
from datetime import datetime

from src.data_processing.kafka.consumers.tweet_consumer import TweetConsumer


class TestTweetConsumer(unittest.TestCase):
    """Test the tweet consumer."""

    @patch('src.data_processing.kafka.consumers.tweet_consumer.TokenMentionProducer')
    @patch('src.data_processing.kafka.consumer.Consumer')
    def setUp(self, mock_consumer, mock_token_producer):
        """Set up test environment."""
        self.mock_consumer = mock_consumer.return_value
        self.mock_token_producer = mock_token_producer.return_value
        self.consumer = TweetConsumer()

    @patch('src.data_processing.kafka.consumers.tweet_consumer.get_db')
    @patch('src.data_processing.kafka.consumers.tweet_consumer.TwitterRepository')
    @patch('src.data_processing.kafka.consumers.tweet_consumer.TwitterDataProcessor')
    def test_handle_message_success(self, mock_processor_class, mock_repo_class, mock_get_db):
        """Test successful handling of a tweet message."""
        # Mock database session
        mock_db = MagicMock()
        mock_get_db.return_value.__next__.return_value = mock_db

        # Mock repository and processor
        mock_repo = mock_repo_class.return_value
        mock_processor = mock_processor_class.return_value

        # Mock return values
        mock_networks = [MagicMock()]
        mock_tokens = [MagicMock()]
        mock_repo.get_blockchain_networks.return_value = mock_networks
        mock_repo.get_known_tokens.return_value = mock_tokens

        mock_stored_tweet = MagicMock()
        mock_stored_tweet.id = 123
        mock_repo.store_tweet.return_value = mock_stored_tweet

        processed_tweet = {
            'tweet_id': '12345',
            'text': 'Test tweet about $SOL',
        }
        mock_processor.prepare_tweet_for_storage.return_value = processed_tweet

        token_mentions = [{'symbol': 'SOL', 'blockchain_network': 'solana'}]
        mock_processor.extract_blockchain_tokens.return_value = token_mentions

        # Setup successful token mention sending
        self.mock_token_producer.send_token_mention.return_value = True

        # Create mock message
        mock_message = MagicMock()
        tweet_data = {
            'tweet_id': '12345',
            'text': 'Test tweet about $SOL',
            'author_username': 'test_user',
            'influencer_id': 1,
            'is_manually_added': False
        }
        # Configure the deserialize_message method to return our test data
        self.consumer.deserialize_message = MagicMock(return_value=tweet_data)

        # Test
        result = self.consumer.handle_message(mock_message)

        # Assert
        self.assertTrue(result)
        mock_processor.prepare_tweet_for_storage.assert_called_once_with(tweet_data)
        mock_repo.store_tweet.assert_called_once_with(processed_tweet)
        mock_processor.extract_blockchain_tokens.assert_called_once()
        self.mock_token_producer.send_token_mention.assert_called_once()

        # Verify the link between influencer and tweet was created
        from src.data_processing.crud.twitter import create_influencer_tweet
        self.assertTrue(create_influencer_tweet.called)

    @patch('src.data_processing.kafka.consumers.tweet_consumer.get_db')
    @patch('src.data_processing.kafka.consumers.tweet_consumer.logger')
    def test_handle_message_empty_data(self, mock_logger, mock_get_db):
        """Test handling of empty message data."""
        # Mock database session
        mock_db = MagicMock()
        mock_get_db.return_value.__next__.return_value = mock_db

        # Create mock message
        mock_message = MagicMock()

        # Configure the deserialize_message method to return None
        self.consumer.deserialize_message = MagicMock(return_value=None)

        # Test
        result = self.consumer.handle_message(mock_message)

        # Assert
        self.assertFalse(result)
        mock_logger.warning.assert_called_once()

    @patch('src.data_processing.kafka.consumers.tweet_consumer.get_db')
    @patch('src.data_processing.kafka.consumers.tweet_consumer.TwitterRepository')
    @patch('src.data_processing.kafka.consumers.tweet_consumer.TwitterDataProcessor')
    @patch('src.data_processing.kafka.consumers.tweet_consumer.logger')
    def test_handle_message_store_error(self, mock_logger, mock_processor_class, mock_repo_class, mock_get_db):
        """Test handling of errors during tweet storage."""
        # Mock database session
        mock_db = MagicMock()
        mock_get_db.return_value.__next__.return_value = mock_db

        # Mock repository and processor
        mock_repo = mock_repo_class.return_value
        mock_processor = mock_processor_class.return_value

        # Mock return values
        mock_networks = [MagicMock()]
        mock_tokens = [MagicMock()]
        mock_repo.get_blockchain_networks.return_value = mock_networks
        mock_repo.get_known_tokens.return_value = mock_tokens

        # Simulate storage error
        mock_repo.store_tweet.return_value = None

        processed_tweet = {
            'tweet_id': '12345',
            'text': 'Test tweet about $SOL',
        }
        mock_processor.prepare_tweet_for_storage.return_value = processed_tweet

        # Create mock message
        mock_message = MagicMock()
        tweet_data = {
            'tweet_id': '12345',
            'text': 'Test tweet about $SOL',
            'author_username': 'test_user'
        }
        # Configure the deserialize_message method to return our test data
        self.consumer.deserialize_message = MagicMock(return_value=tweet_data)

        # Test
        result = self.consumer.handle_message(mock_message)

        # Assert
        self.assertFalse(result)
        mock_logger.error.assert_called_once()


if __name__ == '__main__':
    unittest.main()
