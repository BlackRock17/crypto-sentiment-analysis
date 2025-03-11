import unittest
from unittest.mock import patch, MagicMock
import json
from confluent_kafka import KafkaException

from src.data_processing.kafka.producer import KafkaProducer, TwitterProducer


class TestKafkaProducer(unittest.TestCase):
    """Test the base Kafka producer functionality."""

    @patch('src.data_processing.kafka.producer.Producer')
    def setUp(self, mock_producer):
        """Set up test environment."""
        self.mock_producer = mock_producer.return_value
        self.producer = KafkaProducer(client_id="test-producer")

    def test_serialize_message_dict(self):
        """Test serialization of dictionary messages."""
        message = {"key": "value", "number": 123}
        result = self.producer.serialize_message(message)
        self.assertEqual(json.loads(result.decode('utf-8')), message)

    def test_serialize_message_string(self):
        """Test serialization of string messages."""
        message = "test message"
        result = self.producer.serialize_message(message)
        self.assertEqual(result.decode('utf-8'), message)

    def test_serialize_message_bytes(self):
        """Test serialization of bytes messages."""
        message = b"test message"
        result = self.producer.serialize_message(message)
        self.assertEqual(result, message)

    @patch('src.data_processing.kafka.producer.logger')
    def test_serialize_message_error(self, mock_logger):
        """Test error handling during serialization."""

        # Create an object that will raise an error when serialized to JSON
        class UnserializableObject:
            def __iter__(self):
                # This makes json.dumps raise a TypeError
                return None

        message = UnserializableObject()

        with self.assertRaises(ValueError):
            self.producer.serialize_message(message)

        mock_logger.error.assert_called_once()

    def test_send_success(self):
        """Test successful message sending."""
        # Setup
        self.mock_producer.poll.return_value = None

        # Test
        result = self.producer.send(
            topic="test-topic",
            value={"test": "data"},
            key="test-key"
        )

        # Assert
        self.assertTrue(result)
        self.mock_producer.produce.assert_called_once()
        self.mock_producer.poll.assert_called_once_with(0)

    def test_send_error(self):
        """Test error handling during sending."""
        # Setup
        self.mock_producer.produce.side_effect = KafkaException("Test error")

        # Test
        result = self.producer.send(
            topic="test-topic",
            value={"test": "data"}
        )

        # Assert
        self.assertFalse(result)
        self.mock_producer.produce.assert_called_once()


class TestTwitterProducer(unittest.TestCase):
    """Test the Twitter-specific Kafka producer."""

    @patch('src.data_processing.kafka.producer.Producer')
    def setUp(self, mock_producer):
        """Set up test environment."""
        self.mock_producer = mock_producer.return_value
        self.producer = TwitterProducer()

    @patch('src.data_processing.kafka.producer.KafkaProducer.send')
    def test_send_tweet(self, mock_send):
        """Test sending a tweet."""
        # Setup
        mock_send.return_value = True
        tweet_data = {
            "tweet_id": "12345",
            "text": "Test tweet about #crypto $SOL",
            "author_username": "test_user"
        }

        # Test
        result = self.producer.send_tweet(tweet_data)

        # Assert
        self.assertTrue(result)
        mock_send.assert_called_once()
        # Check that topic and key were correct
        args, kwargs = mock_send.call_args
        self.assertEqual(kwargs['topic'], 'twitter-raw-tweets')
        self.assertEqual(kwargs['key'], '12345')

    @patch('src.data_processing.kafka.producer.datetime')
    @patch('src.data_processing.kafka.producer.KafkaProducer.send')
    def test_send_tweet_adds_timestamp(self, mock_send, mock_datetime):
        """Test that timestamp is added to tweet data if missing."""
        # Setup
        mock_send.return_value = True
        mock_datetime.datetime.now.return_value.isoformat.return_value = "2023-01-01T12:00:00"
        tweet_data = {
            "tweet_id": "12345",
            "text": "Test tweet"
        }

        # Test
        self.producer.send_tweet(tweet_data)

        # Assert
        args, kwargs = mock_send.call_args
        sent_data = kwargs['value']
        self.assertEqual(sent_data['timestamp'], "2023-01-01T12:00:00")


if __name__ == '__main__':
    unittest.main()
