import unittest
from unittest.mock import patch, MagicMock, call
import json
import time
from threading import Thread

from confluent_kafka import KafkaException, Message, KafkaError
from src.data_processing.kafka.consumer import KafkaConsumer, BatchKafkaConsumer


class TestKafkaConsumer(unittest.TestCase):
    """Test the base Kafka consumer functionality."""

    @patch('src.data_processing.kafka.consumer.Consumer')
    def setUp(self, mock_consumer):
        """Set up test environment."""
        self.mock_consumer = mock_consumer.return_value
        self.consumer = KafkaConsumer(
            topics="test-topic",
            group_id="test-group"
        )

    def test_deserialize_message_json(self):
        """Test deserialization of JSON messages."""
        # Create a mock message with JSON data
        mock_message = MagicMock(spec=Message)
        mock_message.value.return_value = json.dumps({"key": "value"}).encode('utf-8')

        result = self.consumer.deserialize_message(mock_message)
        self.assertEqual(result, {"key": "value"})

    def test_deserialize_message_text(self):
        """Test deserialization of text messages."""
        # Create a mock message with text data
        mock_message = MagicMock(spec=Message)
        mock_message.value.return_value = "simple text".encode('utf-8')

        result = self.consumer.deserialize_message(mock_message)
        self.assertEqual(result, "simple text")

    def test_deserialize_message_none(self):
        """Test deserialization of None messages."""
        # Create a mock message with None value
        mock_message = MagicMock(spec=Message)
        mock_message.value.return_value = None

        result = self.consumer.deserialize_message(mock_message)
        self.assertIsNone(result)

    def test_deserialize_message_error(self):
        """Test error handling during deserialization."""
        # Create a mock message that raises an exception when value() is called
        mock_message = MagicMock(spec=Message)
        mock_message.value.side_effect = Exception("Test exception")

        result = self.consumer.deserialize_message(mock_message)
        self.assertIsNone(result)

    @patch('src.data_processing.kafka.consumer.logger')
    def test_handle_message_success(self, mock_logger):
        """Test successful message handling."""
        # Create a mock message
        mock_message = MagicMock(spec=Message)
        mock_message.topic.return_value = "test-topic"
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 123
        mock_message.timestamp.return_value = (0, 1609459200000)  # (type, timestamp)
        mock_message.value.return_value = json.dumps({"key": "value"}).encode('utf-8')

        with patch.object(self.consumer.kafka_logger, 'log_consumer_event'):
            result = self.consumer.handle_message(mock_message)

        self.assertTrue(result)
        self.consumer.kafka_logger.log_consumer_event.assert_called()
        mock_logger.debug.assert_called_once()

    @patch('src.data_processing.kafka.consumer.logger')
    def test_handle_message_error(self, mock_logger):
        """Test error handling during message processing."""
        # Create a mock message that will cause an error
        mock_message = MagicMock(spec=Message)
        mock_message.topic.side_effect = Exception("Test exception")

        result = self.consumer.handle_message(mock_message)
        self.assertFalse(result)
        mock_logger.error.assert_called_once()

    @patch('src.data_processing.kafka.consumer.Thread')
    def test_start_consumer(self, mock_thread):
        """Test starting the consumer."""
        # Setup
        mock_thread_instance = MagicMock()
        mock_thread.return_value = mock_thread_instance

        # Test
        result = self.consumer.start()

        # Assert
        self.assertEqual(result, self.consumer)  # Should return self for chaining
        mock_thread.assert_called_once()
        mock_thread_instance.start.assert_called_once()

    def test_stop_consumer(self):
        """Test stopping the consumer."""
        # Setup: Create a mock thread
        mock_thread = MagicMock()
        self.consumer._consumer_thread = mock_thread

        # Test
        self.consumer.stop()

        # Assert
        mock_thread.join.assert_called_once()
        self.assertTrue(self.consumer._stop_event.is_set())

    @patch('src.data_processing.kafka.consumer.logger')
    def test_consume_loop_handles_errors(self, mock_logger):
        """Test that the consume loop handles errors properly."""
        # # Set up the consumer to run briefly then stop
        # self.consumer._stop_event.set()
        #
        # # Override poll to simulate error
        # self.mock_consumer.poll.side_effect = KafkaException("Test error")
        #
        # # Test
        # self.consumer._consume_loop()
        #
        # # Assert
        # mock_logger.error.assert_called()
        # self.mock_consumer.close.assert_called_once()

        mock_consumer = MagicMock()
        # Настройте poll метода да връща съобщение
        mock_message = MagicMock()
        mock_consumer.poll.return_value = mock_message
        # Настройте message.error() да връща грешка
        mock_error = MagicMock()
        mock_error.code.return_value = 42  # Каквато и да е грешка, различна от _PARTITION_EOF
        mock_message.error.return_value = mock_error

        # Присвояване на моковете
        self.consumer.consumer = mock_consumer

        # Мокиране на logger
        with patch('src.data_processing.kafka.consumer.logger') as mock_logger:
            # Настройване на threading.Thread
            with patch('threading.Thread'):
                # Настройка на self._stop_event.is_set да върне True след първата итерация
                # за да излезе от цикъла
                self.consumer._stop_event = MagicMock()
                self.consumer._stop_event.is_set.side_effect = [False, True]

                # Стартирайте consume_loop
                self.consumer._consume_loop()

                # Проверете дали е имало логване на грешка
                mock_logger.error.assert_called()


class TestBatchKafkaConsumer(unittest.TestCase):
    """Test the batch Kafka consumer functionality."""

    @patch('src.data_processing.kafka.consumer.Consumer')
    def setUp(self, mock_consumer):
        """Set up test environment."""
        self.mock_consumer = mock_consumer.return_value
        self.consumer = BatchKafkaConsumer(
            topics="test-topic",
            group_id="test-group",
            batch_size=10,
            batch_timeout=1.0
        )

    def test_handle_batch(self):
        """Test batch message handling."""
        # Create mock messages
        mock_message1 = MagicMock(spec=Message)
        mock_message2 = MagicMock(spec=Message)

        # Override handle_message to track calls
        self.consumer.handle_message = MagicMock(return_value=True)

        # Test
        result = self.consumer.handle_batch([mock_message1, mock_message2])

        # Assert
        self.assertTrue(result)
        self.assertEqual(self.consumer.handle_message.call_count, 2)
        self.consumer.handle_message.assert_has_calls([
            call(mock_message1),
            call(mock_message2)
        ])

    def test_handle_batch_with_errors(self):
        """Test batch handling when some messages fail."""
        # Create mock messages
        mock_message1 = MagicMock(spec=Message)
        mock_message2 = MagicMock(spec=Message)

        # Override handle_message to return True for first message, False for second
        self.consumer.handle_message = MagicMock(side_effect=[True, False])

        # Test
        result = self.consumer.handle_batch([mock_message1, mock_message2])

        # Assert
        self.assertFalse(result)  # Should return False if any message fails
        self.assertEqual(self.consumer.handle_message.call_count, 2)


if __name__ == '__main__':
    unittest.main()
