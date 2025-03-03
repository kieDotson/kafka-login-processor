#!/usr/bin/env python3
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from consumer import KafkaLoginConsumer
from processor import LoginDataProcessor


class TestKafkaLoginConsumer(unittest.TestCase):
    """Test cases for the KafkaLoginConsumer class."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Patch the imports
        self.processor_patcher = patch('consumer.LoginDataProcessor')
        self.consumer_patcher = patch('consumer.KafkaConsumer')
        self.producer_patcher = patch('consumer.KafkaProducer')
        
        # Get the mocks
        self.mock_processor_class = self.processor_patcher.start()
        self.mock_consumer_class = self.consumer_patcher.start()
        self.mock_producer_class = self.producer_patcher.start()
        
        # Configure mocks
        self.mock_processor = MagicMock(spec=LoginDataProcessor)
        self.mock_processor_class.return_value = self.mock_processor
        
        self.mock_kafka_consumer = MagicMock()
        self.mock_consumer_class.return_value = self.mock_kafka_consumer
        
        self.mock_kafka_producer = MagicMock()
        self.mock_producer_class.return_value = self.mock_kafka_producer
        
        # Create the consumer instance
        self.consumer = KafkaLoginConsumer(
            bootstrap_servers='test:9092',
            input_topic='test-input',
            output_topic='test-output',
            consumer_group='test-group'
        )
        
        # Sample message
        self.sample_message = MagicMock()
        self.sample_message.value = {
            "user_id": "424cdd21-063a-43a7-b91b-7ca1a833afae",
            "app_version": "2.3.0",
            "device_type": "android",
            "ip": "199.172.111.135",
            "locale": "RU",
            "device_id": "593-47-5928",
            "timestamp": "1694479551"
        }
    
    def tearDown(self):
        """Clean up after tests."""
        self.processor_patcher.stop()
        self.consumer_patcher.stop()
        self.producer_patcher.stop()
    
    def test_init(self):
        """Test initialization of consumer."""
        # Check that the processor was created with the correct bootstrap servers
        self.mock_processor_class.assert_called_once_with(bootstrap_servers='test:9092')
        
        # Check that the consumer was created with the correct parameters
        self.mock_consumer_class.assert_called_once()
        consumer_args = self.mock_consumer_class.call_args
        self.assertEqual(consumer_args[0][0], 'test-input')
        self.assertEqual(consumer_args[1]['bootstrap_servers'], 'test:9092')
        self.assertEqual(consumer_args[1]['group_id'], 'test-group')
        
        # Check that the producer was created with the correct parameters
        self.mock_producer_class.assert_called_once()
        producer_args = self.mock_producer_class.call_args
        self.assertEqual(producer_args[1]['bootstrap_servers'], 'test:9092')
    
    def test_run_processes_valid_message(self):
        """Test that run processes valid messages correctly."""
        # Set up the consumer mock to yield one message
        self.mock_kafka_consumer.__iter__.return_value = [self.sample_message]
        
        # Set up processor to return valid processed message
        processed_message = {"processed_data": "value"}
        self.mock_processor.process.return_value = (processed_message, True, {})
        
        # Run the consumer
        self.consumer.run()
        
        # Check that process was called with the correct message
        self.mock_processor.process.assert_called_once_with(self.sample_message.value)
        
        # Check that the producer sent the processed message
        self.mock_kafka_producer.send.assert_called_once_with('test-output', processed_message)
        
        # Check that consumer.commit was called
        self.mock_kafka_consumer.commit.assert_called_once()
    
    def test_run_handles_invalid_message(self):
        """Test that run handles invalid messages correctly."""
        # Set up the consumer mock to yield one message
        self.mock_kafka_consumer.__iter__.return_value = [self.sample_message]
        
        # Set up processor to return invalid processed message
        self.mock_processor.process.return_value = (None, False, {"error": "Invalid message"})
        
        # Run the consumer
        self.consumer.run()
        
        # Check that process was called with the correct message
        self.mock_processor.process.assert_called_once_with(self.sample_message.value)
        
        # Check that the producer did not send any message
        self.mock_kafka_producer.send.assert_not_called()
        
        # Check that consumer.commit was still called (we still processed the message)
        self.mock_kafka_consumer.commit.assert_called_once()
    
    def test_log_insights(self):
        """Test that insights are logged correctly."""
        # Set up some test insights
        insights = {
            "top_device_types": [("android", 10), ("ios", 5)],
            "top_locales": [("US", 8), ("RU", 7)]
        }
        
        # Call log_insights
        with patch('consumer.logger') as mock_logger:
            self.consumer.log_insights(insights)
            
            # Check that logger.info was called
            self.assertTrue(mock_logger.info.called)
    
    def test_shutdown(self):
        """Test proper shutdown."""
        # Call shutdown
        self.consumer.shutdown()
        
        # Check that resources were closed
        self.mock_kafka_producer.close.assert_called_once()
        self.mock_kafka_consumer.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
