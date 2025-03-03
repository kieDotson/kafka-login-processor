#!/usr/bin/env python3
import os
import sys
import unittest
from unittest.mock import MagicMock, patch
import json
import time

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from processor import LoginDataProcessor, DataValidator, DataEnricher, DataAnalyzer


class TestDataValidator(unittest.TestCase):
    """Test cases for the DataValidator class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.validator = DataValidator()
        
        # Sample valid message
        self.valid_message = {
            "user_id": "424cdd21-063a-43a7-b91b-7ca1a833afae",
            "app_version": "2.3.0",
            "device_type": "android",
            "ip": "199.172.111.135",
            "locale": "RU",
            "device_id": "593-47-5928",
            "timestamp": "1694479551"
        }
    
    def test_validate_schema_valid(self):
        """Test that a valid message passes validation."""
        is_valid, reason = self.validator.validate_schema(self.valid_message)
        self.assertTrue(is_valid)
        self.assertEqual(reason, "Validation passed")
    
    def test_validate_schema_missing_field(self):
        """Test that a message with a missing field fails validation."""
        invalid_message = self.valid_message.copy()
        del invalid_message['user_id']
        
        is_valid, reason = self.validator.validate_schema(invalid_message)
        self.assertFalse(is_valid)
        self.assertTrue("Missing required fields" in reason)
    
    def test_validate_schema_invalid_timestamp(self):
        """Test that a message with an invalid timestamp fails validation."""
        invalid_message = self.valid_message.copy()
        invalid_message['timestamp'] = "not_a_timestamp"
        
        is_valid, reason = self.validator.validate_schema(invalid_message)
        self.assertFalse(is_valid)
        self.assertTrue("Invalid timestamp format" in reason)
    
    def test_validate_schema_invalid_ip(self):
        """Test that a message with an invalid IP fails validation."""
        invalid_message = self.valid_message.copy()
        invalid_message['ip'] = "not_an_ip"
        
        is_valid, reason = self.validator.validate_schema(invalid_message)
        self.assertFalse(is_valid)
        self.assertTrue("Invalid IP address format" in reason)


class TestDataEnricher(unittest.TestCase):
    """Test cases for the DataEnricher class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.enricher = DataEnricher()
        
        # Sample message
        self.message = {
            "user_id": "424cdd21-063a-43a7-b91b-7ca1a833afae",
            "app_version": "2.3.0",
            "device_type": "android",
            "ip": "199.172.111.135",
            "locale": "RU",
            "device_id": "593-47-5928",
            "timestamp": "1694479551"
        }
    
    def test_enrich(self):
        """Test that enrichment adds the expected fields."""
        enriched = self.enricher.enrich(self.message)
        
        # Check that original fields are preserved
        for key in self.message:
            self.assertIn(key, enriched)
        
        # Check that new fields are added
        self.assertIn('processed_timestamp', enriched)
        self.assertIn('readable_login_time', enriched)
        self.assertIn('processing_delay_seconds', enriched)
        self.assertIn('day_of_week', enriched)
        self.assertIn('hour_of_day', enriched)
        self.assertIn('region', enriched)
        
        # Check app version parsing
        self.assertEqual(enriched['app_version_major'], 2)
        self.assertEqual(enriched['app_version_minor'], 3)
        self.assertEqual(enriched['app_version_patch'], 0)


class TestDataAnalyzer(unittest.TestCase):
    """Test cases for the DataAnalyzer class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.analyzer = DataAnalyzer()
        
        # Sample enriched message
        self.enriched_message = {
            "user_id": "424cdd21-063a-43a7-b91b-7ca1a833afae",
            "app_version": "2.3.0",
            "device_type": "android",
            "ip": "199.172.111.135",
            "locale": "RU",
            "device_id": "593-47-5928",
            "timestamp": "1694479551",
            "processed_timestamp": 1694479700,
            "readable_login_time": "2023-09-12 15:32:31",
            "processing_delay_seconds": 149,
            "day_of_week": "Tuesday",
            "hour_of_day": 15,
            "region": "Europe/Asia"
        }
    
    def test_analyze(self):
        """Test that analyze updates metrics correctly."""
        self.analyzer.analyze(self.enriched_message)
        
        # Check that metrics are updated
        self.assertEqual(self.analyzer.metrics['device_type_counts']['android'], 1)
        self.assertEqual(self.analyzer.metrics['locale_counts']['RU'], 1)
        self.assertEqual(self.analyzer.metrics['app_version_counts']['2.3.0'], 1)
        self.assertEqual(self.analyzer.metrics['hourly_activity']['15'], 1)
        self.assertEqual(self.analyzer.metrics['daily_activity']['Tuesday'], 1)
    
    def test_get_insights(self):
        """Test that insights are generated correctly."""
        # Add multiple messages with different values
        self.analyzer.analyze(self.enriched_message)
        
        # Change some fields and analyze again
        message2 = self.enriched_message.copy()
        message2['device_type'] = 'ios'
        message2['locale'] = 'US'
        message2['app_version'] = '2.4.0'
        message2['hour_of_day'] = 16
        message2['day_of_week'] = 'Monday'
        self.analyzer.analyze(message2)
        
        # Get insights
        insights = self.analyzer.get_insights()
        
        # Check that insights contain expected sections
        self.assertIn('top_device_types', insights)
        self.assertIn('top_locales', insights)
        self.assertIn('top_app_versions', insights)
        self.assertIn('peak_hours', insights)
        self.assertIn('peak_days', insights)
        
        # Check that the insights contain the correct data
        device_types = dict(insights['top_device_types'])
        self.assertEqual(device_types['android'], 1)
        self.assertEqual(device_types['ios'], 1)


class TestLoginDataProcessor(unittest.TestCase):
    """Test cases for the LoginDataProcessor class."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock the KafkaProducer
        with patch('processor.KafkaProducer') as mock_producer:
            self.processor = LoginDataProcessor(bootstrap_servers='mock:9092')
            # Manually set the metrics producer
            self.processor.metrics_producer = MagicMock()
        
        # Sample message
        self.valid_message = {
            "user_id": "424cdd21-063a-43a7-b91b-7ca1a833afae",
            "app_version": "2.3.0",
            "device_type": "android",
            "ip": "199.172.111.135",
            "locale": "RU",
            "device_id": "593-47-5928",
            "timestamp": "1694479551"
        }
    
    def test_process_valid_message(self):
        """Test processing a valid message."""
        # Process the message
        processed_message, is_valid, _ = self.processor.process(self.valid_message)
        
        # Check that processing was successful
        self.assertTrue(is_valid)
        self.assertIsNotNone(processed_message)
        
        # Check that metrics were incremented
        self.assertEqual(self.processor.metrics['total_processed'], 1)
        self.assertEqual(self.processor.metrics['valid_processed'], 1)
        self.assertEqual(self.processor.metrics['invalid_messages'], 0)
        
        # Check that enriched fields were added
        self.assertIn('processed_timestamp', processed_message)
        self.assertIn('readable_login_time', processed_message)
        self.assertIn('region', processed_message)
    
    def test_process_invalid_message(self):
        """Test processing an invalid message."""
        # Create an invalid message
        invalid_message = self.valid_message.copy()
        del invalid_message['user_id']
        
        # Process the message
        processed_message, is_valid, error_info = self.processor.process(invalid_message)
        
        # Check that processing failed as expected
        self.assertFalse(is_valid)
        self.assertIsNone(processed_message)
        self.assertIn('error', error_info)
        
        # Check that metrics were incremented correctly
        self.assertEqual(self.processor.metrics['total_processed'], 1)
        self.assertEqual(self.processor.metrics['valid_processed'], 0)
        self.assertEqual(self.processor.metrics['invalid_messages'], 1)
        
        # Check that error metrics were sent to Kafka
        self.processor.metrics_producer.send.assert_called()


if __name__ == '__main__':
    unittest.main()