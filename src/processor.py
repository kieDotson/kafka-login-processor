#!/usr/bin/env python3
"""
Data processor module for Kafka login data.
This module handles validation, enrichment, and analytics for login data.
"""
import logging
import time
import json
from datetime import datetime
import ipaddress
import re
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataValidator:
    """
    Validates data against schema and business rules.
    """
    
    def __init__(self):
        """Initialize the validator."""
        # Regular expression patterns for validation
        self.patterns = {
            'user_id': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'),
            'app_version': re.compile(r'^\d+\.\d+\.\d+$'),
            'device_id': re.compile(r'^[A-Za-z0-9\-]+$')
        }
        
        # Valid device types
        self.valid_device_types = {'android', 'ios', 'web', 'desktop', 'iOS'}
        
        # US states and territories
        self.us_states = {
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
            'DC', 'PR', 'VI', 'GU', 'AS', 'MP'
        }
    
    def validate_schema(self, message):
        """
        Validate that a message contains all required fields and valid data.
        
        Args:
            message (dict): The message to validate
            
        Returns:
            tuple: (is_valid, reason) indicating if the message is valid and why it might not be
        """
        # Extract identifying information once
        user_id = message.get('user_id', 'unknown user id')
        device_id = message.get('device_id', 'unknown device id')
        timestamp = message.get('timestamp', 'unknown timestamp')
        
        # Check all required fields at once
        required_fields = {'user_id', 'app_version', 'device_type', 'ip', 'locale', 'device_id', 'timestamp'}
        missing_fields = required_fields - set(message.keys())
        
        if missing_fields:
            missing_list = ', '.join(missing_fields)
            logger.error(f"Missing fields: {missing_list} for message with user_id: {user_id} and device_id: {device_id} at {timestamp}")
            return False, f"Missing required fields: {missing_list}"
        
        # Validate timestamp
        try:
            ts = int(message['timestamp'])
            if ts <= 0:
                logger.error(f"Invalid timestamp value: {ts} for message with user_id: {user_id} and device_id: {device_id} at {timestamp}")
                return False, "Invalid timestamp value"
                
            # Check if timestamp is in the future (with some tolerance)
            current_time = int(time.time())
            if ts > current_time + 3600:  # Allow 1 hour tolerance for clock skew
                logger.error(f"Future timestamp detected: {ts} for message with user_id: {user_id} and device_id: {device_id} at {timestamp}")
                return False, "Timestamp is in the future"
        except (ValueError, TypeError):
            logger.error(f"Invalid timestamp format: {timestamp} for message with user_id: {user_id} and device_id: {device_id} at {timestamp}")
            return False, "Invalid timestamp format"
        
        # Validate IP address
        try:
            ipaddress.ip_address(message['ip'])
        except ValueError:
            logger.error(f"Invalid IP address: {message['ip']} |for message with user_id: {user_id} and device_id: {device_id} at {timestamp}")
            return False, "Invalid IP address format"
        
        # Validate device type
        device_type = message['device_type'].lower()
        if device_type not in self.valid_device_types and message['device_type'] not in self.valid_device_types:
            logger.warning(f"Unrecognized device type: {message['device_type']} for message with user_id: {user_id} and device_id: {device_id} at {timestamp}")
            # Not returning False as we'll still process unrecognized device types
        
        # Validate locale
        locale = message['locale']
        if locale not in self.us_states:
            logger.warning(f"Unknown locale: {locale} for message with user_id: {user_id} and device_id: {device_id} at {timestamp}")
            # Not returning False as we'll still process unknown locales
        
        # Validate user_id format (UUID) if it matches the pattern
        if not isinstance(user_id, str) or (user_id != 'unknown user id' and not self.patterns['user_id'].match(user_id)):
            logger.warning(f"User ID not in UUID format: {user_id} for message with device_id: {device_id} at {timestamp}")
            # Not returning False as we'll still process non-standard user IDs
        
        # Validate app_version format (x.y.z)
        if not self.patterns['app_version'].match(message['app_version']):
            logger.warning(f"App version not in format x.y.z: {message['app_version']} for message with user_id: {user_id} and device_id: {device_id} at {timestamp}")
            # Not returning False as we'll still process non-standard versions
        
        return True, "Validation passed"


class DataEnricher:
    """
    Enriches data with additional information and derived fields.
    """
    
    def __init__(self):
        """Initialize the enricher."""
        # US regions mapping
        self.us_regions = {
            'CT': 'Northeast', 'ME': 'Northeast', 'MA': 'Northeast', 'NH': 'Northeast', 
            'RI': 'Northeast', 'VT': 'Northeast', 'NJ': 'Northeast', 'NY': 'Northeast', 'PA': 'Northeast',
            'IL': 'Midwest', 'IN': 'Midwest', 'MI': 'Midwest', 'OH': 'Midwest', 'WI': 'Midwest',
            'IA': 'Midwest', 'KS': 'Midwest', 'MN': 'Midwest', 'MO': 'Midwest', 'NE': 'Midwest', 
            'ND': 'Midwest', 'SD': 'Midwest',
            'DE': 'South', 'FL': 'South', 'GA': 'South', 'MD': 'South', 'NC': 'South', 'SC': 'South',
            'VA': 'South', 'DC': 'South', 'WV': 'South', 'AL': 'South', 'KY': 'South', 'MS': 'South',
            'TN': 'South', 'AR': 'South', 'LA': 'South', 'OK': 'South', 'TX': 'South',
            'AZ': 'West', 'CO': 'West', 'ID': 'West', 'MT': 'West', 'NV': 'West', 'NM': 'West',
            'UT': 'West', 'WY': 'West', 'AK': 'West', 'CA': 'West', 'HI': 'West', 'OR': 'West', 'WA': 'West'
        }
        
    
    def enrich(self, message):
        """
        Enrich a message with additional data and derived fields.
        
        Args:
            message (dict): The message to enrich
            
        Returns:
            dict: The enriched message
        """
        # Create a copy of the message to avoid modifying the original
        enriched = message.copy()
        
        # Add processing timestamp
        processing_time = int(time.time())
        enriched['processed_timestamp'] = processing_time
        
        # Convert timestamp to readable format
        try:
            login_time = datetime.fromtimestamp(int(message['timestamp']))
            enriched['readable_login_time'] = login_time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Add additional time-based fields
            enriched['day_of_week'] = login_time.strftime('%A')
            enriched['hour_of_day'] = login_time.hour
            
            # Calculate processing delay
            enriched['processing_delay_seconds'] = processing_time - int(message['timestamp'])
        except (ValueError, TypeError):
            logger.warning(f"Could not convert timestamp: {message.get('timestamp')} for message with user_id: {message.get('user_id')} and device_id: {message.get('device_id')}")
            enriched['readable_login_time'] = "unknown login time"
            enriched['processing_delay_seconds'] = 0
            enriched['day_of_week'] = "unknown day of the week"
            enriched['hour_of_day'] = 0
        
        # Parse app version
        try:
            version_parts = message['app_version'].split('.')
            if len(version_parts) >= 3:
                enriched['app_version_major'] = int(version_parts[0])
                enriched['app_version_minor'] = int(version_parts[1])
                enriched['app_version_patch'] = int(version_parts[2])
        except (ValueError, IndexError):
            logger.warning(f"Could not parse app version: {message.get('app_version')} for message with user_id: {message.get('user_id')} and device_id: {message.get('device_id')} at timestamp:{message['timestamp']}")
            enriched['app_version_major'] = 0
            enriched['app_version_minor'] = 0
            enriched['app_version_patch'] = 0
        
        # Add geographic region based on locale
        locale = message.get('locale', '')
        
        # First check if it's a US state
        if locale in self.us_regions:
            enriched['region'] = self.us_regions[locale]
        else:
            enriched['region'] = 'Other'
        
        return enriched


class DataAnalyzer:
    """
    Analyzes data to extract metrics and insights.
    """
    
    def __init__(self):
        """Initialize the analyzer."""
        self.metrics = {
            'device_type_counts': {},
            'locale_counts': {},
            'app_version_counts': {},
            'hourly_activity': {str(h): 0 for h in range(24)},
            'daily_activity': {day: 0 for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']}
        }
    
    def analyze(self, message):
        """
        Analyze a message and update metrics.
        
        Args:
            message (dict): The message to analyze
            
        Returns:
            dict: Analysis metrics for this message
        """
        # Extract fields for analysis
        device_type = message.get('device_type', 'unknown')
        locale = message.get('locale', 'unknown')
        app_version = message.get('app_version', 'unknown')
        
        # Update device type metrics (normalize to lowercase for consistency)
        device_type_key = device_type.lower() if isinstance(device_type, str) else str(device_type)
        self.metrics['device_type_counts'][device_type_key] = \
            self.metrics['device_type_counts'].get(device_type_key, 0) + 1
        
        # Update locale metrics
        self.metrics['locale_counts'][locale] = \
            self.metrics['locale_counts'].get(locale, 0) + 1
        
        # Update app version metrics
        self.metrics['app_version_counts'][app_version] = \
            self.metrics['app_version_counts'].get(app_version, 0) + 1
        
        # Update time-based metrics
        if 'hour_of_day' in message and isinstance(message['hour_of_day'], int):
            hour = str(message['hour_of_day'])
            self.metrics['hourly_activity'][hour] = \
                self.metrics['hourly_activity'].get(hour, 0) + 1
        
        if 'day_of_week' in message and isinstance(message['day_of_week'], str):
            day = message['day_of_week']
            self.metrics['daily_activity'][day] = \
                self.metrics['daily_activity'].get(day, 0) + 1
        
        # Return message-specific analysis
        return {
            'device_type': device_type,
            'locale': locale,
            'app_version': app_version
        }
    
    def get_insights(self):
        """Get current insights from accumulated metrics."""
        insights = {}
        
        # Get top device types
        if self.metrics['device_type_counts']:
            sorted_devices = sorted(
                self.metrics['device_type_counts'].items(), 
                key=lambda x: x[1], 
                reverse=True
            )
            insights['top_device_types'] = sorted_devices[:3]
        
        # Get top locales
        if self.metrics['locale_counts']:
            sorted_locales = sorted(
                self.metrics['locale_counts'].items(), 
                key=lambda x: x[1], 
                reverse=True
            )
            insights['top_locales'] = sorted_locales[:3]
        
        # Get top app versions
        if self.metrics['app_version_counts']:
            sorted_versions = sorted(
                self.metrics['app_version_counts'].items(), 
                key=lambda x: x[1], 
                reverse=True
            )
            insights['top_app_versions'] = sorted_versions[:3]
        
        # Get peak activity hours
        if any(self.metrics['hourly_activity'].values()):
            sorted_hours = sorted(
                self.metrics['hourly_activity'].items(), 
                key=lambda x: x[1], 
                reverse=True
            )
            insights['peak_hours'] = sorted_hours[:3]
        
        # Get peak activity days
        if any(self.metrics['daily_activity'].values()):
            sorted_days = sorted(
                self.metrics['daily_activity'].items(), 
                key=lambda x: x[1], 
                reverse=True
            )
            insights['peak_days'] = sorted_days[:3]
        
        return insights


class LoginDataProcessor:
    """
    Main processor class that orchestrates validation, enrichment, and analysis.
    """
    
    def __init__(self, bootstrap_servers="kafka:9092"):
        """Initialize the processor with its components."""
        self.validator = DataValidator()
        self.enricher = DataEnricher()
        self.analyzer = DataAnalyzer()
        self.bootstrap_servers = bootstrap_servers
        
        # Processing metrics
        self.metrics = {
            'total_processed': 0,
            'valid_processed': 0,
            'invalid_messages': 0,
            'processing_times_ms': []
        }
        
        # Initialize Kafka producer for metrics
        try:
            self.metrics_producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info(f"Metrics producer connected to {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to set up metrics producer: {e}")
            self.metrics_producer = None
    
    def process(self, message):
        """
        Process a single message: validate, enrich, and analyze.
        
        Args:
            message (dict): The message to process
            
        Returns:
            tuple: (processed_message, is_valid, insights)
        """
        start_time = time.time()
        self.metrics['total_processed'] += 1
        
        # Validate message
        is_valid, reason = self.validator.validate_schema(message)
        
        if not is_valid:
            self.metrics['invalid_messages'] += 1
            processing_time_ms = (time.time() - start_time) * 1000
            self.metrics['processing_times_ms'].append(processing_time_ms)
            
            # Send error metrics to Kafka for Streamlit
            try:
                error_message = {
                    'error': reason,
                    'user_id': message.get('user_id', 'unknown user id'),
                    'device_id': message.get('device_id', 'unknown device id'),
                    'timestamp': message.get('timestamp', 'unknown timestamp')
                }
                if self.metrics_producer:
                    self.metrics_producer.send('metrics', error_message)
            except Exception as e:
                logger.error(f"Error sending validation error to Kafka: {e}")
                
            return None, False, {"error": reason}
        
        try:
            # Enrich message
            enriched_message = self.enricher.enrich(message)
            
            # Analyze message
            self.analyzer.analyze(enriched_message)
            
            # Track processing time
            processing_time_ms = (time.time() - start_time) * 1000
            self.metrics['processing_times_ms'].append(processing_time_ms)
            
            # Update metrics
            self.metrics['valid_processed'] += 1
            
            # Calculate average processing time
            if self.metrics['processing_times_ms']:
                avg_processing_time = sum(self.metrics['processing_times_ms']) / len(self.metrics['processing_times_ms'])
                # Keep only the last 1000 processing times to avoid memory growth
                if len(self.metrics['processing_times_ms']) > 1000:
                    self.metrics['processing_times_ms'] = self.metrics['processing_times_ms'][-1000:]
            else:
                avg_processing_time = 0
            
            # Send metrics to Kafka for Streamlit periodically
            if self.metrics['valid_processed'] % 10 == 0:
                insights = self.analyzer.get_insights()
                logger.info(f"Current insights: {json.dumps(insights, indent=2)}")
                
                # Send metrics to Kafka for Streamlit
                metrics_message = {
                    'device_counts': self.analyzer.metrics['device_type_counts'],
                    'locale_counts': self.analyzer.metrics['locale_counts'],
                    'app_version_counts': self.analyzer.metrics['app_version_counts'],
                    'hourly_activity': self.analyzer.metrics['hourly_activity'],
                    'daily_activity': self.analyzer.metrics['daily_activity'],
                    'total_processed': self.metrics['total_processed'],
                    'valid_count': self.metrics['valid_processed'],
                    'invalid_count': self.metrics['invalid_messages'],
                    'processing_time': avg_processing_time,
                    'recent_message': enriched_message
                }
                
                try:
                    if self.metrics_producer:
                        self.metrics_producer.send('metrics', metrics_message)
                except Exception as e:
                    logger.error(f"Error sending metrics to Kafka: {e}")
            
            # Always send the latest message to keep the dashboard updated
            # (separate from the periodic bulk metrics update)
            try:
                if self.metrics_producer:
                    self.metrics_producer.send('metrics', {
                        'recent_message': enriched_message,
                        'processing_time': processing_time_ms
                    })
            except Exception as e:
                logger.error(f"Error sending message update to Kafka: {e}")
            
            return enriched_message, True, "Processing successful"
        except Exception as e:
            logger.error(f"Processing error: {e}")
            self.metrics['processing_errors'] += 1
            processing_time_ms = (time.time() - start_time) * 1000
            self.metrics['processing_times_ms'].append(processing_time_ms)
            
            # Send error= to Kafka for Streamlit
            try:
                error_message = {
                    'error': str(e),
                    'user_id': message.get('user_id', 'unknown user id'),
                    'device_id': message.get('device_id', 'unknown device id'),
                    'timestamp': message.get('timestamp', 'unknown timestamp')
                }
                if self.metrics_producer:
                    self.metrics_producer.send('metrics', error_message)
            except Exception as e2:
                logger.error(f"Error sending error metrics to Kafka: {e2}")
                
            return None, False, f"Processing error: {str(e)}"


# For testing
if __name__ == "__main__":
    # Configure logging for testing
    logging.basicConfig(level=logging.INFO)
    
    # Sample message
    sample_message = {
        "user_id": "424cdd21-063a-43a7-b91b-7ca1a833afae",
        "app_version": "2.3.0",
        "device_type": "android",
        "ip": "199.172.111.135",
        "locale": "RU",
        "device_id": "593-47-5928",
        "timestamp": "1694479551"
    }
    
    # Create processor and process the message
    processor = LoginDataProcessor(bootstrap_servers="localhost:9092")
    result, valid, insights = processor.process(sample_message)
    
    print(f"Valid: {valid}")
    if valid:
        print(f"Processed message: {json.dumps(result, indent=2)}")
    else:
        print(f"Error: {insights}")