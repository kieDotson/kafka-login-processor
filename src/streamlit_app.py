import streamlit as st
import json
import os
import time
from kafka import KafkaConsumer
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import altair as alt
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
BOOTSTRAP_SERVERS = os.environ.get('BOOTSTRAP_SERVERS', 'kafka:9092')
METRICS_TOPIC = os.environ.get('METRICS_TOPIC', 'metrics')

# Set page config
st.set_page_config(
    page_title="Real-Time Login Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Initialize session state to store our metrics
if 'metrics' not in st.session_state:
    st.session_state.metrics = {
        'device_counts': {},
        'locale_counts': {},
        'app_version_counts': {},
        'hourly_activity': {str(h): 0 for h in range(24)},
        'daily_activity': {day: 0 for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']},
        'total_processed': 0,
        'valid_count': 0,
        'invalid_count': 0,
        'recent_messages': [],
        'processing_times': [],
        'recent_errors': []
    }

# Keep track of the last refresh time
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = datetime.now()

def poll_for_metrics():
    """Poll for new metrics from Kafka"""
    logger.info(f"Polling for metrics from Kafka at {BOOTSTRAP_SERVERS}")
    try:
        # Create a consumer with a short timeout
        consumer = KafkaConsumer(
            METRICS_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='streamlit-dashboard',
            consumer_timeout_ms=5000  # Return after 5 seconds if no messages
        )
        
        # Poll for messages and process them
        message_count = 0
        max_messages = 100  # Limit the number of messages to process per poll
        
        # Get messages (non-blocking)
        messages = consumer.poll(timeout_ms=1000, max_records=max_messages)
        
        # Process any messages received
        for tp, msg_list in messages.items():
            for message in msg_list:
                metrics_data = message.value
                message_count += 1
                
                # Update our session state metrics
                if 'device_counts' in metrics_data:
                    st.session_state.metrics['device_counts'] = metrics_data['device_counts']
                
                if 'locale_counts' in metrics_data:
                    st.session_state.metrics['locale_counts'] = metrics_data['locale_counts']
                    
                if 'app_version_counts' in metrics_data:
                    st.session_state.metrics['app_version_counts'] = metrics_data['app_version_counts']
                    
                if 'hourly_activity' in metrics_data:
                    st.session_state.metrics['hourly_activity'] = metrics_data['hourly_activity']
                    
                if 'daily_activity' in metrics_data:
                    st.session_state.metrics['daily_activity'] = metrics_data['daily_activity']
                
                if 'total_processed' in metrics_data:
                    logger.info(f"Total processed {metrics_data['total_processed']}")
                    st.session_state.metrics['total_processed'] = metrics_data['total_processed']
                else:
                    logger.info(f"Noooo total processed count")

                if 'valid_count' in metrics_data:
                    logger.info(f"Valid count {metrics_data['valid_count']}")
                    st.session_state.metrics['valid_count'] = metrics_data['valid_count']
                    
                if 'invalid_count' in metrics_data:
                    logger.info(f"invalid_count {metrics_data['invalid_count']}")
                    st.session_state.metrics['invalid_count'] = metrics_data['invalid_count']
                    
                if 'recent_message' in metrics_data:
                    # Add to recent messages (keep last 10)
                    st.session_state.metrics['recent_messages'].append(metrics_data['recent_message'])
                    if len(st.session_state.metrics['recent_messages']) > 10:
                        st.session_state.metrics['recent_messages'].pop(0)
                        
                if 'processing_time' in metrics_data:
                    # Add to processing times (keep last 100)
                    st.session_state.metrics['processing_times'].append(metrics_data['processing_time'])
                    if len(st.session_state.metrics['processing_times']) > 100:
                        st.session_state.metrics['processing_times'].pop(0)
                        
                if 'error' in metrics_data:
                    # Add to recent errors (keep last 10)
                    st.session_state.metrics['recent_errors'].append({
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'error': metrics_data['error'],
                        'user_id': metrics_data['user_id'],
                        'device_id': metrics_data['device_id']
                    })
                    if len(st.session_state.metrics['recent_errors']) > 10:
                        st.session_state.metrics['recent_errors'].pop(0)
        
        # Close the consumer
        consumer.close()
        
        # Update the last refresh time
        st.session_state.last_refresh = datetime.now()
        logger.info(f"Successfully polled {message_count} messages from Kafka")
        return message_count
    except Exception as e:
        logger.error(f"Error polling Kafka messages: {e}")
        st.error(f"Error polling Kafka messages: {e}")
        return 0

# App title and header
st.title("Real-Time Login Data Dashboard")
st.markdown("### Live monitoring of user login events")

# Poll for latest metrics
messages_received = poll_for_metrics()
if messages_received > 0:
    st.success(f"Received {messages_received} new metrics updates")

# Create tabs for different visualizations
tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Device Analytics", "Regional Analytics", "Messages"])

with tab1:
    # Create metrics columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Processed Messages", st.session_state.metrics['total_processed'])

    with col2:
        st.metric("Valid Messages", st.session_state.metrics['valid_count'])
        
    with col3:
        # Calculate error rate percentage
        total = st.session_state.metrics['total_processed']
        error_rate = (st.session_state.metrics['invalid_count'] / max(1, total)) * 100
        st.metric("Invalid Messages", st.session_state.metrics['invalid_count'], f"{error_rate:.1f}%")
        
    with col4:
        # Calculate average processing time
        avg_time = 0
        if st.session_state.metrics['processing_times']:
            avg_time = sum(st.session_state.metrics['processing_times']) / len(st.session_state.metrics['processing_times'])
        st.metric("Avg Processing Time", f"{avg_time:.2f} ms")
    
    # Processing time chart
    if st.session_state.metrics['processing_times']:
        st.subheader("Processing Time (ms)")
        times_df = pd.DataFrame({
            'index': range(len(st.session_state.metrics['processing_times'])),
            'time': st.session_state.metrics['processing_times']
        })
        line_chart = alt.Chart(times_df).mark_line().encode(
            x='index',
            y='time'
        ).properties(height=200)
        st.altair_chart(line_chart, use_container_width=True)
    
    # Activity by time
    st.subheader("Activity by Hour of Day")
    
    if st.session_state.metrics['hourly_activity']:
        hourly_df = pd.DataFrame({
            'hour': [int(h) for h in st.session_state.metrics['hourly_activity'].keys()],
            'count': list(st.session_state.metrics['hourly_activity'].values())
        }).sort_values('hour')
        
        hourly_chart = alt.Chart(hourly_df).mark_bar().encode(
            x='hour:O',
            y='count'
        ).properties(height=200)
        st.altair_chart(hourly_chart, use_container_width=True)
    
    # Activity by day of week
    st.subheader("Activity by Day of Week")
    
    if st.session_state.metrics['daily_activity']:
        # Define day order
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        
        # Create DataFrame with ordered days
        daily_df = pd.DataFrame({
            'day': list(st.session_state.metrics['daily_activity'].keys()),
            'count': list(st.session_state.metrics['daily_activity'].values())
        })
        
        # Convert day to categorical with the specified order
        daily_df['day'] = pd.Categorical(daily_df['day'], categories=day_order, ordered=True)
        daily_df = daily_df.sort_values('day')
        
        daily_chart = alt.Chart(daily_df).mark_bar().encode(
            x='day:O',
            y='count'
        ).properties(height=200)
        st.altair_chart(daily_chart, use_container_width=True)

with tab2:
    # Device type distribution
    st.subheader("Device Type Distribution")
    
    if st.session_state.metrics['device_counts']:
        device_df = pd.DataFrame({
            'device': list(st.session_state.metrics['device_counts'].keys()),
            'count': list(st.session_state.metrics['device_counts'].values())
        })
        fig = px.pie(device_df, values='count', names='device', hole=.3)
        st.plotly_chart(fig, use_container_width=True)
    
    # App version distribution
    st.subheader("App Version Distribution")
    
    if st.session_state.metrics['app_version_counts']:
        version_df = pd.DataFrame({
            'version': list(st.session_state.metrics['app_version_counts'].keys()),
            'count': list(st.session_state.metrics['app_version_counts'].values())
        }).sort_values('version')
        
        version_chart = alt.Chart(version_df).mark_bar().encode(
            x='version:O',
            y='count'
        ).properties(height=300)
        st.altair_chart(version_chart, use_container_width=True)

with tab3:
    # Regional distribution
    st.subheader("Regional Distribution")
    
    if st.session_state.metrics['locale_counts']:
        # Create tabs for different region visualizations
        region_tab1, region_tab2 = st.tabs(["Map View", "Bar Chart"])
        
        with region_tab1:
            # Map states/regions to their broader region categories for map visualization
            region_mapping = {
                # US Regions
                'AL': {'region': 'South', 'name': 'Alabama', 'country': 'USA'},
                'AK': {'region': 'West', 'name': 'Alaska', 'country': 'USA'},
                'AZ': {'region': 'West', 'name': 'Arizona', 'country': 'USA'},
                'AR': {'region': 'South', 'name': 'Arkansas', 'country': 'USA'},
                'CA': {'region': 'West', 'name': 'California', 'country': 'USA'},
                'CO': {'region': 'West', 'name': 'Colorado', 'country': 'USA'},
                'CT': {'region': 'Northeast', 'name': 'Connecticut', 'country': 'USA'},
                'DE': {'region': 'South', 'name': 'Delaware', 'country': 'USA'},
                'FL': {'region': 'South', 'name': 'Florida', 'country': 'USA'},
                'GA': {'region': 'South', 'name': 'Georgia', 'country': 'USA'},
                'HI': {'region': 'West', 'name': 'Hawaii', 'country': 'USA'},
                'ID': {'region': 'West', 'name': 'Idaho', 'country': 'USA'},
                'IL': {'region': 'Midwest', 'name': 'Illinois', 'country': 'USA'},
                'IN': {'region': 'Midwest', 'name': 'Indiana', 'country': 'USA'},
                'IA': {'region': 'Midwest', 'name': 'Iowa', 'country': 'USA'},
                'KS': {'region': 'Midwest', 'name': 'Kansas', 'country': 'USA'},
                'KY': {'region': 'South', 'name': 'Kentucky', 'country': 'USA'},
                'LA': {'region': 'South', 'name': 'Louisiana', 'country': 'USA'},
                'ME': {'region': 'Northeast', 'name': 'Maine', 'country': 'USA'},
                'MD': {'region': 'South', 'name': 'Maryland', 'country': 'USA'},
                'MA': {'region': 'Northeast', 'name': 'Massachusetts', 'country': 'USA'},
                'MI': {'region': 'Midwest', 'name': 'Michigan', 'country': 'USA'},
                'MN': {'region': 'Midwest', 'name': 'Minnesota', 'country': 'USA'},
                'MS': {'region': 'South', 'name': 'Mississippi', 'country': 'USA'},
                'MO': {'region': 'Midwest', 'name': 'Missouri', 'country': 'USA'},
                'MT': {'region': 'West', 'name': 'Montana', 'country': 'USA'},
                'NE': {'region': 'Midwest', 'name': 'Nebraska', 'country': 'USA'},
                'NV': {'region': 'West', 'name': 'Nevada', 'country': 'USA'},
                'NH': {'region': 'Northeast', 'name': 'New Hampshire', 'country': 'USA'},
                'NJ': {'region': 'Northeast', 'name': 'New Jersey', 'country': 'USA'},
                'NM': {'region': 'West', 'name': 'New Mexico', 'country': 'USA'},
                'NY': {'region': 'Northeast', 'name': 'New York', 'country': 'USA'},
                'NC': {'region': 'South', 'name': 'North Carolina', 'country': 'USA'},
                'ND': {'region': 'Midwest', 'name': 'North Dakota', 'country': 'USA'},
                'OH': {'region': 'Midwest', 'name': 'Ohio', 'country': 'USA'},
                'OK': {'region': 'South', 'name': 'Oklahoma', 'country': 'USA'},
                'OR': {'region': 'West', 'name': 'Oregon', 'country': 'USA'},
                'PA': {'region': 'Northeast', 'name': 'Pennsylvania', 'country': 'USA'},
                'RI': {'region': 'Northeast', 'name': 'Rhode Island', 'country': 'USA'},
                'SC': {'region': 'South', 'name': 'South Carolina', 'country': 'USA'},
                'SD': {'region': 'Midwest', 'name': 'South Dakota', 'country': 'USA'},
                'TN': {'region': 'South', 'name': 'Tennessee', 'country': 'USA'},
                'TX': {'region': 'South', 'name': 'Texas', 'country': 'USA'},
                'UT': {'region': 'West', 'name': 'Utah', 'country': 'USA'},
                'VT': {'region': 'Northeast', 'name': 'Vermont', 'country': 'USA'},
                'VA': {'region': 'South', 'name': 'Virginia', 'country': 'USA'},
                'WA': {'region': 'West', 'name': 'Washington', 'country': 'USA'},
                'WV': {'region': 'South', 'name': 'West Virginia', 'country': 'USA'},
                'WI': {'region': 'Midwest', 'name': 'Wisconsin', 'country': 'USA'},
                'WY': {'region': 'West', 'name': 'Wyoming', 'country': 'USA'},
                'DC': {'region': 'South', 'name': 'Washington D.C.', 'country': 'USA'},
                # Countries
                'US': {'region': 'North America', 'name': 'United States', 'country': 'USA'},
                'CA': {'region': 'North America', 'name': 'Canada', 'country': 'CAN'},
                'MX': {'region': 'North America', 'name': 'Mexico', 'country': 'MEX'},
                'GB': {'region': 'Europe', 'name': 'United Kingdom', 'country': 'GBR'},
                'FR': {'region': 'Europe', 'name': 'France', 'country': 'FRA'},
                'DE': {'region': 'Europe', 'name': 'Germany', 'country': 'DEU'},
                'ES': {'region': 'Europe', 'name': 'Spain', 'country': 'ESP'},
                'IT': {'region': 'Europe', 'name': 'Italy', 'country': 'ITA'},
                'RU': {'region': 'Eurasia', 'name': 'Russia', 'country': 'RUS'},
                'CN': {'region': 'Asia', 'name': 'China', 'country': 'CHN'},
                'JP': {'region': 'Asia', 'name': 'Japan', 'country': 'JPN'},
                'IN': {'region': 'Asia', 'name': 'India', 'country': 'IND'},
                'AU': {'region': 'Oceania', 'name': 'Australia', 'country': 'AUS'},
                'NZ': {'region': 'Oceania', 'name': 'New Zealand', 'country': 'NZL'},
                'BR': {'region': 'South America', 'name': 'Brazil', 'country': 'BRA'},
                'AR': {'region': 'South America', 'name': 'Argentina', 'country': 'ARG'},
            }
            
            # Aggregate data by region
            region_counts = {}
            for locale, count in st.session_state.metrics['locale_counts'].items():
                region_info = region_mapping.get(locale, {'region': 'Other', 'name': locale, 'country': 'Unknown'})
                region = region_info['region']
                region_counts[region] = region_counts.get(region, 0) + count
            
            # Create data for choropleth map
            # First, for US states
            us_states_data = []
            for locale, count in st.session_state.metrics['locale_counts'].items():
                if locale in region_mapping and region_mapping[locale]['country'] == 'USA' and len(locale) == 2:
                    us_states_data.append({
                        'state': region_mapping[locale]['name'],
                        'code': locale,
                        'count': count
                    })
            
            # Create data for region pie chart
            region_data = []
            for region, count in region_counts.items():
                region_data.append({
                    'region': region,
                    'count': count
                })
            
            region_df = pd.DataFrame(region_data)
            
            # Create pie chart for regions
            fig = px.pie(
                region_df, 
                values='count', 
                names='region',
                title='Login Distribution by Region',
                color_discrete_sequence=px.colors.qualitative.Bold
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)
            
            # Create US map if we have US state data
            if us_states_data:
                us_states_df = pd.DataFrame(us_states_data)
                
                # Only create the map if we have actual data
                if not us_states_df.empty:
                    st.subheader("US States Distribution")
                    fig = px.choropleth(
                        us_states_df,
                        locations='code',
                        locationmode='USA-states',
                        color='count',
                        scope='usa',
                        color_continuous_scale='Viridis',
                        title='Login Counts by US State'
                    )
                    st.plotly_chart(fig, use_container_width=True)
        
        with region_tab2:
            # Get top 10 locales
            locale_items = sorted(st.session_state.metrics['locale_counts'].items(), 
                                key=lambda x: x[1], reverse=True)[:10]
            
            locale_df = pd.DataFrame({
                'locale': [item[0] for item in locale_items],
                'count': [item[1] for item in locale_items]
            })
            
            # Add full names where available
            locale_df['name'] = locale_df['locale'].apply(
                lambda x: region_mapping.get(x, {}).get('name', x)
            )
            
            # Create bar chart
            locale_chart = alt.Chart(locale_df).mark_bar().encode(
                x=alt.X('count:Q'),
                y=alt.Y('name:N', sort='-x', title='Location'),
                tooltip=['name', 'count']
            ).properties(height=min(300, 30 * len(locale_df)))
            st.altair_chart(locale_chart, use_container_width=True)
            
            # Show full locale data in a table
            with st.expander("All Locales"):
                all_locale_df = pd.DataFrame({
                    'locale': list(st.session_state.metrics['locale_counts'].keys()),
                    'count': list(st.session_state.metrics['locale_counts'].values())
                }).sort_values('count', ascending=False)
                
                # Add full names where available
                all_locale_df['name'] = all_locale_df['locale'].apply(
                    lambda x: region_mapping.get(x, {}).get('name', x)
                )
                
                st.dataframe(all_locale_df)

with tab4:
    # Recent processed messages
    st.subheader("Recent Processed Messages")
    
    if st.session_state.metrics['recent_messages']:
        for msg in reversed(st.session_state.metrics['recent_messages']):
            with st.expander(f"User: {msg.get('user_id', 'unknown user id')} | Device: {msg.get('device_type', 'unknown user id')}"):
                st.json(msg)
    
    # Recent errors
    st.subheader("Recent Errors")
    
    if st.session_state.metrics['recent_errors']:
        for error in reversed(st.session_state.metrics['recent_errors']):
            st.error(f"Timestamp: [{error['timestamp']}] - User ID: {error['user_id']} with device ID {error['device_id']} had error: {error['error']}")
    else:
        st.success("No recent errors!")

# Debug section
if st.checkbox('Show Debug Info', value=False):
    st.subheader("Kafka Connection Info")
    st.write(f"Bootstrap Servers: {BOOTSTRAP_SERVERS}")
    st.write(f"Metrics Topic: {METRICS_TOPIC}")
    
    st.subheader("Last Refresh")
    st.write(f"Last updated: {st.session_state.last_refresh.strftime('%Y-%m-%d %H:%M:%S')}")
    
    st.subheader("Raw Metrics State")
    st.json(st.session_state.metrics)

# Update interval (in seconds)
update_interval = st.slider('Refresh interval (seconds)', min_value=1, max_value=60, value=5)

# Auto-refresh the page
if st.checkbox('Enable auto-refresh', value=True):
    # Display refresh status
    refresh_placeholder = st.empty()
    refresh_placeholder.info(f"Next refresh in {update_interval} seconds... (Last updated: {st.session_state.last_refresh.strftime('%Y-%m-%d %H:%M:%S')})")
    
    # Wait for the specified interval
    time.sleep(update_interval)
    
    # Rerun the app
    st.rerun()