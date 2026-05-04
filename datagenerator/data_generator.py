#!/usr/bin/env python3
"""
Sample data generator for the streaming challenge.
Produces page_view and ad_click events to Kafka topics with realistic scenarios:
- Out-of-order events
- Late arrivals
- Multiple clicks in attribution windows
"""

import json
import os
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


def create_producer(bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'), max_retries=10):
    """Create Kafka producer with retry logic."""
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"✓ Connected to Kafka at {bootstrap_servers}")
            return producer
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"⚠ Kafka not ready. Retrying in {wait_time}s... ({attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                raise


def generate_test_data():
    """
    Generate test dataset with edge cases for the windowed join challenge.

    Returns:
        tuple: (page_views, ad_clicks) sorted by processing time
    """
    base_time = datetime(2024, 1, 1, 12, 0, 0)

    page_views = []
    ad_clicks = []

    # User 1: Normal scenario - click before page view
    ad_clicks.append({
        'user_id': 'user_1',
        'event_time': (base_time + timedelta(minutes=5)).isoformat(),
        'campaign_id': 'campaign_A',
        'click_id': 'click_1',
        'processing_time': base_time + timedelta(minutes=5, seconds=1)
    })

    page_views.append({
        'user_id': 'user_1',
        'event_time': (base_time + timedelta(minutes=10)).isoformat(),
        'url': 'https://example.com/product1',
        'event_id': 'pv_1',
        'processing_time': base_time + timedelta(minutes=10, seconds=2)
    })

    # User 2: Click arrives AFTER page view (out of order, within lateness)
    page_views.append({
        'user_id': 'user_2',
        'event_time': (base_time + timedelta(minutes=15)).isoformat(),
        'url': 'https://example.com/product2',
        'event_id': 'pv_2',
        'processing_time': base_time + timedelta(minutes=15, seconds=1)
    })

    ad_clicks.append({
        'user_id': 'user_2',
        'event_time': (base_time + timedelta(minutes=12)).isoformat(),
        'campaign_id': 'campaign_B',
        'click_id': 'click_2',
        'processing_time': base_time + timedelta(minutes=16, seconds=0)  # Late arrival
    })

    # User 3: Multiple clicks in window - should pick latest
    ad_clicks.append({
        'user_id': 'user_3',
        'event_time': (base_time + timedelta(minutes=20)).isoformat(),
        'campaign_id': 'campaign_C',
        'click_id': 'click_3a',
        'processing_time': base_time + timedelta(minutes=20, seconds=1)
    })

    ad_clicks.append({
        'user_id': 'user_3',
        'event_time': (base_time + timedelta(minutes=25)).isoformat(),
        'campaign_id': 'campaign_D',
        'click_id': 'click_3b',
        'processing_time': base_time + timedelta(minutes=25, seconds=1)
    })

    page_views.append({
        'user_id': 'user_3',
        'event_time': (base_time + timedelta(minutes=30)).isoformat(),
        'url': 'https://example.com/product3',
        'event_id': 'pv_3',
        'processing_time': base_time + timedelta(minutes=30, seconds=2)
    })

    # User 4: Click outside 30-minute window - should NOT be attributed
    ad_clicks.append({
        'user_id': 'user_4',
        'event_time': (base_time + timedelta(minutes=35)).isoformat(),
        'campaign_id': 'campaign_E',
        'click_id': 'click_4',
        'processing_time': base_time + timedelta(minutes=35, seconds=1)
    })

    page_views.append({
        'user_id': 'user_4',
        'event_time': (base_time + timedelta(minutes=70)).isoformat(),  # 35 minutes later
        'url': 'https://example.com/product4',
        'event_id': 'pv_4',
        'processing_time': base_time + timedelta(minutes=70, seconds=2)
    })

    # User 5: Very late event (beyond allowed lateness) - should be dropped
    ad_clicks.append({
        'user_id': 'user_5',
        'event_time': (base_time + timedelta(minutes=40)).isoformat(),
        'campaign_id': 'campaign_F',
        'click_id': 'click_5',
        'processing_time': base_time + timedelta(minutes=50, seconds=0)  # 10 min late (beyond 2 min lateness)
    })

    page_views.append({
        'user_id': 'user_5',
        'event_time': (base_time + timedelta(minutes=45)).isoformat(),
        'url': 'https://example.com/product5',
        'event_id': 'pv_5',
        'processing_time': base_time + timedelta(minutes=45, seconds=2)
    })

    # User 6: No click - page view with no attribution
    page_views.append({
        'user_id': 'user_6',
        'event_time': (base_time + timedelta(minutes=80)).isoformat(),
        'url': 'https://example.com/product6',
        'event_id': 'pv_6',
        'processing_time': base_time + timedelta(minutes=80, seconds=1)
    })

    return page_views, ad_clicks


def generate_malformed_events():
    """
    Generate malformed events to test dead-letter topic (DLT) routing.
    These should never be processed — the consumer logs the parse error
    and the error handler routes them to ad_clicks.DLT / page_views.DLT.
    """
    base_time = datetime(2024, 1, 1, 12, 0, 0)

    return [
        # Wrong type for event_time (integer instead of string)
        #('ad_clicks', {'user_id': 'user_bad_1', 'event_time': 99999, 'campaign_id': 'campaign_X', 'click_id': 'bad_click_1'}),
        # Missing required field (no event_time)
        # ('page_views', {'user_id': 'user_bad_2', 'url': 'https://example.com/bad', 'event_id': 'bad_pv_1'}),
        # Completely invalid JSON structure
        ('ad_clicks', 'not-a-json-object'),
    ]


def send_events_in_order(producer, page_views, ad_clicks):
    """Send events in processing time order to simulate real streaming."""

    # Combine and sort by processing time
    all_events = []
    for pv in page_views:
        all_events.append(('page_views', pv))
    for click in ad_clicks:
        all_events.append(('ad_clicks', click))

    all_events.sort(key=lambda x: x[1]['processing_time'])

    print("\n📤 Sending events to Kafka in processing order...")
    print("=" * 80)

    for topic, event in all_events:
        # Remove processing_time before sending (metadata only for generator)
        event_copy = {k: v for k, v in event.items() if k != 'processing_time'}

        # Create partition key based on user_id for consistent partitioning
        partition_key = event.get('user_id').encode('utf-8')

        future = producer.send(
            topic,
            key=partition_key,
            value=event_copy
        )

        # Wait for confirmation
        record_metadata = future.get(timeout=10)

        print(f"✓ Sent to {topic} (partition {record_metadata.partition}, offset {record_metadata.offset})")
        print(f"  {json.dumps(event_copy, indent=2)}")
        print("-" * 80)

        # Small delay to simulate real-time streaming
        time.sleep(0.1)

    producer.flush()
    print("\n✓ All events sent successfully!")


def send_malformed_events(bootstrap_servers, malformed_events):
    """Send malformed events to test DLT routing.

    Uses a raw (no-serializer) producer so invalid payloads are sent as-is
    without being re-encoded by the standard json.dumps serializer.
    """
    raw_producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    print("\n⚠️  Sending malformed events to test dead-letter topic routing...")
    print("=" * 80)

    for topic, payload in malformed_events:
        partition_key = b'user_bad'
        value = json.dumps(payload).encode('utf-8') if isinstance(payload, dict) else payload.encode('utf-8')

        future = raw_producer.send(topic, key=partition_key, value=value)
        record_metadata = future.get(timeout=10)

        print(f"✓ Sent malformed event to {topic} (partition {record_metadata.partition}, offset {record_metadata.offset})")
        print(f"  {payload}")
        print("-" * 80)

        time.sleep(0.1)

    raw_producer.flush()
    raw_producer.close()
    print("\n✓ Malformed events sent — check ad_clicks.DLT and page_views.DLT")


def main():
    """Main function to generate and send test data."""
    print("🚀 Starting data generator for streaming challenge...")

    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

    # Create producer
    producer = create_producer(bootstrap_servers)

    # Generate and send well-formed test data
    page_views, ad_clicks = generate_test_data()
    print(f"\n📊 Generated {len(page_views)} page views and {len(ad_clicks)} ad clicks")
    send_events_in_order(producer, page_views, ad_clicks)
    producer.close()

    # Send malformed events to exercise DLT routing
    malformed = generate_malformed_events()
    send_malformed_events(bootstrap_servers, malformed)

    print("\n" + "=" * 80)
    print("✅ Data generation complete!")
    print("\nTopics created:")
    print("  - page_views: Page view events")
    print("  - ad_clicks: Ad click events")
    print("\nExpected behavior:")
    print("  - pv_1 (user_1): Should attribute to click_1 (campaign_A)")
    print("  - pv_2 (user_2): Should attribute to click_2 (campaign_B) - late click")
    print("  - pv_3 (user_3): Should attribute to click_3b (campaign_D) - latest click")
    print("  - pv_4 (user_4): Should NOT attribute - click too old (>30 min)")
    print("  - pv_5 (user_5): Depends on lateness handling - click may be dropped")
    print("  - pv_6 (user_6): Should NOT attribute - no click exists")
    print("\nMalformed events:")
    print("  - bad_click_1: Wrong type for event_time → routed to ad_clicks.DLT")
    print("  - bad_pv_1:    Missing event_time field → routed to page_views.DLT")
    print("  - (raw string): Invalid JSON → routed to ad_clicks.DLT")
    print("=" * 80)


if __name__ == '__main__':
    main()
