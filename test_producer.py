#!/usr/bin/env python3
"""
Test producer for fraud detection pipeline.
Sends enriched transactions directly to transactions.enriched topic.
"""
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import os

def create_test_transactions():
    """Create sample enriched transactions for testing"""
    return [
        {
            "transaction_id": "test_001",
            "card_id": "C00001",
            "merchant_id": "M1234",
            "merchant_name": "Safe Store",
            "amount": 25.50,
            "timestamp": "2023-01-01T10:30:00",
            "lat": 40.7128,
            "lon": -74.0060,
            "merchant_category": "groceries",
            "merchant_risk": 0.005,
            "txn_type": "PAYMENT",
            # Enriched features from Spark
            "amount_z": 0.2,
            "dist_prev": 1.5,
            "tenure_days": 30.0,
            "txn_1m": 1,
            "txn_1h": 3,
            "txn_24h": 12,
            "velocity_flag": 0
        },
        {
            "transaction_id": "test_002",
            "card_id": "C00002",
            "merchant_id": "M5678",
            "merchant_name": "Suspicious Shop",
            "amount": 5000.00,
            "timestamp": "2023-01-01T03:00:00",
            "lat": 40.7128,
            "lon": -74.0060,
            "merchant_category": "jewelry",
            "merchant_risk": 0.08,
            "txn_type": "PAYMENT",
            # Enriched features - high risk indicators
            "amount_z": 3.5,
            "dist_prev": 150.0,
            "tenure_days": 2.0,
            "txn_1m": 5,
            "txn_1h": 8,
            "txn_24h": 15,
            "velocity_flag": 1
        },
        {
            "transaction_id": "test_003",
            "card_id": "C00003",
            "merchant_id": "M9999",
            "merchant_name": "Online Store",
            "amount": 150.00,
            "timestamp": "2023-01-01T14:15:00",
            "lat": 40.7128,
            "lon": -74.0060,
            "merchant_category": "online",
            "merchant_risk": 0.02,
            "txn_type": "PAYMENT",
            # Normal transaction
            "amount_z": -0.1,
            "dist_prev": 0.0,
            "tenure_days": 180.0,
            "txn_1m": 2,
            "txn_1h": 1,
            "txn_24h": 5,
            "velocity_flag": 0
        }
    ]

def main():
    bootstrap_servers = 'localhost:9092'  # Force localhost for local testing
    
    print(f"🔗 Connecting to Kafka at: {bootstrap_servers}")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],  # Force localhost
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=5000,  # Shorter timeout
            retries=1,
            max_in_flight_requests_per_connection=1
        )
        
        # Test connection
        print("🔍 Testing Kafka connection...")
        producer.send('transactions.enriched', {'test': 'connection'})
        producer.flush()
        print("✅ Kafka connection successful!")
        
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        print("💡 Make sure Kafka is running and accessible")
        return
    
    transactions = create_test_transactions()
    
    print("🚀 Sending test transactions to Kafka...")
    
    for i, txn in enumerate(transactions, 1):
        try:
            future = producer.send('transactions.enriched', txn)
            # Wait for acknowledgment
            record_metadata = future.get(timeout=5)
            print(f"✅ Sent transaction {i}: {txn['transaction_id']} - ${txn['amount']} (partition: {record_metadata.partition})")
            time.sleep(0.1)  # Small delay between messages
        except Exception as e:
            print(f"❌ Failed to send transaction {i}: {e}")
    
    producer.close()
    
    print("🎉 All test transactions sent!")
    print("📊 Check the fraud detector logs for predictions...")

if __name__ == "__main__":
    main()
