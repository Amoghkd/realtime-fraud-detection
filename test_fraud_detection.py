#!/usr/bin/env python3
"""
Quick test of the fraud detection pipeline end-to-end.
Tests the ML service /score endpoint with sample transactions.
"""
import requests
import json
from datetime import datetime

# Sample transactions to test
test_transactions = [
    {
        "transaction_id": "test_001",
        "card_id": "C00001",
        "merchant_id": "M1234",
        "merchant_name": "Safe Store",
        "amount": 25.50,
        "timestamp": "2023-01-01 10:30:00",
        "lat": 40.7128,
        "lon": -74.0060,
        "merchant_category": "groceries",
        "merchant_risk": 0.005,
        "txn_type": "PAYMENT",
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
        "amount": 5000.00,  # High amount - fraud indicator
        "timestamp": "2023-01-01 03:00:00",  # Late night - fraud indicator
        "lat": 40.7128,
        "lon": -74.0060,
        "merchant_category": "jewelry",
        "merchant_risk": 0.08,  # High risk category
        "txn_type": "PAYMENT",
        "amount_z": 3.5,  # High z-score - fraud indicator
        "dist_prev": 150.0,  # Far from previous txn - fraud indicator
        "tenure_days": 2.0,  # New customer
        "txn_1m": 5,  # High velocity - fraud indicator  
        "txn_1h": 8,
        "txn_24h": 15,
        "velocity_flag": 1  # Velocity flag triggered
    }
]

def test_ml_service():
    url = "http://localhost:8000/score"
    
    print("🧪 Testing Fraud Detection ML Service")
    print("=" * 50)
    
    for i, txn in enumerate(test_transactions, 1):
        print(f"\n📋 Test Transaction {i}:")
        print(f"  Amount: ${txn['amount']}")
        print(f"  Time: {txn['timestamp']}")
        print(f"  Merchant: {txn['merchant_name']} ({txn['merchant_category']})")
        print(f"  Risk indicators: z-score={txn['amount_z']}, distance={txn['dist_prev']}km, velocity_flag={txn['velocity_flag']}")
        
        try:
            response = requests.post(url, json=txn, timeout=5)
            if response.status_code == 200:
                result = response.json()
                fraud_prob = result.get('probability', 0)
                fraud_label = result.get('label', 0)
                
                print(f"  🤖 ML Result: {fraud_prob:.3f} probability ({'🚨 FRAUD' if fraud_label else '✅ SAFE'})")
                
                if fraud_prob > 0.5:
                    print(f"  ⚠️  HIGH RISK TRANSACTION DETECTED!")
                else:
                    print(f"  ✅ Transaction appears safe")
            else:
                print(f"  ❌ HTTP {response.status_code}: {response.text}")
                
        except requests.exceptions.RequestException as e:
            print(f"  ❌ Request failed: {e}")

if __name__ == "__main__":
    test_ml_service()
