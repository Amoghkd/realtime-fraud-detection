#!/usr/bin/env python3
"""
PRODUCTION Transaction Generator - Real-world simulation
Generates realistic transaction data WITHOUT fraud labels (as it should be in production)
"""
import argparse
import json
import random
import uuid
from datetime import datetime
import numpy as np
from kafka import KafkaProducer
import time

# Merchant categories and risk profiles
MERCHANT_CATEGORIES = [
    "groceries", "restaurants", "gas_stations", "retail", "online", 
    "travel", "entertainment", "healthcare", "utilities", "other"
]

MERCHANT_RISK = {
    "groceries": 0.005,
    "restaurants": 0.01,
    "gas_stations": 0.008,
    "retail": 0.015,
    "online": 0.03,
    "travel": 0.05,
    "entertainment": 0.02,
    "healthcare": 0.005,
    "utilities": 0.002,
    "other": 0.02
}

TRANSACTION_TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"]

class TransactionAgent:
    def __init__(self, card_id, region, spending_profile="normal"):
        self.card_id = card_id
        self.region = region  # (lat, lon)
        self.spending_profile = spending_profile
        
    def make_transaction(self, merchant):
        # Generate realistic transaction amounts based on merchant category
        if merchant["category"] == "groceries":
            amount = np.random.lognormal(3.5, 0.8)  # ~$30-200
        elif merchant["category"] == "restaurants":
            amount = np.random.lognormal(2.8, 0.6)  # ~$15-80
        elif merchant["category"] == "gas_stations":
            amount = np.random.lognormal(3.2, 0.5)  # ~$25-100
        elif merchant["category"] == "retail":
            amount = np.random.lognormal(3.8, 1.0)  # ~$45-300
        elif merchant["category"] == "online":
            amount = np.random.lognormal(3.0, 1.2)  # ~$20-500
        elif merchant["category"] == "travel":
            amount = np.random.lognormal(5.0, 0.8)  # ~$150-2000
        else:
            amount = np.random.lognormal(3.0, 0.8)  # Default
            
        # Ensure minimum amount
        amount = max(amount, 0.01)
        
        # Choose transaction type based on merchant
        if merchant["category"] in ["online", "travel"]:
            txn_type = random.choice(["PAYMENT", "DEBIT"])
        else:
            txn_type = random.choice(TRANSACTION_TYPES)
        
        # Generate location near merchant
        lat, lon = self.region
        lat += np.random.normal(0, 0.02)
        lon += np.random.normal(0, 0.02)
        
        # PRODUCTION TRANSACTION - NO FRAUD LABEL!
        return {
            "transaction_id": str(uuid.uuid4()),
            "card_id": self.card_id,
            "merchant_id": merchant["merchant_id"],
            "merchant_name": merchant["merchant_name"],
            "merchant_category": merchant["category"],
            "merchant_risk": MERCHANT_RISK.get(merchant["category"], 0.01),
            "txn_type": txn_type,
            "amount": round(amount, 2),
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            # NOTE: NO is_fraud field - this is unknown and will be determined by fraud detection!
        }

def generate_merchants(count=2500):
    merchants = []
    for i in range(count):
        category = random.choice(MERCHANT_CATEGORIES)
        merchants.append({
            "merchant_id": f"M{i+1000}",
            "merchant_name": f"Merchant_{category}_{i}",
            "category": category,
            "lat": random.uniform(25, 45),
            "lon": random.uniform(-125, -65),
        })
    return merchants

def generate_agents(count=500):
    agents = []
    for i in range(count):
        lat = random.uniform(25, 45)
        lon = random.uniform(-125, -65)
        spending_profile = random.choice(["low", "normal", "high"])
        agent = TransactionAgent(f"C{i:06d}", (lat, lon), spending_profile)
        agents.append(agent)
    return agents

def stream_loop(producer, agents, merchants, rate_per_sec, topic):
    wait = 1.0 / max(1, rate_per_sec)
    print(f"🚀 Starting PRODUCTION transaction stream at {rate_per_sec} TPS to topic '{topic}'")
    print("⚠️  NOTE: Transactions DO NOT include fraud labels - fraud detection will determine this!")
    
    i = 0
    while True:
        for _ in range(rate_per_sec):
            agent = random.choice(agents)
            merchant = random.choice(merchants)
            msg = agent.make_transaction(merchant)
            
            if producer is None:
                print(json.dumps(msg))
            else:
                producer.send(topic, value=msg)
                
            i += 1
            if i % 100 == 0:
                print(f"📊 Sent {i} production transactions (no fraud labels)")
                
        time.sleep(wait)

def main():
    parser = argparse.ArgumentParser(description="Production Transaction Generator")
    parser.add_argument("--kafka", action="store_true", help="Send to Kafka (default: print to stdout)")
    parser.add_argument("--rate", type=int, default=10, help="Transactions per second")
    parser.add_argument("--topic", default="transactions.raw", help="Kafka topic")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    
    args = parser.parse_args()
    
    print("🏭 Production Transaction Generator")
    print("=" * 50)
    print("✅ Generates realistic transaction data")
    print("⚠️  NO fraud labels included (production-ready)")
    print("🎯 Fraud detection system will classify transactions")
    print("=" * 50)
    
    # Generate merchants and agents
    merchants = generate_merchants(2500)
    agents = generate_agents(500)
    
    producer = None
    if args.kafka:
        print(f"🔗 Connecting to Kafka at {args.bootstrap_servers}")
        producer = KafkaProducer(
            bootstrap_servers=[args.bootstrap_servers],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )
        print(f"✅ Connected to Kafka topic: {args.topic}")
    else:
        print("📄 Printing to stdout (use --kafka to send to Kafka)")
    
    try:
        stream_loop(producer, agents, merchants, args.rate, args.topic)
    except KeyboardInterrupt:
        print(f"\n🛑 Stopping transaction generator")
        if producer:
            producer.close()

if __name__ == "__main__":
    main()
