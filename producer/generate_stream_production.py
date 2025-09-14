#!/usr/bin/env python3
"""
Streaming Fraud Detection with ONNX and proper alerts:
- HIGH_VALUE
- IMPOSSIBLE_TRAVEL
- VELOCITY
- ML_ALERT
"""
import argparse
import json
import random
import uuid
from datetime import datetime, timedelta
import numpy as np
from kafka import KafkaProducer
import time
import os
import logging
import math


# ------------------ Logging ------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

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
    "healthcare": 0.002,
    "utilities": 0.002,
    "other": 0.02
}

TRANSACTION_TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"]

class TransactionAgent:
    """Represents a customer with a spending profile and location."""
    def __init__(self, card_id, region, spending_profile="normal"):
        self.card_id = card_id
        self.region = region  # (lat, lon)
        self.spending_profile = spending_profile
        self.last_transaction_time = None
        self.last_location = region
        
    def make_transaction(self, merchant):
        """Generates a normal, non-fraudulent transaction."""
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
        
        # Update agent state
        self.last_transaction_time = datetime.utcnow()
        self.last_location = (lat, lon)
        
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
            "timestamp": self.last_transaction_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "lat": round(lat, 6),
            "lon": round(lon, 6),
        }

def inject_fraudulent_transaction(producer, topic, agent, merchants, fraud_type):
    """Generates an intentional fraudulent transaction."""
    merchant = random.choice(merchants)
    now = datetime.utcnow()
    
    if fraud_type == "HIGH_VALUE":
        logger.info(f"Injecting a HIGH_VALUE fraud transaction for card {agent.card_id}")
        amount = random.uniform(10000, 50000)
        # Set a late-night hour to also trigger the ML rule
        fraud_time = now.replace(hour=random.choice([0, 1, 2, 3, 4, 23]))
        
        # Update agent state
        agent.last_transaction_time = fraud_time
        agent.last_location = (merchant["lat"], merchant["lon"])
        
        msg = {
            "transaction_id": str(uuid.uuid4()),
            "card_id": agent.card_id,
            "merchant_id": merchant["merchant_id"],
            "merchant_name": merchant["merchant_name"],
            "merchant_category": merchant["category"],
            "merchant_risk": MERCHANT_RISK.get(merchant["category"], 0.01),
            "txn_type": "PAYMENT",
            "amount": round(amount, 2),
            "timestamp": fraud_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "lat": round(merchant["lat"], 6),
            "lon": round(merchant["lon"], 6),
        }
        producer.send(topic, value=msg)
        return 1

    elif fraud_type == "IMPOSSIBLE_TRAVEL":
        logger.info(f"Injecting an IMPOSSIBLE_TRAVEL fraud transaction for card {agent.card_id}")
        # Find a merchant far away from the agent's last known location
        far_merchants = [m for m in merchants if haversine_distance(agent.last_location[0], agent.last_location[1], m["lat"], m["lon"]) > 2000]
        if not far_merchants:
            merchant = random.choice(merchants)
        else:
            merchant = random.choice(far_merchants)
            
        # Simulate an instant transaction to trigger the alert
        fraud_time = agent.last_transaction_time + timedelta(minutes=5) if agent.last_transaction_time else now
        
        # Update agent state
        agent.last_transaction_time = fraud_time
        agent.last_location = (merchant["lat"], merchant["lon"])
        
        msg = {
            "transaction_id": str(uuid.uuid4()),
            "card_id": agent.card_id,
            "merchant_id": merchant["merchant_id"],
            "merchant_name": merchant["merchant_name"],
            "merchant_category": merchant["category"],
            "merchant_risk": MERCHANT_RISK.get(merchant["category"], 0.01),
            "txn_type": "PAYMENT",
            "amount": round(random.uniform(10, 500), 2),
            "timestamp": fraud_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "lat": round(merchant["lat"], 6),
            "lon": round(merchant["lon"], 6),
        }
        producer.send(topic, value=msg)
        return 1

    elif fraud_type == "VELOCITY":
        logger.info(f"Injecting a VELOCITY fraud transaction burst for card {agent.card_id}")
        num_transactions = 10
        for _ in range(num_transactions):
            merchant = random.choice(merchants)
            now = datetime.utcnow()
            agent.last_transaction_time = now
            agent.last_location = (merchant["lat"], merchant["lon"])
            
            msg = {
                "transaction_id": str(uuid.uuid4()),
                "card_id": agent.card_id,
                "merchant_id": merchant["merchant_id"],
                "merchant_name": merchant["merchant_name"],
                "merchant_category": merchant["category"],
                "merchant_risk": MERCHANT_RISK.get(merchant["category"], 0.01),
                "txn_type": "PAYMENT",
                "amount": round(random.uniform(10, 100), 2),
                "timestamp": now.strftime("%Y-%m-%d %H:%M:%S.%f"),
                "lat": round(merchant["lat"], 6),
                "lon": round(merchant["lon"], 6),
            }
            producer.send(topic, value=msg) # Send immediately
            time.sleep(0.01) # Add a very small delay
        return num_transactions # Return the number of transactions sent

    elif fraud_type == "ML":
        logger.info(f"Injecting an ML-only fraud transaction for card {agent.card_id}")
        # Create a transaction with a high ML risk profile that may not be a high-value transaction
        merchant = random.choice(merchants)
        fraud_time = now
        
        # Update agent state
        agent.last_transaction_time = fraud_time
        agent.last_location = (merchant["lat"], merchant["lon"])
        
        msg = {
            "transaction_id": str(uuid.uuid4()),
            "card_id": agent.card_id,
            "merchant_id": merchant["merchant_id"],
            "merchant_name": merchant["merchant_name"],
            "merchant_category": "online",  # online merchants often have higher risk
            "merchant_risk": MERCHANT_RISK.get("online", 0.01) * 2, # Double the risk
            "txn_type": "PAYMENT",
            "amount": round(random.uniform(500, 1000), 2),
            "timestamp": fraud_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "lat": round(merchant["lat"], 6),
            "lon": round(merchant["lon"], 6),
        }
        producer.send(topic, value=msg)
        return 1

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculates the distance between two points on Earth."""
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

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

def stream_loop(producer, agents, merchants, rate_per_sec, topic, fraud_rate):
    """The main loop for streaming transactions."""
    wait_time = 1.0 / max(1, rate_per_sec)
    fraud_types = ["HIGH_VALUE", "IMPOSSIBLE_TRAVEL", "VELOCITY", "ML"]
    
    logger.info(f"🚀 Starting transaction stream at {rate_per_sec} TPS to topic '{topic}'")
    logger.info(f"🚨 Injecting fraud at a rate of {fraud_rate * 100:.2f}%")
    
    total_transactions_sent = 0
    while True:
        num_transactions_sent_in_this_second = 0
        start_time = time.time()
        
        while num_transactions_sent_in_this_second < rate_per_sec:
            # Randomly choose if a transaction should be fraudulent
            if random.random() < fraud_rate:
                fraud_type = random.choice(fraud_types)
                agent = random.choice(agents)
                num_sent = inject_fraudulent_transaction(producer, topic, agent, merchants, fraud_type)
                if num_sent is not None:
                    num_transactions_sent_in_this_second += num_sent
            else:
                agent = random.choice(agents)
                merchant = random.choice(merchants)
                msg = agent.make_transaction(merchant)
                producer.send(topic, value=msg)
                num_transactions_sent_in_this_second += 1

            if num_transactions_sent_in_this_second % 100 == 0:
                logger.info(f"📊 Sent {num_transactions_sent_in_this_second} transactions in this second...")
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        sleep_duration = max(0, 1.0 - elapsed_time)
        time.sleep(sleep_duration)

        total_transactions_sent += num_transactions_sent_in_this_second
        logger.info(f"Total transactions sent: {total_transactions_sent}")

def main():
    parser = argparse.ArgumentParser(description="Production Transaction Generator")
    parser.add_argument("--kafka", action="store_true", help="Send to Kafka (default: print to stdout)")
    parser.add_argument("--rate", type=int, default=10,help="Transactions per second")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "transactions-raw"), help="Kafka topic")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9094"),
        help="Kafka bootstrap servers"
    )
    parser.add_argument("--fraud-rate", type=float, default=0.01, help="Percentage of transactions to be fraudulent")
    
    args = parser.parse_args()
    
    logger.info("🏭 Production Transaction Generator")
    logger.info("=" * 50)
    
    # Generate merchants and agents
    merchants = generate_merchants(2500)
    agents = generate_agents(500)
    
    producer = None
    if args.kafka:
        logger.info(f"🔗 Connecting to Kafka at {args.bootstrap_servers}")
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers.split(','),
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        logger.info(f"✅ Connected to Kafka topic: {args.topic}")
    else:
        logger.info("📄 Printing to stdout (use --kafka to send to Kafka)")
        
    try:
        stream_loop(producer, agents, merchants, args.rate, args.topic, args.fraud_rate)
    except KeyboardInterrupt:
        logger.info(f"\n🛑 Stopping transaction generator")
        if producer:
            producer.close()

if __name__ == "__main__":
    main()