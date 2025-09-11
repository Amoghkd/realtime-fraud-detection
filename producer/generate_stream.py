#!/usr/bin/env python3
"""
Streaming transaction generator (lightweight) that produces raw transactions and
optionally publishes them to Kafka (topic: transactions.raw). Intended for
real-time testing and connected to Spark Structured Streaming.

Usage examples:
  # produce to stdout (for debugging)
  python generate_stream.py --rate 100 --n_customers 100

  # stream to Kafka (requires Kafka reachable at KAFKA_BOOTSTRAP)
  python generate_stream.py --rate 500 --n_customers 1000 --kafka

"""
import argparse
import os
import random
import time
import uuid
from datetime import datetime

import json
import numpy as np
import pandas as pd
from faker import Faker
from kafka import KafkaProducer

fake = Faker()
TXN_TYPES = ["PAYMENT", "TRANSFER", "CASH_IN", "CASH_OUT"]
MERCHANT_CATEGORIES = [
    "electronics", "jewelry", "groceries", "travel", "fashion", "entertainment", "restaurants", "gas"
]
MERCHANT_RISK = {"electronics": 0.02, "jewelry": 0.08, "groceries": 0.005, "travel": 0.03, "fashion": 0.01, "entertainment": 0.01, "restaurants": 0.01, "gas": 0.003}


def make_merchants(n_merchants=200):
    merchants = []
    for i in range(n_merchants):
        mid = f"M{2000 + i}"
        merchants.append({"merchant_id": mid, "merchant_name": fake.company(), "category": random.choice(MERCHANT_CATEGORIES)})
    return merchants


class Agent:
    def __init__(self, card_id, balance, region):
        self.card_id = card_id
        self.balance = balance
        self.region = region
        self.txn_type_probs = {"PAYMENT": 0.6, "TRANSFER": 0.2, "CASH_IN": 0.1, "CASH_OUT": 0.1}

    def make_transaction(self, merchant):
        txn_type = random.choices(list(self.txn_type_probs.keys()), list(self.txn_type_probs.values()))[0]
        amount = round(np.random.lognormal(mean=2.5, sigma=1.0), 2)
        if txn_type in ["PAYMENT", "CASH_OUT", "TRANSFER"]:
            if self.balance < amount:
                amount = round(self.balance * random.uniform(0.5, 1.0), 2)
            self.balance = max(0.0, self.balance - amount)
        else:
            self.balance += amount
        lat, lon = self.region
        lat += np.random.normal(0, 0.02)
        lon += np.random.normal(0, 0.02)
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
            "is_fraud": 0,
        }


def stream_loop(producer, agents, merchants, rate_per_sec, topic):
    # simple Poisson-like emission
    wait = 1.0 / max(1, rate_per_sec)
    i = 0
    while True:
        for _ in range(rate_per_sec):
            agent = random.choice(agents)
            merchant = random.choice(merchants)
            msg = agent.make_transaction(merchant)
            if producer is None:
                print(json.dumps(msg))
            else:
                producer.send(topic, value=json.dumps(msg).encode('utf-8'))
            i += 1
        time.sleep(wait)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rate', type=int, default=100, help='messages per second')
    parser.add_argument('--n_customers', type=int, default=500)
    parser.add_argument('--kafka', action='store_true', help='send to Kafka')
    parser.add_argument('--kafka-bootstrap', default=os.environ.get('KAFKA_BOOTSTRAP', 'localhost:9092'))
    parser.add_argument('--topic', default='transactions.raw')
    args = parser.parse_args()

    merchants = make_merchants(200)
    agents = []
    for i in range(args.n_customers):
        region = (random.uniform(12, 28), random.uniform(72, 88))
        balance = random.uniform(1000, 20000)
        agents.append(Agent(card_id=f"C{i:06d}", balance=balance, region=region))

    producer = None
    if args.kafka:
        producer = KafkaProducer(bootstrap_servers=[args.kafka_bootstrap])

    try:
        stream_loop(producer, agents, merchants, args.rate, args.topic)
    except KeyboardInterrupt:
        print('\nStopped')


if __name__ == '__main__':
    main()
