#!/usr/bin/env python3
import argparse
import time
import json
import pandas as pd
import os
from kafka import KafkaProducer


def produce(file_path, delay_ms=100, bootstrap_servers=None, topic="transactions.raw"):
    if bootstrap_servers is None:
        bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP', 'localhost:9092')
    df = pd.read_csv(file_path)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for _, row in df.iterrows():
        payload = row.dropna().to_dict()
        producer.send(topic, payload)
        time.sleep(delay_ms / 1000.0)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default="../data/transactions_50k.csv")
    parser.add_argument("--delay-ms", type=int, default=20)
    parser.add_argument("--bootstrap", default=None)
    parser.add_argument("--topic", default="transactions.raw")
    args = parser.parse_args()

    produce(args.file, args.delay_ms, args.bootstrap, args.topic)
