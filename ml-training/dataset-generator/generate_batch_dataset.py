#!/usr/bin/env python3
"""
Batch dataset generator (PaySim-inspired) that produces a large raw CSV and an
enriched CSV/Parquet with precomputed ML features (rolling counts, distances,
velocity flags, tenure, relative amounts). Intended for offline ML training.

Usage:
  python generate_batch_dataset.py --n_customers 1000 --days 30 --out_raw ../data/transactions_500k.csv --out_enriched ../data/transactions_500k_enriched.parquet

Notes:
- This is single-node and uses pandas; for very large datasets consider running
  the enrichment step using Spark (see spark-job/feature_extraction.py).
"""
import argparse
import os
import random
import uuid
from datetime import datetime, timedelta
import math

import numpy as np
import pandas as pd
from faker import Faker

fake = Faker()

TXN_TYPES = ["PAYMENT", "TRANSFER", "CASH_IN", "CASH_OUT"]
MERCHANT_CATEGORIES = [
    "electronics",
    "jewelry",
    "groceries",
    "travel",
    "fashion",
    "entertainment",
    "restaurants",
    "gas",
]

MERCHANT_RISK = {
    "electronics": 0.02,
    "jewelry": 0.08,
    "groceries": 0.005,
    "travel": 0.03,
    "fashion": 0.01,
    "entertainment": 0.01,
    "restaurants": 0.01,
    "gas": 0.003,
}


class Agent:
    def __init__(self, card_id, balance, region):
        self.card_id = card_id
        self.balance = balance
        self.region = region
        self.txn_type_probs = {"PAYMENT": 0.6, "TRANSFER": 0.2, "CASH_IN": 0.1, "CASH_OUT": 0.1}

    def make_transaction(self, ts, merchant):
        txn_type = random.choices(list(self.txn_type_probs.keys()), list(self.txn_type_probs.values()))[0]
        amount = round(np.random.lognormal(mean=2.5, sigma=1.0), 2)
        merchant_id = merchant["merchant_id"]
        merchant_name = merchant["merchant_name"]
        merchant_cat = merchant["category"]
        merchant_risk = MERCHANT_RISK.get(merchant_cat, 0.01)

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
            "merchant_id": merchant_id,
            "merchant_name": merchant_name,
            "merchant_category": merchant_cat,
            "merchant_risk": merchant_risk,
            "txn_type": txn_type,
            "amount": round(amount, 2),
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "is_fraud": 0,
        }


def make_merchants(n_merchants=500):
    merchants = []
    for i in range(n_merchants):
        mid = f"M{1000 + i}"
        merchants.append({
            "merchant_id": mid,
            "merchant_name": fake.company(),
            "category": random.choice(MERCHANT_CATEGORIES),
        })
    return merchants


def haversine(lat1, lon1, lat2, lon2):
    try:
        lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])
    except Exception:
        return 0.0
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    return 6371 * c


def compute_time_windows_counts(secs, window_seconds):
    n = len(secs)
    counts = np.zeros(n, dtype=int)
    left = 0
    for i in range(n):
        window_start = secs[i] - window_seconds
        while left < i and secs[left] < window_start:
            left += 1
        counts[i] = i - left + 1
    return counts


def inject_fraud(df, blacklist, fraud_ratio, seed=42):
    random.seed(seed)
    n = len(df)
    n_frauds = max(1, int(n * fraud_ratio))
    fraud_indices = df.sample(n=n_frauds, random_state=seed).index.tolist()
    for idx in fraud_indices:
        rule = random.choice(["spike", "blacklist", "travel"])
        df.at[idx, "is_fraud"] = 1
        if rule == "spike":
            df.at[idx, "amount"] = round(df.at[idx, "amount"] * random.uniform(10, 50), 2)
        elif rule == "blacklist":
            bl = random.choice(blacklist)
            df.at[idx, "merchant_id"] = bl
            df.at[idx, "merchant_name"] = "BLACKLISTED"
        elif rule == "travel":
            df.at[idx, "lat"] = df.at[idx, "lat"] + random.uniform(5.0, 30.0)
            df.at[idx, "lon"] = df.at[idx, "lon"] + random.uniform(5.0, 30.0)
    return df


def enrich(df):
    pdf = df.copy()
    pdf['timestamp_dt'] = pd.to_datetime(pdf['timestamp'])
    pdf = pdf.sort_values(['card_id', 'timestamp_dt']).reset_index(drop=True)
    pdf['hour'] = pdf['timestamp_dt'].dt.hour
    pdf['dayofweek'] = pdf['timestamp_dt'].dt.dayofweek
    pdf['amount_z'] = pdf.groupby('card_id')['amount'].transform(lambda x: (x - x.mean()) / (x.std() + 1e-6))
    pdf['prev_lat'] = pdf.groupby('card_id')['lat'].shift(1)
    pdf['prev_lon'] = pdf.groupby('card_id')['lon'].shift(1)
    pdf['dist_prev'] = pdf.apply(lambda r: haversine(r['prev_lat'], r['prev_lon'], r['lat'], r['lon']) if not pd.isna(r['prev_lat']) else 0.0, axis=1)
    first_ts = pdf.groupby('card_id')['timestamp_dt'].transform('min')
    pdf['tenure_days'] = (pdf['timestamp_dt'] - first_ts).dt.total_seconds() / 86400.0
    pdf['ts_seconds'] = pdf['timestamp_dt'].astype('datetime64[s]').astype('int64')

    def group_windows(g):
        secs = g['ts_seconds'].values
        g = g.copy()
        g['txn_1h'] = compute_time_windows_counts(secs, 3600)
        g['txn_24h'] = compute_time_windows_counts(secs, 86400)
        g['txn_1m'] = compute_time_windows_counts(secs, 60)
        return g

    pdf = pdf.groupby('card_id', sort=False).apply(group_windows).reset_index(drop=True)
    pdf['velocity_flag'] = (pdf['txn_1m'] >= 5).astype(int)
    return pdf


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--n_customers', type=int, default=2000)
    parser.add_argument('--days', type=int, default=30)
    parser.add_argument('--fraud_ratio', type=float, default=0.02)
    parser.add_argument('--out_raw', default='data/transactions_batch_raw.csv')
    parser.add_argument('--out_enriched', default='data/transactions_batch_enriched.parquet')
    parser.add_argument('--seed', type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    np.random.seed(args.seed)

    out_dir = os.path.dirname(args.out_raw) or '.'
    os.makedirs(out_dir, exist_ok=True)

    merchants = make_merchants(500)
    blacklist = random.sample([m['merchant_id'] for m in merchants], k=10)

    agents = []
    for i in range(args.n_customers):
        region = (random.uniform(12, 28), random.uniform(72, 88))
        balance = random.uniform(1000, 20000)
        agents.append(Agent(card_id=f"C{i:06d}", balance=balance, region=region))

    rows = []
    start_date = datetime(2023, 1, 1)
    for day in range(args.days):
        for hour in range(24):
            for agent in agents:
                if random.random() < 0.3:
                    ts = start_date + timedelta(days=day, hours=hour, seconds=random.randint(0, 3599))
                    merchant = random.choice(merchants)
                    rows.append(agent.make_transaction(ts, merchant))

    df = pd.DataFrame(rows)
    df = inject_fraud(df, blacklist, args.fraud_ratio, seed=args.seed)
    df = df.sort_values('timestamp').reset_index(drop=True)
    df.to_csv(args.out_raw, index=False)
    print(f"[✓] Raw batch written -> {args.out_raw} ({len(df)} rows)")

    # Enrich and write parquet
    enriched = enrich(df)
    enriched.to_parquet(args.out_enriched, index=False)
    print(f"[✓] Enriched batch written -> {args.out_enriched} ({len(enriched)} rows)")

    # merchant blacklist
    bl_df = pd.DataFrame({'merchant_id': blacklist})
    bl_df.to_csv(os.path.join(out_dir, 'merchant_blacklist.csv'), index=False)


if __name__ == '__main__':
    main()
