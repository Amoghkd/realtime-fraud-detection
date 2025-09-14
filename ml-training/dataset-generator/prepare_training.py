#!/usr/bin/env python3
"""
Prepare training dataset from raw PaySim-lite transactions.
Reads raw CSV (transactions), computes time features, rolling counts (1m/1h/24h),
prev-distance, tenure, amount z-score per card, velocity flag, and writes an
enriched CSV plus train/test split (time-based).

Usage:
  python prepare_training.py --input ../data/transactions_50k.csv \
    --out_enriched ../data/transactions_50k_enriched.csv \
    --out_train ../data/train.csv --out_test ../data/test.csv --test-frac 0.2

This script uses pandas and numpy and is intended to be run locally prior to
training the ML model. It keeps deterministic behavior via the --seed flag.
"""
import argparse
import math
import numpy as np
import pandas as pd


def haversine(lat1, lon1, lat2, lon2):
    try:
        lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])
    except Exception:
        return 0.0
    # convert degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371
    return c * r


def compute_time_windows_counts(secs, window_seconds):
    """Given a numpy array of POSIX seconds sorted ascending, compute sliding-window counts
    where each position i counts number of events in (secs[i] - window_seconds, secs[i]]
    Returns an integer numpy array of same length.
    This is O(n) per group using a two-pointer approach.
    """
    n = len(secs)
    counts = np.zeros(n, dtype=int)
    left = 0
    for i in range(n):
        window_start = secs[i] - window_seconds
        # move left pointer until secs[left] >= window_start
        while left < i and secs[left] < window_start:
            left += 1
        counts[i] = i - left + 1
    return counts


def prepare(input_path, out_enriched, out_train, out_test, test_frac=0.2, label_noise=0.0, seed=42):
    np.random.seed(seed)

    df = pd.read_csv(input_path)
    df['timestamp_dt'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values(['card_id', 'timestamp_dt']).reset_index(drop=True)

    # time features
    df['hour'] = df['timestamp_dt'].dt.hour
    df['dayofweek'] = df['timestamp_dt'].dt.dayofweek
    df['is_weekend'] = df['dayofweek'].isin([5, 6]).astype(int)

    # amount z-score per card (fallback to global if std=0)
    def zscore_group(x):
        std = x.std()
        if std == 0 or np.isnan(std):
            return (x - x.mean()) * 0.0
        return (x - x.mean()) / (std + 1e-6)

    df['amount_z'] = df.groupby('card_id')['amount'].transform(zscore_group)

    # prev lat/lon and distance
    df['prev_lat'] = df.groupby('card_id')['lat'].shift(1)
    df['prev_lon'] = df.groupby('card_id')['lon'].shift(1)
    df['dist_prev'] = df.apply(lambda r: haversine(r['prev_lat'], r['prev_lon'], r['lat'], r['lon']) if not pd.isna(r['prev_lat']) else 0.0, axis=1)

    # tenure (days since first txn per card)
    first_ts = df.groupby('card_id')['timestamp_dt'].transform('min')
    df['tenure_days'] = (df['timestamp_dt'] - first_ts).dt.total_seconds() / 86400.0

    # efficient rolling counts per card
    df['ts_seconds'] = df['timestamp_dt'].astype('datetime64[s]').astype('int64')

    # we'll compute per-card windows using groupby apply (keeps memory modest for reasonable datasets)
    def group_windows(g):
        secs = g['ts_seconds'].values
        g = g.copy()
        g['txn_1m'] = compute_time_windows_counts(secs, 60)
        g['txn_1h'] = compute_time_windows_counts(secs, 3600)
        g['txn_24h'] = compute_time_windows_counts(secs, 86400)
        return g

    df = df.groupby('card_id', sort=False).apply(group_windows).reset_index(drop=True)

    df['velocity_flag'] = (df['txn_1m'] >= 5).astype(int)

    # optional label noise: flip a fraction of labels randomly
    if label_noise > 0.0:
        n_flip = int(len(df) * label_noise)
        flip_idx = df.sample(n=n_flip, random_state=seed).index
        df.loc[flip_idx, 'is_fraud'] = 1 - df.loc[flip_idx, 'is_fraud']

    # time-based train/test split using global time quantile
    cutoff = df['timestamp_dt'].quantile(1.0 - test_frac)
    train = df[df['timestamp_dt'] <= cutoff].copy()
    test = df[df['timestamp_dt'] > cutoff].copy()

    # select columns to output
    out_cols = [
        'transaction_id', 'card_id', 'merchant_id', 'merchant_name', 'merchant_category', 'merchant_risk',
        'txn_type', 'amount', 'timestamp', 'lat', 'lon', 'is_fraud',
        'hour', 'dayofweek', 'is_weekend', 'amount_z', 'dist_prev', 'tenure_days',
        'txn_1m', 'txn_1h', 'txn_24h', 'velocity_flag'
    ]

    df[out_cols].to_csv(out_enriched, index=False)
    train[out_cols].to_csv(out_train, index=False)
    test[out_cols].to_csv(out_test, index=False)

    print(f"[✓] Wrote enriched dataset -> {out_enriched} ({len(df)} rows)")
    print(f"[✓] Wrote train -> {out_train} ({len(train)} rows), test -> {out_test} ({len(test)} rows)")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', default='data/transactions_50k.csv')
    parser.add_argument('--out_enriched', default='data/transactions_50k_enriched.csv')
    parser.add_argument('--out_train', default='data/train.csv')
    parser.add_argument('--out_test', default='data/test.csv')
    parser.add_argument('--test-frac', type=float, default=0.2)
    parser.add_argument('--label-noise', type=float, default=0.0)
    parser.add_argument('--seed', type=int, default=42)
    args = parser.parse_args()

    prepare(args.input, args.out_enriched, args.out_train, args.out_test,
            test_frac=args.test_frac, label_noise=args.label_noise, seed=args.seed)
