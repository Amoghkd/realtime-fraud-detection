#!/usr/bin/env python3
import pandas as pd
import numpy as np
import argparse
import os
from sklearn.metrics import classification_report, roc_auc_score
import lightgbm as lgb
import joblib


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', default='data/transactions_batch_enriched.parquet', help='Path to enriched training data')
    parser.add_argument('--model_out', default='ml-training/model.pkl', help='Output model path')
    args = parser.parse_args()

    # Read enriched data (already has all features computed)
    if args.data.endswith('.parquet'):
        df = pd.read_parquet(args.data)
    else:
        df = pd.read_csv(args.data)
    
    print(f"Loaded {len(df)} transactions from {args.data}")

    # Ensure we have the fraud label
    if 'is_fraud' not in df.columns:
        if 'fraud_label' in df.columns:
            df['is_fraud'] = df['fraud_label'].astype(int)
        else:
            raise ValueError("No fraud label column found (expected 'is_fraud' or 'fraud_label')")
    
    df['is_fraud'] = df['is_fraud'].astype(int)

    # Select ML features (all the enriched features from batch generator)
    features = [
        'amount', 'amount_z', 'merchant_risk', 'hour', 'dayofweek',
        'dist_prev', 'tenure_days', 'txn_1m', 'txn_1h', 'txn_24h', 'velocity_flag'
    ]
    
    # Ensure all features exist (fallback to 0 if missing)
    for f in features:
        if f not in df.columns:
            df[f] = 0
            print(f"Warning: Missing feature {f}, using 0")

    X = df[features].fillna(0)
    y = df['is_fraud']

    # Time-based train/test split (80/20)
    df['timestamp_dt'] = pd.to_datetime(df['timestamp'])
    split_time = df['timestamp_dt'].quantile(0.8)
    train_idx = df['timestamp_dt'] <= split_time
    
    X_train, X_test = X[train_idx], X[~train_idx]
    y_train, y_test = y[train_idx], y[~train_idx]

    print(f"Training set: {len(X_train)} samples ({y_train.sum()} fraud)")
    print(f"Test set: {len(X_test)} samples ({y_test.sum()} fraud)")

    # Train LightGBM model
    clf = lgb.LGBMClassifier(
        n_estimators=200,
        learning_rate=0.1,
        num_leaves=31,
        random_state=42
    )
    
    clf.fit(X_train, y_train)

    # Evaluate
    preds = clf.predict(X_test)
    probs = clf.predict_proba(X_test)[:, 1]

    print('\n=== Classification Report (Test Set) ===')
    print(classification_report(y_test, preds, digits=4))
    
    try:
        auc = roc_auc_score(y_test, probs)
        print(f'AUC: {auc:.4f}')
    except Exception as e:
        print(f'Could not compute AUC: {e}')

    # Save model
    os.makedirs(os.path.dirname(args.model_out), exist_ok=True)
    joblib.dump(clf, args.model_out)
    print(f'\n[✓] Model saved to {args.model_out}')
