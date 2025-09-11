#!/usr/bin/env python3
"""
PRODUCTION-READY Fraud Detection Schema
Schema for real production transactions WITHOUT fraud labels
"""

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def get_production_transaction_schema():
    """
    Schema for REAL production transactions
    Note: NO is_fraud field - fraud detection determines this!
    """
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("merchant_risk", DoubleType(), True),
        StructField("txn_type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        # NO is_fraud field in production!
    ])

def get_simulation_transaction_schema():
    """
    Schema for testing/simulation with known fraud labels
    Used only for testing and validation
    """
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("merchant_risk", DoubleType(), True),
        StructField("txn_type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("is_fraud", IntegerType(), True),  # Only for testing
    ])
