#!/usr/bin/env python3
"""
Streaming Fraud Detection with ONNX and proper alerts:
- HIGH_VALUE
- IMPOSSIBLE_TRAVEL
- VELOCITY
- ML_ALERT
"""

import logging
import os
import math
import numpy as np
import onnxruntime as ort

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# ------------------ Logging ------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ------------------ Config from Environment ------------------
# Get KAFKA_BOOTSTRAP_SERVERS from environment, no hardcoded value
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
if not KAFKA_BOOTSTRAP_SERVERS:
    logger.error("KAFKA_BOOTSTRAP_SERVERS environment variable is not set. Exiting.")
    exit(1)

print(f"Using Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
ONNX_MODEL_PATH = os.getenv("ML_MODEL_PATH", "model.onnx")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

RULES = {
    "high_value": "High value or suspicious ML score, including late night high amounts",
    "impossible_travel": "Impossible travel detected",
    "velocity": "More than 5 transactions in 1 minute",
    "ml_alert": "ML model detected fraud"
}

# ------------------ ML scoring UDF ------------------
def ml_fraud_score_worker(amount, hour, merchant_risk, velocity_flag):
    if not hasattr(ml_fraud_score_worker, "session"):
        if os.path.exists(ONNX_MODEL_PATH):
            ml_fraud_score_worker.session = ort.InferenceSession(ONNX_MODEL_PATH, providers=["CPUExecutionProvider"])
        else:
            ml_fraud_score_worker.session = None

    amount_val = float(amount or 0)
    hour_val = float(hour or 12)
    merchant_risk_val = float(merchant_risk or 0)
    velocity_flag_val = float(velocity_flag or 0)

    if ml_fraud_score_worker.session:
        features = np.array([[amount_val, 0.0, merchant_risk_val, hour_val, 1.0,
                              0.0, 100.0, 1.0, 1.0, 1.0, velocity_flag_val]], dtype=np.float32)
        input_name = ml_fraud_score_worker.session.get_inputs()[0].name
        pred = ml_fraud_score_worker.session.run(None, {input_name: features})
        return float(pred[1][0][1]) if len(pred) > 1 else float(pred[0][0])
    else:
        # fallback scoring
        score = 0.0
        if amount_val > 5000: score += 0.3
        elif amount_val > 2000: score += 0.15
        elif amount_val > 1000: score += 0.05
        if 0 <= hour_val <= 5: score += 0.2
        elif 22 <= hour_val <= 23: score += 0.15
        score += merchant_risk_val * 0.3
        if velocity_flag_val > 0: score += 0.25
        return min(max(score, 0.0), 1.0)

ml_udf = udf(ml_fraud_score_worker, DoubleType())

# ------------------ Haversine UDF ------------------
def haversine(lat1, lon1, lat2, lon2):
    try:
        if None in (lat1, lon1, lat2, lon2):
            return 0.0
        R = 6371
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    except Exception:
        return 0.0

haversine_udf = udf(haversine, DoubleType())

# ------------------ Batch Processing ------------------
# Note: Removed the default value from the function signature
def process_batch(batch_df, batch_id, kafka_servers):
    if batch_df.count() == 0:
        return

    df = batch_df.selectExpr("CAST(value AS STRING)") \
        .select(
            get_json_object(col("value"), "$.transaction_id").alias("transaction_id"),
            get_json_object(col("value"), "$.card_id").alias("card_id"),
            get_json_object(col("value"), "$.amount").cast("double").alias("amount"),
            get_json_object(col("value"), "$.merchant_name").alias("merchant_name"),
            get_json_object(col("value"), "$.merchant_risk").cast("double").alias("merchant_risk"),
            get_json_object(col("value"), "$.lat").cast("double").alias("lat"),
            get_json_object(col("value"), "$.lon").cast("double").alias("lon"),
            get_json_object(col("value"), "$.timestamp").alias("event_time")
        ).withColumn("event_time", to_timestamp("event_time")) \
         .withColumn("hour", hour(col("event_time"))) \
         .withColumn("velocity_flag", lit(0)) \
         .withColumn("ml_fraud_score", ml_udf(col("amount"), col("hour"), col("merchant_risk"), col("velocity_flag")))

    # Lag-based computations
    window = Window.partitionBy("card_id").orderBy("event_time")
    df = df.withColumn("prev_lat", lag("lat").over(window)) \
           .withColumn("prev_lon", lag("lon").over(window)) \
           .withColumn("prev_time", lag("event_time").over(window)) \
           .withColumn("distance_km", haversine_udf(col("lat"), col("lon"), col("prev_lat"), col("prev_lon"))) \
           .withColumn("time_diff_min", when(col("prev_time").isNotNull(),
                                             (unix_timestamp(col("event_time")) - unix_timestamp(col("prev_time"))) / 60
                                            ).otherwise(0.0)) \
           .withColumn("event_ts", unix_timestamp("event_time"))

    # Velocity count
    velocity_window = Window.partitionBy("card_id").orderBy("event_ts").rangeBetween(-60, 0)
    df = df.withColumn("txn_count_1min", count("*").over(velocity_window))

    # Alerts
    alerts = {
        "HIGH_VALUE": df.filter((col("amount") > 5000) |
                                ((col("amount") > 1000) & ((col("hour") >= 23) | (col("hour") <= 6))) |
                                (col("ml_fraud_score") > 0.7)),
        "IMPOSSIBLE_TRAVEL": df.filter((col("distance_km") > 500) & (col("time_diff_min") < 30)),
        "VELOCITY": df.filter(col("txn_count_1min") > 5),
        "ML_ALERT": df.filter(col("ml_fraud_score") > 0.3)
    }

    def write_to_kafka(alert_df, topic, reason_key):
        if alert_df.count() == 0:
            return
        (alert_df.withColumn("alert_type", lit(topic))
                 .withColumn("reason", lit(RULES[reason_key]))
                 .selectExpr("CAST(transaction_id AS STRING) AS key",
                             """to_json(named_struct(
                                'transaction_id', transaction_id,
                                'card_id', card_id,
                                'amount', amount,
                                'alert_type', alert_type,
                                'reason', reason,
                                'ml_fraud_score', ml_fraud_score,
                                'distance_km', distance_km,
                                'time_diff_min', time_diff_min,
                                'txn_count_1min', txn_count_1min
                             )) AS value""")
                 .write
                 .format("kafka")
                 .option("kafka.bootstrap.servers", kafka_servers)
                 .option("topic", f"alerts.{topic.lower()}")
                 .save())

    for k, df_alert in alerts.items():
        write_to_kafka(df_alert, k, k.lower())

# ------------------ Main ------------------
def main():
    spark = SparkSession.builder \
        .appName("FraudDetectionKafka") \
        .master(SPARK_MASTER) \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
          .option("subscribe", "transactions.raw")
          .option("startingOffsets", "latest")
          .load())

    query = df.writeStream \
              .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, batch_id, kafka_servers=KAFKA_BOOTSTRAP_SERVERS)) \
              .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()