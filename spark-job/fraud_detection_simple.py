#!/usr/bin/env python3
"""
Fraud Detection System - ONNX + JSON Rule Config
"""

import os
import logging
import json
import onnxruntime as ort
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ONNX_MODEL_PATH = os.getenv("ML_MODEL_PATH", "model.onnx")
RULES_FILE = os.getenv("RULES_PATH", "rules.json")

ONNX_SESSION = None
RULES = {}


def load_onnx_model():
    global ONNX_SESSION
    if ONNX_SESSION is None:
        if os.path.exists(ONNX_MODEL_PATH):
            logger.info(f"Loading ONNX model from {ONNX_MODEL_PATH}")
            ONNX_SESSION = ort.InferenceSession(
                ONNX_MODEL_PATH, providers=["CPUExecutionProvider"]
            )
        else:
            logger.warning("ONNX model not found. Using rule-based fallback.")
            ONNX_SESSION = "RULE_BASED"


def ml_fraud_score(amount, hour, merchant_risk, velocity_flag):
    """ONNX scoring UDF with fallback"""
    try:
        if ONNX_SESSION is None:
            load_onnx_model()

        amount_val = float(amount or 0)
        hour_val = float(hour or 12)
        merchant_risk_val = float(merchant_risk or 0)
        velocity_flag_val = float(velocity_flag or 0)

        if ONNX_SESSION != "RULE_BASED":
            features = np.array([[
                amount_val, 0.0, merchant_risk_val, hour_val, 1.0,
                0.0, 100.0, 1.0, 1.0, 1.0, velocity_flag_val
            ]], dtype=np.float32)

            input_name = ONNX_SESSION.get_inputs()[0].name
            pred = ONNX_SESSION.run(None, {input_name: features})
            prob = float(pred[1][0][1]) if len(pred) > 1 else float(pred[0][0])
            return prob

        # Fallback simple scoring
        score = 0.0
        if amount_val > 5000: score += 0.3
        elif amount_val > 2000: score += 0.15
        elif amount_val > 1000: score += 0.05
        if 0 <= hour_val <= 5: score += 0.2
        elif 22 <= hour_val <= 23: score += 0.15
        score += merchant_risk_val * 0.3
        if velocity_flag_val > 0: score += 0.25
        return min(max(score, 0.0), 1.0)

    except Exception as e:
        logger.warning(f"ONNX scoring error: {e}")
        return 0.0


def load_rules():
    global RULES
    try:
        with open(RULES_FILE, "r") as f:
            RULES = json.load(f)
        logger.info(f"Loaded {len(RULES)} rules from {RULES_FILE}")
    except Exception as e:
        logger.error(f"Failed to load rules: {e}")
        RULES = {}


def process_batch(batch_df, batch_id):
    try:
        if batch_df.count() == 0:
            logger.info(f"Batch {batch_id}: No transactions")
            return

        enriched_df = batch_df.select(
            get_json_object(col("value").cast("string"), "$.transaction_id").alias("transaction_id"),
            get_json_object(col("value").cast("string"), "$.amount").cast("double").alias("amount"),
            get_json_object(col("value").cast("string"), "$.merchant_name").alias("merchant_name"),
            get_json_object(col("value").cast("string"), "$.merchant_risk").cast("double").alias("merchant_risk"),
            get_json_object(col("value").cast("string"), "$.user_id").alias("user_id"),
            col("timestamp").alias("event_time")
        ).withColumn("hour", hour(col("event_time"))) \
         .withColumn("velocity_flag", when((col("amount") > 1000) & (col("hour").between(0, 6)), 1).otherwise(0))

        spark = SparkSession.getActiveSession()
        ml_score_udf = spark.udf.register("ml_score", ml_fraud_score, DoubleType())
        scored_df = enriched_df.withColumn(
            "ml_fraud_score",
            ml_score_udf(col("amount"), col("hour"), col("merchant_risk"), col("velocity_flag"))
        )

        # Apply rules dynamically
        for topic, rule in RULES.items():
            condition_expr = expr(rule["condition"])  # Convert string condition into Spark SQL expression
            alerts_df = scored_df.filter(condition_expr).select(
                col("transaction_id"),
                col("amount"),
                col("merchant_name"),
                col("ml_fraud_score"),
                lit(topic.upper()).alias("alert_type"),
                lit(rule["reason"]).alias("reason"),
                current_timestamp().alias("alert_timestamp")
            )

            if alerts_df.count() > 0:
                logger.info(f"Batch {batch_id}: Writing {alerts_df.count()} alerts to {topic}")
                alerts_df.selectExpr("to_json(struct(*)) AS value") \
                    .write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                    .option("topic", f"alerts.{topic}") \
                    .mode("append") \
                    .save()

                alerts_df.show(truncate=False)

    except Exception as e:
        logger.error(f"Batch {batch_id}: Error {e}")


def main():
    logger.info("Starting ONNX fraud detection system")
    load_onnx_model()
    load_rules()


    spark = SparkSession.builder \
        .appName("FraudDetectionLocalTest") \
        .master("local[*]") \
        .getOrCreate()

    # Load static file instead of Kafka
    df = spark.read.json("trans.json")

    # Call batch processor directly
    process_batch(df, 0)

    spark.stop()


    # spark = SparkSession.builder \
    #     .appName("FraudDetectionONNX") \
    #     .config("spark.sql.adaptive.enabled", "true") \
    #     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    #     .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    #     .config("spark.driver.memory", "1g") \
    #     .getOrCreate()

    # df = spark.readStream.format("kafka") \
    #     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    #     .option("subscribe", "transactions.raw") \
    #     .option("startingOffsets", "latest") \
    #     .load()
    
   

    # query = df.writeStream \
    #     .foreachBatch(process_batch) \
    #     .option("checkpointLocation", "/tmp/spark-checkpoints-onnx") \
    #     .trigger(processingTime="10 seconds") \
    #     .start()

    # query.awaitTermination()


if __name__ == "__main__":
    main()
