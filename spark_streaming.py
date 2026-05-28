"""
WikiGuard - Real-Time Edit Fraud Detection
spark_streaming.py -- Consumes Kafka stream, runs ML fraud detection, writes results
"""

import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, udf, window, count, avg, abs as spark_abs,
    current_timestamp, to_timestamp, expr, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, FloatType
)

# -- Spark Session
spark = SparkSession.builder \
    .appName("WikiGuard-FraudDetection") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.streaming.checkpointLocation", "C:/tmp/wikiguard_checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -- Schema
edit_schema = StructType([
    StructField("timestamp",    LongType(),   True),
    StructField("title",        StringType(), True),
    StructField("user",         StringType(), True),
    StructField("bot",          IntegerType(),True),
    StructField("wiki",         StringType(), True),
    StructField("type",         StringType(), True),
    StructField("namespace",    IntegerType(),True),
    StructField("old_length",   IntegerType(),True),
    StructField("new_length",   IntegerType(),True),
    StructField("length_delta", IntegerType(),True),
    StructField("comment",      StringType(), True),
    StructField("minor",        IntegerType(),True),
    StructField("patrolled",    IntegerType(),True),
])

# -- Read from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "wiki-edits") \
    .option("startingOffsets", "latest") \
    .load()

edits = raw_stream.select(
    from_json(col("value").cast("string"), edit_schema).alias("data")
).select("data.*")

edits = edits.withColumn(
    "event_time",
    to_timestamp(col("timestamp").cast("long"))
)

# -- ML Fraud Scoring (Rule-Based + Weighted Score)
@udf(FloatType())
def fraud_score(bot, length_delta, old_length, comment, minor, namespace):
    score = 0.0

    if length_delta is not None and old_length and old_length > 0:
        deletion_ratio = -length_delta / max(old_length, 1)
        if deletion_ratio > 0.9:
            score += 40.0
        elif deletion_ratio > 0.5:
            score += 20.0

    if length_delta is not None and length_delta > 5000:
        score += 15.0

    if not comment or comment.strip() == "":
        score += 10.0
    suspicious_words = ["test", "haha", "lol", "vandal", "delete", "fuck", "stupid"]
    if comment and any(w in comment.lower() for w in suspicious_words):
        score += 20.0

    if namespace not in (0, 1):
        score += 5.0

    if minor == 1 and length_delta is not None and abs(length_delta) > 2000:
        score += 15.0

    if bot == 1:
        score -= 30.0

    return float(max(0.0, min(100.0, score)))

@udf(StringType())
def fraud_label(score):
    if score is None:
        return "CLEAN"
    if score >= 60:
        return "HIGH_RISK"
    if score >= 30:
        return "SUSPICIOUS"
    return "CLEAN"

scored = edits.withColumn(
    "fraud_score",
    fraud_score(
        col("bot"), col("length_delta"), col("old_length"),
        col("comment"), col("minor"), col("namespace")
    )
).withColumn("fraud_label", fraud_label(col("fraud_score")))

# -- Output 1: Console
console_query = scored.select(
    "event_time", "wiki", "title", "user", "bot",
    "length_delta", "fraud_score", "fraud_label", "comment"
).writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 20) \
    .trigger(processingTime="5 seconds") \
    .start()

# -- Output 2: JSON file sink for dashboard
flagged = scored.filter(col("fraud_label") != "CLEAN")

file_query = flagged.select(
    col("event_time").cast("string"),
    "wiki", "title", "user", "bot",
    "length_delta", "fraud_score", "fraud_label", "comment"
).writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "C:/tmp/wikiguard_output") \
    .option("checkpointLocation", "C:/tmp/wikiguard_checkpoint_file") \
    .trigger(processingTime="5 seconds") \
    .start()

# -- Output 3: Windowed aggregation
windowed_stats = scored \
    .withWatermark("event_time", "30 seconds") \
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds"),
        col("wiki")
    ).agg(
        count("*").alias("total_edits"),
        count(col("fraud_label") == lit("HIGH_RISK")).alias("high_risk_count"),
        avg("fraud_score").alias("avg_fraud_score")
    )

stats_query = windowed_stats.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="30 seconds") \
    .start()

print("WikiGuard Fraud Detection Engine - RUNNING")
print("Kafka -> Spark Streaming -> ML Scoring -> Dashboard")
print("Press Ctrl+C to stop")

spark.streams.awaitAnyTermination()