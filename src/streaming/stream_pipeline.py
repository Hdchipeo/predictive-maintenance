"""Apache Spark Structured Streaming Pipeline for RUL Prediction."""

import os
import sys
import time
import argparse
try:
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, DoubleType
    )
    from src.streaming.spark_session import build_spark_session
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
from src.config import spark_cfg, kafka_cfg, dataset_cfg
from src.streaming.inference_engine import StreamingInferenceEngine
from src.streaming.influx_sink import InfluxDBSink
from src.common.logger import get_logger

logger = get_logger("StreamingPipeline")


def get_sensor_event_schema():
    """Define exact schema for incoming C-MAPSS JSON events."""
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required for cluster streaming execution.")

    fields = [
        StructField("timestamp", StringType(), True),
        StructField("engine_id", IntegerType(), False),
        StructField("cycle", IntegerType(), False),
        StructField("setting_1", DoubleType(), True),
        StructField("setting_2", DoubleType(), True),
        StructField("setting_3", DoubleType(), True),
    ]
    for i in range(1, 22):
        fields.append(StructField(f"sensor_{i}", DoubleType(), True))

    return StructType(fields)


def run_streaming_pipeline(
    bootstrap_servers: str = kafka_cfg.bootstrap_servers,
    topic: str = kafka_cfg.topic_raw,
    checkpoint_dir: str = spark_cfg.checkpoint_location,
    local_test: bool = False
) -> None:
    """Launch Spark Structured Streaming job."""
    logger.info("Initializing Streaming engine...")
    inference_engine = StreamingInferenceEngine()
    influx_sink = InfluxDBSink()

    if local_test:
        logger.info("Running local dry-run simulation mode...")
        from src.preprocessing.loader import load_data
        sample_df = load_data(mode="test").head(20)
        results = inference_engine.predict_microbatch(sample_df)
        influx_sink.write_predictions_batch(results)
        logger.info(f"Local test processed {len(results)} records successfully:")
        for r in results[:5]:
            logger.info(
                f"Engine #{r['engine_id']} (Cycle {r['cycle']}): "
                f"RUL={r['predicted_rul']} | Health={r['health_score']}% | Status={r['status']} ({r['severity']})"
            )
        return

    if not PYSPARK_AVAILABLE:
        logger.error("PySpark is not installed locally. Please run in Docker or install pyspark.")
        return

    spark = build_spark_session(include_kafka_package=True)
    schema = get_sensor_event_schema()

    # Read from Kafka Topic
    logger.info(f"Subscribing to Kafka topic '{topic}' at {bootstrap_servers}...")
    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON value
    df_parsed = (
        df_raw.select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )

    # Stateful batch processor
    def process_microbatch(batch_df, batch_id):
        t0 = time.time()
        record_count = batch_df.count()
        if record_count == 0:
            return

        logger.info(f"[Batch #{batch_id}] Processing {record_count} incoming sensor records...")
        pdf = batch_df.toPandas()

        # Vectorized ML inference and status classification
        inferred_records = inference_engine.predict_microbatch(pdf)

        # InfluxDB Sink
        influx_sink.write_predictions_batch(inferred_records)

        duration = time.time() - t0
        logger.info(
            f"[Batch #{batch_id}] Completed in {duration:.2f}s "
            f"({record_count / max(duration, 1e-6):.1f} eps)"
        )

    # Stream query activation
    logger.info(f"Starting stream writer with checkpoint at {checkpoint_dir}...")
    query = (
        df_parsed.writeStream
        .trigger(processingTime=spark_cfg.trigger_interval)
        .foreachBatch(process_microbatch)
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )

    logger.info("Pipeline running. Awaiting termination (Ctrl+C to stop)...")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping streaming query...")
        query.stop()
    finally:
        influx_sink.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Structured Streaming Pipeline")
    parser.add_argument("--local-test", action="store_true", help="Run in local test mode")
    parser.add_argument("--topic", type=str, default=kafka_cfg.topic_raw, help="Kafka topic")
    parser.add_argument("--brokers", type=str, default=kafka_cfg.bootstrap_servers, help="Kafka brokers")
    args = parser.parse_args()

    run_streaming_pipeline(
        bootstrap_servers=args.brokers,
        topic=args.topic,
        local_test=args.local_test
    )
