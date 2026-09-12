"""SparkSession builder configured for Kafka Structured Streaming and PyArrow."""

import os
from typing import Optional
from pyspark.sql import SparkSession
from src.config import spark_cfg
from src.common.logger import get_logger

logger = get_logger("SparkSessionBuilder")


def build_spark_session(
    app_name: Optional[str] = None,
    master: Optional[str] = None,
    include_kafka_package: bool = True
) -> SparkSession:
    """Create or retrieve a tuned SparkSession for streaming.
    
    Configures:
    - Arrow vectorized execution
    - Dynamic allocation and minimal shuffle partitions for micro-batches
    - Spark Kafka connector package
    """
    app = app_name or spark_cfg.app_name
    spark_master = master or spark_cfg.master

    builder = (
        SparkSession.builder
        .appName(app)
        .master(spark_master)
        .config("spark.sql.shuffle.partitions", spark_cfg.shuffle_partitions)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
    )

    if include_kafka_package:
        # Standard Spark 3.5.x Kafka connector package coordinate
        kafka_pkg = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
        builder = builder.config("spark.jars.packages", kafka_pkg)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession '{app}' created (Master: {spark_master}, Version: {spark.version})")
    return spark
