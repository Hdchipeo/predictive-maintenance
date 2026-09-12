"""Central configuration module for Predictive Maintenance Big Data System."""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import List

# Base Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
MODELS_DIR = PROJECT_ROOT / "models"
CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints"

# Ensure runtime directories exist
for directory in [DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, MODELS_DIR, CHECKPOINT_DIR]:
    directory.mkdir(parents=True, exist_ok=True)


@dataclass
class DatasetConfig:
    """NASA C-MAPSS dataset configuration."""
    dataset_name: str = os.getenv("CMAPSS_DATASET", "FD001")
    index_columns: List[str] = field(default_factory=lambda: ["engine_id", "cycle"])
    setting_columns: List[str] = field(default_factory=lambda: ["setting_1", "setting_2", "setting_3"])
    sensor_columns: List[str] = field(default_factory=lambda: [f"sensor_{i}" for i in range(1, 22)])

    # Sensors with near-zero variance in FD001 (drop to reduce memory and noise)
    # sensor_1, 5, 10, 16, 18, 19 have zero standard deviation in FD001
    drop_sensors: List[str] = field(default_factory=lambda: [
        "sensor_1", "sensor_5", "sensor_10", "sensor_16", "sensor_18", "sensor_19"
    ])

    # Piecewise RUL upper limit (widely adopted in turbofan literature to prevent early-life penalty)
    rul_clip_limit: float = float(os.getenv("RUL_CLIP_LIMIT", "125.0"))
    
    # Feature window size
    rolling_window_sizes: List[int] = field(default_factory=lambda: [10, 20])


@dataclass
class KafkaConfig:
    """Apache Kafka broker configuration."""
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_raw: str = os.getenv("KAFKA_TOPIC_RAW", "engine_sensor_raw")
    topic_predictions: str = os.getenv("KAFKA_TOPIC_PRED", "rul_predictions")
    topic_alerts: str = os.getenv("KAFKA_TOPIC_ALERTS", "engine_alerts")
    client_id: str = "predmaint-sensor-producer"
    group_id: str = "predmaint-spark-consumer-group"


@dataclass
class SparkConfig:
    """Apache Spark Structured Streaming configuration."""
    app_name: str = "PredictiveMaintenanceStreaming"
    master: str = os.getenv("SPARK_MASTER", "local[*]")
    checkpoint_location: str = str(CHECKPOINT_DIR / "spark_stream")
    trigger_interval: str = os.getenv("SPARK_TRIGGER_INTERVAL", "2 seconds")
    shuffle_partitions: str = os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")


@dataclass
class InfluxDBConfig:
    """InfluxDB v2 configuration."""
    url: str = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    token: str = os.getenv("INFLUXDB_TOKEN", "predmaint-secret-admin-token-2026")
    org: str = os.getenv("INFLUXDB_ORG", "predmaint_org")
    bucket: str = os.getenv("INFLUXDB_BUCKET", "turbofan_telemetry")
    batch_size: int = int(os.getenv("INFLUXDB_BATCH_SIZE", "500"))
    flush_interval_ms: int = int(os.getenv("INFLUXDB_FLUSH_INTERVAL", "1000"))


@dataclass
class AlertConfig:
    """Equipment health and alert status thresholds."""
    rul_warning_threshold: float = 50.0   # 20 < RUL <= 50 -> WARNING
    rul_critical_threshold: float = 20.0  # RUL <= 20 -> CRITICAL
    rul_reference: float = 125.0          # Baseline for Health Score calculation


# Singleton Instances
dataset_cfg = DatasetConfig()
kafka_cfg = KafkaConfig()
spark_cfg = SparkConfig()
influx_cfg = InfluxDBConfig()
alert_cfg = AlertConfig()
