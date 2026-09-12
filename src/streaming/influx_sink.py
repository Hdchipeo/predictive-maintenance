"""InfluxDB v2 Time-Series Sink for Spark Streaming."""

import time
from typing import List, Dict, Any, Optional
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from src.config import influx_cfg
from src.common.logger import get_logger

logger = get_logger("InfluxDBSink")


class InfluxDBSink:
    """Batch writer for InfluxDB v2 with fallback logging."""

    def __init__(
        self,
        url: Optional[str] = None,
        token: Optional[str] = None,
        org: Optional[str] = None,
        bucket: Optional[str] = None,
    ):
        self.url = url or influx_cfg.url
        self.token = token or influx_cfg.token
        self.org = org or influx_cfg.org
        self.bucket = bucket or influx_cfg.bucket
        self.client: Optional[InfluxDBClient] = None
        self.write_api = None
        self.mock_mode = False
        self._init_connection()

    def _init_connection(self) -> None:
        """Establish connection and verify InfluxDB server health."""
        try:
            self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
            health = self.client.health()
            if health.status == "pass":
                self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
                logger.info(f"Connected to InfluxDB at {self.url} (Org: {self.org}, Bucket: {self.bucket})")
                self.mock_mode = False
                return
            else:
                logger.warning(f"InfluxDB health status: {health.status}. Falling back to MOCK mode.")
        except Exception as e:
            logger.warning(f"Could not connect to InfluxDB ({e}). Operating in MOCK mode.")

        self.mock_mode = True

    def write_predictions_batch(self, records: List[Dict[str, Any]]) -> bool:
        """Write a batch of inferred engine states into InfluxDB."""
        if not records:
            return True

        if self.mock_mode or self.write_api is None:
            # Mock mode: simulate write success
            return True

        points = []
        for r in records:
            point = (
                Point("engine_health")
                .tag("engine_id", str(r.get("engine_id", 0)))
                .tag("status", str(r.get("status", "HEALTHY")))
                .field("cycle", int(r.get("cycle", 0)))
                .field("predicted_rul", float(r.get("predicted_rul", 0.0)))
                .field("health_score", float(r.get("health_score", 100.0)))
            )

            # Record core sensor indicators
            for s in ["sensor_2", "sensor_3", "sensor_4", "sensor_7", "sensor_11", "sensor_12"]:
                if s in r:
                    point = point.field(s, float(r[s]))

            if "timestamp" in r:
                point = point.time(r["timestamp"], WritePrecision.NS)
            points.append(point)

        try:
            self.write_api.write(bucket=self.bucket, org=self.org, record=points)
            return True
        except Exception as e:
            logger.error(f"Error writing batch to InfluxDB: {e}")
            return False

    def close(self) -> None:
        """Close InfluxDB client connection."""
        if self.client is not None:
            self.client.close()
            logger.info("InfluxDB connection closed.")
