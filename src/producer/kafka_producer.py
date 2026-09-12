"""Resilient Kafka Producer client with auto-reconnect and mock fallback."""

import json
import time
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from src.config import kafka_cfg
from src.common.logger import get_logger

logger = get_logger("KafkaProducerClient")


class ResilientKafkaProducer:
    """Kafka Producer wrapper with connection retries and fallback."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        retries: int = 5,
        backoff_seconds: float = 2.0,
        mock_mode: bool = False
    ):
        self.bootstrap_servers = bootstrap_servers or kafka_cfg.bootstrap_servers
        self.retries = retries
        self.backoff_seconds = backoff_seconds
        self.producer: Optional[KafkaProducer] = None
        self.mock_mode: bool = mock_mode
        if not self.mock_mode:
            self._init_producer()

    def _init_producer(self) -> None:
        """Attempt connection to Kafka cluster."""
        for attempt in range(1, self.retries + 1):
            try:
                logger.info(
                    f"Connecting to Kafka broker(s) at {self.bootstrap_servers} (Attempt {attempt}/{self.retries})..."
                )
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
                    acks="all",
                    retries=3,
                    linger_ms=10,
                    batch_size=16384
                )
                logger.info("Kafka Producer successfully connected.")
                self.mock_mode = False
                return
            except NoBrokersAvailable:
                logger.warning(f"No Kafka brokers available at {self.bootstrap_servers}.")
                if attempt < self.retries:
                    time.sleep(self.backoff_seconds)
            except Exception as e:
                logger.warning(f"Connection failed: {e}")
                if attempt < self.retries:
                    time.sleep(self.backoff_seconds)

        logger.warning(
            "Could not connect to Kafka. Operating in DRY-RUN / MOCK mode (logging only)."
        )
        self.mock_mode = True

    def send_event(
        self,
        topic: str,
        value: Dict[str, Any],
        key: Optional[Any] = None
    ) -> bool:
        """Publish event to Kafka topic or mock sink."""
        if self.mock_mode or self.producer is None:
            # Mock mode: successfully simulated
            return True

        try:
            future = self.producer.send(topic, key=key, value=value)
            return True
        except Exception as e:
            logger.error(f"Failed to publish to topic {topic}: {e}")
            return False

    def flush(self) -> None:
        """Flush producer message buffer."""
        if self.producer is not None:
            self.producer.flush()

    def close(self) -> None:
        """Close producer connection."""
        if self.producer is not None:
            self.producer.close()
            logger.info("Kafka Producer closed.")
