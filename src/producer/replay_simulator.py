"""Streaming sensor replay simulator for NASA C-MAPSS telemetry."""

import time
import argparse
from datetime import datetime, timezone
from typing import Optional
import pandas as pd

from src.config import dataset_cfg, kafka_cfg
from src.preprocessing.loader import load_data
from src.producer.kafka_producer import ResilientKafkaProducer
from src.common.logger import get_logger

logger = get_logger("ReplaySimulator")


def run_replay(
    mode: str = "test",
    rate_eps: float = 20.0,
    max_events: Optional[int] = None,
    interleaved: bool = True,
    dry_run: bool = False
) -> int:
    """Stream C-MAPSS sensor records into Kafka topic.
    
    Args:
        mode: 'train' or 'test' subset
        rate_eps: Replay speed in events per second (0 for maximum throughput)
        max_events: Maximum number of records to emit (None for entire file)
        interleaved: If True, cycle 1 for all engines, then cycle 2, etc. (realistic fleet telemetry)
        dry_run: If True, run without sending to real Kafka
    """
    logger.info(f"Loading {mode}_{dataset_cfg.dataset_name} for replay streaming...")
    df = load_data(subset=dataset_cfg.dataset_name, mode=mode)

    if interleaved:
        df = df.sort_values(by=["cycle", "engine_id"])
    else:
        df = df.sort_values(by=["engine_id", "cycle"])

    producer = ResilientKafkaProducer(mock_mode=dry_run)

    sleep_interval = 1.0 / rate_eps if rate_eps > 0 else 0.0
    sent_count = 0
    start_time = time.time()

    logger.info(f"Starting replay stream at target rate: {rate_eps} events/sec...")

    try:
        for idx, row in df.iterrows():
            if max_events is not None and sent_count >= max_events:
                break

            record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "engine_id": int(row["engine_id"]),
                "cycle": int(row["cycle"]),
                "setting_1": float(row["setting_1"]),
                "setting_2": float(row["setting_2"]),
                "setting_3": float(row["setting_3"]),
            }

            # Add sensor values
            for col in dataset_cfg.sensor_columns:
                if col in row:
                    record[col] = float(row[col])

            key = str(record["engine_id"])
            success = producer.send_event(
                topic=kafka_cfg.topic_raw,
                value=record,
                key=key
            )

            if success:
                sent_count += 1

            if sent_count % 500 == 0:
                elapsed = time.time() - start_time
                actual_eps = sent_count / max(elapsed, 1e-6)
                logger.info(f"Streamed {sent_count} events (Actual throughput: {actual_eps:.1f} eps)")

            if sleep_interval > 0:
                time.sleep(sleep_interval)

    except KeyboardInterrupt:
        logger.info("Replay stream stopped by user.")
    finally:
        producer.flush()
        producer.close()
        total_time = time.time() - start_time
        logger.info(
            f"Replay completed: {sent_count} total events sent in {total_time:.2f}s "
            f"({sent_count / max(total_time, 1e-6):.1f} avg eps)"
        )

    return sent_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NASA C-MAPSS Sensor Streaming Replayer")
    parser.add_argument("--mode", type=str, default="test", choices=["train", "test"], help="Dataset split")
    parser.add_argument("--rate", type=float, default=20.0, help="Events per second (0 for max speed)")
    parser.add_argument("--max-events", type=int, default=None, help="Maximum events to send")
    parser.add_argument("--sequential", action="store_true", help="Stream engine by engine instead of interleaved")
    parser.add_argument("--dry-run", action="store_true", help="Run without connecting to Kafka")
    args = parser.parse_args()

    run_replay(
        mode=args.mode,
        rate_eps=args.rate,
        max_events=args.max_events,
        interleaved=not args.sequential,
        dry_run=args.dry_run
    )
