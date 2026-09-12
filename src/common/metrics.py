"""Metrics tracking for latency, throughput, and memory consumption."""

import time
import psutil
from dataclasses import dataclass, field
from typing import Dict, Any


@dataclass
class PipelineMetrics:
    """Stores performance telemetry for Big Data benchmarking."""
    total_events_processed: int = 0
    start_time: float = field(default_factory=time.time)
    last_batch_time: float = field(default_factory=time.time)
    batch_latencies_ms: list = field(default_factory=list)

    def record_batch(self, count: int, duration_seconds: float) -> None:
        """Record batch throughput and latency."""
        self.total_events_processed += count
        latency_ms = duration_seconds * 1000.0
        self.batch_latencies_ms.append(latency_ms)
        self.last_batch_time = time.time()

    def get_summary(self) -> Dict[str, Any]:
        """Compute real-time summary stats."""
        total_time = max(time.time() - self.start_time, 1e-6)
        throughput = self.total_events_processed / total_time
        avg_latency = (
            sum(self.batch_latencies_ms) / len(self.batch_latencies_ms)
            if self.batch_latencies_ms else 0.0
        )
        process = psutil.Process()
        mem_info = process.memory_info()

        return {
            "total_events": self.total_events_processed,
            "elapsed_seconds": round(total_time, 2),
            "throughput_eps": round(throughput, 2),
            "avg_latency_ms": round(avg_latency, 2),
            "memory_rss_mb": round(mem_info.rss / (1024 * 1024), 2),
            "cpu_percent": process.cpu_percent(interval=None)
        }
