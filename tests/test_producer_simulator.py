"""Unit tests for sensor streaming replay simulator."""

import pytest
from src.producer.replay_simulator import run_replay


def test_replay_simulator_dry_run():
    """Verify replay simulator runs and respects max_events in dry-run mode."""
    events_sent = run_replay(
        mode="test",
        rate_eps=0.0,  # maximum speed
        max_events=25,
        interleaved=True,
        dry_run=True
    )
    assert events_sent == 25
