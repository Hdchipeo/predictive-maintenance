"""Unit tests for Health Score and alert thresholding."""

import pytest
from src.streaming.alert_engine import compute_health_score, evaluate_engine_status


def test_compute_health_score():
    """Verify health score normalization (0 to 100%)."""
    # Brand new engine (RUL >= 125)
    assert compute_health_score(150.0, baseline_rul=125.0) == 100.0
    assert compute_health_score(125.0, baseline_rul=125.0) == 100.0

    # Half life
    assert compute_health_score(62.5, baseline_rul=125.0) == 50.0

    # Failed engine
    assert compute_health_score(0.0, baseline_rul=125.0) == 0.0
    assert compute_health_score(-5.0, baseline_rul=125.0) == 0.0


def test_evaluate_engine_status():
    """Verify multi-tier status and color coding."""
    # Healthy tier (> 50)
    status, color = evaluate_engine_status(75.0)
    assert status == "HEALTHY"
    assert color == "green"

    # Warning tier (20 < RUL <= 50)
    status, color = evaluate_engine_status(35.0)
    assert status == "WARNING"
    assert color == "yellow"

    # Critical tier (<= 20)
    status, color = evaluate_engine_status(12.0)
    assert status == "CRITICAL"
    assert color == "red"

    status, color = evaluate_engine_status(0.0)
    assert status == "CRITICAL"
    assert color == "red"
