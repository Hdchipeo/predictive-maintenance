"""Equipment health scoring and multi-tier alert engine."""

from typing import Tuple
from src.config import alert_cfg


def compute_health_score(predicted_rul: float, baseline_rul: float = alert_cfg.rul_reference) -> float:
    """Compute normalized equipment Health Score percentage (0.0% to 100.0%).
    
    Formula:
        Health Score = 100.0 * min(Predicted RUL / Reference RUL, 1.0)
    """
    ratio = max(0.0, float(predicted_rul)) / max(baseline_rul, 1.0)
    return round(min(ratio, 1.0) * 100.0, 2)


def evaluate_engine_status(predicted_rul: float) -> Tuple[str, str]:
    """Classify operational health status and severity tier.
    
    Returns:
        (status_label, severity_color)
        - HEALTHY (green): RUL > 50
        - WARNING (yellow): 20 < RUL <= 50
        - CRITICAL (red): RUL <= 20
    """
    if predicted_rul > alert_cfg.rul_warning_threshold:
        return "HEALTHY", "green"
    elif predicted_rul > alert_cfg.rul_critical_threshold:
        return "WARNING", "yellow"
    else:
        return "CRITICAL", "red"
