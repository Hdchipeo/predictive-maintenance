"""Feature engineering module for time-series sensor data."""

import pandas as pd
from typing import List, Optional
from src.config import dataset_cfg
from src.common.logger import get_logger

logger = get_logger("FeatureEngineering")


def extract_rolling_features(
    df: pd.DataFrame,
    sensor_cols: Optional[List[str]] = None,
    windows: Optional[List[int]] = None
) -> pd.DataFrame:
    """Compute rolling mean, rolling std, and trend/delta features per engine.
    
    Args:
        df: Input DataFrame containing 'engine_id', 'cycle', and sensor columns
        sensor_cols: List of sensors to compute features for
        windows: List of window sizes (cycles)
    """
    df_out = df.copy().sort_values(["engine_id", "cycle"])
    win_list = windows or dataset_cfg.rolling_window_sizes

    if sensor_cols is None:
        sensor_cols = [
            c for c in df_out.columns
            if c.startswith("sensor_") and c not in dataset_cfg.drop_sensors
        ]

    logger.info(f"Generating rolling features for {len(sensor_cols)} sensors with windows: {win_list}")

    for w in win_list:
        # Group by engine_id to prevent data leakage across engines
        grouped = df_out.groupby("engine_id")[sensor_cols]

        # 1. Rolling Mean
        rolling_mean = grouped.transform(lambda x: x.rolling(w, min_periods=1).mean())
        rolling_mean.columns = [f"{col}_mean_{w}" for col in sensor_cols]

        # 2. Rolling Standard Deviation
        rolling_std = grouped.transform(lambda x: x.rolling(w, min_periods=1).std()).fillna(0.0)
        rolling_std.columns = [f"{col}_std_{w}" for col in sensor_cols]

        # 3. Rolling Trend / Delta
        rolling_delta = grouped.transform(lambda x: x.diff(w)).fillna(0.0)
        rolling_delta.columns = [f"{col}_delta_{w}" for col in sensor_cols]

        df_out = pd.concat([df_out, rolling_mean, rolling_std, rolling_delta], axis=1)

    logger.info(f"Engineered DataFrame shape: {df_out.shape}")
    return df_out


def get_feature_columns(df: pd.DataFrame) -> List[str]:
    """Retrieve all model input feature names (excluding metadata and targets)."""
    exclude_cols = set(dataset_cfg.index_columns + ["raw_rul", "RUL"])
    features = [c for c in df.columns if c not in exclude_cols]
    return features
