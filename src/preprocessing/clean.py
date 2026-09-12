"""Data cleaning and variance pruning module."""

import pandas as pd
from typing import List, Optional, Tuple
from src.config import dataset_cfg
from src.common.logger import get_logger

logger = get_logger("DataCleaning")


def prune_low_variance_sensors(
    df: pd.DataFrame,
    variance_threshold: float = 1e-4,
    explicit_drop: Optional[List[str]] = None
) -> Tuple[pd.DataFrame, List[str]]:
    """Remove sensors that exhibit near-zero variance.
    
    Args:
        df: Input DataFrame
        variance_threshold: Minimum variance threshold to retain a sensor
        explicit_drop: Explicit list of column names to drop
    """
    to_drop = list(explicit_drop or dataset_cfg.drop_sensors)
    sensor_cols = [c for c in df.columns if c.startswith("sensor_")]

    # Automatically identify additional invariant sensors
    var_series = df[sensor_cols].var()
    auto_drop = var_series[var_series < variance_threshold].index.tolist()
    combined_drop = list(set(to_drop + auto_drop))

    # Keep only sensors that exist in df
    actual_drop = [c for c in combined_drop if c in df.columns]
    cleaned_df = df.drop(columns=actual_drop, errors="ignore")

    logger.info(f"Dropped {len(actual_drop)} invariant sensors: {actual_drop}")
    return cleaned_df, actual_drop


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Validate data integrity: drop duplicates and handle missing values."""
    init_rows = len(df)
    df_clean = df.drop_duplicates()
    if len(df_clean) < init_rows:
        logger.warning(f"Removed {init_rows - len(df_clean)} duplicate rows.")

    # Handle missing values if any
    null_counts = df_clean.isna().sum().sum()
    if null_counts > 0:
        logger.warning(f"Found {null_counts} missing values. Forward-filling.")
        df_clean = df_clean.groupby("engine_id").ffill().bfill()

    return df_clean
