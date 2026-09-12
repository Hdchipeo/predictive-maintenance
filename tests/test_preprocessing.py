"""Unit tests for data loader, RUL calculation, and feature engineering."""

import pytest
import numpy as np
import pandas as pd

from src.preprocessing.label_rul import add_rul_target
from src.preprocessing.clean import prune_low_variance_sensors, clean_dataframe
from src.preprocessing.feature_engineering import extract_rolling_features, get_feature_columns


@pytest.fixture
def sample_telemetry_df():
    """Create a minimal synthetic DataFrame for testing."""
    records = []
    for engine in [1, 2]:
        for cycle in range(1, 51):
            records.append({
                "engine_id": engine,
                "cycle": cycle,
                "setting_1": 0.0,
                "setting_2": 0.0,
                "setting_3": 100.0,
                "sensor_1": 518.67,  # constant
                "sensor_2": 642.0 + cycle * 0.1,
                "sensor_3": 1585.0 + cycle * 0.2,
                "sensor_4": 1400.0 + cycle * 0.15,
                "sensor_5": 14.62,   # constant
            })
    return pd.DataFrame(records)


def test_add_rul_target(sample_telemetry_df):
    """Verify RUL calculation and piecewise clipping."""
    df_rul = add_rul_target(sample_telemetry_df, clip_limit=30.0)

    assert "raw_rul" in df_rul.columns
    assert "RUL" in df_rul.columns

    # For engine 1 at cycle 1: max_cycle is 50, raw_rul is 49, clipped RUL is 30.0
    e1_c1 = df_rul[(df_rul["engine_id"] == 1) & (df_rul["cycle"] == 1)].iloc[0]
    assert e1_c1["raw_rul"] == 49
    assert e1_c1["RUL"] == 30.0

    # For engine 1 at cycle 50: raw_rul is 0, clipped RUL is 0
    e1_c50 = df_rul[(df_rul["engine_id"] == 1) & (df_rul["cycle"] == 50)].iloc[0]
    assert e1_c50["raw_rul"] == 0
    assert e1_c50["RUL"] == 0.0


def test_prune_low_variance_sensors(sample_telemetry_df):
    """Verify constant sensors are correctly pruned."""
    cleaned, dropped = prune_low_variance_sensors(sample_telemetry_df)

    assert "sensor_1" in dropped
    assert "sensor_5" in dropped
    assert "sensor_1" not in cleaned.columns
    assert "sensor_5" not in cleaned.columns
    assert "sensor_2" in cleaned.columns


def test_extract_rolling_features(sample_telemetry_df):
    """Verify rolling mean, std, and delta columns are computed."""
    df_feats = extract_rolling_features(
        sample_telemetry_df,
        sensor_cols=["sensor_2", "sensor_3"],
        windows=[5]
    )

    assert "sensor_2_mean_5" in df_feats.columns
    assert "sensor_2_std_5" in df_feats.columns
    assert "sensor_2_delta_5" in df_feats.columns

    # Verify no NaN values
    assert df_feats["sensor_2_mean_5"].isna().sum() == 0
    assert df_feats["sensor_2_std_5"].isna().sum() == 0

    feature_cols = get_feature_columns(df_feats)
    assert "engine_id" not in feature_cols
    assert "cycle" not in feature_cols
    assert "sensor_2_mean_5" in feature_cols
