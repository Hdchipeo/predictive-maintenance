"""Big Data Benchmark and Performance Profiler."""

import time
import argparse
import pandas as pd
from typing import Dict, Any

from src.config import dataset_cfg
from src.preprocessing.loader import load_data
from src.preprocessing.label_rul import add_rul_target
from src.preprocessing.clean import prune_low_variance_sensors, clean_dataframe
from src.preprocessing.feature_engineering import extract_rolling_features, get_feature_columns
from src.training.train_rf import train_random_forest_regressor
from src.training.train_xgboost import train_xgboost_regressor
from src.training.evaluate import evaluate_model, generate_comparison_table
from src.training.export import save_model_artifact
from src.common.logger import get_logger

logger = get_logger("BenchmarkRunner")


def run_full_benchmark() -> str:
    """Execute end-to-end data preparation, training, and benchmarking."""
    logger.info("=== STARTING PREDICTIVE MAINTENANCE BENCHMARK ===")
    
    # 1. Load and prepare data
    raw_train = load_data(mode="train")
    raw_test = load_data(mode="test")

    logger.info("Labeling RUL targets...")
    train_labeled = add_rul_target(raw_train)
    test_labeled = add_rul_target(raw_test)

    logger.info("Pruning low-variance sensors...")
    train_cleaned, dropped_sensors = prune_low_variance_sensors(clean_dataframe(train_labeled))
    test_cleaned, _ = prune_low_variance_sensors(clean_dataframe(test_labeled), explicit_drop=dropped_sensors)

    logger.info("Engineering time-series rolling features...")
    train_features = extract_rolling_features(train_cleaned)
    test_features = extract_rolling_features(test_cleaned)

    feature_cols = get_feature_columns(train_features)
    logger.info(f"Total features engineered: {len(feature_cols)}")

    X_train = train_features[feature_cols]
    y_train = train_features["RUL"]
    X_test = test_features[feature_cols]
    y_test = test_features["RUL"]

    # 2. Model 1: Random Forest Baseline
    logger.info("--- Training Random Forest Baseline ---")
    rf_model, rf_train_metrics = train_random_forest_regressor(X_train, y_train)
    rf_eval = evaluate_model(rf_model, X_test, y_test, model_name="Random Forest (Baseline)")

    # 3. Model 2: XGBoost Production Model
    logger.info("--- Training XGBoost Regressor ---")
    xgb_model, xgb_train_metrics = train_xgboost_regressor(X_train, y_train)
    xgb_eval = evaluate_model(xgb_model, X_test, y_test, model_name="XGBoost (Production)")

    # 4. Compare Models
    reports = [rf_eval, xgb_eval]
    table_markdown = generate_comparison_table(reports)
    print("\n" + table_markdown + "\n")

    # 5. Export Winning Model (XGBoost)
    winner_model = xgb_model if xgb_eval["rmse"] <= rf_eval["rmse"] else rf_model
    winner_name = "xgboost_rul" if winner_model is xgb_model else "rf_rul"
    winner_eval = xgb_eval if winner_model is xgb_model else rf_eval

    save_model_artifact(
        model=winner_model,
        model_name=winner_name,
        feature_columns=feature_cols,
        evaluation_metrics=winner_eval,
        version="v1.0"
    )

    logger.info("Benchmark complete. Model artifact saved successfully.")
    return table_markdown


if __name__ == "__main__":
    run_full_benchmark()
