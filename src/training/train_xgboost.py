"""XGBoost Regressor model for high-performance RUL prediction."""

import time
from typing import Tuple, Dict, Any
from xgboost import XGBRegressor
import pandas as pd

from src.common.logger import get_logger

logger = get_logger("TrainXGBoost")


def train_xgboost_regressor(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    n_estimators: int = 300,
    max_depth: int = 6,
    learning_rate: float = 0.05,
    subsample: float = 0.8,
    colsample_bytree: float = 0.8,
    random_state: int = 42
) -> Tuple[XGBRegressor, Dict[str, Any]]:
    """Train XGBoost model optimized for tabular sensor degradation."""
    logger.info(
        f"Training XGBoost (n_estimators={n_estimators}, max_depth={max_depth}, lr={learning_rate})..."
    )
    start_time = time.time()

    model = XGBRegressor(
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=learning_rate,
        subsample=subsample,
        colsample_bytree=colsample_bytree,
        objective="reg:squarederror",
        random_state=random_state,
        n_jobs=-1,
        tree_method="hist"
    )
    model.fit(X_train, y_train)
    train_duration = time.time() - start_time

    logger.info(f"XGBoost training completed in {train_duration:.2f}s")
    metrics = {
        "model_type": "XGBRegressor",
        "train_time_seconds": train_duration,
        "n_estimators": n_estimators,
        "max_depth": max_depth,
        "learning_rate": learning_rate,
        "feature_count": X_train.shape[1]
    }
    return model, metrics
