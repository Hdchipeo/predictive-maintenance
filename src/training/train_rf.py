"""Random Forest Regressor baseline model for RUL prediction."""

import time
from typing import Tuple, Dict, Any
from sklearn.ensemble import RandomForestRegressor
import pandas as pd

from src.common.logger import get_logger

logger = get_logger("TrainRandomForest")


def train_random_forest_regressor(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    n_estimators: int = 150,
    max_depth: int = 15,
    random_state: int = 42
) -> Tuple[RandomForestRegressor, Dict[str, Any]]:
    """Train Random Forest baseline model."""
    logger.info(f"Training Random Forest (n_estimators={n_estimators}, max_depth={max_depth})...")
    start_time = time.time()

    model = RandomForestRegressor(
        n_estimators=n_estimators,
        max_depth=max_depth,
        random_state=random_state,
        n_jobs=-1
    )
    model.fit(X_train, y_train)
    train_duration = time.time() - start_time

    logger.info(f"Random Forest training finished in {train_duration:.2f}s")
    metrics = {
        "model_type": "RandomForestRegressor",
        "train_time_seconds": train_duration,
        "n_estimators": n_estimators,
        "max_depth": max_depth,
        "feature_count": X_train.shape[1]
    }
    return model, metrics
