"""Real-time ML inference engine with stateful online feature store for streaming."""

import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional
from pathlib import Path
from collections import deque

from src.config import MODELS_DIR, dataset_cfg
from src.training.export import load_model_artifact
from src.streaming.alert_engine import compute_health_score, evaluate_engine_status
from src.common.logger import get_logger

logger = get_logger("InferenceEngine")


class StreamingInferenceEngine:
    """Manages model artifacts, stateful engine rolling buffers, and streaming predictions."""

    def __init__(self, model_dir: Optional[Path] = None, max_buffer_size: int = 25):
        self.model_dir = model_dir or (MODELS_DIR / "latest")
        self.max_buffer_size = max_buffer_size
        self.engine_buffers: Dict[int, deque] = {}
        self.model = None
        self.metadata = None
        self.feature_columns: List[str] = []
        self._load_model()

    def _load_model(self) -> None:
        """Load model binary and expected feature schema."""
        if not self.model_dir.exists():
            logger.warning(
                f"No model artifact found at {self.model_dir}. Inference will use degradation heuristic until model trained."
            )
            return

        try:
            self.model, self.metadata = load_model_artifact(self.model_dir)
            self.feature_columns = self.metadata.get("feature_columns", [])
            logger.info(
                f"Inference Engine initialized with model '{self.metadata['model_name']}' "
                f"({len(self.feature_columns)} features)"
            )
        except Exception as e:
            logger.error(f"Failed to load model from {self.model_dir}: {e}")

    def _update_and_extract_online_features(self, row: pd.Series) -> Dict[str, float]:
        """Update sliding window state buffer and compute rolling features for a single event."""
        engine_id = int(row["engine_id"])
        if engine_id not in self.engine_buffers:
            self.engine_buffers[engine_id] = deque(maxlen=self.max_buffer_size)

        # Buffer only numerical sensor columns
        sensor_dict = {
            col: float(row[col])
            for col in row.index
            if col.startswith("sensor_") or col.startswith("setting_")
        }
        self.engine_buffers[engine_id].append(sensor_dict)
        buffer_list = list(self.engine_buffers[engine_id])
        n_history = len(buffer_list)

        features = dict(sensor_dict)

        # Dynamic sensor columns (excluding dropped ones)
        active_sensors = [
            s for s in sensor_dict.keys()
            if s.startswith("sensor_") and s not in dataset_cfg.drop_sensors
        ]

        for w in dataset_cfg.rolling_window_sizes:
            window_slice = buffer_list[-w:] if n_history >= w else buffer_list
            for s in active_sensors:
                vals = [item[s] for item in window_slice if s in item]
                if vals:
                    mean_val = float(np.mean(vals))
                    std_val = float(np.std(vals)) if len(vals) > 1 else 0.0
                    delta_val = float(vals[-1] - vals[0]) if len(vals) > 1 else 0.0
                else:
                    mean_val = float(row.get(s, 0.0))
                    std_val = 0.0
                    delta_val = 0.0

                features[f"{s}_mean_{w}"] = mean_val
                features[f"{s}_std_{w}"] = std_val
                features[f"{s}_delta_{w}"] = delta_val

        return features

    def predict_microbatch(self, batch_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Perform stateful feature extraction and vectorized inference on a micro-batch."""
        if batch_df.empty:
            return []

        df_in = batch_df.copy().sort_values(by=["engine_id", "cycle"])
        extracted_rows = []

        # 1. State-aware online feature enrichment
        for idx in range(len(df_in)):
            row = df_in.iloc[idx]
            feats = self._update_and_extract_online_features(row)
            feats["engine_id"] = int(row.get("engine_id", 0))
            feats["cycle"] = int(row.get("cycle", 0))
            feats["timestamp"] = row.get("timestamp", None)
            extracted_rows.append(feats)

        feature_df = pd.DataFrame(extracted_rows)

        # 2. Model prediction
        if self.model is not None and self.feature_columns:
            for col in self.feature_columns:
                if col not in feature_df.columns:
                    feature_df[col] = 0.0

            X = feature_df[self.feature_columns]
            preds = self.model.predict(X)
            preds = np.clip(preds, 0.0, dataset_cfg.rul_clip_limit)
        else:
            # Fallback heuristic degradation (if model not available)
            preds = np.maximum(0.0, 140.0 - feature_df["cycle"].values)

        # 3. Format telemetry points with derived metrics
        results = []
        for idx in range(len(feature_df)):
            row = feature_df.iloc[idx]
            pred_rul = float(preds[idx])
            health_score = compute_health_score(pred_rul)
            status, severity = evaluate_engine_status(pred_rul)

            record = {
                "timestamp": row.get("timestamp", None),
                "engine_id": int(row.get("engine_id", 0)),
                "cycle": int(row.get("cycle", 0)),
                "predicted_rul": round(pred_rul, 2),
                "health_score": health_score,
                "status": status,
                "severity": severity,
                "sensor_2": float(row.get("sensor_2", 0.0)),
                "sensor_3": float(row.get("sensor_3", 0.0)),
                "sensor_4": float(row.get("sensor_4", 0.0)),
                "sensor_7": float(row.get("sensor_7", 0.0)),
                "sensor_11": float(row.get("sensor_11", 0.0)),
                "sensor_12": float(row.get("sensor_12", 0.0)),
            }
            results.append(record)

        return results
