"""Model evaluation and comparative benchmarking module."""

import time
import numpy as np
import pandas as pd
from typing import Dict, Any, List
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from src.common.logger import get_logger

logger = get_logger("ModelEvaluation")


def compute_nasa_scoring_function(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Compute the asymmetric NASA PHM 2008 scoring function.
    
    Penalizes late predictions (y_pred > y_true, risking failure) much more 
    severely than early predictions (y_pred < y_true, early maintenance).
    
    Formula:
        d = y_pred - y_true
        s_i = exp(-d / 13) - 1 for d < 0 (early)
        s_i = exp(d / 10) - 1 for d >= 0 (late)
    """
    d = y_pred - y_true
    score = np.sum(np.where(d < 0, np.exp(-d / 13.0) - 1.0, np.exp(d / 10.0) - 1.0))
    return float(score)


def evaluate_model(
    model: Any,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    model_name: str = "Model"
) -> Dict[str, Any]:
    """Calculate standard regression metrics and latency benchmarks."""
    # Batch inference timing
    t0 = time.time()
    predictions = model.predict(X_test)
    batch_latency = (time.time() - t0) * 1000.0 / len(X_test)  # ms per sample

    # Single-sample inference latency (100 samples average)
    sample_df = X_test.iloc[:100]
    t_single_start = time.time()
    for i in range(len(sample_df)):
        _ = model.predict(sample_df.iloc[[i]])
    single_latency = (time.time() - t_single_start) * 1000.0 / len(sample_df)

    y_arr = np.array(y_test)
    pred_arr = np.array(predictions)

    mae = mean_absolute_error(y_arr, pred_arr)
    rmse = np.sqrt(mean_squared_error(y_arr, pred_arr))
    r2 = r2_score(y_arr, pred_arr)
    nasa_score = compute_nasa_scoring_function(y_arr, pred_arr)

    report = {
        "model_name": model_name,
        "mae": round(float(mae), 3),
        "rmse": round(float(rmse), 3),
        "r2": round(float(r2), 4),
        "nasa_score": round(nasa_score, 1),
        "batch_latency_ms_per_sample": round(batch_latency, 4),
        "single_latency_ms": round(single_latency, 4),
    }

    logger.info(
        f"[{model_name}] MAE: {report['mae']} | RMSE: {report['rmse']} | "
        f"R2: {report['r2']} | NASA Score: {report['nasa_score']} | "
        f"Latency: {report['batch_latency_ms_per_sample']}ms/sample"
    )
    return report


def generate_comparison_table(reports: List[Dict[str, Any]]) -> str:
    """Format comparative evaluation into a clean Markdown table."""
    headers = [
        "Model", "MAE (cycles)", "RMSE (cycles)", "R² Score", 
        "NASA PHM Score", "Inference Latency (ms)"
    ]
    rows = []
    for r in reports:
        rows.append([
            r["model_name"],
            f"{r['mae']:.2f}",
            f"{r['rmse']:.2f}",
            f"{r['r2']:.4f}",
            f"{r['nasa_score']:.1f}",
            f"{r['batch_latency_ms_per_sample']:.3f}"
        ])

    col_widths = [max(len(str(val)) for val in col) for col in zip(headers, *rows)]
    
    def format_row(row):
        return "| " + " | ".join(str(val).ljust(width) for val, width in zip(row, col_widths)) + " |"

    header_line = format_row(headers)
    separator_line = "|-" + "-|-".join("-" * width for width in col_widths) + "-|"
    data_lines = [format_row(row) for row in rows]

    return "\n".join([header_line, separator_line] + data_lines)
