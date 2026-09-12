"""Model artifact serialization and versioning module."""

import json
import joblib
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

from src.config import MODELS_DIR, dataset_cfg
from src.common.logger import get_logger

logger = get_logger("ModelExporter")


def save_model_artifact(
    model: Any,
    model_name: str,
    feature_columns: List[str],
    evaluation_metrics: Dict[str, Any],
    version: str = "v1.0"
) -> Path:
    """Save model binary (.joblib) alongside comprehensive JSON metadata."""
    artifact_dir = MODELS_DIR / f"{model_name}_{version}"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    # 1. Save binary weights
    model_file = artifact_dir / "model.joblib"
    joblib.dump(model, model_file)

    # 2. Save metadata & schema contract
    metadata = {
        "model_name": model_name,
        "version": version,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "dataset": dataset_cfg.dataset_name,
        "rul_clip_limit": dataset_cfg.rul_clip_limit,
        "feature_count": len(feature_columns),
        "feature_columns": feature_columns,
        "metrics": evaluation_metrics
    }
    meta_file = artifact_dir / "metadata.json"
    with open(meta_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    # 3. Create 'latest' pointer
    latest_dir = MODELS_DIR / "latest"
    if latest_dir.exists():
        if latest_dir.is_symlink():
            latest_dir.unlink()
        else:
            import shutil
            shutil.rmtree(latest_dir)
    try:
        latest_dir.symlink_to(artifact_dir.name, target_is_directory=True)
    except Exception:
        # Fallback if symlinks restricted: save directly in latest folder
        latest_dir.mkdir(parents=True, exist_ok=True)
        joblib.dump(model, latest_dir / "model.joblib")
        with open(latest_dir / "metadata.json", "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

    logger.info(f"Exported versioned model artifact to: {artifact_dir}")
    return artifact_dir


def load_model_artifact(artifact_path: Path) -> Tuple[Any, Dict[str, Any]]:
    """Load model binary and its corresponding metadata."""
    from typing import Tuple
    model_file = artifact_path / "model.joblib"
    meta_file = artifact_path / "metadata.json"

    if not model_file.exists():
        raise FileNotFoundError(f"Model file not found at: {model_file}")

    model = joblib.load(model_file)
    with open(meta_file, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    logger.info(f"Loaded model '{metadata['model_name']}' version {metadata['version']}")
    return model, metadata
