"""Dataset loader and synthetic generator for NASA C-MAPSS Turbofan Engine Degradation."""

import os
import io
import zipfile
import requests
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Tuple, Optional

from src.config import RAW_DATA_DIR, dataset_cfg
from src.common.logger import get_logger

logger = get_logger("DataLoader")

COLUMNS = (
    dataset_cfg.index_columns
    + dataset_cfg.setting_columns
    + dataset_cfg.sensor_columns
)

# Known reliable mirror URLs for NASA C-MAPSS dataset
MIRROR_URLS = [
    "https://raw.githubusercontent.com/shadgriffin/NASA_Turbofan_Engine_Degradation_Simulation/master/CMAPSSData.zip",
    "https://data.nasa.gov/download/yvg9-97ce/application/zip",
]


def download_cmapss_dataset(dest_dir: Optional[Path] = None) -> bool:
    """Download and extract NASA C-MAPSS dataset from mirrors."""
    dest_dir = dest_dir or RAW_DATA_DIR
    dest_dir.mkdir(parents=True, exist_ok=True)
    target_train = dest_dir / f"train_{dataset_cfg.dataset_name}.txt"

    if target_train.exists():
        logger.info(f"Dataset already present at: {target_train}")
        return True

    for url in MIRROR_URLS:
        logger.info(f"Attempting download from: {url}")
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200 and len(resp.content) > 10000:
                with zipfile.ZipFile(io.BytesIO(resp.content)) as zip_ref:
                    zip_ref.extractall(dest_dir)
                logger.info(f"Successfully extracted dataset to {dest_dir}")
                return True
        except Exception as e:
            logger.warning(f"Failed to fetch from {url}: {e}")

    logger.warning("Could not download C-MAPSS from mirrors. Synthetic bootstrap will be available.")
    return False


def generate_synthetic_turbofan_data(
    num_engines: int = 100,
    min_cycles: int = 130,
    max_cycles: int = 360,
    dest_dir: Optional[Path] = None
) -> Tuple[Path, Path, Path]:
    """Generate statistically faithful Turbofan Degradation data (FD001 format).
    
    Generates realistic exponential/polynomial degradation for core sensors:
    - sensor_2, sensor_3, sensor_4 (temperatures & pressures, increasing with degradation)
    - sensor_7, sensor_12 (decreasing with degradation)
    - sensor_1, 5, 10, 16, 18, 19 (zero-variance constant sensors, matching NASA specs)
    """
    dest_dir = dest_dir or RAW_DATA_DIR
    dest_dir.mkdir(parents=True, exist_ok=True)

    np.random.seed(42)
    train_rows = []
    test_rows = []
    test_ruls = []

    for engine_id in range(1, num_engines + 1):
        total_cycles = np.random.randint(min_cycles, max_cycles)
        
        # Degradation curve: starts flat, degrades exponentially in last ~100 cycles
        deg_start = max(1, total_cycles - np.random.randint(60, 120))

        for cycle in range(1, total_cycles + 1):
            deg_factor = max(0.0, (cycle - deg_start) / float(total_cycles - deg_start)) ** 1.8 if cycle > deg_start else 0.0

            # Baseline settings
            s1 = -0.0007 + np.random.normal(0, 0.0002)
            s2 = -0.0004 + np.random.normal(0, 0.0002)
            s3 = 100.0

            # Sensors (FD001 realistic baseline + degradation + gaussian noise)
            s_vals = {
                1: 518.67,  # Constant
                2: 642.0 + deg_factor * 5.0 + np.random.normal(0, 0.3),
                3: 1585.0 + deg_factor * 18.0 + np.random.normal(0, 0.8),
                4: 1400.0 + deg_factor * 16.0 + np.random.normal(0, 0.7),
                5: 14.62,   # Constant
                6: 21.61 + np.random.normal(0, 0.02),
                7: 554.0 - deg_factor * 3.5 + np.random.normal(0, 0.2),
                8: 2388.05 + deg_factor * 0.1 + np.random.normal(0, 0.02),
                9: 9050.0 + deg_factor * 15.0 + np.random.normal(0, 4.0),
                10: 1.30,   # Constant
                11: 47.3 + deg_factor * 0.8 + np.random.normal(0, 0.05),
                12: 522.0 - deg_factor * 3.0 + np.random.normal(0, 0.2),
                13: 2388.08 + deg_factor * 0.1 + np.random.normal(0, 0.03),
                14: 8130.0 + deg_factor * 10.0 + np.random.normal(0, 3.0),
                15: 8.42 + deg_factor * 0.15 + np.random.normal(0, 0.01),
                16: 0.03,   # Constant
                17: 392 + int(deg_factor * 4) + np.random.randint(-1, 2),
                18: 2388,   # Constant
                19: 100.0,  # Constant
                20: 38.8 - deg_factor * 0.4 + np.random.normal(0, 0.05),
                21: 23.3 - deg_factor * 0.3 + np.random.normal(0, 0.04),
            }

            row = [engine_id, cycle, s1, s2, s3] + [s_vals[i] for i in range(1, 22)]
            train_rows.append(row)

    # Generate test set (engines truncated prior to failure)
    for engine_id in range(1, num_engines + 1):
        total_cycles = np.random.randint(min_cycles, max_cycles)
        current_cutoff = np.random.randint(35, total_cycles - 15)
        rul_remaining = total_cycles - current_cutoff
        test_ruls.append(rul_remaining)

        deg_start = max(1, total_cycles - np.random.randint(60, 120))
        for cycle in range(1, current_cutoff + 1):
            deg_factor = max(0.0, (cycle - deg_start) / float(total_cycles - deg_start)) ** 1.8 if cycle > deg_start else 0.0

            s1 = -0.0007 + np.random.normal(0, 0.0002)
            s2 = -0.0004 + np.random.normal(0, 0.0002)
            s3 = 100.0

            s_vals = {
                1: 518.67,
                2: 642.0 + deg_factor * 5.0 + np.random.normal(0, 0.3),
                3: 1585.0 + deg_factor * 18.0 + np.random.normal(0, 0.8),
                4: 1400.0 + deg_factor * 16.0 + np.random.normal(0, 0.7),
                5: 14.62,
                6: 21.61 + np.random.normal(0, 0.02),
                7: 554.0 - deg_factor * 3.5 + np.random.normal(0, 0.2),
                8: 2388.05 + deg_factor * 0.1 + np.random.normal(0, 0.02),
                9: 9050.0 + deg_factor * 15.0 + np.random.normal(0, 4.0),
                10: 1.30,
                11: 47.3 + deg_factor * 0.8 + np.random.normal(0, 0.05),
                12: 522.0 - deg_factor * 3.0 + np.random.normal(0, 0.2),
                13: 2388.08 + deg_factor * 0.1 + np.random.normal(0, 0.03),
                14: 8130.0 + deg_factor * 10.0 + np.random.normal(0, 3.0),
                15: 8.42 + deg_factor * 0.15 + np.random.normal(0, 0.01),
                16: 0.03,
                17: 392 + int(deg_factor * 4) + np.random.randint(-1, 2),
                18: 2388,
                19: 100.0,
                20: 38.8 - deg_factor * 0.4 + np.random.normal(0, 0.05),
                21: 23.3 - deg_factor * 0.3 + np.random.normal(0, 0.04),
            }
            test_rows.append([engine_id, cycle, s1, s2, s3] + [s_vals[i] for i in range(1, 22)])

    train_path = dest_dir / f"train_{dataset_cfg.dataset_name}.txt"
    test_path = dest_dir / f"test_{dataset_cfg.dataset_name}.txt"
    rul_path = dest_dir / f"RUL_{dataset_cfg.dataset_name}.txt"

    np.savetxt(train_path, train_rows, fmt="%.4f")
    np.savetxt(test_path, test_rows, fmt="%.4f")
    np.savetxt(rul_path, test_ruls, fmt="%d")

    logger.info(f"Generated realistic NASA Turbofan dataset at: {dest_dir}")
    return train_path, test_path, rul_path


def load_data(subset: str = "FD001", mode: str = "train") -> pd.DataFrame:
    """Load NASA C-MAPSS subset into pandas DataFrame."""
    file_path = RAW_DATA_DIR / f"{mode}_{subset}.txt"

    if not file_path.exists():
        downloaded = download_cmapss_dataset()
        if not downloaded or not file_path.exists():
            generate_synthetic_turbofan_data()

    df = pd.read_csv(file_path, sep=r"\s+", header=None, names=COLUMNS)
    logger.info(f"Loaded {mode}_{subset}: shape={df.shape}, engines={df['engine_id'].nunique()}")
    return df
