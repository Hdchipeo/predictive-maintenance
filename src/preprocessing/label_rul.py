"""Remaining Useful Life (RUL) target labeling module."""

import pandas as pd
from typing import Optional
from src.config import dataset_cfg
from src.common.logger import get_logger

logger = get_logger("LabelRUL")


def add_rul_target(
    df: pd.DataFrame,
    clip_limit: Optional[float] = None
) -> pd.DataFrame:
    """Compute Remaining Useful Life (RUL) for each engine cycle.
    
    Formula:
        Raw RUL = max_cycle(engine_id) - current_cycle
        Clipped RUL = min(Raw RUL, clip_limit)
        
    Args:
        df: Input DataFrame containing 'engine_id' and 'cycle'
        clip_limit: Upper bound for RUL (default from config: 125.0)
    """
    df_out = df.copy()
    max_cycles = df_out.groupby("engine_id")["cycle"].transform("max")
    df_out["raw_rul"] = max_cycles - df_out["cycle"]

    limit = clip_limit if clip_limit is not None else dataset_cfg.rul_clip_limit
    df_out["RUL"] = df_out["raw_rul"].clip(upper=limit)

    logger.info(
        f"Calculated RUL: min={df_out['RUL'].min():.1f}, "
        f"max={df_out['RUL'].max():.1f}, mean={df_out['RUL'].mean():.1f}"
    )
    return df_out
