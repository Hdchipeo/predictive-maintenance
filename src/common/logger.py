"""Production-grade structured logger for Big Data & Streaming operations."""

import sys
import logging
from typing import Optional


def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """Return a configured logger instance with formatted timestamps and module context.
    
    Args:
        name: Name of the logger module
        level: Logging level (defaults to INFO)
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    log_level = level if level is not None else logging.INFO
    logger.setLevel(log_level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] [%(name)s:%(lineno)d] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False

    return logger
