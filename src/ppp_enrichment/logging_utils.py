"""Shared logging setup for all pipeline modules."""

from __future__ import annotations

import logging
from pathlib import Path

from .config import AppConfig


def configure_logging(config: AppConfig) -> None:
    """Configure process-wide logging once at startup.

    TODO:
    - Add JSON logging formatter option for production jobs.
    - Add per-stage log file routing if needed.
    """
    config.logs_dir.mkdir(parents=True, exist_ok=True)
    log_file: Path = config.logs_dir / "ppp_enrichment.log"

    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )


def get_logger(name: str) -> logging.Logger:
    """Return a module logger using the shared logging namespace."""
    return logging.getLogger(name)
