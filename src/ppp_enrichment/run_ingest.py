"""CLI entrypoint: PPP raw CSV -> ``data/output/borrowers_base.csv``."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from . import config, ingest
from .config import get_config
from .ingest import build_borrowers_base


def _configure_ingest_logging() -> None:
    """Attach console + ``logs/ingest.log`` handlers for pipeline loggers."""
    cfg = get_config()
    level = getattr(logging, cfg.log_level.upper(), logging.INFO)
    logs_dir = cfg.logs_dir
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "ingest.log"
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    root = logging.getLogger()
    root.setLevel(level)

    def _has_ingest_file_handler() -> bool:
        ingest_resolved = log_file.resolve()
        for handler in root.handlers:
            if isinstance(handler, logging.FileHandler):
                try:
                    if Path(handler.baseFilename).resolve() == ingest_resolved:
                        return True
                except OSError:
                    continue
        return False

    if not _has_ingest_file_handler():
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        root.addHandler(file_handler)

    has_stream = any(type(handler) is logging.StreamHandler for handler in root.handlers)
    if not has_stream:
        stream_handler = logging.StreamHandler(sys.stderr)
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(level)
        root.addHandler(stream_handler)


def main() -> None:
    _configure_ingest_logging()

    raw_path = Path(config.PPP_RAW_PATH)
    if not raw_path.exists():
        raise FileNotFoundError(
            f"Expected PPP raw CSV at {raw_path}. Download the SBA PPP FOIA extract and save it as ppp-war.csv."
        )

    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df = build_borrowers_base(
        input_paths=[config.PPP_RAW_PATH],
        states=None,
        min_loan_amount=None,
        naics_codes=None,
    )
    out_path = Path(config.BORROWERS_BASE_PATH)
    df.to_csv(out_path, index=False)
    print(f"Saved {len(df)} rows to {out_path}")


if __name__ == "__main__":
    main()
