"""Split a PPP CSV into fixed-size row chunks in the same directory.

Usage::
    python -m src.ppp_enrichment.run_split_ppp_csv
    python -m src.ppp_enrichment.run_split_ppp_csv --path data/input/ppp-war.csv --rows 50000
    python -m src.ppp_enrichment.run_split_ppp_csv --path data/input/ppp-war.csv --out-dir data/input/ppp-war_parts
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

from . import config, ingest


def _normalize_column_name(column: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", column.strip().lower())
    return re.sub(r"_+", "_", normalized).strip("_")


def _load_split_source_csv(path: Path) -> pd.DataFrame:
    # SBA PPP exports are frequently Windows-1252/Latin-1 encoded.
    if path.name.lower() == "leads_full.csv":
        df = pd.read_csv(path, dtype="string", low_memory=False, encoding="latin-1")
        df.columns = [_normalize_column_name(col) for col in df.columns]
        return ingest.apply_mojibake_repair_string_columns(df)
    return ingest.load_ppp_csv(path)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Split PPP CSV into row chunks.")
    parser.add_argument(
        "--path",
        type=Path,
        default=config.PPP_RAW_PATH,
        help=f"Source CSV (default: {config.PPP_RAW_PATH})",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=50_000,
        help="Max rows per output file.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Directory for chunk files (default: same folder as source).",
    )
    ns = parser.parse_args(argv)

    src: Path = ns.path.expanduser().resolve()
    chunk_size = ns.rows

    if chunk_size < 1:
        print("--rows must be >= 1", file=sys.stderr)
        sys.exit(1)

    if not src.exists():
        print(f"File not found: {src}", file=sys.stderr)
        sys.exit(1)

    df = _load_split_source_csv(src)
    n_total = len(df)
    stem = src.stem
    out_dir = (
        ns.out_dir.expanduser().resolve()
        if ns.out_dir is not None
        else src.parent
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    n_parts = (n_total + chunk_size - 1) // chunk_size if n_total else 0
    for part_idx in range(n_parts):
        start = part_idx * chunk_size
        end = min(start + chunk_size, n_total)
        chunk = df.iloc[start:end]
        out_path = out_dir / f"{stem}_part{part_idx + 1:03d}.csv"
        chunk.to_csv(out_path, index=False, encoding=config.CSV_WRITE_ENCODING)

    print(
        f"Split {src.name} ({n_total} rows) into {n_parts} file(s) "
        f"of up to {chunk_size} rows in {out_dir}",
    )


if __name__ == "__main__":
    main()
