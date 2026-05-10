"""Drop PPP CSV columns that ingest does not map (``TARGET_SCHEMA`` / aliases only).

Reads the configured raw PPP path, backs up once, writes a slimmer CSV (UTF-8).

Usage::
    python -m src.ppp_enrichment.run_trim_ppp_raw
    python -m src.ppp_enrichment.run_trim_ppp_raw --dry-run --path path/to/file.csv
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from . import config, ingest


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Remove unused PPP columns (ingest-aligned).")
    parser.add_argument(
        "--path",
        type=Path,
        default=config.PPP_RAW_PATH,
        help=f"CSV to trim (default: {config.PPP_RAW_PATH})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print keep/drop column lists without writing.",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip copy to <name>.full-columns-backup.csv before overwrite.",
    )
    ns = parser.parse_args(argv)

    csv_path: Path = ns.path
    if not csv_path.exists():
        print(f"File not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    allowed = ingest.allowed_ppp_source_column_names()
    df = ingest.load_ppp_csv(csv_path)
    present = list(df.columns)
    keep = [c for c in present if c in allowed]
    drop = [c for c in present if c not in allowed]

    print(f"Rows: {len(df)}")
    print(f"Keep {len(keep)} columns ({len(drop)} dropped).")

    missing = ingest.missing_required_ppp_columns(df[keep])
    if missing:
        print(
            "Refusing trim: retained columns cannot satisfy required fields: "
            + ", ".join(missing),
            file=sys.stderr,
        )
        sys.exit(2)

    if ns.dry_run:
        print("\nKeeping:")
        for c in sorted(keep):
            print(f"  {c}")
        print("\nWould drop:")
        for c in sorted(drop):
            print(f"  {c}")
        return

    trimmed = df[keep].copy()
    backup = csv_path.with_name(csv_path.stem + ".full-columns-backup" + csv_path.suffix)
    if not ns.no_backup and not backup.exists():
        shutil.copy2(csv_path, backup)
        print(f"Backed up original to {backup}")

    trimmed.to_csv(csv_path, index=False, encoding=config.CSV_WRITE_ENCODING)
    print(f"Wrote trimmed CSV ({len(keep)} columns) to {csv_path}")


if __name__ == "__main__":
    main()
