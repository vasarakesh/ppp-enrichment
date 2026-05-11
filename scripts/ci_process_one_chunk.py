#!/usr/bin/env python3
"""GitHub Actions / CI: pick one PPP chunk, run enrichment, keep only clean CSV, delete chunk.

Run from repo root with PYTHONPATH=.

    PYTHONPATH=. python scripts/ci_process_one_chunk.py

Exits 0 immediately if no ``data/input/queue/ppp-war_part*.csv`` files remain.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _next_chunk(data_input: Path) -> Path | None:
    parts = sorted(data_input.glob("ppp-war_part*.csv"))
    return parts[0] if parts else None


def main() -> int:
    root = _repo_root()
    data_input = root / "data" / "input" / "queue"
    clean_dir = root / "clean_github_results"
    clean_dir.mkdir(parents=True, exist_ok=True)

    chunk = _next_chunk(data_input)
    if chunk is None:
        print("No ppp-war_part*.csv chunks left; skipping.")
        return 0

    env = dict(os.environ)
    env.setdefault("PYTHONPATH", str(root))
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest_clean = clean_dir / f"{chunk.stem}_{ts}.csv"

    print(f"Using chunk {chunk.relative_to(root)}")

    with tempfile.TemporaryDirectory() as td:
        run_dir = Path(td) / "pipeline_run"
        cmd = [
            sys.executable,
            "-m",
            "src.ppp_enrichment.run_pipeline",
            "--leads",
            "2000",
            "--oversample-factor",
            "3",
            "--run-dir",
            str(run_dir),
        ]
        subprocess.run(cmd, cwd=root, env=env, check=True)

        out_dir = root / "data" / "output"
        clean_files = sorted(out_dir.glob("clean_leads_*.csv"), key=lambda p: p.stat().st_mtime)
        if not clean_files:
            raise RuntimeError(f"No clean_leads_*.csv in {out_dir}")
        shutil.copy2(clean_files[-1], dest_clean)
        print(f"Clean export copied to {dest_clean.relative_to(root)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
