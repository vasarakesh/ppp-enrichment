"""Verify Python version, third-party deps, and project layout for PPP enrichment."""

from __future__ import annotations

import sys


def main() -> None:
    if sys.version_info < (3, 9):
        print(f"ERROR: Python 3.9 or newer is required; this interpreter is {sys.version.split()[0]}.")
        sys.exit(1)

    try:
        import pandas  # noqa: F401
        import httpx  # noqa: F401
        from bs4 import BeautifulSoup  # noqa: F401
        import lxml  # noqa: F401
        from ddgs import DDGS  # noqa: F401
        from dotenv import load_dotenv  # noqa: F401
    except ImportError as exc:
        print(f"ERROR: Missing or broken third-party dependency: {exc}")
        print("Install runtime packages with: pip install -r requirements.txt")
        sys.exit(1)

    try:
        from . import ingest, domains, crawler, extract, rules, run_pipeline, config
    except ImportError as exc:
        print(f"ERROR: Could not load project modules: {exc}")
        sys.exit(1)

    cfg = config.get_config()
    for path in (cfg.input_dir, cfg.output_dir, cfg.logs_dir):
        path.mkdir(parents=True, exist_ok=True)

    ppp_path = config.PPP_RAW_PATH
    ppp_ok = ppp_path.is_file()
    if ppp_ok:
        print(f"PPP raw file found at {ppp_path} (OK).")
    else:
        print(
            "WARNING: PPP FOIA raw CSV is missing. Download the PPP loan-level FOIA file "
            "from the SBA and save it as:\n"
            f"  {ppp_path}\n"
            "(Expected filename: ppp-war.csv under data/input/.)"
        )

    py_ver = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    print(
        "Environment OK: Python version, dependencies, and project modules loaded successfully."
    )
    print(f"  Python: {py_ver}")
    print(f"  PPP raw file present: {ppp_ok}")


if __name__ == "__main__":
    main()
