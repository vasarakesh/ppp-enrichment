"""CLI entrypoint: enrich first 50 borrowers with DuckDuckGo-backed domain guesses.

Reads ``BORROWERS_BASE_PATH`` under ``OUTPUT_DIR``, runs ``attach_domains_to_borrowers``
(no paid search API / no key required), writes ``BORROWERS_WITH_DOMAINS_SAMPLE_PATH``
under ``ENRICHED_DIR``, and prints a short summary.
"""

from __future__ import annotations

import pandas as pd

from . import config, domains
from .config import get_config
from .logging_utils import configure_logging


def main() -> None:
    configure_logging(get_config())
    config.ENRICHED_DIR.mkdir(parents=True, exist_ok=True)

    base_path = config.BORROWERS_BASE_PATH
    if not base_path.exists():
        raise FileNotFoundError(
            f"Expected borrowers base CSV at {base_path}. "
            "Run ingest first so borrowers_base.csv exists."
        )

    df = pd.read_csv(base_path)
    n = min(50, len(df))
    sample = df.iloc[:n].copy()

    enriched = domains.attach_domains_to_borrowers(sample)

    out_path = config.BORROWERS_WITH_DOMAINS_SAMPLE_PATH
    enriched.to_csv(out_path, index=False)

    col = enriched["website_domain"]
    non_null_mask = col.notna() & (col.astype(str).str.strip().ne(""))
    n_resolved = int(non_null_mask.sum())

    print(f"Wrote {len(enriched)} rows to {out_path}")
    print(f"Rows with non-null website_domain: {n_resolved}")


if __name__ == "__main__":
    main()
