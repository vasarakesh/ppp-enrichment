"""Single-command orchestration: ingest → domains → enrichment → clean export.

Reads PPP chunks from ``data/input/queue/ppp-war_part*.csv`` unless ``--ppp-csv`` is set.

Usage::
    python -m src.ppp_enrichment.run_pipeline
    python -m src.ppp_enrichment.run_pipeline --leads 500
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from . import crawler, domains, extract, ingest, rules
from . import config
from . import run_export_clean
from .config import get_config
from .logging_utils import configure_logging, get_logger

_CLEAN_COLS = run_export_clean._CLEAN_COLS  # noqa: SLF001 — reuse column map only

# Cap how many raw PPP rows we pull from the working file in one run.
MAX_BORROWERS_PER_RUN = 2000


def get_next_chunk() -> Path:
    """Return the alphabetically first ``ppp-war_part*.csv`` under ``data/input/queue/``."""
    queue_dir = config.CHUNK_QUEUE_DIR
    if not queue_dir.is_dir():
        print("Queue is empty. No chunks remaining.")
        sys.exit(0)
    parts = sorted(queue_dir.glob("ppp-war_part*.csv"))
    if not parts:
        print("Queue is empty. No chunks remaining.")
        sys.exit(0)
    return parts[0]


def _cell_to_domain(value: object) -> str:
    """Return crawler-normal host key or empty string if missing."""
    if value is None:
        return ""
    if isinstance(value, float):
        try:
            if pd.isna(value):
                return ""
        except (TypeError, ValueError):
            pass
    text = str(value).strip()
    if not text or text.casefold() == "nan":
        return ""
    return crawler.normalize_domain_key(text)


def _run_enrichment(sample_df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """Crawl domains, extract contacts, apply rules — same shape as ``run_enrich_sample``."""
    if sample_df.empty:
        raise ValueError("Borrowers sample with domains is empty.")

    domain_keys_series = sample_df["website_domain"].map(_cell_to_domain)
    domain_list = domain_keys_series.loc[domain_keys_series.ne("")].drop_duplicates().tolist()

    logger.info("Crawling %d unique domains", len(domain_list))
    crawl_map = crawler.crawl_domains(domain_list)

    domain_contacts: dict[str, extract.ContactInfo] = {}
    for domain_key, pages in crawl_map.items():
        domain_contacts[domain_key] = extract.extract_contact_info(pages)

    enriched_rows: list[dict] = []
    for _, row in sample_df.iterrows():
        row_dict = row.to_dict()
        dk = domain_keys_series[row.name]
        contact_info = domain_contacts.get(dk) if dk else extract.extract_contact_info([])
        if contact_info is None:
            contact_info = extract.extract_contact_info([])

        company_name = row_dict.get("company_name") or ""
        if isinstance(company_name, float):
            company_name = "" if pd.isna(company_name) else str(company_name)
        else:
            company_name = str(company_name) if company_name is not None else ""

        merged = dict(row_dict)
        merged.update(
            rules.choose_best_contact(
                company_name=company_name,
                contact_info=contact_info,
            )
        )
        enriched_rows.append(merged)

    return pd.DataFrame(enriched_rows)


def _validate_enriched_columns(df: pd.DataFrame) -> None:
    missing = [
        internal for _, internal in _CLEAN_COLS if internal not in df.columns
    ]
    if missing:
        raise KeyError(f"Enriched frame missing required columns: {missing}")


def _build_clean_leads(
    enriched_df: pd.DataFrame, max_rows: int
) -> tuple[pd.DataFrame, int, dict[str, int]]:
    """Filter, clean columns, limit to ``max_rows``; returns output, count, and stats."""
    _validate_enriched_columns(enriched_df)
    df = enriched_df.copy()
    email_ok = df.apply(
        lambda row: run_export_clean._email_passes_clean_filters(  # noqa: SLF001
            row["email"],
            row["website_domain"],
        ),
        axis=1,
    )
    filtered = df.loc[email_ok].copy()

    out = pd.DataFrame({display: filtered[internal] for display, internal in _CLEAN_COLS})

    for display in out.columns:
        series = out[display].astype("string").str.strip()

        if display == "Email Address":
            series = series.str.lower().str.strip()
        elif display in ("First Name", "Second Name"):
            series = series.str.title()

        out[display] = series

    before_missing_phone = len(out)
    missing_phone_mask = out["Phone Number"].apply(run_export_clean._is_missing_phone_value)  # noqa: SLF001
    out = out.loc[~missing_phone_mask].copy()
    dropped_missing_phone = before_missing_phone - len(out)

    normalized_digits = out["Phone Number"].astype("string").apply(
        lambda x: re.sub(r"\D+", "", str(x))
    )
    fake_phone_mask = normalized_digits.apply(run_export_clean._is_fake_phone_digits)  # noqa: SLF001
    out = out.loc[~fake_phone_mask].copy()
    dropped_fake_phone = int(fake_phone_mask.sum())

    out["Phone Number"] = out["Phone Number"].astype("string").str.strip()

    before_dedup = len(out)
    out = out.drop_duplicates(
        subset=["Company Name", "Email Address", "Phone Number"],
        keep="first",
    )
    dropped_duplicates = before_dedup - len(out)

    if len(out) > max_rows:
        out = out.iloc[:max_rows].copy()
    clean_count = len(out)
    stats = {
        "dropped_missing_phone": int(dropped_missing_phone),
        "dropped_fake_phone": int(dropped_fake_phone),
        "dropped_duplicates": int(dropped_duplicates),
    }
    return out, clean_count, stats


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PPP enrichment: ingest, domains, crawl/extract/rules, clean export.",
    )
    parser.add_argument(
        "--leads",
        type=int,
        default=1000,
        help="Target number of clean leads to produce (default: 1000).",
    )
    parser.add_argument(
        "--oversample-factor",
        type=float,
        default=3.0,
        help="Borrower sample size = int(leads * factor) to absorb attrition (default: 3.0).",
    )
    parser.add_argument(
        "--ppp-csv",
        type=Path,
        default=None,
        help=(
            "Read PPP rows from this file instead of the default master file "
            "(e.g. a single chunk). Implies skipping master PPP removal unless overridden."
        ),
    )
    parser.add_argument(
        "--update-master-ppp",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Write remaining PPP rows back to config.PPP_RAW_PATH after a run "
            "(default true for the master file only; not used when using --ppp-csv)."
        ),
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help=(
            "Directory for transient intermediates only (default: system temp). "
            "Final clean CSV always goes to data/output/clean_leads_<UTC>.csv."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logger = get_logger(__name__)
    args = _parse_args(argv)
    leads: int = args.leads
    oversample: float = args.oversample_factor
    explicit_ppp_csv = args.ppp_csv is not None
    ppp_csv_path = args.ppp_csv.resolve() if explicit_ppp_csv else get_next_chunk()

    if leads < 1:
        raise ValueError("--leads must be a positive integer.")
    if oversample <= 0:
        raise ValueError("--oversample-factor must be positive.")

    app_cfg = get_config()
    configure_logging(app_cfg)

    today_str = date.today().strftime("%Y%m%d")
    cleanup_run_dir = False
    if args.run_dir is not None:
        run_dir = Path(args.run_dir).expanduser().resolve()
        run_dir.mkdir(parents=True, exist_ok=True)
    else:
        run_dir = Path(tempfile.mkdtemp(prefix="ppp_pipeline_"))
        cleanup_run_dir = True

    run_log = app_cfg.logs_dir / f"pipeline_Data_{leads}_{today_str}.log"
    run_handler = logging.FileHandler(run_log, encoding="utf-8")
    run_handler.setLevel(logging.INFO)
    run_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    )
    root_log = logging.getLogger()
    root_log.addHandler(run_handler)

    try:
        logger.info(
            "Pipeline start: requested_leads=%d oversample=%s run_dir=%s",
            leads,
            oversample,
            run_dir,
        )

        if not ppp_csv_path.exists():
            raise FileNotFoundError(f"PPP chunk file not found: {ppp_csv_path}")

        full_df = ingest.load_ppp_csv(ppp_csv_path)

        target_borrowers = int(leads * oversample)
        target_borrowers = min(target_borrowers, MAX_BORROWERS_PER_RUN, len(full_df))
        logger.info(
            "Raw PPP rows available: %s; target_borrowers this run: %s.",
            len(full_df),
            target_borrowers,
        )

        raw_sample = full_df.iloc[:target_borrowers].copy()
        slice_path = run_dir / "_raw_sample_for_ingest.csv"
        raw_sample.to_csv(slice_path, index=False)
        try:
            borrower_sample = ingest.build_borrowers_base(
                input_paths=[slice_path],
                write_output=False,
            ).reset_index(drop=True)
        finally:
            slice_path.unlink(missing_ok=True)

        logger.info(
            "Borrower base sample: %s rows (target_borrowers=%s).",
            len(borrower_sample),
            target_borrowers,
        )

        with_domains = domains.attach_domains_to_borrowers(borrower_sample.copy())

        wd = with_domains["website_domain"]
        rows_with_domain = int(wd.notna().sum())

        enriched = _run_enrichment(with_domains, logger)
        logger.info("Enriched sample: %s rows (in memory).", len(enriched))

        clean_df, clean_count, clean_stats = _build_clean_leads(enriched, leads)

        config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        utc_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M")
        clean_path = config.OUTPUT_DIR / f"clean_leads_{utc_stamp}.csv"
        clean_df.to_csv(clean_path, index=False, encoding=config.CSV_WRITE_ENCODING)
        logger.info("Clean export: %s rows -> %s", clean_count, clean_path)

        logger.info(
            "Chunk mode: skipping master PPP remainder write (processed file: %s).",
            ppp_csv_path,
        )

        print(f"Requested leads: {leads}")
        print(f"Borrower sample size: {target_borrowers}")
        print(f"Rows with domain: {rows_with_domain}")
        print(f"Rows dropped due to missing phone: {clean_stats['dropped_missing_phone']}")
        print(f"Rows dropped due to fake phone: {clean_stats['dropped_fake_phone']}")
        print(f"Duplicate rows removed: {clean_stats['dropped_duplicates']}")
        print(f"Clean leads produced: {clean_count}")
        print(f"Clean leads file (UTC): {clean_path}")
        if not cleanup_run_dir:
            print(f"Intermediate directory: {run_dir}")

        if not explicit_ppp_csv:
            os.remove(ppp_csv_path)
            remaining_n = len(
                sorted(config.CHUNK_QUEUE_DIR.glob("ppp-war_part*.csv"))
            )
            print(
                f"Processed chunk: {ppp_csv_path.name} — deleted from queue.\n"
                f"Chunks remaining: {remaining_n}"
            )
    finally:
        root_log.removeHandler(run_handler)
        run_handler.close()
        if cleanup_run_dir:
            shutil.rmtree(run_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
