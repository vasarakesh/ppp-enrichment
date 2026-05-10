"""Single-command orchestration: ingest → domains → enrichment → clean export.

Usage::
    python -m src.ppp_enrichment.run_pipeline --leads 500
"""

from __future__ import annotations

import argparse
import logging
import re
import shutil
from datetime import date
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


def _loan_number_column_name(df: pd.DataFrame) -> str:
    """Resolve the normalized loan id column in a FOIA-style PPP frame."""
    for alias in ingest.CANONICAL_ALIASES["loan_number"]:
        if alias in df.columns:
            return alias
    raise ValueError(
        "PPP raw CSV has no recognizable loan number column "
        f"(checked aliases for loan_number: {ingest.CANONICAL_ALIASES['loan_number']!r})."
    )


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


def _read_enriched_strict(enriched_path: Path) -> pd.DataFrame:
    df = pd.read_csv(enriched_path)
    missing = [
        internal for _, internal in _CLEAN_COLS if internal not in df.columns
    ]
    if missing:
        raise KeyError(f"Enriched CSV missing required columns: {missing}")
    return df


def _build_clean_leads(
    enriched_path: Path, max_rows: int
) -> tuple[pd.DataFrame, int, dict[str, int]]:
    """Filter, clean columns, limit to ``max_rows``; returns output, count, and stats."""
    df = _read_enriched_strict(enriched_path)
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
        required=True,
        help="Target number of clean leads to produce.",
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
        help="Directory for intermediate + clean outputs (default under data/output by date/leads).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logger = get_logger(__name__)
    args = _parse_args(argv)
    leads: int = args.leads
    oversample: float = args.oversample_factor
    ppp_input = args.ppp_csv.resolve() if args.ppp_csv is not None else None
    chunk_mode = ppp_input is not None
    # Chunk imports are ephemeral; never trim the master PPP remainder from chunk mode.
    update_master_ppp: bool = (not chunk_mode) and bool(args.update_master_ppp)

    if leads < 1:
        raise ValueError("--leads must be a positive integer.")
    if oversample <= 0:
        raise ValueError("--oversample-factor must be positive.")

    app_cfg = get_config()
    configure_logging(app_cfg)

    today_str = date.today().strftime("%Y%m%d")
    if args.run_dir is not None:
        run_dir = Path(args.run_dir).expanduser().resolve()
        run_dir.mkdir(parents=True, exist_ok=True)
    else:
        run_dir = config.OUTPUT_DIR / f"Data_{leads}_{today_str}"
        run_dir.mkdir(parents=True, exist_ok=True)

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

        ppp_csv_path = ppp_input if ppp_input is not None else config.PPP_RAW_PATH

        if not chunk_mode:
            if not config.PPP_RAW_BACKUP_PATH.exists():
                if not config.PPP_RAW_PATH.exists():
                    raise FileNotFoundError(
                        f"PPP raw file not found: {config.PPP_RAW_PATH} "
                        "(cannot create backup or load working set)."
                    )
                shutil.copy2(config.PPP_RAW_PATH, config.PPP_RAW_BACKUP_PATH)
                logger.info(
                    "Created one-time raw backup: %s <- %s",
                    config.PPP_RAW_BACKUP_PATH,
                    config.PPP_RAW_PATH,
                )
        else:
            if not ppp_csv_path.exists():
                raise FileNotFoundError(f"PPP chunk file not found: {ppp_csv_path}")

        full_df = ingest.load_ppp_csv(ppp_csv_path)
        loan_col = _loan_number_column_name(full_df)

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

        borrower_base_path = run_dir / "borrowers_base_sample.csv"
        borrower_sample.to_csv(borrower_base_path, index=False)
        logger.info(
            "Wrote borrower base sample: %s rows (target_borrowers=%s) -> %s",
            len(borrower_sample),
            target_borrowers,
            borrower_base_path,
        )

        with_domains = domains.attach_domains_to_borrowers(borrower_sample.copy())
        with_domains_path = run_dir / "borrowers_with_domains.csv"
        with_domains.to_csv(with_domains_path, index=False)

        wd = with_domains["website_domain"]
        rows_with_domain = int(wd.notna().sum())

        enriched = _run_enrichment(with_domains, logger)
        enriched_path = run_dir / "enriched_borrowers.csv"
        enriched.to_csv(enriched_path, index=False)
        logger.info(
            "Wrote enriched sample: %s rows -> %s",
            len(enriched),
            enriched_path,
        )

        clean_df, clean_count, clean_stats = _build_clean_leads(enriched_path, leads)
        clean_path = run_dir / f"Vaishnavi_Clean_{clean_count}_1.csv"
        clean_df.to_csv(clean_path, index=False, encoding=config.CSV_WRITE_ENCODING)
        logger.info("Clean export: %s rows -> %s", clean_count, clean_path)

        if chunk_mode:
            logger.info(
                "Chunk mode: skipping master PPP remainder write (processed file: %s).",
                ppp_csv_path,
            )
        elif update_master_ppp:
            remaining_df = full_df[~full_df[loan_col].isin(raw_sample[loan_col])]
            remaining_df.to_csv(config.PPP_RAW_PATH, index=False)
            remaining_n = len(remaining_df)
            msg_remaining = f"Remaining raw PPP rows after this run: {remaining_n}."
            logger.info(msg_remaining)
            print(msg_remaining)
        else:
            logger.info("--no-update-master-ppp: skipping write of remainder to master PPP.")

        print(f"Requested leads: {leads}")
        print(f"Borrower sample size: {target_borrowers}")
        print(f"Rows with domain: {rows_with_domain}")
        print(f"Rows dropped due to missing phone: {clean_stats['dropped_missing_phone']}")
        print(f"Rows dropped due to fake phone: {clean_stats['dropped_fake_phone']}")
        print(f"Duplicate rows removed: {clean_stats['dropped_duplicates']}")
        print(f"Clean leads produced: {clean_count}")
        print(f"Run directory: {run_dir}")
    finally:
        root_log.removeHandler(run_handler)
        run_handler.close()


if __name__ == "__main__":
    main()
