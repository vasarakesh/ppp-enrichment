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
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from . import crawler, domains, extract, ingest, rules
from .domains import is_gov_or_edu_domain
from . import config
from . import run_export_clean
from .config import get_config
from .logging_utils import configure_logging, get_logger

_CLEAN_COLS = run_export_clean._CLEAN_COLS  # noqa: SLF001 — reuse column map only
# Borrower + domain stages always aim to provide these; everything else may be absent on partial/empty frames.
_ENRICHED_ESSENTIAL_INTERNALS = frozenset({"company_name", "website_domain"})

# Cap how many raw PPP rows we pull from the working file in one run.
MAX_BORROWERS_PER_RUN = 2000

# Phase time budgets (seconds). GitHub-hosted jobs hard-stop at 6h; we exit before that.
DOMAIN_PHASE_LIMIT = 3 * 60 * 60   # cap SERP+scoring so crawl/clean fit in the 6h job
CRAWL_PHASE_LIMIT = 4 * 60 * 60    # starts after domain resolution, not at job start
CLEAN_PHASE_LIMIT = 2 * 60 * 60
TOTAL_LIMIT = 6 * 60 * 60

RUN_START: float = 0.0
CRAWL_PHASE_START: float = 0.0


def _elapsed() -> float:
    return time.time() - RUN_START


def _crawl_elapsed() -> float:
    return time.time() - CRAWL_PHASE_START


def _domain_deadline() -> float:
    return RUN_START + min(DOMAIN_PHASE_LIMIT, TOTAL_LIMIT)


def _crawl_deadline() -> float:
    return min(CRAWL_PHASE_START + CRAWL_PHASE_LIMIT, RUN_START + TOTAL_LIMIT)


def _total_deadline() -> float:
    return RUN_START + TOTAL_LIMIT


def _clean_deadline() -> float:
    return RUN_START + TOTAL_LIMIT


def _log_phase_timing(phase: str, started_at: float, logger: logging.Logger) -> float:
    seconds = time.perf_counter() - started_at
    logger.info("[TIMING] %s: %.1f seconds", phase, seconds)
    print(f"[TIMING] {phase}: {seconds:.1f} seconds")
    return seconds


def _check_total_budget(logger: logging.Logger) -> str | None:
    """Return ``total`` when the 6h job limit is hit."""
    elapsed = _elapsed()
    if elapsed >= TOTAL_LIMIT:
        logger.warning(
            "[TIME BUDGET] Total 6h limit reached (elapsed=%.0fs).", elapsed
        )
        print("[TIME BUDGET] Total 6h limit reached. Stopping immediately.")
        return "total"
    return None


def _check_crawl_budget(logger: logging.Logger) -> str | None:
    """Return a crawl-stop reason; crawl clock starts after domain resolution."""
    total_stop = _check_total_budget(logger)
    if total_stop is not None:
        return total_stop
    crawl_elapsed = _crawl_elapsed()
    if crawl_elapsed >= CRAWL_PHASE_LIMIT:
        logger.warning(
            "[TIME BUDGET] 4h crawl limit reached (crawl_elapsed=%.0fs, job_elapsed=%.0fs).",
            crawl_elapsed,
            _elapsed(),
        )
        print(
            "[TIME BUDGET] 4h crawl limit reached. Skipping further HTTP crawl; "
            "still merging borrower rows for clean export."
        )
        return "crawl"
    return None


def _remaining_clean_seconds() -> float:
    elapsed = _elapsed()
    return min(CLEAN_PHASE_LIMIT, TOTAL_LIMIT - elapsed)


def _clean_budget_exhausted(logger: logging.Logger) -> bool:
    if _elapsed() >= _clean_deadline():
        logger.warning("[TIME BUDGET] Clean phase time budget exhausted.")
        print("[TIME BUDGET] 2h clean limit (or total 6h) reached during clean phase.")
        return True
    return False


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


def _log_http_stats(logger: logging.Logger) -> None:
    stats = crawler.get_last_crawl_http_stats()
    payload = stats.as_log_dict()
    logger.info(
        "[STATS] http_requests=%d http_3xx=%d http_4xx=%d http_5xx=%d "
        "http_errors=%d domains_skipped_budget=%d",
        payload["http_requests"],
        payload["http_3xx"],
        payload["http_4xx"],
        payload["http_5xx"],
        payload["http_errors"],
        payload["domains_skipped_budget"],
    )
    print(
        "[STATS] "
        f"http_requests={payload['http_requests']} "
        f"http_3xx={payload['http_3xx']} "
        f"http_4xx={payload['http_4xx']} "
        f"http_5xx={payload['http_5xx']} "
        f"http_errors={payload['http_errors']} "
        f"domains_skipped_budget={payload['domains_skipped_budget']}"
    )


def _run_enrichment(
    sample_df: pd.DataFrame,
    logger: logging.Logger,
    *,
    max_http_requests: int | None = None,
) -> tuple[pd.DataFrame, int, str | None]:
    """Crawl domains, extract contacts, apply rules.

    Returns ``(enriched_df, leads_processed, stop_reason)``.
    """
    if sample_df.empty:
        raise ValueError("Borrowers sample with domains is empty.")

    domain_keys_series = sample_df["website_domain"].map(_cell_to_domain)
    domain_list = [
        d
        for d in domain_keys_series.loc[domain_keys_series.ne("")].drop_duplicates().tolist()
        if not is_gov_or_edu_domain(d)
    ]

    stop_reason = _check_crawl_budget(logger)
    crawl_map: dict[str, list] = {}
    if stop_reason is None:
        logger.info(
            "Crawling %d unique domains (crawl deadline in %.0fs)",
            len(domain_list),
            max(0.0, _crawl_deadline() - time.time()),
        )
        crawl_started = time.perf_counter()
        crawl_map = crawler.crawl_domains(
            domain_list,
            deadline=_crawl_deadline(),
            max_requests=max_http_requests,
        )
        _log_phase_timing("HTTP crawl", crawl_started, logger)
        _log_http_stats(logger)
        stop_reason = _check_crawl_budget(logger)

    extract_started = time.perf_counter()
    domain_contacts: dict[str, extract.ContactInfo] = {}
    for domain_key, pages in crawl_map.items():
        domain_contacts[domain_key] = extract.extract_contact_info(
            pages,
            accepted_domain=domain_key,
        )

    enriched_rows: list[dict] = []
    processed = 0
    for _, row in sample_df.iterrows():
        # Only the 6h total limit stops row merge; crawl limit skips HTTP only.
        if _check_total_budget(logger):
            stop_reason = "total"
            break

        row_dict = row.to_dict()
        dk = domain_keys_series[row.name]
        contact_info = (
            domain_contacts.get(dk)
            if dk
            else extract.extract_contact_info([], accepted_domain=None)
        )
        if contact_info is None:
            contact_info = extract.extract_contact_info([], accepted_domain=None)

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
                accepted_domain=dk or None,
            )
        )
        enriched_rows.append(merged)
        processed += 1

    _log_phase_timing("Extract + rules merge", extract_started, logger)
    return pd.DataFrame(enriched_rows), processed, stop_reason


def _prepare_enriched_columns_for_clean(
    df: pd.DataFrame, logger: logging.Logger
) -> pd.DataFrame:
    """Guarantee clean-export source columns exist so partial runs and empty frames never raise KeyError.

    Columns required for the final CSV mapping are aligned with ``_CLEAN_COLS``. Anything not present
    in the enriched frame (common after a hard timeout before the first row, or sparse dict rows) is
    added with empty strings. Only ``company_name`` and ``website_domain`` are treated as pipeline
    essentials for logging severity; missing contact fields are optional gaps.
    """
    out = df.copy()
    for _, internal in _CLEAN_COLS:
        if internal not in out.columns:
            if internal in _ENRICHED_ESSENTIAL_INTERNALS:
                logger.warning(
                    "Enriched frame missing essential column %r; padding with empty strings "
                    "so clean export can complete.",
                    internal,
                )
            else:
                logger.warning(
                    "Enriched frame missing optional column %r; padding with empty strings.",
                    internal,
                )
            out[internal] = ""
    return out


def _build_clean_leads(
    enriched_df: pd.DataFrame,
    max_rows: int,
    logger: logging.Logger,
) -> tuple[pd.DataFrame, int, dict[str, int]]:
    """Filter, clean columns, limit to ``max_rows``; returns output, count, and stats."""
    if _clean_budget_exhausted(logger):
        return pd.DataFrame(columns=[display for display, _ in _CLEAN_COLS]), 0, {
            "dropped_missing_phone": 0,
            "dropped_fake_phone": 0,
            "dropped_duplicates": 0,
        }

    df = _prepare_enriched_columns_for_clean(enriched_df, logger)
    email_ok = df.apply(
        lambda row: run_export_clean._email_passes_clean_filters(  # noqa: SLF001
            row["email"],
            row["website_domain"],
        ),
        axis=1,
    )
    filtered = df.loc[email_ok].copy()

    if _clean_budget_exhausted(logger):
        return pd.DataFrame(columns=[display for display, _ in _CLEAN_COLS]), 0, {
            "dropped_missing_phone": 0,
            "dropped_fake_phone": 0,
            "dropped_duplicates": 0,
        }

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


def _update_chunk_file(
    ppp_csv_path: Path,
    raw_sample: pd.DataFrame,
    full_df: pd.DataFrame,
    processed: int,
    target_borrowers: int,
) -> tuple[int, str]:
    """Write remaining rows back to the chunk or delete when fully consumed."""
    remaining_df = pd.concat(
        [
            raw_sample.iloc[processed:].copy(),
            full_df.iloc[target_borrowers:].copy(),
        ],
        ignore_index=True,
    )
    remaining_n = len(remaining_df)
    if remaining_n == 0:
        ppp_csv_path.unlink(missing_ok=True)
        return 0, "deleted"
    remaining_df.to_csv(ppp_csv_path, index=False, encoding=config.CSV_WRITE_ENCODING)
    return remaining_n, f"updated with {remaining_n} remaining rows"


def main(argv: list[str] | None = None) -> None:
    logger = get_logger(__name__)
    args = _parse_args(argv)
    global RUN_START
    RUN_START = time.time()
    pipeline_started = time.perf_counter()

    leads: int = args.leads
    oversample: float = args.oversample_factor
    ppp_csv_path = args.ppp_csv.resolve() if args.ppp_csv is not None else get_next_chunk()

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

    target_borrowers = 0
    processed = 0
    crawl_stop_reason: str | None = None
    clean_path: Path | None = None
    clean_count = 0
    clean_stats = {
        "dropped_missing_phone": 0,
        "dropped_fake_phone": 0,
        "dropped_duplicates": 0,
    }
    rows_with_domain = 0
    remaining_n = 0
    chunk_status = "unchanged"
    raw_sample = pd.DataFrame()

    try:
        logger.info(
            "Pipeline start: target_leads=%d oversample=%s run_dir=%s "
            "budgets(domain=%ds crawl=%ds clean=%ds total=%ds) "
            "http(request=%ss lead_domain=%ss crawl_per_domain=%ss ddg_backends=%s)",
            leads,
            oversample,
            run_dir,
            DOMAIN_PHASE_LIMIT,
            CRAWL_PHASE_LIMIT,
            CLEAN_PHASE_LIMIT,
            TOTAL_LIMIT,
            app_cfg.request_timeout_seconds,
            app_cfg.domain_lead_time_limit_seconds,
            app_cfg.crawler_per_domain_time_limit_seconds,
            app_cfg.ddg_backends,
        )

        if not ppp_csv_path.exists():
            raise FileNotFoundError(f"PPP chunk file not found: {ppp_csv_path}")

        load_started = time.perf_counter()
        full_df = ingest.load_ppp_csv(ppp_csv_path)
        _log_phase_timing("Load PPP chunk", load_started, logger)

        target_borrowers = int(leads * oversample)
        target_borrowers = min(target_borrowers, MAX_BORROWERS_PER_RUN, len(full_df))
        logger.info(
            "Target leads: %d; borrower oversample target: %d (cap %d, chunk rows %d).",
            leads,
            target_borrowers,
            MAX_BORROWERS_PER_RUN,
            len(full_df),
        )

        raw_sample = full_df.iloc[:target_borrowers].copy()
        ingest_started = time.perf_counter()
        slice_path = run_dir / "_raw_sample_for_ingest.csv"
        raw_sample.to_csv(slice_path, index=False)
        try:
            borrower_sample = ingest.build_borrowers_base(
                input_paths=[slice_path],
                write_output=False,
            ).reset_index(drop=True)
        finally:
            slice_path.unlink(missing_ok=True)
        _log_phase_timing("Build borrower base", ingest_started, logger)

        logger.info(
            "Borrower base sample: %s rows (target_borrowers=%s).",
            len(borrower_sample),
            target_borrowers,
        )

        if _check_total_budget(logger):
            crawl_stop_reason = "total"

        domains_started = time.perf_counter()
        with_domains = domains.attach_domains_to_borrowers(
            borrower_sample.copy(),
            deadline=_domain_deadline(),
        )
        _log_phase_timing("Domain resolution", domains_started, logger)

        wd = with_domains["website_domain"]
        rows_with_domain = int(wd.notna().sum())

        global CRAWL_PHASE_START
        CRAWL_PHASE_START = time.time()
        logger.info(
            "Crawl phase clock started (domain phase used %.0fs of job).",
            CRAWL_PHASE_START - RUN_START,
        )

        max_http_requests = min(
            app_cfg.crawler_max_requests_per_run,
            max(500, target_borrowers * 8),
        )
        enrich_started = time.perf_counter()
        enriched, processed, enrich_stop = _run_enrichment(
            with_domains,
            logger,
            max_http_requests=max_http_requests,
        )
        _log_phase_timing("Enrichment (crawl + extract + rules)", enrich_started, logger)
        if enrich_stop is not None:
            crawl_stop_reason = enrich_stop

        logger.info(
            "Enriched sample: %s rows in memory; leads_processed=%d.",
            len(enriched),
            processed,
        )
        has_domain_mask = enriched["website_domain"].notna() & enriched["website_domain"].astype(str).str.strip().ne("")
        has_email_mask = enriched.get("email", pd.Series(dtype=object)).notna() & enriched["email"].astype(str).str.strip().ne("")
        has_phone_mask = enriched.get("phone", pd.Series(dtype=object)).notna() & enriched["phone"].astype(str).str.strip().ne("")
        logger.info(
            "[STATS] enrichment_funnel domains=%d email=%d phone=%d (of %d rows)",
            int(has_domain_mask.sum()),
            int(has_email_mask.sum()),
            int(has_phone_mask.sum()),
            len(enriched),
        )
        print(
            f"[STATS] enrichment_funnel domains={int(has_domain_mask.sum())} "
            f"email={int(has_email_mask.sum())} phone={int(has_phone_mask.sum())}"
        )

        if enriched is None or len(enriched) == 0:
            print("[CLEAN] No enriched rows this run; skipping clean_leads export.")
            logger.warning("[CLEAN] No enriched rows; updating chunk only.")
            remaining_n, chunk_status = _update_chunk_file(
                ppp_csv_path, raw_sample, full_df, processed, target_borrowers
            )
            print(f"Target leads: {leads}")
            print(f"Target borrower rows: {target_borrowers}")
            print(f"Actual leads processed: {processed}")
            print(f"Remaining in chunk: {remaining_n}")
            print(f"Chunk status: {chunk_status}")
            print("=== RUN COMPLETE (no enrichment) ===")
            sys.exit(0)

        remaining_for_clean = _remaining_clean_seconds()
        if remaining_for_clean <= 0:
            print(
                "[TIME BUDGET] No time left for clean phase; exiting cleanly without writing output."
            )
            logger.warning(
                "[TIME BUDGET] No clean-phase time remaining; chunk left unchanged."
            )
            remaining_n, chunk_status = _update_chunk_file(
                ppp_csv_path, raw_sample, full_df, processed, target_borrowers
            )
            print(f"Target leads: {leads}")
            print(f"Actual leads processed: {processed}")
            print(f"Remaining in chunk: {remaining_n}")
            print(f"Chunk status: {chunk_status}")
            sys.exit(0)

        clean_started = time.perf_counter()
        clean_df, clean_count, clean_stats = _build_clean_leads(enriched, leads, logger)
        _log_phase_timing("Clean leads build", clean_started, logger)

        if _clean_budget_exhausted(logger):
            crawl_stop_reason = crawl_stop_reason or "clean"

        config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        utc_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M")
        clean_path = config.OUTPUT_DIR / f"clean_leads_{utc_stamp}.csv"
        output_dir_cwd = os.path.abspath("data/output/")
        output_dir_config = os.path.abspath(str(config.OUTPUT_DIR))
        print(f"[DEBUG] Before CSV write — cwd output dir: {output_dir_cwd}")
        print(f"[DEBUG] Before CSV write — config OUTPUT_DIR: {output_dir_config}")
        print(f"[DEBUG] Before CSV write — target file: {os.path.abspath(str(clean_path))}")
        print(
            f"[DEBUG] Before CSV write — clean_count={clean_count} len(clean_df)={len(clean_df)}"
        )
        try:
            files_before = os.listdir("data/output/")
        except OSError as exc:
            files_before = [f"<listdir failed: {exc}>"]
        print(f"[DEBUG] Before CSV write — files in data/output/: {files_before}")

        if len(clean_df) > 0 or clean_count > 0:
            clean_df.to_csv(clean_path, index=False, encoding=config.CSV_WRITE_ENCODING)
            logger.info("Clean export: %s rows -> %s", clean_count, clean_path)
        else:
            logger.warning("Clean export produced 0 rows; no CSV written.")
            clean_path = None

        try:
            files_after = os.listdir("data/output/")
        except OSError as exc:
            files_after = [f"<listdir failed: {exc}>"]
        print(f"[DEBUG] After CSV write — files in data/output/: {files_after}")
        print(f"[DEBUG] After CSV write — clean_path={clean_path!r} exists={clean_path.exists() if clean_path else False}")

        remaining_n, chunk_status = _update_chunk_file(
            ppp_csv_path, raw_sample, full_df, processed, target_borrowers
        )

        logger.info(
            "Chunk file %s (%s).",
            ppp_csv_path,
            chunk_status,
        )

        skipped = max(len(with_domains) - processed, 0)
        total_seconds = time.perf_counter() - pipeline_started

        print(f"Target leads: {leads}")
        print(f"Target borrower rows: {target_borrowers}")
        print(f"Actual leads processed: {processed}")
        print(f"Rows with domain: {rows_with_domain}")
        print(f"Rows dropped due to missing phone: {clean_stats['dropped_missing_phone']}")
        print(f"Rows dropped due to fake phone: {clean_stats['dropped_fake_phone']}")
        print(f"Duplicate rows removed: {clean_stats['dropped_duplicates']}")
        print(f"Clean leads produced: {clean_count}")
        print(f"Rows skipped (time budget): {skipped}")
        if clean_path is not None:
            print(f"Clean leads file (UTC): {clean_path}")
        if crawl_stop_reason:
            print(f"Time budget stop reason: {crawl_stop_reason}")
        if not cleanup_run_dir:
            print(f"Intermediate directory: {run_dir}")
        print("=== RUN COMPLETE ===")
        print(f"Processed: {processed} leads")
        print(f"Remaining in chunk: {remaining_n} leads")
        if clean_path is not None:
            print(f"Output saved to: {clean_path}")
        print(f"Chunk status: {chunk_status}")
        print(f"[TIMING] Total pipeline: {total_seconds:.1f} seconds")
        logger.info(
            "[STATS] target_leads=%d target_borrowers=%d leads_processed=%d "
            "clean_leads=%d remaining_chunk=%d stop_reason=%s",
            leads,
            target_borrowers,
            processed,
            clean_count,
            remaining_n,
            crawl_stop_reason or "none",
        )
    finally:
        root_log.removeHandler(run_handler)
        run_handler.close()
        if cleanup_run_dir:
            shutil.rmtree(run_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
