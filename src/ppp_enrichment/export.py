"""Export enriched borrower records and QA metrics."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Sequence

import pandas as pd

from .config import get_config
from .crawler import crawl_domains
from .domains import attach_domains_to_borrowers
from .extract import ContactInfo, extract_contact_info
from .ingest import build_borrowers_base
from .logging_utils import get_logger
from .rules import choose_best_contact

logger = get_logger(__name__)


def _normalize_domain(value: object) -> str:
    text = str(value or "").strip().lower()
    return text[4:] if text.startswith("www.") else text


def _build_domain_contact_map(domains: Sequence[str]) -> dict[str, ContactInfo]:
    unique_domains = sorted({_normalize_domain(domain) for domain in domains if _normalize_domain(domain)})
    if not unique_domains:
        logger.info("No valid domains available for crawling.")
        return {}

    logger.info("Crawling %d unique domains for contact extraction", len(unique_domains))
    crawled = crawl_domains(unique_domains)
    mapping: dict[str, ContactInfo] = {}
    for domain in unique_domains:
        pages = crawled.get(domain, [])
        mapping[domain] = extract_contact_info(pages)
    logger.info("Built contact map for %d domains", len(mapping))
    return mapping


def build_enriched_borrowers(
    base_df: pd.DataFrame | None = None,
    *,
    input_paths: Sequence[str | Path] | None = None,
    domains_df: pd.DataFrame | None = None,
    domain_contact_map: dict[str, ContactInfo] | None = None,
    states: Sequence[str] | None = None,
    minimum_loan_amount: float | None = None,
    naics_codes: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Build enriched borrowers with domain/contact/rules assembly.

    Composable usage:
    - Provide `base_df` to skip ingest.
    - Provide `domains_df` to skip domain resolution.
    - Provide `domain_contact_map` to skip crawl/extraction.
    """
    working_base = (
        base_df.copy()
        if base_df is not None
        else build_borrowers_base(
            input_paths=input_paths,
            states=states,
            minimum_loan_amount=minimum_loan_amount,
            naics_codes=naics_codes,
        )
    )
    logger.info("Starting enrichment assembly for %d borrowers", len(working_base))

    working_domains = domains_df.copy() if domains_df is not None else attach_domains_to_borrowers(working_base)
    logger.info("Domain attachment complete for %d borrowers", len(working_domains))

    if domain_contact_map is None:
        domains = working_domains.get("website_domain", pd.Series(dtype="string")).dropna().astype("string")
        domain_contact_map = _build_domain_contact_map(domains.tolist())

    enriched = working_domains.copy()

    def _resolve_contact_row(row: pd.Series) -> dict:
        domain = _normalize_domain(row.get("website_domain"))
        contact_info = domain_contact_map.get(
            domain,
            ContactInfo(
                owner_first_name=None,
                owner_last_name=None,
                owner_role=None,
                email=None,
                phone=None,
                candidates=[],
                data_sources=[],
            ),
        )
        return choose_best_contact(company_name=str(row.get("company_name", "") or ""), contact_info=contact_info)

    enrichment = enriched.apply(_resolve_contact_row, axis=1, result_type="expand")
    output_columns = [
        "website_domain",
        "owner_first_name",
        "owner_last_name",
        "owner_role",
        "email",
        "phone",
        "name_is_synthetic",
        "email_is_generic",
        "email_confidence",
        "data_sources",
    ]
    for column in output_columns:
        if column in enrichment:
            enriched[column] = enrichment[column]

    logger.info("Enrichment assembly complete for %d borrowers", len(enriched))
    return enriched


def export_enriched_to_files(df: pd.DataFrame) -> dict[str, Path]:
    """Write CSV/Excel to `data/` with timestamped file names."""
    cfg = get_config()
    output_dir = cfg.data_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = output_dir / f"enriched_borrowers_{timestamp}.csv"
    excel_path = output_dir / f"enriched_borrowers_{timestamp}.xlsx"

    df.to_csv(csv_path, index=False)
    df.to_excel(excel_path, index=False)

    logger.info("Saved enriched CSV to %s", csv_path)
    logger.info("Saved enriched Excel to %s", excel_path)
    return {"csv_path": csv_path, "excel_path": excel_path}


def summarize_enrichment_quality(df: pd.DataFrame) -> dict[str, float]:
    """Log and return basic QA metrics for enriched output."""
    total = int(len(df))
    if total == 0:
        metrics = {
            "total_borrowers_processed": 0,
            "pct_with_website_domain": 0.0,
            "pct_with_non_synthetic_names": 0.0,
            "pct_with_non_generic_emails": 0.0,
        }
        logger.info("QA summary: %s", metrics)
        return metrics

    has_domain = df.get("website_domain", pd.Series([pd.NA] * total)).notna()
    has_domain &= df.get("website_domain", pd.Series([""] * total)).astype("string").str.strip().ne("")

    non_synthetic = ~df.get("name_is_synthetic", pd.Series([True] * total)).fillna(True).astype(bool)
    non_generic = ~df.get("email_is_generic", pd.Series([True] * total)).fillna(True).astype(bool)

    metrics = {
        "total_borrowers_processed": total,
        "pct_with_website_domain": float(has_domain.mean() * 100.0),
        "pct_with_non_synthetic_names": float(non_synthetic.mean() * 100.0),
        "pct_with_non_generic_emails": float(non_generic.mean() * 100.0),
    }

    logger.info("Total borrowers processed: %d", metrics["total_borrowers_processed"])
    logger.info("%% with website_domain != null: %.2f", metrics["pct_with_website_domain"])
    logger.info("%% with non-synthetic names: %.2f", metrics["pct_with_non_synthetic_names"])
    logger.info("%% with non-generic emails: %.2f", metrics["pct_with_non_generic_emails"])
    return metrics


def export_enriched_results(final_df: pd.DataFrame, output_dir: Path) -> dict[str, Path]:
    """Backward-compatible export wrapper."""
    del output_dir  # preserved in signature for existing callers
    logger.info("Preparing exports for %d enriched records", len(final_df))
    return export_enriched_to_files(final_df)
