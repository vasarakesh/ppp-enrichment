"""Pipeline entrypoint wiring all enrichment stages together."""

from __future__ import annotations

from pathlib import Path

from .config import get_config
from .crawler import crawl_company_pages
from .domains import resolve_domains
from .export import export_enriched_results
from .extract import extract_contacts
from .ingest import load_and_normalize_borrowers
from .logging_utils import configure_logging, get_logger
from .rules import apply_fallback_rules

logger = get_logger(__name__)


def run_pipeline(input_csv: Path) -> None:
    """Run the full enrichment flow from ingest to export.

    TODO:
    - Add CLI args for run options.
    - Add checkpointing/restart support for long runs.
    - Add structured run summary artifact.
    """
    config = get_config()
    configure_logging(config)

    borrowers_df = load_and_normalize_borrowers(input_csv=input_csv)
    domains_df = resolve_domains(borrowers_df=borrowers_df, config=config)
    crawled_df = crawl_company_pages(domains_df=domains_df, config=config)
    contacts_df = extract_contacts(crawled_df=crawled_df)
    final_df = apply_fallback_rules(contacts_df=contacts_df)
    outputs = export_enriched_results(final_df=final_df, output_dir=config.output_dir)

    logger.info("Pipeline scaffold finished. Output placeholders: %s", outputs)


if __name__ == "__main__":
    sample_path = Path("data/input/ppp_source.csv")
    run_pipeline(input_csv=sample_path)
