"""Central configuration for the PPP enrichment pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv


# Project paths (derived from package location — no hard-coded OS paths).
# parents[2] = repo root (…/src/ppp_enrichment/config.py -> ppp_enrichment -> src -> root).
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

# Raw PPP data (SBA FOIA extract; place file as shown or symlink to ppp-raw.csv).
PPP_RAW_PATH = INPUT_DIR / "ppp-war.csv"
# One-time copy of the original raw file (created on first pipeline run if missing).
PPP_RAW_BACKUP_PATH = INPUT_DIR / "ppp-war-backup.csv"

# Borrower base derived from ingest (not “enriched” yet).
BORROWERS_BASE_PATH = OUTPUT_DIR / "borrowers_base.csv"

# Enriched outputs: PPP + website + owner/contact info.
ENRICHED_DIR = OUTPUT_DIR / "enriched"
BORROWERS_WITH_DOMAINS_SAMPLE_PATH = ENRICHED_DIR / "borrowers_with_domains_sample.csv"
ENRICHED_SAMPLE_PATH = ENRICHED_DIR / "enriched_borrowers_sample.csv"

# Clean final leads (narrow columns for outbound use).
CLEAN_DIR = OUTPUT_DIR / "clean"

# Clean exports committed from scheduled GitHub jobs (tracked in git; intermediates elsewhere).
GITHUB_CLEAN_RESULTS_DIR = BASE_DIR / "clean_github_results"

# UTF-8 with BOM: Excel on Windows otherwise assumes ANSI and shows mojibake (e.g. "Â‚Ã‚...").
CSV_WRITE_ENCODING = "utf-8-sig"

# DuckDuckGo (domains stage): free search, no API key.
DDG_MAX_RESULTS = 10
DDG_REGION = "us-en"
DDG_DELAY_BETWEEN_QUERIES = 1.0

# Resolver progress logs (borrowers processed between messages).
DOMAIN_PROGRESS_LOG_EVERY = 25

# Crawler: cap pages per host (home + extras) and HTTP timeout (seconds).
CRAWLER_MAX_PAGES_PER_DOMAIN = 4  # home + up to 3 internal links
CRAWLER_REQUEST_TIMEOUT = 10.0


@dataclass(frozen=True)
class AppConfig:
    """Runtime configuration container loaded from environment."""

    project_root: Path
    data_dir: Path
    input_dir: Path
    output_dir: Path
    logs_dir: Path
    default_ppp_input_filename: str
    default_eidl_input_filename: str
    default_prac_input_filename: str
    borrowers_base_output_filename: str
    domain_fetch_requests_per_second: float
    page_timeout_seconds: int
    domain_min_score_threshold: float
    log_level: str
    max_concurrency: int
    request_timeout_seconds: int
    crawler_concurrency: int
    crawler_per_host_concurrency: int
    crawler_max_pages_per_domain: int
    crawler_request_timeout: float
    crawler_user_agent: str
    crawler_delay_per_host: float


def get_config() -> AppConfig:
    """Build and return app configuration.

    TODO:
    - Add stricter validation for required keys and path existence.
    - Support stage-specific settings (crawler, extraction, export).
    """
    project_root = BASE_DIR
    env_path = project_root / "config" / ".env"
    load_dotenv(env_path)

    return AppConfig(
        project_root=project_root,
        data_dir=project_root / "data",
        input_dir=project_root / "data" / "input",
        output_dir=project_root / "data" / "output",
        logs_dir=project_root / "logs",
        default_ppp_input_filename=os.getenv("DEFAULT_PPP_INPUT_FILENAME", "ppp_raw.csv"),
        default_eidl_input_filename=os.getenv("DEFAULT_EIDL_INPUT_FILENAME", "eidl_raw.csv"),
        default_prac_input_filename=os.getenv("DEFAULT_PRAC_INPUT_FILENAME", "prac_raw.csv"),
        borrowers_base_output_filename=os.getenv("BORROWERS_BASE_OUTPUT_FILENAME", "borrowers_base.csv"),
        domain_fetch_requests_per_second=float(
            os.getenv("DOMAIN_FETCH_REQUESTS_PER_SECOND", "3.0")
        ),
        page_timeout_seconds=int(os.getenv("PAGE_TIMEOUT_SECONDS", "10")),
        domain_min_score_threshold=float(os.getenv("DOMAIN_MIN_SCORE_THRESHOLD", "0.38")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        max_concurrency=int(os.getenv("MAX_CONCURRENCY", "10")),
        request_timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20")),
        crawler_concurrency=int(os.getenv("CRAWLER_CONCURRENCY", "15")),
        crawler_per_host_concurrency=int(os.getenv("CRAWLER_PER_HOST_CONCURRENCY", "2")),
        crawler_max_pages_per_domain=int(
            os.getenv("CRAWLER_MAX_PAGES_PER_DOMAIN", str(CRAWLER_MAX_PAGES_PER_DOMAIN))
        ),
        crawler_request_timeout=float(
            os.getenv("CRAWLER_REQUEST_TIMEOUT", str(CRAWLER_REQUEST_TIMEOUT))
        ),
        crawler_user_agent=os.getenv(
            "CRAWLER_USER_AGENT",
            "PPP-EnrichmentBot/1.0 (+https://example.com/bot)",
        ),
        crawler_delay_per_host=float(os.getenv("CRAWLER_DELAY_PER_HOST", "0.5")),
    )
