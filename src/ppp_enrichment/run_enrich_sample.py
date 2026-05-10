"""CLI: enrich borrowers_with_domains_sample with crawl + extract + rules.

Loads ``BORROWERS_WITH_DOMAINS_SAMPLE_PATH``, crawls distinct ``website_domain`` values,
runs contact extraction per domain, applies ``choose_best_contact`` per row, writes
``ENRICHED_SAMPLE_PATH`` under ``ENRICHED_DIR``.
"""

from __future__ import annotations

import pandas as pd

from . import crawler, extract, rules
from . import config
from .config import get_config
from .logging_utils import configure_logging


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


def main() -> None:
    configure_logging(get_config())
    config.ENRICHED_DIR.mkdir(parents=True, exist_ok=True)

    sample_path = config.BORROWERS_WITH_DOMAINS_SAMPLE_PATH
    if not sample_path.exists():
        raise FileNotFoundError(f"Missing sample CSV: {sample_path}")

    df = pd.read_csv(sample_path)
    if df.empty:
        raise ValueError(f"Sample is empty: {sample_path}")

    domain_keys_series = df["website_domain"].map(_cell_to_domain)
    has_domain = domain_keys_series.ne("")
    domains = domain_keys_series.loc[has_domain].drop_duplicates().tolist()

    crawl_map = crawler.crawl_domains(domains)

    domain_contacts: dict[str, extract.ContactInfo] = {}
    for domain_key, pages in crawl_map.items():
        domain_contacts[domain_key] = extract.extract_contact_info(pages)

    enriched_rows: list[dict] = []
    for _, row in df.iterrows():
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

    enriched = pd.DataFrame(enriched_rows)

    out_path = config.ENRICHED_SAMPLE_PATH
    enriched.to_csv(out_path, index=False)

    n_total = len(enriched)
    n_with_domain = int(enriched["website_domain"].notna().sum())
    n_with_email = int(enriched["email"].notna().sum())

    mask_best = (
        enriched["email"].notna()
        & (~enriched["name_is_synthetic"])
        & (~enriched["email_is_generic"])
    )
    n_natural_specific = int(mask_best.sum())

    print(f"Saved {n_total} rows to {out_path}")
    print(
        f"Total rows: {n_total}, "
        f"with domain: {n_with_domain}, "
        f"with email: {n_with_email}, "
        f"with non-synthetic name and non-generic email: {n_natural_specific}"
    )


if __name__ == "__main__":
    main()
