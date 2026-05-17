"""Apply business fallback rules for outbound enrichment."""

from __future__ import annotations

from dataclasses import dataclass
import re
from urllib.parse import urlparse

import pandas as pd

from .domains import is_gov_or_edu_domain
from .extract import ContactInfo, PersonCandidate
from .logging_utils import get_logger

logger = get_logger(__name__)

_COMPANY_SUFFIXES = {
    "inc",
    "llc",
    "llp",
    "corp",
    "corporation",
    "company",
    "co",
    "funding",
    "fund",
}
_OWNER_ROLE_MARKERS = (
    "chief executive officer",
    "chief executive",
    "founder",
    "owner",
    "ceo",
    "president",
)
_GENERIC_EMAIL_PREFIXES = {
    "info",
    "contact",
    "hello",
    "support",
    "admin",
    "office",
    "team",
    "sales",
    "inquiries",
    "enquiries",
}
_EMAIL_CONFIDENCE = {
    "explicit_owner_company_domain": 0.96,
    "explicit_owner_foreign_domain": 0.82,
    "personal_company_domain": 0.86,
    "generic_company_domain": 0.58,
    "phone_only": 0.32,
    "no_contact_signals": 0.05,
    "missing_contact_info": 0.0,
}
_GENERIC_PREFIX_ORDER = ("info", "contact", "hello", "office", "support", "sales", "admin")


@dataclass(frozen=True)
class _RuleSettings:
    owner_role_markers: tuple[str, ...]
    generic_email_prefixes: set[str]
    company_suffixes: set[str]
    email_confidence: dict[str, float]


_RULES = _RuleSettings(
    owner_role_markers=_OWNER_ROLE_MARKERS,
    generic_email_prefixes=_GENERIC_EMAIL_PREFIXES,
    company_suffixes=_COMPANY_SUFFIXES,
    email_confidence=_EMAIL_CONFIDENCE,
)


def _normalize_host(host: str) -> str:
    normalized = (host or "").strip().lower()
    if normalized.startswith("www."):
        return normalized[4:]
    return normalized


def _extract_company_domain_from_sources(data_sources: list[str]) -> str:
    for source in data_sources:
        host = _normalize_host(urlparse(source).netloc)
        if host:
            return host
    return ""


def _as_list(value: object) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _parse_person_name(name: str | None) -> tuple[str, str]:
    tokens = [token for token in re.split(r"[\s\W_]+", (name or "").strip()) if token]
    if not tokens:
        return "", ""
    if len(tokens) == 1:
        return tokens[0], ""
    return tokens[0], tokens[-1]


def _has_explicit_owner_role(role: str | None) -> bool:
    """True when role mentions founder/owner/ceo/president."""
    role_l = (role or "").strip().lower()
    if not role_l:
        return False
    return any(marker in role_l for marker in _RULES.owner_role_markers)


def _email_domain(email: str | None) -> str:
    if not email or "@" not in email:
        return ""
    return _normalize_host(email.rsplit("@", 1)[1])


def _is_generic_email(email: str | None) -> bool:
    if not email or "@" not in email:
        return False
    local_part = email.split("@", 1)[0].lower()
    return local_part in _RULES.generic_email_prefixes


def _generic_prefix_rank(email: str) -> tuple[int, str]:
    local = email.split("@", 1)[0].lower()
    if local in _GENERIC_PREFIX_ORDER:
        return (_GENERIC_PREFIX_ORDER.index(local), local)
    supplemental = sorted(
        prefix for prefix in _RULES.generic_email_prefixes if prefix not in _GENERIC_PREFIX_ORDER
    )
    tier = len(_GENERIC_PREFIX_ORDER) + supplemental.index(local)
    return (tier, local)


def _candidate_sort_score(candidate: PersonCandidate, company_domain: str) -> tuple[float, float]:
    email = candidate.email
    domain_match = bool(company_domain and _email_domain(email) == company_domain)
    role_boost = 1.5 if _has_explicit_owner_role(candidate.role) else 0.0
    email_boost = 1.0 if email else 0.0
    domain_boost = 1.0 if domain_match else 0.0
    generic_penalty = -0.75 if _is_generic_email(email) else 0.0
    score = float(candidate.score) + role_boost + email_boost + domain_boost + generic_penalty
    return score, float(candidate.score)


def _leadership_priority(candidate: PersonCandidate, company_domain: str) -> tuple[bool, float]:
    on_domain = bool(company_domain and _email_domain(candidate.email) == company_domain)
    return on_domain, float(candidate.score)


def synthesize_name_from_company(company_name: str) -> tuple[str, str, bool]:
    cleaned = (company_name or "").strip()
    if not cleaned:
        return "", "", True

    tokens = [token for token in re.split(r"[\s\W_]+", cleaned) if token]
    while tokens and tokens[-1].lower().rstrip(".") in _RULES.company_suffixes:
        tokens.pop()

    if not tokens:
        return "", "", True
    if len(tokens) == 1:
        return tokens[0], "", True
    return tokens[0], tokens[1], True


def _person_names_from_candidate(candidate: PersonCandidate) -> tuple[str, str]:
    return _parse_person_name(candidate.name)


def _unique_emails_from_candidates(candidates: list[PersonCandidate]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for cand in candidates:
        if cand.email:
            lowered = cand.email.strip().lower()
            if lowered not in seen:
                seen.add(lowered)
                ordered.append(lowered)
    return ordered


def _phone_from_accepted_domain(
    phone: str | None,
    source_url: str | None,
    company_domain: str,
) -> bool:
    """RULE 3: phone must come from a page on the accepted domain."""
    if not phone or not company_domain:
        return bool(phone)
    if not source_url:
        return False
    host = _normalize_host(urlparse(source_url).netloc)
    accepted = _normalize_host(company_domain)
    return host == accepted or host.endswith("." + accepted)


def choose_best_contact(
    company_name: str,
    contact_info: ContactInfo | None,
    *,
    accepted_domain: str | None = None,
) -> dict:
    """Pick the outbound contact bundle from structured scrape results."""
    sources = [str(src) for src in _as_list(contact_info.data_sources)] if contact_info else []

    base = {
        "owner_first_name": "",
        "owner_last_name": "",
        "owner_role": None,
        "email": None,
        "phone": None,
        "name_is_synthetic": False,
        "email_is_generic": False,
        "email_confidence": _RULES.email_confidence["no_contact_signals"],
        "data_sources": sources,
    }

    if contact_info is None:
        first_name, last_name, synthetic = synthesize_name_from_company(company_name)
        base.update(
            {
                "owner_first_name": first_name or "",
                "owner_last_name": last_name or "",
                "owner_role": None,
                "email": None,
                "phone": None,
                "name_is_synthetic": synthetic or True,
                "email_is_generic": False,
                "email_confidence": _RULES.email_confidence["missing_contact_info"],
                "data_sources": [],
            }
        )
        return base

    company_domain = _normalize_host(accepted_domain or "") or _extract_company_domain_from_sources(
        contact_info.data_sources
    )
    if company_domain and is_gov_or_edu_domain(company_domain):
        company_domain = ""
    candidates = list(contact_info.candidates or [])
    aggregated_emails = list(contact_info.all_emails or [])
    aggregated_phones = list(contact_info.all_phones or [])
    if not aggregated_emails:
        aggregated_emails = _unique_emails_from_candidates(candidates)

    def _email_on_accepted(email: str | None) -> bool:
        if not email:
            return False
        if not company_domain:
            return True
        return _email_domain(email) == company_domain

    candidates = [
        cand
        for cand in candidates
        if (not cand.email or _email_on_accepted(cand.email))
        and (not cand.phone or _phone_from_accepted_domain(cand.phone, cand.source_url, company_domain))
    ]
    aggregated_emails = [email for email in aggregated_emails if _email_on_accepted(email)]
    if company_domain:
        phones_from_candidates = [
            cand.phone
            for cand in candidates
            if cand.phone and _phone_from_accepted_domain(cand.phone, cand.source_url, company_domain)
        ]
        aggregated_phones = phones_from_candidates or [
            phone for phone in aggregated_phones if phone
        ]
    else:
        aggregated_phones = list(contact_info.all_phones or [])

    ownership_pool = [
        candidate
        for candidate in candidates
        if candidate.email and _has_explicit_owner_role(candidate.role)
    ]
    personal_email_pool = [
        candidate
        for candidate in candidates
        if candidate.email
        and not _has_explicit_owner_role(candidate.role)
        and not _is_generic_email(candidate.email)
    ]
    generic_email_pool = [
        email
        for email in aggregated_emails
        if _is_generic_email(email)
    ]

    if ownership_pool:
        selected = max(ownership_pool, key=lambda cand: _leadership_priority(cand, company_domain))
        first_name, last_name = _person_names_from_candidate(selected)
        on_domain = bool(company_domain and _email_domain(selected.email) == company_domain)
        conf_key = (
            "explicit_owner_company_domain" if on_domain else "explicit_owner_foreign_domain"
        )
        base.update(
            {
                "owner_first_name": first_name,
                "owner_last_name": last_name,
                "owner_role": selected.role,
                "email": selected.email,
                "phone": selected.phone or (aggregated_phones[0] if aggregated_phones else None),
                "name_is_synthetic": False,
                "email_is_generic": False,
                "email_confidence": _RULES.email_confidence[conf_key],
            }
        )
        return base

    if personal_email_pool:
        selected = max(
            personal_email_pool,
            key=lambda cand: _candidate_sort_score(cand, company_domain),
        )
        first_name, last_name = _person_names_from_candidate(selected)
        base.update(
            {
                "owner_first_name": first_name,
                "owner_last_name": last_name,
                "owner_role": selected.role or None,
                "email": selected.email,
                "phone": selected.phone or (aggregated_phones[0] if aggregated_phones else None),
                "name_is_synthetic": False,
                "email_is_generic": False,
                "email_confidence": _RULES.email_confidence["personal_company_domain"],
            }
        )
        return base

    if generic_email_pool:
        ranked = sorted(generic_email_pool, key=_generic_prefix_rank)
        chosen = ranked[0]
        synthetic_first, synthetic_last, synthetic_flag = synthesize_name_from_company(company_name)
        primary_phone = aggregated_phones[0] if aggregated_phones else None
        base.update(
            {
                "owner_first_name": synthetic_first,
                "owner_last_name": synthetic_last,
                "owner_role": "Generic contact",
                "email": chosen,
                "phone": primary_phone,
                "name_is_synthetic": True,
                "email_is_generic": True,
                "email_confidence": _RULES.email_confidence["generic_company_domain"],
            }
        )
        return base

    if aggregated_emails:
        chosen = aggregated_emails[0]
        synthetic_first, synthetic_last, synthetic_flag = synthesize_name_from_company(company_name)
        primary_phone = aggregated_phones[0] if aggregated_phones else None
        base.update(
            {
                "owner_first_name": synthetic_first,
                "owner_last_name": synthetic_last,
                "owner_role": "Website contact",
                "email": chosen,
                "phone": primary_phone,
                "name_is_synthetic": synthetic_flag or True,
                "email_is_generic": _is_generic_email(chosen),
                "email_confidence": _RULES.email_confidence["generic_company_domain"],
            }
        )
        return base

    if not aggregated_emails and aggregated_phones:
        synth_first, synth_last, synth_flag = synthesize_name_from_company(company_name)
        base.update(
            {
                "owner_first_name": synth_first,
                "owner_last_name": synth_last,
                "owner_role": "Phone-only contact",
                "email": None,
                "phone": aggregated_phones[0],
                "name_is_synthetic": synth_flag or True,
                "email_is_generic": False,
                "email_confidence": _RULES.email_confidence["phone_only"],
            }
        )
        return base

    synth_first, synth_last, synth_flag = synthesize_name_from_company(company_name)
    base.update(
        {
            "owner_first_name": synth_first or "",
            "owner_last_name": synth_last or "",
            "owner_role": None,
            "email": None,
            "phone": None,
            "name_is_synthetic": synth_flag or True,
            "email_is_generic": False,
            "email_confidence": _RULES.email_confidence["no_contact_signals"],
        }
    )
    return base


def apply_fallback_rules(contacts_df: pd.DataFrame) -> pd.DataFrame:
    """Apply synthetic-name and email fallback business rules."""

    logger.info("Applying business rules to %d records", len(contacts_df))

    def _contact_info_from_row(row: pd.Series) -> ContactInfo:
        candidates_raw = _as_list(row.get("candidates", []))
        candidates: list[PersonCandidate] = []
        for candidate_raw in candidates_raw:
            if isinstance(candidate_raw, PersonCandidate):
                candidates.append(candidate_raw)
                continue
            if isinstance(candidate_raw, dict):
                candidates.append(
                    PersonCandidate(
                        name=str(candidate_raw.get("name", "") or ""),
                        role=candidate_raw.get("role"),
                        email=candidate_raw.get("email"),
                        phone=candidate_raw.get("phone"),
                        source_url=str(candidate_raw.get("source_url", "") or ""),
                        score=float(candidate_raw.get("score", 0.0) or 0.0),
                    )
                )

        data_sources = [str(src) for src in _as_list(row.get("data_sources", []))]
        emails_list: list[str] = []
        for entry in _as_list(row.get("all_emails", [])):
            token = entry.strip().lower() if isinstance(entry, str) else str(entry).strip().lower()
            if token and "@" in token:
                emails_list.append(token)
        phones_list = [
            str(p).strip() for p in _as_list(row.get("all_phones", [])) if isinstance(p, str) and p.strip()
        ]
        if not emails_list:
            emails_list = _unique_emails_from_candidates(candidates)
        unique_emails_seen: set[str] = set()
        unique_emails: list[str] = []
        for em in emails_list:
            lowered = em.lower()
            if lowered not in unique_emails_seen:
                unique_emails_seen.add(lowered)
                unique_emails.append(lowered)
        phones_seen: set[str] = set()
        uniq_phones: list[str] = []
        for ph in phones_list:
            if ph not in phones_seen:
                phones_seen.add(ph)
                uniq_phones.append(ph)

        return ContactInfo(
            owner_first_name=row.get("owner_first_name"),
            owner_last_name=row.get("owner_last_name"),
            owner_role=row.get("owner_role"),
            email=row.get("email"),
            phone=row.get("phone"),
            candidates=candidates,
            data_sources=data_sources,
            all_emails=unique_emails,
            all_phones=uniq_phones,
        )

    output = contacts_df.copy()
    best_contact_rows = output.apply(
        lambda row: choose_best_contact(
            company_name=str(row.get("company_name", "") or ""),
            contact_info=_contact_info_from_row(row),
        ),
        axis=1,
        result_type="expand",
    )

    for column in [
        "owner_first_name",
        "owner_last_name",
        "owner_role",
        "email",
        "phone",
        "name_is_synthetic",
        "email_is_generic",
        "email_confidence",
        "data_sources",
    ]:
        output[column] = best_contact_rows[column]

    return output
