"""Extract owner/contact data from crawled HTML content."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import re
from urllib.parse import unquote, urlparse

from bs4 import BeautifulSoup
import pandas as pd

from .crawler import PageContent
from .domains import is_gov_or_edu_domain
from .logging_utils import get_logger

logger = get_logger(__name__)

_EMAIL_RE = re.compile(r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b")
_MAILTO_RE = re.compile(
    r"mailto:\s*([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})",
    re.IGNORECASE,
)
_PHONE_RE = re.compile(
    r"""
    \b
    (?:
        (?:\+\s*1|1)[\s\-.]*(?:\(\s*\d{3}\s*\)|\d{3})[\s\-.]*\d{3}[\s\-.]*\d{4}
        |
        \(\s*\d{3}\s*\)[\s\-.]*\d{3}[\s\-.]*\d{4}
        |
        \d{3}[\s\-.]\d{3}[\s\-.]\d{4}
        |
        \d{3}\.\d{3}\.\d{4}
        |
        (?:\+\s*1\s+|\b1\s+)?(?:\(?\d{3}\)?)\s+\d{3}\s+\d{4}\b
    )
    """,
    re.VERBOSE | re.IGNORECASE,
)
_NAME_RE = re.compile(r"\b([A-Z][a-z]+ [A-Z][a-z]+)\b")
_ROLE_RE = re.compile(
    r"\b("
    r"owner|founder|co-founder|ceo|chief executive officer|president|principal|"
    r"managing partner|partner|director|head"
    r")\b",
    re.IGNORECASE,
)


def _normalize_phone_digits(phone_raw: str) -> str | None:
    """Return US phone as 11 digits starting with ``1``, or ``None`` if not valid."""
    if not phone_raw:
        return None
    digits = re.sub(r"\D+", "", phone_raw)
    if len(digits) == 11 and digits.startswith("1"):
        return digits
    if len(digits) == 10:
        return "1" + digits
    return None


_PAGE_PRIORITY = {
    "about": 4,
    "team": 4,
    "leadership": 4,
    "staff": 4,
    "contact": 3,
    "home": 2,
    "other": 0,
}
_HIGH_VALUE_ROLE_WORDS = {"owner", "founder", "co-founder", "ceo", "chief executive officer"}


def _page_sort_key(page: PageContent) -> tuple[int, int, str]:
    """Prioritize about/team/leadership/staff, then contact, then home; longer pages break ties."""
    t = (page.page_type or "other").lower()
    tier_val = _PAGE_PRIORITY.get(t, _PAGE_PRIORITY["other"])
    text_len = len(page.html or "")
    sized = min(text_len // 5000, 9)
    return (-tier_val, -sized, page.url or "")


def _normalize_host(host: str) -> str:
    host_l = (host or "").lower().strip()
    if host_l.startswith("www."):
        return host_l[4:]
    return host_l


@dataclass(frozen=True)
class PersonCandidate:
    name: str
    role: str | None
    email: str | None
    phone: str | None
    source_url: str
    score: float


@dataclass(frozen=True)
class ContactInfo:
    owner_first_name: str | None
    owner_last_name: str | None
    owner_role: str | None
    email: str | None
    phone: str | None
    candidates: list[PersonCandidate] = field(default_factory=list)
    data_sources: list[str] = field(default_factory=list)
    all_emails: list[str] = field(default_factory=list)
    all_phones: list[str] = field(default_factory=list)


def _url_on_accepted_domain(url: str, accepted_domain: str) -> bool:
    """True when ``url`` is on ``accepted_domain`` (exact host or subdomain)."""
    page_host = _normalize_host(urlparse(url).netloc)
    accepted = _normalize_host(accepted_domain)
    if not page_host or not accepted:
        return False
    return page_host == accepted or page_host.endswith("." + accepted)


def _email_on_domain(email: str, accepted_domain: str) -> bool:
    if "@" not in email:
        return False
    email_host = _normalize_host(email.rsplit("@", 1)[1])
    accepted = _normalize_host(accepted_domain)
    return bool(email_host and accepted and email_host == accepted)


def extract_emails(
    html: str,
    domain: str | None = None,
    *,
    same_domain_only: bool = False,
) -> list[str]:
    """Extract unique emails from HTML (mailto + plain regex).

    When ``same_domain_only`` is True, return only addresses on ``domain`` (RULE 2).
    """
    raw = html or ""
    seen: set[str] = set()
    ordered: list[str] = []

    def _push(addr: str) -> None:
        normalized = addr.strip().lower()
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        ordered.append(normalized)

    soup = BeautifulSoup(raw, "lxml")
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        if not href.lower().startswith("mailto:"):
            continue
        inner = href.split(":", 1)[1].split("?", 1)[0].strip()
        candidate = unquote(inner).strip()
        if "@" in candidate:
            _push(candidate.split("&")[0])

    for m in _MAILTO_RE.findall(raw):
        candidate = unquote(m.strip()).split("?")[0].strip()
        if "@" in candidate:
            _push(candidate)

    for m in _EMAIL_RE.findall(raw):
        _push(m.strip())

    if not domain:
        return ordered

    dom = _normalize_host(domain).lstrip("@")
    if not dom:
        return ordered

    domain_hits = [
        e
        for e in ordered
        if _normalize_host(e.rsplit("@", 1)[-1]) == dom and not is_gov_or_edu_domain(e)
    ]
    if same_domain_only:
        return domain_hits
    rest = [e for e in ordered if e not in domain_hits]
    return domain_hits + rest


def extract_phones(html: str) -> list[str]:
    """Extract US phones from ``tel:`` links and regex matches.

    Each match is normalized to an 11-digit string (leading country code ``1``)
    via :func:`_normalize_phone_digits`. Invalid numbers are omitted. Order is
    preserved with deduplication by digit string.
    """
    seen: set[str] = set()
    phones: list[str] = []
    raw_html = html or ""

    soup = BeautifulSoup(raw_html, "lxml")
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        if not href.lower().startswith("tel:"):
            continue
        inner = href.split(":", 1)[1]
        candidate = unquote(inner.split(";", 1)[0].split("?", 1)[0]).strip()
        normalized = _normalize_phone_digits(candidate)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        phones.append(normalized)

    for raw in _PHONE_RE.findall(raw_html):
        normalized = _normalize_phone_digits(raw.replace("=", "").strip())
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        phones.append(normalized)

    return phones


def _company_domain_from_pages(pages: list[PageContent]) -> str:
    for page in pages:
        host = _normalize_host(urlparse(page.url).netloc)
        if host:
            return host
    return ""


def _page_weight(page_type: str) -> int:
    return _PAGE_PRIORITY.get((page_type or "other").lower(), _PAGE_PRIORITY["other"])


def _extract_name_and_role(text: str) -> tuple[str | None, str | None]:
    role_match = _ROLE_RE.search(text)
    if not role_match:
        return None, None
    role = role_match.group(1)
    names = _NAME_RE.findall(text)
    if not names:
        return None, role

    role_pos = role_match.start()
    best_name = min(names, key=lambda n: abs(text.find(n) - role_pos))
    return best_name, role


def _score_candidate(
    *,
    role: str | None,
    email: str | None,
    phone: str | None,
    source_url: str,
    page_type: str,
    company_domain: str,
) -> float:
    score = float(_page_weight(page_type))
    role_l = (role or "").lower()
    if role_l:
        score += 2.0
        if any(word in role_l for word in _HIGH_VALUE_ROLE_WORDS):
            score += 4.0
    if email:
        score += 1.5
        email_domain = _normalize_host(email.split("@", 1)[1]) if "@" in email else ""
        if company_domain and email_domain == company_domain:
            score += 3.0
    if phone:
        score += 1.0
    if source_url:
        score += 0.1
    return score


def _extract_candidates_from_page(
    page: PageContent,
    company_domain: str,
    *,
    same_domain_only: bool,
) -> list[PersonCandidate]:
    soup = BeautifulSoup(page.html or "", "lxml")
    candidates: list[PersonCandidate] = []
    seen: set[tuple[str, str | None, str | None, str | None]] = set()

    blocks = soup.find_all(["section", "article", "div", "li", "p", "tr"])
    for block in blocks:
        text = block.get_text(" ", strip=True)
        if not text or len(text) > 500:
            continue
        name, role = _extract_name_and_role(text)
        if not name:
            continue
        block_html = str(block)
        emails_list = extract_emails(
            block_html,
            domain=company_domain or None,
            same_domain_only=same_domain_only,
        )
        phones_list = extract_phones(block_html)
        phones_iter = phones_list or [None]
        if emails_list:
            for em in emails_list:
                phone = phones_list[0] if phones_list else None
                score = _score_candidate(
                    role=role,
                    email=em,
                    phone=phone,
                    source_url=page.url,
                    page_type=page.page_type,
                    company_domain=company_domain,
                )
                key = (name, role, em, phone)
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(
                    PersonCandidate(
                        name=name,
                        role=role,
                        email=em,
                        phone=phone,
                        source_url=page.url,
                        score=score,
                    )
                )
        else:
            for phone in phones_iter:
                score = _score_candidate(
                    role=role,
                    email=None,
                    phone=phone,
                    source_url=page.url,
                    page_type=page.page_type,
                    company_domain=company_domain,
                )
                key = (name, role, None, phone)
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(
                    PersonCandidate(
                        name=name,
                        role=role,
                        email=None,
                        phone=phone,
                        source_url=page.url,
                        score=score,
                    )
                )

    page_text = soup.get_text(" ", strip=True)
    if not candidates:
        name, role = _extract_name_and_role(page_text)
        if name:
            emails_list = extract_emails(
                page.html or "",
                domain=company_domain or None,
                same_domain_only=same_domain_only,
            )
            phones_list = extract_phones(page.html or "")
            if emails_list:
                for em in emails_list:
                    phone = phones_list[0] if phones_list else None
                    candidates.append(
                        PersonCandidate(
                            name=name,
                            role=role,
                            email=em,
                            phone=phone,
                            source_url=page.url,
                            score=_score_candidate(
                                role=role,
                                email=em,
                                phone=phone,
                                source_url=page.url,
                                page_type=page.page_type,
                                company_domain=company_domain,
                            ),
                        )
                    )
            elif phones_list:
                for phone in phones_list:
                    candidates.append(
                        PersonCandidate(
                            name=name,
                            role=role,
                            email=None,
                            phone=phone,
                            source_url=page.url,
                            score=_score_candidate(
                                role=role,
                                email=None,
                                phone=phone,
                                source_url=page.url,
                                page_type=page.page_type,
                                company_domain=company_domain,
                            ),
                        )
                    )

    return candidates


def extract_contact_info(
    pages: list[PageContent],
    accepted_domain: str | None = None,
) -> ContactInfo:
    """Gather structured person candidates plus domain-wide email/phone fallback lists.

    When ``accepted_domain`` is set, only pages on that host are used (RULE 2/3) and
    emails must use that domain — not redirect/CDN/third-party hosts.
    """
    if not pages:
        return ContactInfo(
            owner_first_name=None,
            owner_last_name=None,
            owner_role=None,
            email=None,
            phone=None,
            candidates=[],
            data_sources=[],
            all_emails=[],
            all_phones=[],
        )

    accepted = _normalize_host(accepted_domain or "")
    if accepted and is_gov_or_edu_domain(accepted):
        accepted = ""
    sorted_pages = sorted(pages, key=_page_sort_key)
    if accepted:
        sorted_pages = [p for p in sorted_pages if _url_on_accepted_domain(p.url, accepted)]
    company_domain = accepted or _company_domain_from_pages(sorted_pages)
    same_domain_only = bool(accepted)

    all_candidates: list[PersonCandidate] = []
    data_sources: list[str] = []
    seen_urls: set[str] = set()
    pooled_emails: list[str] = []
    pooled_phones: list[str] = []

    for page in sorted_pages:
        if page.url not in seen_urls:
            seen_urls.add(page.url)
            data_sources.append(page.url)

        candidates = _extract_candidates_from_page(
            page,
            company_domain=company_domain,
            same_domain_only=same_domain_only,
        )
        all_candidates.extend(candidates)
        pooled_emails.extend(
            extract_emails(
                page.html or "",
                domain=company_domain or None,
                same_domain_only=same_domain_only,
            )
        )
        pooled_phones.extend(extract_phones(page.html or ""))

    all_candidates.sort(key=lambda candidate: candidate.score, reverse=True)

    def _uniq_preserve(xs: list[str]) -> list[str]:
        seen_f: set[str] = set()
        out_f: list[str] = []
        for item in xs:
            if item in seen_f:
                continue
            seen_f.add(item)
            out_f.append(item)
        return out_f

    all_emails = _uniq_preserve(pooled_emails)
    all_phones = _uniq_preserve(pooled_phones)

    return ContactInfo(
        owner_first_name=None,
        owner_last_name=None,
        owner_role=None,
        email=None,
        phone=None,
        candidates=all_candidates,
        data_sources=data_sources,
        all_emails=all_emails,
        all_phones=all_phones,
    )


def extract_contacts(crawled_df: pd.DataFrame) -> pd.DataFrame:
    """Parse crawled pages and attach extracted owner/contact fields."""
    logger.info("Extracting contacts from %d crawled records", len(crawled_df))
    output = crawled_df.copy()

    def _coerce_pages(value: object) -> list[PageContent]:
        if isinstance(value, list):
            return [page for page in value if isinstance(page, PageContent)]
        return []

    contact_infos = output.get("crawled_pages", pd.Series([[]] * len(output))).map(
        lambda pages: extract_contact_info(_coerce_pages(pages))
    )

    output["owner_first_name"] = contact_infos.map(lambda info: info.owner_first_name)
    output["owner_last_name"] = contact_infos.map(lambda info: info.owner_last_name)
    output["owner_role"] = contact_infos.map(lambda info: info.owner_role)
    output["email"] = contact_infos.map(lambda info: info.email)
    output["phone"] = contact_infos.map(lambda info: info.phone)
    output["all_emails"] = contact_infos.map(lambda info: list(info.all_emails))
    output["all_phones"] = contact_infos.map(lambda info: list(info.all_phones))
    output["candidates"] = contact_infos.map(
        lambda info: [asdict(candidate) for candidate in info.candidates]
    )
    output["data_sources"] = contact_infos.map(lambda info: info.data_sources)

    return output
