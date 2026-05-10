"""Resolve company records to probable website domains."""

from __future__ import annotations

import re
import time
from difflib import SequenceMatcher
from typing import Any
from urllib.parse import urlparse

import httpx
import pandas as pd
from ddgs import DDGS

from .config import (
    DDG_MAX_RESULTS,
    DDG_REGION,
    DOMAIN_PROGRESS_LOG_EVERY,
    AppConfig,
    get_config,
)
from .logging_utils import get_logger

logger = get_logger(__name__)

_TAG_RE = re.compile(r"<[^>]+>")
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.IGNORECASE | re.DOTALL)
_OG_SITE_NAME_RE = re.compile(
    r"""<meta[^>]+property=["']og:site_name["'][^>]+content=["'](.*?)["'][^>]*>""",
    re.IGNORECASE | re.DOTALL,
)
_TEXT_RE = re.compile(r"\s+")
_GENERIC_TOKEN_RE = re.compile(r"[^a-z0-9 ]+", re.IGNORECASE)
_MAX_QUERY_STEPS = 3

# Hosts to skip when turning SERP links into candidate company domains (search/review/directory/social).
_SERP_DOMAIN_BLOCKLIST: frozenset[str] = frozenset(
    {
        "wikipedia.org",
        "wikimedia.org",
        "wikidata.org",
        "wiktionary.org",
        "yandex.com",
        "mojeek.com",
        "facebook.com",
        "linkedin.com",
        "twitter.com",
        "instagram.com",
        "tiktok.com",
        "x.com",
        "youtube.com",
        "reddit.com",
        "pinterest.com",
        "medium.com",
        "tumblr.com",
        "quora.com",
        "duckduckgo.com",
        "google.com",
        "bing.com",
        "yahoo.com",
        "baidu.com",
        "yelp.com",
        "tripadvisor.com",
        "trustpilot.com",
        "bbb.org",
        "bbb.com",
        "glassdoor.com",
        "indeed.com",
        "ziprecruiter.com",
        "monster.com",
        "crunchbase.com",
        "bloomberg.com",
        "zoominfo.com",
        "rocketreach.co",
        "apollo.io",
        "dnb.com",
        "manta.com",
        "yellowpages.com",
        "superpages.com",
        "mapquest.com",
        "foursquare.com",
        "opencorporates.com",
        "amazon.com",
        "ebay.com",
        "etsy.com",
    }
)

_MIN_TOKEN_LEN = 3
_STRONG_ALIGNMENT_MIN_SCORE = 0.34

# Legal / entity suffix tokens removed from the end (and internal "noise") for querying & matching.
_COMPANY_SUFFIX_PATTERN = re.compile(
    r"""
    (?ix)
    (?:
        ^|\s
    )
    (?:[&.,]\s*|)
    (?:
        l\.?\s?l\.?\s?c\.?|plc|plc\.|inc\.?|incorporated|corp\.?|corporation|
        ltd\.?|limited|company|co\.|llp|pc\.?|llc|pllc|\bco\b|\bltd\b|\bpc\b|\bpa\b|\bdds\b|\bdmd\b
    )
    (?=\s|$)
    """,
    re.VERBOSE,
)

_LEGAL_NOISE_WORDS: frozenset[str] = frozenset(
    {
        "inc",
        "incorporated",
        "llc",
        "pllc",
        "llp",
        "corp",
        "corporation",
        "co",
        "company",
        "ltd",
        "limited",
        "church",
        "institute",
        "task",
        "force",
    }
)
_STOPWORDS: frozenset[str] = frozenset(
    {
        "the",
        "of",
        "and",
        "for",
        "at",
        "in",
        "on",
        "to",
        "by",
        "from",
        "with",
        "a",
        "an",
    }
)

_domain_cache: dict[tuple[str, str, str], tuple[str | None, float]] = {}


def _squeeze_ws(value: str) -> str:
    return _TEXT_RE.sub(" ", value).strip()


def _to_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "<na>"}:
        return None
    return text


def _strip_company_suffixes(company_name: str) -> str:
    """Remove punctuation noise and trailing entity suffixes (LLC, Inc, etc.)."""
    name = company_name.strip()
    if not name:
        return ""
    # Remove commas / ampersands used as separators; keep alphanumeric + spaces for tokens.
    name = _GENERIC_TOKEN_RE.sub(" ", name.lower())
    prev = None
    while prev != name:
        prev = name
        name = _COMPANY_SUFFIX_PATTERN.sub(" ", name)
        name = _squeeze_ws(name)
    return name


def _normalize_name_like_text(value: str) -> str:
    cleaned = _GENERIC_TOKEN_RE.sub(" ", (value or "").lower())
    cleaned = _squeeze_ws(cleaned)
    return cleaned


def _brand_tokens(name_like: str) -> list[str]:
    text = _normalize_name_like_text(name_like)
    if not text:
        return []
    tokens = [
        t
        for t in text.split()
        if len(t) >= _MIN_TOKEN_LEN and t not in _LEGAL_NOISE_WORDS and t not in _STOPWORDS
    ]
    if not tokens:
        raw = text.split()
        return [max(raw, key=len)] if raw else []
    joined = "".join(tokens)
    if len(tokens) >= 2 and len(joined) >= 8:
        tokens.append(joined)
    return list(dict.fromkeys(tokens))


def _normalized_company_tokens(company_name: str) -> list[str]:
    """Tokens for overlap scoring after suffix stripping."""
    stripped = _strip_company_suffixes(company_name)
    if not stripped:
        return []
    tokens = [t for t in stripped.split() if len(t) >= _MIN_TOKEN_LEN]
    short = stripped.split()
    # Keep one short distinctive token if the whole name collapses otherwise (e.g. "Bob Co", "3M").
    if not tokens and short:
        return [max(short, key=len)] if short else []
    return tokens


def _build_domain_queries(company_name: str, city: str | None, state: str | None) -> list[str]:
    """
    Ordered search phrases: "name city state", "name state", "name".
    Uses suffix-stripped, punctuation-normalized name for all steps.
    """
    base = _strip_company_suffixes(company_name)
    if not base:
        return []

    segments: list[str] = [base]
    ct = (_to_text(city) or "").strip()
    st = (_to_text(state) or "").strip()
    if ct:
        segments.append(ct)
    if st:
        segments.append(st)

    q_full = _squeeze_ws(" ".join(segments))
    queries: list[str] = [q_full]

    if st:
        q_state = _squeeze_ws(f"{base} {st}")
        if q_state != q_full:
            queries.append(q_state)

    if base != q_full and base not in queries:
        queries.append(base)

    seen: set[str] = set()
    out: list[str] = []
    for q in queries:
        if q and q not in seen:
            seen.add(q)
            out.append(q)
    return out[:_MAX_QUERY_STEPS]


def _normalize_url_to_domain(url: str | None) -> str | None:
    if not url:
        return None
    candidate = url.strip()
    if not candidate:
        return None
    if "://" not in candidate:
        candidate = "https://" + candidate
    parsed = urlparse(candidate)
    host = (parsed.netloc or "").lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host or None


def _primary_company_search_query(company_name: str, city: str | None, state: str | None) -> str:
    """Single search phrase used for fast domain guess (first step of the legacy multi-query list)."""
    queries = _build_domain_queries(company_name, city, state)
    return queries[0] if queries else ""


def _serp_host_is_blocked(host: str) -> bool:
    h = host.lower().strip()
    for root in _SERP_DOMAIN_BLOCKLIST:
        if h == root or h.endswith(f".{root}"):
            return True
    return False


def search_company_domains(company_name: str, city: str | None, state: str | None) -> list[str]:
    """One DuckDuckGo text search; return non-blacklisted hostnames in SERP order (deduped).

    No HTTP fetches to company sites — only ``ddgs.text`` and URL parsing.
    """
    company = _to_text(company_name) or ""
    if not company.strip():
        return []

    query = _primary_company_search_query(company_name, city, state)
    if not query.strip():
        return []

    logger.debug(
        "DuckDuckGo text query=%r max_results=%d region=%s",
        query,
        DDG_MAX_RESULTS,
        DDG_REGION,
    )

    urls_from_hits: list[str] = []
    try:
        with DDGS() as ddgs:
            iterator = ddgs.text(query, max_results=DDG_MAX_RESULTS, region=DDG_REGION)
            for result in iterator or []:
                if not isinstance(result, dict):
                    continue
                raw = result.get("href") or result.get("url") or result.get("link")
                if raw:
                    urls_from_hits.append(str(raw))
    except Exception as exc:
        logger.warning("DuckDuckGo search failed for query %r: %s", query, exc)
        return []

    ordered: list[str] = []
    seen: set[str] = set()
    for url in urls_from_hits:
        domain = _normalize_url_to_domain(url)
        if not domain or domain in seen:
            continue
        if _serp_host_is_blocked(domain):
            continue
        seen.add(domain)
        ordered.append(domain)
    return ordered


class _RateLimiter:
    """Simple synchronous token spacing limiter (requests/sec)."""

    def __init__(self, requests_per_second: float) -> None:
        self.min_interval = 0.0 if requests_per_second <= 0 else 1.0 / requests_per_second
        self._next_allowed = 0.0

    def wait_turn(self) -> None:
        if self.min_interval <= 0:
            return
        now = time.monotonic()
        if now < self._next_allowed:
            time.sleep(self._next_allowed - now)
            now = time.monotonic()
        self._next_allowed = now + self.min_interval


def _extract_page_features(html: str) -> tuple[str, str]:
    title_match = _TITLE_RE.search(html)
    title = _squeeze_ws(_TAG_RE.sub(" ", title_match.group(1))) if title_match else ""
    og_match = _OG_SITE_NAME_RE.search(html)
    og_site_name = _squeeze_ws(_TAG_RE.sub(" ", og_match.group(1))) if og_match else ""
    h1_match = _H1_RE.search(html)
    h1 = _squeeze_ws(_TAG_RE.sub(" ", h1_match.group(1))) if h1_match else ""
    plain = _squeeze_ws(_TAG_RE.sub(" ", html))
    brand_text = _squeeze_ws(f"{og_site_name} {title} {h1}".lower())
    headline = _squeeze_ws(f"{title} {h1}".lower())
    return headline, plain.lower(), brand_text


def _token_hits(tokens: list[str], haystack_lower: str) -> int:
    return sum(1 for t in tokens if t and t.lower() in haystack_lower)


def _location_bonus(page_lower: str, city: str | None, state: str | None, zip_code: str | None) -> float:
    bonus = 0.0
    if city and city.strip() and city.lower() in page_lower:
        bonus += 0.12
    if state and state.strip() and state.lower() in page_lower:
        bonus += 0.08
    if zip_code and zip_code in page_lower:
        bonus += 0.08
    return min(bonus, 0.22)


def _name_overlap_score(tokens: list[str], headline_lower: str, page_lower: str) -> float:
    if not tokens:
        return 0.0
    n = float(len(tokens))
    hits_head = _token_hits(tokens, headline_lower)
    frac_head = hits_head / n

    hits_page = _token_hits(tokens, page_lower)
    # Headline weighted more heavily than full page body.
    base = min(0.82, 0.52 * frac_head + 0.28 * min(1.0, hits_page / n))

    stripped_joined = _squeeze_ws(" ".join(tokens).lower())
    if stripped_joined and len(stripped_joined) >= 5 and stripped_joined in headline_lower:
        base = max(base, 0.72)
    if stripped_joined and len(stripped_joined) >= 6 and stripped_joined in page_lower:
        base = max(base, 0.52)

    return min(1.0, base)


def _domain_label_tokens(candidate_domain: str) -> list[str]:
    host = (candidate_domain or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    label = host.split(".", 1)[0]
    parts = [p for p in re.split(r"[^a-z0-9]+", label) if p]
    tokens = [p for p in parts if len(p) >= _MIN_TOKEN_LEN]
    compact = "".join(parts)
    if compact and len(compact) >= _MIN_TOKEN_LEN:
        tokens.append(compact)
    return list(dict.fromkeys(tokens))


def _token_overlap_ratio(a_tokens: list[str], b_tokens: list[str]) -> float:
    if not a_tokens or not b_tokens:
        return 0.0
    a_set = set(a_tokens)
    b_set = set(b_tokens)
    inter = len(a_set & b_set)
    return inter / float(max(len(a_set), len(b_set)))


def _fuzzy_brand_ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def _strong_name_domain_alignment_score(
    company_name: str,
    candidate_domain: str,
    brand_text: str,
) -> float:
    company_tokens = _brand_tokens(company_name)
    brand_tokens = _brand_tokens(brand_text)
    domain_tokens = _domain_label_tokens(candidate_domain)
    company_norm = "".join(company_tokens)
    brand_norm = "".join(brand_tokens)
    domain_norm = "".join(domain_tokens)

    overlap_company_brand = _token_overlap_ratio(company_tokens, brand_tokens)
    overlap_company_domain = _token_overlap_ratio(company_tokens, domain_tokens)
    fuzzy_company_brand = _fuzzy_brand_ratio(company_norm, brand_norm)
    fuzzy_company_domain = _fuzzy_brand_ratio(company_norm, domain_norm)

    return (
        0.35 * overlap_company_brand
        + 0.30 * overlap_company_domain
        + 0.20 * fuzzy_company_brand
        + 0.15 * fuzzy_company_domain
    )


def _score_domain_sync(
    client: httpx.Client,
    limiter: _RateLimiter,
    page_timeout_seconds: int,
    candidate_domain: str,
    company_name: str,
    city: str | None,
    state: str | None,
    zip_code: str | None = None,
) -> float:
    tokens = _normalized_company_tokens(company_name)
    if not tokens:
        return 0.0

    html: str | None = None
    for scheme in ("https", "http"):
        url = f"{scheme}://{candidate_domain}/"
        limiter.wait_turn()
        try:
            response = client.get(url, follow_redirects=True, timeout=page_timeout_seconds)
            if response.status_code >= 400:
                continue
            html = response.text[:500_000]
            break
        except httpx.TimeoutException:
            logger.debug("Timeout fetching %s", url)
        except httpx.HTTPError:
            logger.debug("HTTP error fetching %s", url)

    if not html:
        return 0.0

    headline_lower, page_lower, brand_text = _extract_page_features(html)

    overlap = _name_overlap_score(tokens, headline_lower, page_lower)
    overlap = max(
        overlap,
        _strong_name_domain_alignment_score(
            company_name=company_name,
            candidate_domain=candidate_domain,
            brand_text=brand_text,
        ),
    )
    overlap += _location_bonus(page_lower, city, state, zip_code)

    if overlap < 0.08:
        return 0.0
    return max(0.0, min(1.0, overlap))


def score_domain(candidate_domain: str, company_name: str, city: str | None, state: str | None) -> float:
    """Score a candidate domain by company and optional location cues on homepage."""
    config = get_config()
    limiter = _RateLimiter(config.domain_fetch_requests_per_second)
    with httpx.Client(follow_redirects=True, timeout=config.request_timeout_seconds) as client:
        return _score_domain_sync(
            client=client,
            limiter=limiter,
            page_timeout_seconds=config.page_timeout_seconds,
            candidate_domain=candidate_domain,
            company_name=company_name,
            city=city,
            state=state,
            zip_code=None,
        )


def _cache_key_parts(company_name: str, city: str | None, state: str | None) -> tuple[str, str, str]:
    return (
        _to_text(company_name) or "",
        (_to_text(city) or "").strip(),
        (_to_text(state) or "").strip(),
    )


def resolve_domain_for_row(
    row: pd.Series,
    *,
    client: httpx.Client | None = None,
    config: AppConfig | None = None,
) -> tuple[str | None, float]:
    """Resolve a single borrower row to a fast SERP-first domain guess (cached).

    One DuckDuckGo search per cache miss; no httpx GETs to score candidates here.
    ``client`` / ``config`` are accepted for backward-compatible call sites.
    """
    _ = (client, config or get_config())

    company_name = _to_text(row.get("company_name")) or ""
    city = _to_text(row.get("city"))
    state = _to_text(row.get("state"))
    key = _cache_key_parts(company_name, city, state)
    if key in _domain_cache:
        logger.debug("Domain cache hit for %s / %s / %s", key[0], key[1], key[2])
        return _domain_cache[key]

    query = _primary_company_search_query(company_name, city, state)
    candidates = search_company_domains(company_name, city, state)
    website_domain: str | None = None
    domain_score = 0.0
    if candidates:
        cfg = config or get_config()
        limiter = _RateLimiter(cfg.domain_fetch_requests_per_second)
        with httpx.Client(follow_redirects=True, timeout=cfg.request_timeout_seconds) as local_client:
            best_domain: str | None = None
            best_score = 0.0
            for candidate in candidates:
                score = _score_domain_sync(
                    client=local_client,
                    limiter=limiter,
                    page_timeout_seconds=cfg.page_timeout_seconds,
                    candidate_domain=candidate,
                    company_name=company_name,
                    city=city,
                    state=state,
                    zip_code=_to_text(row.get("zip")),
                )
                if score > best_score:
                    best_score = score
                    best_domain = candidate
            if best_domain and best_score >= _STRONG_ALIGNMENT_MIN_SCORE:
                website_domain = best_domain
                domain_score = best_score

    display_company = company_name if company_name else "<unknown>"
    logger.info(
        "Resolved domain for %s: %s from query '%s'",
        display_company,
        website_domain or "None",
        query,
    )

    result = (website_domain, domain_score)
    _domain_cache[key] = result
    return result


def _series_from_namedtuple(nt: Any) -> pd.Series:
    return pd.Series(nt._asdict())


def _attach_domains_sync(df: pd.DataFrame, config: AppConfig) -> pd.DataFrame:
    result = df.copy()
    if result.empty:
        result["website_domain"] = pd.Series(dtype="string")
        result["domain_score"] = pd.Series(dtype="float64")
        return result

    result["website_domain"] = pd.Series([None] * len(result), index=result.index, dtype="object")
    result["domain_score"] = pd.Series([0.0] * len(result), index=result.index, dtype="float64")

    total = len(result)
    non_null = 0
    for i, nt in enumerate(result.itertuples(index=True), start=1):
        row_series = _series_from_namedtuple(nt)
        idx = row_series["Index"]
        company = _to_text(row_series.get("company_name")) or "<unknown>"
        try:
            best_domain, best_score = resolve_domain_for_row(
                row_series,
                config=config,
            )
        except Exception as exc:
            logger.warning("Domain resolution failed for '%s': %s", company, exc)
            best_domain, best_score = None, 0.0

        result.at[idx, "website_domain"] = best_domain
        result.at[idx, "domain_score"] = float(best_score)
        if best_domain:
            non_null += 1

        if i % DOMAIN_PROGRESS_LOG_EVERY == 0 or i == total:
            logger.info(
                "Domain resolution progress: %d/%d rows processed; %d with non-null website_domain",
                i,
                total,
                non_null,
            )

    return result


def attach_domains_to_borrowers(df: pd.DataFrame) -> pd.DataFrame:
    """Resolve and attach ``website_domain`` and ``domain_score`` columns."""
    config = get_config()
    logger.info("Resolving domains for %d borrowers", len(df))
    return _attach_domains_sync(df=df, config=config)


def resolve_domains(borrowers_df: pd.DataFrame, config: AppConfig) -> pd.DataFrame:
    """Backward-compatible entrypoint used by pipeline."""
    logger.info("resolve_domains called: Using DuckDuckGo search (no API key)")
    return _attach_domains_sync(df=borrowers_df, config=config)
