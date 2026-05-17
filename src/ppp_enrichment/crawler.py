"""Crawl company websites while respecting robots.txt policies."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import logging
import re
import time
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import httpx
import pandas as pd

from .config import AppConfig
from .logging_utils import get_logger

logger = get_logger(__name__)


def _silence_verbose_http_loggers() -> None:
    """Avoid per-request httpx/httpcore INFO lines on the console; summaries use ``logger`` only."""
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


_KEYWORD_TO_PAGE_TYPE = {
    "contact": "contact",
    "team": "team",
    "leadership": "team",
    "staff": "team",
    "about": "about",
    "company": "about",
    "our-story": "about",
}
# Link anchor text or URL path must suggest these (internal links only; see _extract_candidate_links).
_KEYWORD_PATTERN = re.compile(
    r"(about|company|our-story|contact|team|staff|leadership)",
    re.IGNORECASE,
)

# Small redirect cap per request (client-level).
_CRAWLER_MAX_REDIRECTS = 5
_NO_RETRY_STATUS_CODES = frozenset({403, 404, 410})
_RETRY_BACKOFF_SECONDS = 0.5


@dataclass
class HttpStats:
    """Aggregated HTTP outcomes for the most recent ``crawl_domains`` call."""

    requests: int = 0
    status_3xx: int = 0
    status_4xx: int = 0
    status_5xx: int = 0
    errors: int = 0
    domains_skipped_budget: int = 0

    def record_status(self, status_code: int) -> None:
        self.requests += 1
        if 300 <= status_code < 400:
            self.status_3xx += 1
        elif 400 <= status_code < 500:
            self.status_4xx += 1
        elif status_code >= 500:
            self.status_5xx += 1

    def record_error(self) -> None:
        self.requests += 1
        self.errors += 1

    def as_log_dict(self) -> dict[str, int]:
        return {
            "http_requests": self.requests,
            "http_3xx": self.status_3xx,
            "http_4xx": self.status_4xx,
            "http_5xx": self.status_5xx,
            "http_errors": self.errors,
            "domains_skipped_budget": self.domains_skipped_budget,
        }


_last_crawl_stats = HttpStats()


def get_last_crawl_http_stats() -> HttpStats:
    """Return stats from the latest ``crawl_domains`` invocation."""
    return _last_crawl_stats


def reset_crawl_http_stats() -> HttpStats:
    """Reset and return a fresh stats object (used at crawl start)."""
    global _last_crawl_stats
    _last_crawl_stats = HttpStats()
    return _last_crawl_stats


@dataclass
class _CrawlBudget:
    """Shared crawl limits: wall-clock deadline and max HTTP requests."""

    deadline: float | None = None
    max_requests: int | None = None
    stats: HttpStats = field(default_factory=HttpStats)
    max_retries: int = 1
    _stopped: bool = field(default=False, repr=False)

    def should_stop(self) -> bool:
        if self._stopped:
            return True
        if self.deadline is not None and time.time() >= self.deadline:
            self._stopped = True
            return True
        if self.max_requests is not None and self.stats.requests >= self.max_requests:
            self._stopped = True
            return True
        return False

    def mark_domain_skipped(self) -> None:
        self.stats.domains_skipped_budget += 1


@dataclass(frozen=True)
class PageContent:
    url: str
    html: str
    status_code: int
    page_type: str


class _HostRateLimiter:
    def __init__(self, per_host_concurrency: int, delay_per_host: float) -> None:
        self._per_host_concurrency = per_host_concurrency
        self._delay_per_host = max(0.0, delay_per_host)
        self._host_semaphores: dict[str, asyncio.Semaphore] = {}
        self._host_locks: dict[str, asyncio.Lock] = {}
        self._last_request_at: dict[str, float] = {}

    def _get_semaphore(self, host: str) -> asyncio.Semaphore:
        if host not in self._host_semaphores:
            self._host_semaphores[host] = asyncio.Semaphore(self._per_host_concurrency)
        return self._host_semaphores[host]

    def _get_lock(self, host: str) -> asyncio.Lock:
        if host not in self._host_locks:
            self._host_locks[host] = asyncio.Lock()
        return self._host_locks[host]

    async def wait_turn(self, host: str) -> None:
        lock = self._get_lock(host)
        async with lock:
            now = time.monotonic()
            last_seen = self._last_request_at.get(host)
            if last_seen is not None:
                wait_for = self._delay_per_host - (now - last_seen)
                if wait_for > 0:
                    await asyncio.sleep(wait_for)
            self._last_request_at[host] = time.monotonic()

    async def run(self, host: str, coro):
        sem = self._get_semaphore(host)
        async with sem:
            await self.wait_turn(host)
            return await coro


def _normalize_domain(domain: str) -> str:
    raw = (domain or "").strip()
    if not raw:
        return ""
    if "://" in raw:
        parsed = urlparse(raw)
        return (parsed.netloc or "").lower()
    return raw.lower().strip("/")


def normalize_domain_key(domain: str) -> str:
    """Normalize a domain string or homepage URL before ``crawl_domains`` / lookups."""
    return _normalize_domain(domain)


def _normalize_host(host: str) -> str:
    host_l = host.lower()
    if host_l.startswith("www."):
        return host_l[4:]
    return host_l


def _is_internal_link(base_host: str, candidate_url: str) -> bool:
    parsed = urlparse(candidate_url)
    candidate_host = parsed.netloc.lower()
    base_host_n = _normalize_host(base_host)
    candidate_host_n = _normalize_host(candidate_host)
    return candidate_host_n == base_host_n or candidate_host_n.endswith("." + base_host_n)


def _build_candidate_type(url: str, anchor_text: str) -> str:
    signal = f"{url} {anchor_text}".lower()
    for keyword, page_type in _KEYWORD_TO_PAGE_TYPE.items():
        if keyword in signal:
            return page_type
    return "other"


def _path_dedupe_key(url: str) -> str:
    """Normalize URL to a path key so /About and /about dedupe."""
    parsed = urlparse(url)
    host = _normalize_host(parsed.netloc.lower())
    path = (parsed.path or "/").rstrip("/").lower() or "/"
    return f"{host}:{path}"


def _select_extra_page_urls(
    candidates: list[tuple[str, str]],
    max_extra: int,
) -> list[tuple[str, str]]:
    """Pick up to ``max_extra`` URLs: at most one contact, one about, one team page (document order)."""
    if max_extra <= 0:
        return []

    seen_keys: set[str] = set()
    have_contact = False
    have_about = False
    have_team = False
    selected: list[tuple[str, str]] = []

    for url, page_type in candidates:
        if len(selected) >= max_extra:
            break
        if page_type == "other":
            continue
        key = _path_dedupe_key(url)
        if key in seen_keys:
            continue
        if page_type == "contact" and have_contact:
            continue
        if page_type == "about" and have_about:
            continue
        if page_type == "team" and have_team:
            continue

        seen_keys.add(key)
        selected.append((url, page_type))
        if page_type == "contact":
            have_contact = True
        elif page_type == "about":
            have_about = True
        elif page_type == "team":
            have_team = True

    return selected


def _extract_candidate_links(home_url: str, html: str) -> list[tuple[str, str]]:
    parsed_home = urlparse(home_url)
    if not parsed_home.netloc:
        return []

    soup = BeautifulSoup(html, "html.parser")
    candidates: list[tuple[str, str]] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        if not href:
            continue
        absolute = urljoin(home_url, href)
        parsed_abs = urlparse(absolute)
        if parsed_abs.scheme not in {"http", "https"}:
            continue
        if not _is_internal_link(parsed_home.netloc, absolute):
            continue

        anchor_text = anchor.get_text(" ", strip=True)
        signal = f"{anchor_text} {parsed_abs.path}".lower()
        if not _KEYWORD_PATTERN.search(signal):
            continue

        normalized = absolute.split("#", 1)[0]
        if normalized in seen:
            continue
        seen.add(normalized)
        candidates.append((normalized, _build_candidate_type(normalized, anchor_text)))

    return candidates


def _parse_robots_txt(robots_content: str, user_agent: str, path: str) -> bool:
    ua = user_agent.lower()
    lines = []
    for raw_line in robots_content.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if line:
            lines.append(line)

    groups: list[dict[str, list[str]]] = []
    current_group: dict[str, list[str]] | None = None

    for line in lines:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key_l = key.strip().lower()
        value_s = value.strip()

        if key_l == "user-agent":
            if current_group is None or current_group.get("rules_started"):
                current_group = {"agents": [], "allow": [], "disallow": [], "rules_started": []}
                groups.append(current_group)
            current_group["agents"].append(value_s.lower())
            continue

        if key_l in {"allow", "disallow"} and current_group is not None:
            current_group["rules_started"] = ["1"]
            current_group[key_l].append(value_s)

    matching_groups = [
        g for g in groups if any(agent == "*" or agent in ua for agent in g["agents"])
    ]
    if not matching_groups:
        return True

    best_group = max(matching_groups, key=lambda g: max((len(a) for a in g["agents"]), default=0))
    candidates: list[tuple[int, bool]] = []
    for allow_rule in best_group["allow"]:
        if allow_rule and path.startswith(allow_rule):
            candidates.append((len(allow_rule), True))
    for disallow_rule in best_group["disallow"]:
        if disallow_rule == "":
            continue
        if path.startswith(disallow_rule):
            candidates.append((len(disallow_rule), False))
    if not candidates:
        return True
    longest, allowed = max(candidates, key=lambda item: item[0])
    _ = longest
    return allowed


async def _request_with_limits(
    client: httpx.AsyncClient,
    limiter: _HostRateLimiter,
    url: str,
    budget: _CrawlBudget | None = None,
) -> httpx.Response | None:
    if budget is not None and budget.should_stop():
        return None

    host = urlparse(url).netloc.lower()
    max_attempts = 1 + (budget.max_retries if budget is not None else 0)

    for attempt in range(max_attempts):
        if budget is not None and budget.should_stop():
            return None
        try:
            response = await limiter.run(
                host,
                client.get(url, follow_redirects=True),
            )
        except httpx.RequestError:
            if budget is not None:
                budget.stats.record_error()
            if attempt + 1 < max_attempts:
                await asyncio.sleep(_RETRY_BACKOFF_SECONDS * (attempt + 1))
                continue
            raise

        if budget is not None:
            budget.stats.record_status(response.status_code)

        if response.status_code in _NO_RETRY_STATUS_CODES:
            return response
        if response.status_code >= 500 and attempt + 1 < max_attempts:
            await asyncio.sleep(_RETRY_BACKOFF_SECONDS * (attempt + 1))
            continue
        return response

    return None


async def is_allowed_by_robots(
    client: httpx.AsyncClient,
    limiter: _HostRateLimiter,
    domain: str,
    user_agent: str,
    target_url: str | None = None,
    budget: _CrawlBudget | None = None,
) -> bool:
    host = _normalize_domain(domain)
    if not host:
        return False
    if budget is not None and budget.should_stop():
        return False

    schemes = ["https", "http"]
    robots_content = None
    for scheme in schemes:
        robots_url = f"{scheme}://{host}/robots.txt"
        try:
            response = await _request_with_limits(client, limiter, robots_url, budget=budget)
        except httpx.RequestError:
            continue
        if response is None:
            return False
        if response.status_code >= 400:
            if response.status_code == 404:
                return True
            continue
        robots_content = response.text
        break

    if robots_content is None:
        return True

    path = "/"
    if target_url:
        parsed_target = urlparse(target_url)
        path = parsed_target.path or "/"
    return _parse_robots_txt(robots_content, user_agent=user_agent, path=path)


async def _fetch_homepage(
    client: httpx.AsyncClient,
    limiter: _HostRateLimiter,
    domain: str,
    budget: _CrawlBudget | None = None,
) -> tuple[str | None, httpx.Response | None]:
    host = _normalize_domain(domain)
    if not host:
        return None, None
    if budget is not None and budget.should_stop():
        return None, None

    for scheme in ("https", "http"):
        home_url = f"{scheme}://{host}/"
        try:
            response = await _request_with_limits(client, limiter, home_url, budget=budget)
        except httpx.RequestError:
            continue
        if response is None:
            return None, None
        return home_url, response
    return None, None


async def _crawl_single_domain(
    client: httpx.AsyncClient,
    limiter: _HostRateLimiter,
    domain: str,
    user_agent: str,
    *,
    max_pages_per_domain: int,
    budget: _CrawlBudget | None = None,
    per_domain_time_limit: float | None = None,
) -> list[PageContent]:
    fetch_ok = 0
    fetch_fail = 0
    domain_started = time.monotonic()

    def _domain_time_exceeded() -> bool:
        if per_domain_time_limit is None:
            return False
        return (time.monotonic() - domain_started) >= per_domain_time_limit

    if budget is not None and budget.should_stop():
        return []
    if _domain_time_exceeded():
        logger.info("Crawl %s: skipped (per-domain %ss limit)", domain, per_domain_time_limit)
        return []

    allowed = await is_allowed_by_robots(
        client=client,
        limiter=limiter,
        domain=domain,
        user_agent=user_agent,
        budget=budget,
    )
    if not allowed:
        logger.info(
            "Crawl %s: fetched=0 failed=0 (skipped: robots.txt disallow for home path)",
            domain,
        )
        return []

    home_url, home_response = await _fetch_homepage(client, limiter, domain, budget=budget)
    if not home_url or home_response is None:
        fetch_fail += 1
        logger.info("Crawl %s: fetched=0 failed=%d (home unreachable)", domain, fetch_fail)
        return []

    pages: list[PageContent] = [
        PageContent(
            url=str(home_response.url),
            html=home_response.text if home_response.text else "",
            status_code=home_response.status_code,
            page_type="home",
        )
    ]
    if home_response.status_code < 400:
        fetch_ok += 1
    else:
        fetch_fail += 1

    if home_response.status_code >= 400:
        logger.info("Crawl %s: fetched=%d failed=%d", domain, fetch_ok, fetch_fail)
        return pages

    max_pages = max(1, int(max_pages_per_domain))
    max_extra = max(0, max_pages - 1)
    raw_candidates = _extract_candidate_links(str(home_response.url), home_response.text or "")
    selected = _select_extra_page_urls(raw_candidates, max_extra)

    for candidate_url, page_type in selected:
        if _domain_time_exceeded():
            logger.info("Crawl %s: stopped extra pages (per-domain time limit)", domain)
            break
        if budget is not None and budget.should_stop():
            break
        try:
            allowed_candidate = await is_allowed_by_robots(
                client=client,
                limiter=limiter,
                domain=domain,
                user_agent=user_agent,
                target_url=candidate_url,
                budget=budget,
            )
            if not allowed_candidate:
                fetch_fail += 1
                logger.debug("Skipping %s due to robots rule", candidate_url)
                continue

            response = await _request_with_limits(client, limiter, candidate_url, budget=budget)
            if response is None:
                break
            pages.append(
                PageContent(
                    url=str(response.url),
                    html=response.text if response.text else "",
                    status_code=response.status_code,
                    page_type=page_type,
                )
            )
            if response.status_code < 400:
                fetch_ok += 1
            else:
                fetch_fail += 1
        except httpx.RequestError:
            fetch_fail += 1
            logger.debug("Fetch failed for candidate url=%s", candidate_url, exc_info=True)
            continue

    logger.info("Crawl %s: fetched=%d failed=%d", domain, fetch_ok, fetch_fail)
    return pages


async def _crawl_domains_async(
    domains: list[str],
    concurrency: int,
    per_host_concurrency: int,
    request_timeout: float,
    user_agent: str,
    delay_per_host: float,
    *,
    max_pages_per_domain: int,
    deadline: float | None = None,
    max_requests: int | None = None,
    max_retries: int = 1,
    budget: _CrawlBudget | None = None,
    per_domain_time_limit: float | None = None,
) -> dict[str, list[PageContent]]:
    timeout = httpx.Timeout(request_timeout)
    limiter = _HostRateLimiter(
        per_host_concurrency=per_host_concurrency,
        delay_per_host=delay_per_host,
    )
    headers = {"User-Agent": user_agent}
    results: dict[str, list[PageContent]] = {}
    semaphore = asyncio.Semaphore(max(1, concurrency))
    if budget is None:
        budget = _CrawlBudget(
            deadline=deadline,
            max_requests=max_requests,
            stats=reset_crawl_http_stats(),
            max_retries=max_retries,
        )
    else:
        budget.deadline = deadline if deadline is not None else budget.deadline
        budget.max_requests = max_requests if max_requests is not None else budget.max_requests
        budget.max_retries = max_retries

    async with httpx.AsyncClient(
        timeout=timeout,
        headers=headers,
        max_redirects=_CRAWLER_MAX_REDIRECTS,
    ) as client:
        async def run_one(domain: str) -> None:
            if budget.should_stop():
                budget.mark_domain_skipped()
                results[domain] = []
                return
            async with semaphore:
                if budget.should_stop():
                    budget.mark_domain_skipped()
                    results[domain] = []
                    return
                try:
                    results[domain] = await _crawl_single_domain(
                        client=client,
                        limiter=limiter,
                        domain=domain,
                        user_agent=user_agent,
                        max_pages_per_domain=max_pages_per_domain,
                        budget=budget,
                        per_domain_time_limit=per_domain_time_limit,
                    )
                except Exception:
                    logger.exception("Unexpected crawl failure for domain=%s", domain)
                    logger.info("Crawl %s: fetched=0 failed=1 (unexpected error)", domain)
                    results[domain] = []

        await asyncio.gather(*(run_one(domain) for domain in domains))

    global _last_crawl_stats
    _last_crawl_stats = budget.stats
    return results


def crawl_domains(
    domains: list[str],
    *,
    deadline: float | None = None,
    max_requests: int | None = None,
) -> dict[str, list[PageContent]]:
    if not domains:
        reset_crawl_http_stats()
        return {}
    from .config import get_config

    _silence_verbose_http_loggers()
    cfg = get_config()
    req_cap = max_requests if max_requests is not None else cfg.crawler_max_requests_per_run
    return asyncio.run(
        _crawl_domains_async(
            domains=domains,
            concurrency=cfg.crawler_concurrency,
            per_host_concurrency=cfg.crawler_per_host_concurrency,
            request_timeout=cfg.crawler_request_timeout,
            user_agent=cfg.crawler_user_agent,
            delay_per_host=cfg.crawler_delay_per_host,
            max_pages_per_domain=cfg.crawler_max_pages_per_domain,
            deadline=deadline,
            max_requests=req_cap,
            max_retries=cfg.crawler_max_retries,
            per_domain_time_limit=float(cfg.crawler_per_domain_time_limit_seconds),
        )
    )


def crawl_company_pages(domains_df: pd.DataFrame, config: AppConfig) -> pd.DataFrame:
    """Crawl domains from dataframe and attach page snapshots in-memory."""
    _silence_verbose_http_loggers()
    logger.info("Crawling websites for %d domain records", len(domains_df))
    output = domains_df.copy()
    domains = (
        output.get("website_domain", pd.Series(dtype=str))
        .dropna()
        .astype(str)
        .map(str.strip)
        .loc[lambda series: series.ne("")]
        .tolist()
    )
    crawl_map = asyncio.run(
        _crawl_domains_async(
            domains=domains,
            concurrency=config.crawler_concurrency,
            per_host_concurrency=config.crawler_per_host_concurrency,
            request_timeout=config.crawler_request_timeout,
            user_agent=config.crawler_user_agent,
            delay_per_host=config.crawler_delay_per_host,
            max_pages_per_domain=config.crawler_max_pages_per_domain,
            per_domain_time_limit=float(config.crawler_per_domain_time_limit_seconds),
        )
    )
    output["crawled_pages"] = output.get("website_domain", "").map(
        lambda domain: crawl_map.get(str(domain).strip(), [])
    )
    return output
