"""Export cleaned enriched leads into strict and relaxed tier CSV files."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from . import config
from .domains import is_gov_or_edu_domain


_CHUNK_SIZE = 500

_TLD_COMMON = frozenset(
    {
        "com",
        "net",
        "org",
        "edu",
        "gov",
        "mil",
        "int",
        "co",
        "io",
        "ai",
        "app",
        "us",
        "uk",
        "ca",
        "au",
        "de",
        "fr",
        "info",
        "biz",
        "me",
        "tv",
        "cc",
        "ly",
        "to",
        "ws",
        "site",
        "online",
        "store",
        "shop",
    }
)

_EMAIL_BLOCKED_SUBSTRINGS = (
    "idealist",
    "nippo",
    "example",
    "dummy",
    "noreply",
    "no-reply",
    "donotreply",
)

_EMAIL_BLOCKED_WORD_RE = re.compile(r"\btest\b", re.IGNORECASE)


_CLEAN_COLS = [
    ("First Name", "owner_first_name"),
    ("Second Name", "owner_last_name"),
    ("Email Address", "email"),
    ("Phone Number", "phone"),
    ("Company Name", "company_name"),
    ("Company URL", "website_domain"),
]


def _format_phone_1_aaa_bbb_cccc(value: str) -> str:
    """Normalize arbitrary phone string to 1-AAA-BBB-CCCC or '' if invalid."""
    if value is None:
        return ""
    digits = re.sub(r"\D+", "", str(value))
    if _is_fake_phone_digits(digits):
        return ""
    if len(digits) == 11 and digits.startswith("1"):
        pass
    elif len(digits) == 10:
        digits = "1" + digits
    else:
        return ""
    area = digits[1:4]
    prefix = digits[4:7]
    line = digits[7:11]
    return f"1-{area}-{prefix}-{line}"


def _is_missing_phone_value(value: object) -> bool:
    if pd.isna(value):
        return True
    text = str(value).strip().lower()
    return text in {"", "na", "n/a", "nan", "none", "null", "<na>"}


def _is_unusable_email(value: object) -> bool:
    if pd.isna(value):
        return True
    email = str(value).strip().lower()
    if not email or "@" not in email:
        return True
    local_part, domain = email.rsplit("@", 1)
    if not local_part or not domain or "." not in domain:
        return True
    if domain == "sentry-next.wixpress.com":
        return True
    if domain.endswith(".wixpress.com") and re.fullmatch(r"[0-9a-f]{32}", local_part):
        return True
    return False


def _host_tokens(website_domain: object) -> set[str]:
    host = str(website_domain or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if not host:
        return set()
    parts = re.split(r"[.\-_]+", host)
    return {
        p
        for p in parts
        if len(p) >= 3 and p not in _TLD_COMMON
    }


def _email_tokens(email: object) -> set[str]:
    if pd.isna(email):
        return set()
    text = str(email).strip().lower()
    if "@" not in text:
        return set()
    local_part, domain = text.rsplit("@", 1)
    tokens: set[str] = set()
    for piece in re.split(r"[.\-_]+", local_part):
        if len(piece) >= 3:
            tokens.add(piece)
    labels = domain.split(".")
    for part in labels[:-1]:
        if len(part) >= 3 and part not in _TLD_COMMON:
            tokens.add(part)
    return tokens


def _normalize_host(host: object) -> str:
    normalized = str(host or "").strip().lower()
    if normalized.startswith("www."):
        return normalized[4:]
    return normalized


def _email_domain_matches_website(email: object, website_domain: object) -> bool:
    """Email @-domain must equal accepted website_domain (RULE 4 — not token overlap)."""
    if pd.isna(email) or pd.isna(website_domain):
        return False
    text = str(email).strip().lower()
    host = _normalize_host(website_domain)
    if not text or "@" not in text or not host:
        return False
    email_host = _normalize_host(text.rsplit("@", 1)[1])
    return email_host == host


def _email_contains_blocked_word(value: object) -> bool:
    if pd.isna(value):
        return False
    lowered = str(value).strip().lower()
    for needle in _EMAIL_BLOCKED_SUBSTRINGS:
        if needle in lowered:
            return True
    return bool(_EMAIL_BLOCKED_WORD_RE.search(lowered))


def _website_domain_passes_filters(website_domain: object) -> bool:
    if pd.isna(website_domain):
        return False
    host = str(website_domain).strip()
    if not host:
        return False
    return not is_gov_or_edu_domain(host)


def _email_passes_clean_filters(email: object, website_domain: object) -> bool:
    if _is_unusable_email(email):
        return False
    if _email_contains_blocked_word(email):
        return False
    if not _website_domain_passes_filters(website_domain):
        return False
    if not pd.isna(email):
        text = str(email).strip().lower()
        if "@" in text and is_gov_or_edu_domain(text.rsplit("@", 1)[1]):
            return False
    if not _email_domain_matches_website(email, website_domain):
        return False
    return True


def _nanp_second_digit_invalid(digits: str) -> bool:
    """US NANP: after leading 1, first area-code digit must not be 0 or 1."""
    raw = re.sub(r"\D+", "", str(digits or ""))
    if len(raw) == 10:
        raw = "1" + raw
    if len(raw) != 11 or not raw.startswith("1"):
        return False
    return raw[1] in "01"


def _is_fake_phone_digits(digits: str) -> bool:
    raw = str(digits or "")
    if _nanp_second_digit_invalid(raw):
        return True
    if len(raw) == 11 and raw.startswith("1"):
        core = raw[1:]
    elif len(raw) == 10:
        core = raw
    else:
        return False
    if len(set(core)) == 1:
        return True
    return core in {"1234567890", "9876543210"}


def main(*, enriched_path: Path | None = None) -> None:
    src = enriched_path if enriched_path is not None else config.ENRICHED_SAMPLE_PATH
    enriched_path_resolved = Path(src)
    if not enriched_path_resolved.exists():
        raise FileNotFoundError(f"Missing enriched CSV: {enriched_path_resolved}")

    df = pd.read_csv(enriched_path_resolved)
    n_loaded = len(df)

    df["email"] = df["email"].astype("string").str.replace("mailto:", "", regex=False).str.strip()
    df["website_domain"] = df["website_domain"].astype("string").str.strip()
    df["phone"] = df["phone"].astype("string").str.strip()

    email_ok = df.apply(
        lambda row: _email_passes_clean_filters(row["email"], row["website_domain"]),
        axis=1,
    )
    phone_digits = df["phone"].apply(lambda x: re.sub(r"\D+", "", str(x)))
    phone_nonempty = df["phone"].ne("")
    phone_len_ok = phone_digits.str.len().between(10, 11)
    phone_strict_ok = phone_nonempty & phone_len_ok
    phone_relaxed_ok = email_ok

    mask_tier1 = email_ok & phone_strict_ok
    df_tier1 = df.loc[mask_tier1].copy()

    phone_digits_t1 = phone_digits.loc[df_tier1.index]
    fake_phone_mask_t1 = phone_digits_t1.apply(_is_fake_phone_digits)
    dropped_fake_phone_t1 = int(fake_phone_mask_t1.sum())
    df_tier1 = df_tier1.loc[~fake_phone_mask_t1].copy()

    out_tier1 = pd.DataFrame()
    for display, internal in _CLEAN_COLS:
        if internal == "phone":
            continue
        series = df_tier1[internal].astype("string").str.strip()
        if display == "Email Address":
            series = series.str.lower().str.strip()
        elif display in ("First Name", "Second Name"):
            series = series.str.title()
        out_tier1[display] = series
    out_tier1["Phone Number"] = df_tier1["phone"].astype("string").str.strip().values

    before_phone_filter_t1 = len(out_tier1)
    missing_phone_mask_t1 = out_tier1["Phone Number"].apply(_is_missing_phone_value)
    out_tier1 = out_tier1.loc[~missing_phone_mask_t1].copy()
    dropped_missing_phone_t1 = before_phone_filter_t1 - len(out_tier1)

    before_dedup_t1 = len(out_tier1)
    out_tier1 = out_tier1.drop_duplicates(
        subset=["Company Name", "Email Address", "Phone Number"],
        keep="first",
    )
    dropped_duplicates_t1 = before_dedup_t1 - len(out_tier1)

    mask_base_relaxed = phone_relaxed_ok
    df_relaxed = df.loc[mask_base_relaxed].copy()

    key_t1 = set(
        zip(
            out_tier1["Company Name"].astype("string"),
            out_tier1["Email Address"].astype("string"),
        )
    )

    def _in_tier1(row: pd.Series) -> bool:
        return (str(row["company_name"]), str(row["email"]).lower().strip()) in key_t1

    mask_not_in_tier1 = ~df_relaxed.apply(_in_tier1, axis=1)
    df_tier2 = df_relaxed.loc[mask_not_in_tier1].copy()

    out_tier2 = pd.DataFrame()
    for display, internal in _CLEAN_COLS:
        if internal == "phone":
            continue
        series = df_tier2[internal].astype("string").str.strip()
        if display == "Email Address":
            series = series.str.lower().str.strip()
        elif display in ("First Name", "Second Name"):
            series = series.str.title()
        out_tier2[display] = series

    phone_digits_t2 = df_tier2["phone"].astype("string").apply(
        lambda x: re.sub(r"\D+", "", str(x))
    )
    fake_phone_mask_t2 = phone_digits_t2.apply(_is_fake_phone_digits)
    dropped_fake_phone_t2 = int(fake_phone_mask_t2.sum())
    out_tier2["Phone Number"] = df_tier2["phone"].astype("string").str.strip().values

    out_tier2 = out_tier2.loc[~fake_phone_mask_t2].copy()

    before_phone_filter_t2 = len(out_tier2)
    missing_phone_mask_t2 = out_tier2["Phone Number"].apply(_is_missing_phone_value)
    out_tier2 = out_tier2.loc[~missing_phone_mask_t2].copy()
    dropped_missing_phone_t2 = before_phone_filter_t2 - len(out_tier2)

    before_dedup_t2 = len(out_tier2)
    out_tier2 = out_tier2.drop_duplicates(
        subset=["Company Name", "Email Address", "Phone Number"],
        keep="first",
    )
    dropped_duplicates_t2 = before_dedup_t2 - len(out_tier2)

    config.CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    n_t1 = len(out_tier1)
    n_t2 = len(out_tier2)

    paths_written: list[Path] = []
    if n_t1 > 0:
        path_t1 = config.CLEAN_DIR / f"Vaishnavi_Tier1_Strict_{n_t1}.csv"
        out_tier1.to_csv(path_t1, index=False)
        paths_written.append(path_t1)

    if n_t2 > 0:
        path_t2 = config.CLEAN_DIR / f"Vaishnavi_Tier2_Relaxed_{n_t2}.csv"
        out_tier2.to_csv(path_t2, index=False)
        paths_written.append(path_t2)

    dropped_missing_phone_total = dropped_missing_phone_t1 + dropped_missing_phone_t2
    dropped_duplicates_total = dropped_duplicates_t1 + dropped_duplicates_t2
    dropped_fake_phone_total = dropped_fake_phone_t1 + dropped_fake_phone_t2

    print(f"Total enriched rows loaded: {n_loaded}")
    print(f"Rows dropped due to missing phone: {dropped_missing_phone_total}")
    print(f"Rows dropped due to fake phone: {dropped_fake_phone_total}")
    print(f"Duplicate rows removed: {dropped_duplicates_total}")
    print(f"Tier1 strict leads (phone mandatory): {n_t1}")
    print(f"Tier2 relaxed leads (email+domain, phone optional): {n_t2}")
    print(f"Number of output files written: {len(paths_written)}")
    for p in paths_written:
        print(p)


if __name__ == "__main__":
    main()
