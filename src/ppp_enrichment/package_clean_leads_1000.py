"""Package clean_leads_*.csv into deduped, filtered packs of exactly 1000 rows."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

from . import config


PACK_SIZE = 1000
PACK_DIR_NAME = "Clean Leads 1000"
PACK_FILE_PREFIX = "Clean Leads 1000"
CLEAN_COLUMNS = [
    "First Name",
    "Second Name",
    "Email Address",
    "Phone Number",
    "Company Name",
    "Company URL",
]
# Case-insensitive phrase / word denylist checked against all lead text fields.
_PROHIBITED_PHRASES = (
    "investments",
    "consulting",
    "law",
    "sales",
    "customer service",
    "real estate",
    "advisors",
    "school finance",
    "financial ministries",
    "inverse church orders",
    "visit appointments",
    "god wealth inquiries",
    "legal",
    "government",
    "admission",
)


def _phrase_pattern(phrase: str) -> re.Pattern[str]:
    parts = [re.escape(p) for p in phrase.split()]
    body = r"\s+".join(parts)
    return re.compile(rf"(?<!\w){body}(?!\w)", re.IGNORECASE)


_PROHIBITED_RES = tuple(_phrase_pattern(p) for p in _PROHIBITED_PHRASES)


def pack_dir() -> Path:
    return config.OUTPUT_DIR / PACK_DIR_NAME


def _normalize_text(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _row_blob(row: pd.Series) -> str:
    return " ".join(_normalize_text(row.get(col, "")) for col in CLEAN_COLUMNS)


def contains_prohibited_words(text: str) -> bool:
    if not text:
        return False
    return any(pat.search(text) for pat in _PROHIBITED_RES)


def _ensure_clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in CLEAN_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    return out[CLEAN_COLUMNS]


def _load_clean_csvs(paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in paths:
        try:
            frame = pd.read_csv(path, dtype=str, keep_default_na=False)
        except Exception as exc:  # noqa: BLE001 — skip corrupt sources, keep packing
            print(f"[WARN] Skipping unreadable file {path.name}: {exc}")
            continue
        frames.append(_ensure_clean_columns(frame))
    if not frames:
        return pd.DataFrame(columns=CLEAN_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def list_source_clean_leads(output_dir: Path | None = None) -> list[Path]:
    root = output_dir or config.OUTPUT_DIR
    return sorted(root.glob("clean_leads_*.csv"))


def list_existing_packs(directory: Path | None = None) -> list[Path]:
    root = directory or pack_dir()
    if not root.is_dir():
        return []
    pattern = re.compile(rf"^{re.escape(PACK_FILE_PREFIX)} (\d+)\.csv$", re.IGNORECASE)
    found: list[tuple[int, Path]] = []
    for path in root.glob(f"{PACK_FILE_PREFIX} *.csv"):
        match = pattern.match(path.name)
        if match:
            found.append((int(match.group(1)), path))
    found.sort(key=lambda item: item[0])
    return [path for _, path in found]


def next_pack_id(directory: Path | None = None) -> int:
    existing = list_existing_packs(directory)
    if not existing:
        return 1
    pattern = re.compile(rf"^{re.escape(PACK_FILE_PREFIX)} (\d+)\.csv$", re.IGNORECASE)
    max_id = 0
    for path in existing:
        match = pattern.match(path.name)
        if match:
            max_id = max(max_id, int(match.group(1)))
    return max_id + 1


def _email_key(value: object) -> str:
    return _normalize_text(value).lower()


def _phone_key(value: object) -> str:
    return re.sub(r"\D+", "", _normalize_text(value))


def _drop_duplicate_contact_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Keep first row per non-empty email and per non-empty phone (normalized)."""
    working = df.copy()
    working["_email_key"] = working["Email Address"].map(_email_key)
    working["_phone_key"] = working["Phone Number"].map(_phone_key)

    email_filled = working["_email_key"].ne("")

    # Same email → one row (blank emails are not treated as duplicates of each other).
    email_deduped = pd.concat(
        [
            working.loc[email_filled].drop_duplicates(subset=["_email_key"], keep="first"),
            working.loc[~email_filled],
        ],
        ignore_index=False,
    ).sort_index()

    # Same phone → one row (blank phones are not treated as duplicates of each other).
    phone_filled = email_deduped["_phone_key"].ne("")
    phone_deduped = pd.concat(
        [
            email_deduped.loc[phone_filled].drop_duplicates(
                subset=["_phone_key"], keep="first"
            ),
            email_deduped.loc[~phone_filled],
        ],
        ignore_index=False,
    ).sort_index()

    return phone_deduped.drop(columns=["_email_key", "_phone_key"]).reset_index(drop=True)


def filter_and_dedupe(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Drop prohibited-word rows and duplicates by email or phone number."""
    working = _ensure_clean_columns(df)
    before = len(working)

    for col in CLEAN_COLUMNS:
        working[col] = working[col].map(_normalize_text)

    prohibited_mask = working.apply(lambda row: contains_prohibited_words(_row_blob(row)), axis=1)
    prohibited_n = int(prohibited_mask.sum())
    working = working.loc[~prohibited_mask].copy()

    # Empty contact rows are not useful as leads.
    empty_mask = (
        (working["Email Address"] == "")
        & (working["Phone Number"] == "")
        & (working["Company Name"] == "")
    )
    empty_n = int(empty_mask.sum())
    working = working.loc[~empty_mask].copy()

    before_dedupe = len(working)
    working = _drop_duplicate_contact_keys(working)
    deduped_n = before_dedupe - len(working)

    stats = {
        "input_rows": before,
        "prohibited_removed": prohibited_n,
        "empty_removed": empty_n,
        "duplicates_removed": deduped_n,
        "eligible_rows": len(working),
    }
    return working.reset_index(drop=True), stats


def exclude_already_packaged(
    candidates: pd.DataFrame, packaged: pd.DataFrame
) -> pd.DataFrame:
    """Skip candidates whose email or phone already appears in packaged leads."""
    if packaged.empty or candidates.empty:
        return candidates.reset_index(drop=True)

    packaged_emails = {
        key
        for key in (_email_key(v) for v in packaged["Email Address"])
        if key
    }
    packaged_phones = {
        key
        for key in (_phone_key(v) for v in packaged["Phone Number"])
        if key
    }

    def _is_new(row: pd.Series) -> bool:
        email = _email_key(row.get("Email Address", ""))
        phone = _phone_key(row.get("Phone Number", ""))
        if email and email in packaged_emails:
            return False
        if phone and phone in packaged_phones:
            return False
        return True

    keep_mask = [_is_new(row) for _, row in candidates.iterrows()]
    return candidates.loc[keep_mask].reset_index(drop=True)


def write_packs(
    leads: pd.DataFrame,
    *,
    directory: Path | None = None,
    start_id: int | None = None,
    pack_size: int = PACK_SIZE,
) -> list[Path]:
    """Write complete packs of ``pack_size`` only; leftover rows are not written."""
    root = directory or pack_dir()
    root.mkdir(parents=True, exist_ok=True)
    pack_id = start_id if start_id is not None else next_pack_id(root)
    written: list[Path] = []

    total = len(leads)
    complete = (total // pack_size) * pack_size
    if complete == 0:
        return written

    for offset in range(0, complete, pack_size):
        chunk = leads.iloc[offset : offset + pack_size]
        path = root / f"{PACK_FILE_PREFIX} {pack_id}.csv"
        chunk.to_csv(path, index=False, encoding=config.CSV_WRITE_ENCODING)
        written.append(path)
        pack_id += 1
    return written


def package_clean_leads_1000(
    *,
    output_dir: Path | None = None,
    rebuild: bool = False,
    pack_size: int = PACK_SIZE,
) -> dict[str, object]:
    """Build Clean Leads 1000 packs from ``data/output/clean_leads_*.csv``.

    By default, only leads not already present in existing packs are written.
    With ``rebuild=True``, existing packs are deleted and rebuilt from scratch.
    """
    root = output_dir or config.OUTPUT_DIR
    dest = root / PACK_DIR_NAME
    sources = list_source_clean_leads(root)
    source_df = _load_clean_csvs(sources)
    filtered, filter_stats = filter_and_dedupe(source_df)

    if rebuild:
        for path in list_existing_packs(dest):
            path.unlink(missing_ok=True)
        candidates = filtered
        already_n = 0
        start_id = 1
    else:
        existing_paths = list_existing_packs(dest)
        packaged_df = _load_clean_csvs(existing_paths)
        already_n = len(packaged_df)
        before_exclude = len(filtered)
        candidates = exclude_already_packaged(filtered, packaged_df)
        already_n = before_exclude - len(candidates)
        start_id = next_pack_id(dest)

    written = write_packs(
        candidates, directory=dest, start_id=start_id, pack_size=pack_size
    )
    remainder = len(candidates) - (len(written) * pack_size)

    result: dict[str, object] = {
        "source_files": len(sources),
        **filter_stats,
        "already_packaged_skipped": already_n,
        "new_eligible": len(candidates),
        "packs_written": len(written),
        "pack_paths": [str(p) for p in written],
        "remainder_under_1000": remainder,
        "pack_dir": str(dest),
        "rebuild": rebuild,
    }
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Dedupe + prohibited-word filter clean_leads_*.csv, then write "
            f"exactly-{PACK_SIZE}-row packs under data/output/{PACK_DIR_NAME}/."
        )
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Delete existing packs and rebuild from all current clean_leads files.",
    )
    parser.add_argument(
        "--pack-size",
        type=int,
        default=PACK_SIZE,
        help=f"Rows per pack (default {PACK_SIZE}).",
    )
    args = parser.parse_args(argv)

    result = package_clean_leads_1000(rebuild=args.rebuild, pack_size=args.pack_size)

    print("=== Clean Leads 1000 packaging ===")
    print(f"Source files: {result['source_files']}")
    print(f"Input rows: {result['input_rows']}")
    print(f"Prohibited removed: {result['prohibited_removed']}")
    print(f"Empty removed: {result['empty_removed']}")
    print(f"Duplicates removed: {result['duplicates_removed']}")
    print(f"Eligible after filters: {result['eligible_rows']}")
    print(f"Already packaged skipped: {result['already_packaged_skipped']}")
    print(f"New eligible: {result['new_eligible']}")
    print(f"Packs written: {result['packs_written']}")
    print(f"Remainder (<{args.pack_size}): {result['remainder_under_1000']}")
    print(f"Output dir: {result['pack_dir']}")
    for path in result["pack_paths"]:
        print(f"  wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
