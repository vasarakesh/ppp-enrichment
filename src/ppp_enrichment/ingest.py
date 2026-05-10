"""Ingest PPP/EIDL CSV data and normalize borrower fields."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Iterable, Sequence
import pandas as pd

from .config import get_config
from .logging_utils import get_logger

logger = get_logger(__name__)

# Mojibake from UTF-8 decoded as Latin-1 often shows as U+00C2 / U+00C3 runes (looks like Â / Ã).
_UTF8_LATIN1_MOJIBAKE_HINT = re.compile("[\u00c2\u00c3]")

TARGET_SCHEMA = [
    "company_name",
    "address",
    "city",
    "state",
    "zip",
    "naics_code",
    "business_type",
    "loan_number",
    "initial_approval_amount",
    "current_approval_amount",
    "loan_funded_amount",
    "loan_status",
    "loan_status_date",
    "forgiveness_amount",
    "forgiveness_date",
    "jobs_reported",
    "originating_lender",
    "servicing_lender",
    "date_approved",
]

# Required logical fields (output names). Validation fails if no source column maps to any of these.
REQUIRED_SCHEMA_TARGETS: tuple[str, ...] = (
    "company_name",
    "address",
    "city",
    "state",
    "zip",
    "naics_code",
    "initial_approval_amount",
    "current_approval_amount",
    "loan_funded_amount",
    "loan_status",
    "loan_status_date",
    "forgiveness_amount",
    "forgiveness_date",
    "originating_lender",
    "servicing_lender",
)

CANONICAL_ALIASES: dict[str, list[str]] = {
    "company_name": [
        "company_name",
        "borrower_name",
        "business_name",
        "legal_business_name",
        "entity_name",
        "borrower",
        "name",
        "borrowername",
        "borrower_name_text",
        "borrower_business_name",
    ],
    "address": [
        "address",
        "street",
        "street_address",
        "borrower_address",
        "borroweraddress",
        "borrower_address_1",
        "borrower_street",
        "location_address",
    ],
    "city": ["city", "borrower_city", "borrowercity", "location_city"],
    "state": ["state", "borrower_state", "borrowerstate", "location_state"],
    "zip": ["zip", "zip_code", "zipcode", "borrower_zip", "borrowerzip", "postal_code"],
    "naics_code": [
        "naics_code",
        "naics",
        "naicscode",
        "national_industry_classification_system_code",
    ],
    "business_type": [
        "business_type",
        "entity_type",
        "business_structure",
        "businesstype",
        "business_type_description",
    ],
    "loan_number": [
        "loan_number",
        "sba_loan_number",
        "loan_id",
        "loan_num",
        "loannumber",
        "ppp_loan_number",
        "sba_ppp_loan_number",
        "paycheck_protection_program_loan_number",
    ],
    "initial_approval_amount": [
        "initial_approval_amount",
        "initialapprovalamount",
        "approval_amount",
        "original_loan_amount",
        "gross_approval",
    ],
    "current_approval_amount": [
        "current_approval_amount",
        "currentapprovalamount",
        "loan_amount",
        "approved_amount",
        "current_loan_amount",
    ],
    "loan_funded_amount": [
        "loan_funded_amount",
        "currentapprovalamount",
        "disbursement_amount",
        "funded_amount",
        "ppp_loan_amount",
        "ppploanamount",
        "paycheck_protection_loan_amount",
        "loan_funded",
    ],
    "loan_status": [
        "loan_status",
        "status",
        "loan_status_description",
        "loanstatus",
        "ppp_loan_status",
    ],
    "loan_status_date": [
        "loan_status_date",
        "status_date",
        "loanstatusdate",
        "loan_status_effective_date",
        "last_status_date",
    ],
    "forgiveness_amount": [
        "forgiveness_amount",
        "forgivenessamount",
        "forgiveness_amount_total",
        "forgiven_amount",
        "loan_forgiveness_amount",
    ],
    "forgiveness_date": [
        "forgiveness_date",
        "forgivenessdate",
        "loan_forgiveness_date",
        "forgiveness_payment_date",
    ],
    "jobs_reported": ["jobs_reported", "jobsreported", "jobs", "number_of_employees"],
    "originating_lender": [
        "originating_lender",
        "lender",
        "originatinglender",
        "originating_lender_name",
        "lender_name",
        "bank_name",
    ],
    "servicing_lender": [
        "servicing_lender",
        "servicinglendername",
        "servicing_lender_name",
        "servicinglender",
        "servicing_lender_location_name",
    ],
    "date_approved": [
        "date_approved",
        "approval_date",
        "dateapproved",
        "sba_approval_date",
        "date_approval",
    ],
}

US_STATE_TO_CODE = {
    "ALABAMA": "AL",
    "ALASKA": "AK",
    "ARIZONA": "AZ",
    "ARKANSAS": "AR",
    "CALIFORNIA": "CA",
    "COLORADO": "CO",
    "CONNECTICUT": "CT",
    "DELAWARE": "DE",
    "DISTRICT OF COLUMBIA": "DC",
    "FLORIDA": "FL",
    "GEORGIA": "GA",
    "HAWAII": "HI",
    "IDAHO": "ID",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "KANSAS": "KS",
    "KENTUCKY": "KY",
    "LOUISIANA": "LA",
    "MAINE": "ME",
    "MARYLAND": "MD",
    "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSISSIPPI": "MS",
    "MISSOURI": "MO",
    "MONTANA": "MT",
    "NEBRASKA": "NE",
    "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM",
    "NEW YORK": "NY",
    "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND",
    "OHIO": "OH",
    "OKLAHOMA": "OK",
    "OREGON": "OR",
    "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN",
    "TEXAS": "TX",
    "UTAH": "UT",
    "VERMONT": "VT",
    "VIRGINIA": "VA",
    "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI",
    "WYOMING": "WY",
}


def _normalize_column_name(column: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", column.strip().lower())
    return re.sub(r"_+", "_", normalized).strip("_")


def repair_utf8_mojibake_text(value: object, *, max_passes: int = 3) -> object:
    """If UTF-8 bytes were decoded as Latin-1, undo one or two layers of that mismatch."""
    try:
        if pd.isna(value):
            return value
    except (TypeError, ValueError):
        return value
    if not isinstance(value, str) or not value:
        return value
    if _UTF8_LATIN1_MOJIBAKE_HINT.search(value) is None:
        return value
    current = value
    for _ in range(max_passes):
        try:
            nxt = current.encode("latin-1").decode("utf-8")
        except (UnicodeDecodeError, UnicodeEncodeError):
            return current
        if nxt == current:
            break
        current = nxt
    return current


def apply_mojibake_repair_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Repair mojibake in object/string columns (only rows matching the Â/Ã-style hint)."""
    string_cols = list(df.select_dtypes(include=["object", "string"]).columns)
    if not string_cols:
        return df
    out = df.copy()
    for col in string_cols:
        series = out[col].astype("string")
        hint = series.str.contains(_UTF8_LATIN1_MOJIBAKE_HINT, na=False, regex=True)
        if not hint.any():
            continue
        fixed = series.copy()
        fixed.loc[hint] = series.loc[hint].map(repair_utf8_mojibake_text)
        out[col] = fixed.astype("string")
    return out


def allowed_ppp_source_column_names() -> frozenset[str]:
    """Normalized header names recognized by ``build_borrowers_base`` (``TARGET_SCHEMA`` only)."""
    names: set[str] = set()
    for target in TARGET_SCHEMA:
        names.update(CANONICAL_ALIASES[target])
    return frozenset(names)


def _resolve_paths(input_paths: Sequence[str | Path] | None) -> list[Path]:
    if input_paths is not None and len(input_paths) == 0:
        raise ValueError("input_paths must be a non-empty list of PPP CSV paths.")
    if input_paths:
        return [Path(path) for path in input_paths]
    config = get_config()
    return [config.input_dir / config.default_ppp_input_filename]


def _first_existing_column(df: pd.DataFrame, aliases: Iterable[str]) -> str | None:
    for alias in aliases:
        if alias in df.columns:
            return alias
    return None


def _missing_required_targets(df: pd.DataFrame) -> list[str]:
    missing: list[str] = []
    for target in REQUIRED_SCHEMA_TARGETS:
        if _first_existing_column(df, CANONICAL_ALIASES[target]) is None:
            missing.append(target)
    return missing


def missing_required_ppp_columns(df: pd.DataFrame) -> list[str]:
    """Names in ``REQUIRED_SCHEMA_TARGETS`` with no matching column in ``df``."""
    return _missing_required_targets(df)


def _clean_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.select_dtypes(include=["object", "string"]).columns:
        cleaned = df[column].astype("string").str.strip()
        df[column] = cleaned.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    return df


def _normalize_state_codes(series: pd.Series) -> pd.Series:
    states = series.astype("string").str.strip().str.upper()

    def _map_one(value: object) -> object:
        if pd.isna(value):
            return value
        text = str(value).strip()
        return US_STATE_TO_CODE.get(text, text)

    return states.map(_map_one)


def _coerce_numeric_with_logging(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        return
    raw = df[col]
    before_non_null = raw.notna().sum()
    cleaned = raw.astype("string").str.replace(r"[$,%]", "", regex=True)
    coerced = pd.to_numeric(cleaned, errors="coerce")
    after_non_null = coerced.notna().sum()
    lost = int(before_non_null - after_non_null)
    df[col] = coerced
    if lost:
        logger.warning(
            "Column %r: coerced %s values to NaN (non-null before=%s, after=%s).",
            col,
            lost,
            int(before_non_null),
            int(after_non_null),
        )


def load_ppp_csv(path: Path) -> pd.DataFrame:
    """Load a PPP-style CSV and normalize source column names to lowercase snake_case."""
    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"PPP CSV not found at path: {csv_path}")

    logger.info("Loading PPP CSV from %s", csv_path)
    last_error: Exception | None = None
    df = None
    for encoding in ("utf-8-sig", "utf-8", "latin1"):
        try:
            df = pd.read_csv(
                csv_path,
                dtype="string",
                low_memory=False,
                encoding=encoding,
                encoding_errors="replace",
            )
            logger.info("Loaded PPP CSV (encoding=%s)", encoding)
            break
        except UnicodeDecodeError as exc:
            last_error = exc
    if df is None:
        assert last_error is not None
        raise last_error
    df.columns = [_normalize_column_name(col) for col in df.columns]
    df = apply_mojibake_repair_string_columns(df)
    n_rows = len(df)
    n_cols = len(df.columns)
    logger.info(
        "Loaded PPP CSV rows=%s columns=%s (path=%s)",
        n_rows,
        n_cols,
        csv_path,
    )
    logger.debug("Normalized columns for %s: %s", csv_path, sorted(df.columns.tolist()))
    return df


def build_borrowers_base(
    input_paths: Sequence[str | Path] | None = None,
    states: Sequence[str] | None = None,
    min_loan_amount: float | None = None,
    naics_codes: Sequence[str] | None = None,
    *,
    minimum_loan_amount: float | None = None,
    output_path: str | Path | None = None,
    write_output: bool = False,
) -> pd.DataFrame:
    """Build normalized borrower rows from one or more PPP FOIA-style CSV paths.

    ``minimum_loan_amount`` is a backward-compatible alias for ``min_loan_amount``.
    If both are set, ``min_loan_amount`` wins.
    """
    effective_min_loan = min_loan_amount if min_loan_amount is not None else minimum_loan_amount

    resolved_paths = _resolve_paths(input_paths)
    logger.info("Building borrowers base from %s input file(s).", len(resolved_paths))

    frames: list[pd.DataFrame] = []
    for source_path in resolved_paths:
        source_df = load_ppp_csv(Path(source_path))
        source_df["source_file"] = Path(source_path).name
        frames.append(source_df)

    combined = pd.concat(frames, ignore_index=True)
    logger.info("Concatenated %s rows across all inputs (combined shape=%s).", len(combined), combined.shape)

    if combined.empty:
        raise ValueError("PPP ingestion produced an empty combined DataFrame; cannot build borrowers_base.")

    missing_req = _missing_required_targets(combined)
    if missing_req:
        avail = sorted(combined.columns.tolist())
        raise ValueError(
            "Required PPP columns could not be mapped from the normalized source headers after loading. "
            f"Missing required logical fields ({len(missing_req)}): {', '.join(missing_req)}. "
            "Add aliases to CANONICAL_ALIASES if the vendor renamed these columns. "
            f"Available normalized columns ({len(avail)}): {avail}"
        )

    borrowers = pd.DataFrame(index=combined.index)
    mapping_used: dict[str, str] = {}
    for target in TARGET_SCHEMA:
        source_col = _first_existing_column(combined, CANONICAL_ALIASES[target])
        if source_col is None:
            borrowers[target] = pd.NA
        else:
            borrowers[target] = combined[source_col]
            mapping_used[target] = source_col

    logger.debug("Column mapping target<-source: %s", mapping_used)

    borrowers = _clean_string_columns(borrowers)
    borrowers["state"] = _normalize_state_codes(borrowers["state"])
    borrowers["zip"] = borrowers["zip"].astype("string").str.extract(r"(\d{5})", expand=False)
    borrowers["naics_code"] = borrowers["naics_code"].astype("string").str.extract(r"(\d+)", expand=False)

    numeric_columns = [
        "initial_approval_amount",
        "current_approval_amount",
        "loan_funded_amount",
        "forgiveness_amount",
        "jobs_reported",
    ]
    for col in numeric_columns:
        _coerce_numeric_with_logging(borrowers, col)

    for col in ["loan_status_date", "forgiveness_date", "date_approved"]:
        borrowers[col] = pd.to_datetime(borrowers[col], errors="coerce")

    rows_loaded = len(borrowers)
    borrowers_filtered = borrowers

    if states:
        normalized_states = {state.strip().upper() for state in states}
        normalized_states = {US_STATE_TO_CODE.get(s, s) for s in normalized_states}
        borrowers_filtered = borrowers_filtered[borrowers_filtered["state"].isin(normalized_states)]
        logger.info("After state filter: %s rows (states=%s).", len(borrowers_filtered), sorted(normalized_states))

    if effective_min_loan is not None:
        funded = borrowers_filtered["loan_funded_amount"].fillna(0)
        borrowers_filtered = borrowers_filtered[funded >= float(effective_min_loan)]
        logger.info(
            "After min loan_funded_amount filter (>= %s): %s rows.",
            effective_min_loan,
            len(borrowers_filtered),
        )

    if naics_codes:
        keep = {str(code).strip() for code in naics_codes if str(code).strip()}
        n_series = borrowers_filtered["naics_code"].astype("string")
        borrowers_filtered = borrowers_filtered[n_series.isin(keep)]
        logger.info("After NAICS filter: %s rows (codes=%s).", len(borrowers_filtered), sorted(keep))

    borrowers_out = borrowers_filtered.reset_index(drop=True)

    n_final = len(borrowers_out)
    pct_removed = (rows_loaded - n_final) / rows_loaded * 100.0 if rows_loaded > 0 else 0.0
    logger.info(
        "Borrowers base complete: %s rows after all filters (loaded %s rows; %.2f%% removed by filters).",
        n_final,
        rows_loaded,
        pct_removed,
    )

    if write_output:
        config = get_config()
        destination = (
            Path(output_path)
            if output_path is not None
            else config.output_dir / config.borrowers_base_output_filename
        )
        destination.parent.mkdir(parents=True, exist_ok=True)
        borrowers_out.to_csv(destination, index=False)
        logger.info("Wrote borrowers base CSV to %s", destination)

    return borrowers_out


def load_and_normalize_borrowers(input_csv: Path) -> pd.DataFrame:
    """Backward-compatible wrapper used by pipeline scaffold."""
    return build_borrowers_base(input_paths=[input_csv])
