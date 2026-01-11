"""Google Colab PRO validator for NiFi-generated airline delay streaming data.

What it does
- Loads ORIGINAL benchmark CSV (Airline_Delay_Cause.csv)
- Loads GENERATED NDJSON/TXT sample (Kafka/ClickHouse export)
- Runs Power BI-oriented data quality checks (schema, types, nulls, non-negativity)
- Validates key constraints (arr_del15 <= arr_flights, delay components sum to arr_delay)
- Compares KPIs vs original overall + by month (seasonality)
- Produces clear plots (monthly curves + cause shares)

How to use in Colab
1) Upload this script into Colab (or paste it into a cell)
2) Run it, choose upload option
3) Upload ORIGINAL CSV first, then GENERATED NDJSON

Notes
- This validator assumes each record is an aggregated bucket (airport+carrier+month),
  so *_ct fields are validated against arr_del15 (not against small per-flight thresholds).
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd


# ---------- Optional Colab helpers ----------
try:
    from google.colab import files  # type: ignore

    IN_COLAB = True
except Exception:
    files = None
    IN_COLAB = False


# ---------- Configuration ----------
REQUIRED_COLUMNS = [
    "year",
    "month",
    "carrier",
    "carrier_name",
    "airport",
    "airport_name",
    "arr_flights",
    "arr_del15",
    "arr_cancelled",
    "arr_diverted",
    "arr_delay",
    "carrier_ct",
    "weather_ct",
    "nas_ct",
    "security_ct",
    "late_aircraft_ct",
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay",
]

CAUSE_CT_COLS = ["carrier_ct", "weather_ct", "nas_ct", "security_ct", "late_aircraft_ct"]
CAUSE_DELAY_COLS = [
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay",
]

# Power BI safety thresholds (tunable)
MAX_ALLOWED_ARR_DELAY = 100_000  # minutes (per aggregated record) - guardrail
MAX_ALLOWED_ARR_FLIGHTS = 5_000_000

# KPI comparison tolerances (generated vs original)
TOL_DELAY_RATE_ABS = 0.02  # +/- 2 percentage points
TOL_CANCEL_RATE_ABS = 0.01
TOL_DIVERT_RATE_ABS = 0.005
TOL_AVG_DELAY_PER_FLIGHT_ABS_MIN = 2.0

# Power BI outputs
WRITE_CANONICALIZED_GENERATED = True
CANONICALIZED_GENERATED_CSV = "generated_powerbi.csv"
CANONICALIZED_GENERATED_NDJSON = "generated_powerbi.ndjson"


@dataclass
class CheckResult:
    ok: bool
    errors: List[str]
    warnings: List[str]


def valid_record_mask(df: pd.DataFrame) -> pd.Series:
    """Records that match the pipeline's 'sane' constraints.

    This mirrors what we typically want before loading into ClickHouse/Power BI.
    """

    flights = df["arr_flights"].fillna(0)
    del15 = df["arr_del15"].fillna(0)
    delay = df["arr_delay"].fillna(0)

    mask = flights > 0
    mask &= del15 <= flights
    mask &= delay >= 0
    mask &= delay < MAX_ALLOWED_ARR_DELAY

    # Ensure component delays are non-negative and not NaN for the mask
    for c in CAUSE_DELAY_COLS:
        mask &= df[c].fillna(0) >= 0

    # Cause counts are aggregated; must be within delayed flights
    for c in CAUSE_CT_COLS:
        mask &= df[c].fillna(0) >= 0
        mask &= df[c].fillna(0) <= del15

    return mask


def clean_reference_original(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """Clean the ORIGINAL CSV just enough to be usable as a benchmark.

    We do not want the validation to fail because the benchmark file contains nulls
    (common in public datasets) or tiny negative artifacts. We still report what
    we changed.
    """

    notes: List[str] = []
    out = df.copy()

    numeric_cols = [
        "arr_flights",
        "arr_del15",
        "arr_cancelled",
        "arr_diverted",
        "arr_delay",
    ] + CAUSE_CT_COLS + CAUSE_DELAY_COLS

    # Fill NaNs with 0 for numeric metrics (names/codes stay as-is)
    na_before = int(out[numeric_cols].isna().sum().sum())
    out[numeric_cols] = out[numeric_cols].fillna(0)
    if na_before > 0:
        notes.append(f"Filled {na_before} numeric nulls with 0 in ORIGINAL")

    # Clip tiny negatives to 0 (sometimes appear due to rounding/import issues)
    neg_cells = 0
    for c in numeric_cols:
        neg_mask = out[c] < 0
        count = int(neg_mask.sum())
        if count:
            neg_cells += count
            out.loc[neg_mask, c] = 0
    if neg_cells:
        notes.append(f"Clipped {neg_cells} negative numeric values to 0 in ORIGINAL")

    return out, notes


def filter_original_years(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """Optionally filter original reference to a year window.

    Reason: generated data is for a single (synthetic) year, while the original
    spans many years. Seasonality can drift across decades.
    """

    notes: List[str] = []
    if "year" not in df.columns:
        return df, notes

    years = sorted({int(y) for y in df["year"].dropna().unique().tolist()})
    if not years:
        return df, notes

    print("\n🗓️ Reference years for ORIGINAL:")
    print("1) Use ALL years")
    print("2) Use LAST 5 years")
    print("3) Use a custom range")
    choice = input("Enter choice (1/2/3): ").strip() or "1"

    if choice == "2":
        max_year = max(years)
        min_year = max_year - 4
        out = df[(df["year"] >= min_year) & (df["year"] <= max_year)].copy()
        notes.append(f"Filtered ORIGINAL to last 5 years: {min_year}-{max_year}")
        return out, notes

    if choice == "3":
        rng = input(f"Enter range like {min(years)}-{max(years)}: ").strip()
        try:
            a, b = rng.split("-")
            a_i = int(a.strip())
            b_i = int(b.strip())
            lo, hi = (a_i, b_i) if a_i <= b_i else (b_i, a_i)
            out = df[(df["year"] >= lo) & (df["year"] <= hi)].copy()
            notes.append(f"Filtered ORIGINAL to year range: {lo}-{hi}")
            return out, notes
        except Exception:
            notes.append("Invalid range input; using ALL years")

    return df, notes


def build_canonical_dimensions(original_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build canonical dimension tables to avoid name fragmentation in Power BI.

    Returns:
    - dim_carrier: columns [carrier, carrier_name]
    - dim_airport: columns [airport, airport_name]
    """

    # Carrier canonical name = most frequent non-null name for that carrier code.
    carrier_pairs = original_df[["carrier", "carrier_name"]].dropna().astype("string")
    carrier_pairs = carrier_pairs[carrier_pairs["carrier"] != ""]
    carrier_pairs = carrier_pairs[carrier_pairs["carrier_name"] != ""]
    carrier_counts = (
        carrier_pairs.groupby(["carrier", "carrier_name"], as_index=False)
        .size()
        .sort_values(["carrier", "size"], ascending=[True, False])
    )
    dim_carrier = carrier_counts.drop_duplicates(subset=["carrier"], keep="first")[
        ["carrier", "carrier_name"]
    ].reset_index(drop=True)

    # Airport canonical name = most frequent non-null name for that airport code.
    airport_pairs = original_df[["airport", "airport_name"]].dropna().astype("string")
    airport_pairs = airport_pairs[airport_pairs["airport"] != ""]
    airport_pairs = airport_pairs[airport_pairs["airport_name"] != ""]
    airport_counts = (
        airport_pairs.groupby(["airport", "airport_name"], as_index=False)
        .size()
        .sort_values(["airport", "size"], ascending=[True, False])
    )
    dim_airport = airport_counts.drop_duplicates(subset=["airport"], keep="first")[
        ["airport", "airport_name"]
    ].reset_index(drop=True)

    return dim_carrier, dim_airport


def name_consistency_checks(
    generated_df: pd.DataFrame,
    dim_carrier: pd.DataFrame,
    dim_airport: pd.DataFrame,
) -> CheckResult:
    """Warn if generated names disagree with canonical names.

    This is the most common source of Power BI "split" categories.
    """

    warnings: List[str] = []
    errors: List[str] = []

    try:
        carrier_map = dict(zip(dim_carrier["carrier"].astype(str), dim_carrier["carrier_name"].astype(str)))
        airport_map = dict(zip(dim_airport["airport"].astype(str), dim_airport["airport_name"].astype(str)))

        g = generated_df.copy()
        g["carrier"] = g["carrier"].astype("string")
        g["carrier_name"] = g["carrier_name"].astype("string")
        g["airport"] = g["airport"].astype("string")
        g["airport_name"] = g["airport_name"].astype("string")

        g["carrier_name_expected"] = g["carrier"].astype(str).map(carrier_map)
        g["airport_name_expected"] = g["airport"].astype(str).map(airport_map)

        carrier_mismatch = g.dropna(subset=["carrier_name_expected", "carrier_name"])
        carrier_mismatch = carrier_mismatch[carrier_mismatch["carrier_name"] != carrier_mismatch["carrier_name_expected"]]

        airport_mismatch = g.dropna(subset=["airport_name_expected", "airport_name"])
        airport_mismatch = airport_mismatch[airport_mismatch["airport_name"] != airport_mismatch["airport_name_expected"]]

        if len(carrier_mismatch) > 0:
            examples = (
                carrier_mismatch[["carrier", "carrier_name", "carrier_name_expected"]]
                .drop_duplicates()
                .head(8)
                .to_dict(orient="records")
            )
            warnings.append(
                f"GENERATED: {len(carrier_mismatch)} rows have carrier_name not matching canonical mapping (examples: {examples})"
            )

        if len(airport_mismatch) > 0:
            examples = (
                airport_mismatch[["airport", "airport_name", "airport_name_expected"]]
                .drop_duplicates()
                .head(8)
                .to_dict(orient="records")
            )
            warnings.append(
                f"GENERATED: {len(airport_mismatch)} rows have airport_name not matching canonical mapping (examples: {examples})"
            )
    except Exception:
        # Non-fatal; keep validator usable.
        pass

    return CheckResult(ok=True, errors=errors, warnings=warnings)


def apply_canonical_names(
    generated_df: pd.DataFrame,
    dim_carrier: pd.DataFrame,
    dim_airport: pd.DataFrame,
) -> Tuple[pd.DataFrame, List[str]]:
    """Return a copy of generated_df with canonical names applied.

    This does not change carrier/airport *codes*; it only normalizes the
    label columns to prevent Power BI category fragmentation.
    """

    notes: List[str] = []
    out = generated_df.copy()

    try:
        carrier_map = dict(zip(dim_carrier["carrier"].astype(str), dim_carrier["carrier_name"].astype(str)))
        airport_map = dict(zip(dim_airport["airport"].astype(str), dim_airport["airport_name"].astype(str)))

        if "carrier" in out.columns and "carrier_name" in out.columns:
            expected = out["carrier"].astype("string").astype(str).map(carrier_map)
            old = out["carrier_name"].astype("string")
            replace_mask = expected.notna() & old.notna() & (old.astype(str) != expected.astype(str))
            changed = int(replace_mask.sum())
            out.loc[replace_mask, "carrier_name"] = expected[replace_mask].astype("string")
            if changed:
                notes.append(f"Canonicalized carrier_name for {changed:,} rows")

        if "airport" in out.columns and "airport_name" in out.columns:
            expected = out["airport"].astype("string").astype(str).map(airport_map)
            old = out["airport_name"].astype("string")
            replace_mask = expected.notna() & old.notna() & (old.astype(str) != expected.astype(str))
            changed = int(replace_mask.sum())
            out.loc[replace_mask, "airport_name"] = expected[replace_mask].astype("string")
            if changed:
                notes.append(f"Canonicalized airport_name for {changed:,} rows")
    except Exception:
        # Non-fatal; outputs are optional.
        return generated_df.copy(), ["Failed to canonicalize generated names (non-fatal)"]

    return out, notes


def write_ndjson(df: pd.DataFrame, path: str) -> None:
    safe = df.copy()
    safe = safe.where(pd.notna(safe), None)
    with open(path, "w", encoding="utf-8") as f:
        for record in safe.to_dict(orient="records"):
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _coerce_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def load_ndjson(path: str) -> pd.DataFrame:
    records: List[dict] = []
    bad = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                bad += 1

    if not records:
        raise ValueError("No valid JSON records found in generated file")

    df = pd.DataFrame(records)
    if bad:
        print(f"⚠️ Skipped {bad} invalid JSON lines")
    return df


def load_inputs_interactive() -> Tuple[pd.DataFrame, pd.DataFrame, str, str]:
    """Returns: (original_df, generated_df, original_name, generated_name)."""

    def _upload_one(prompt: str) -> Tuple[str, bytes]:
        if not IN_COLAB:
            raise RuntimeError("Upload mode requires Google Colab")
        print(prompt)
        uploaded = files.upload()  # type: ignore
        name = list(uploaded.keys())[0]
        return name, uploaded[name]

    print("📥 Choose input method:")
    print("1) Upload files (recommended in Colab)")
    print("2) Use local workspace paths (for dev/test)")
    choice = input("Enter choice (1 or 2): ").strip()

    if choice == "1":
        original_name, original_bytes = _upload_one("📤 Upload ORIGINAL CSV (Airline_Delay_Cause.csv)")
        gen_name, gen_bytes = _upload_one("📤 Upload GENERATED NDJSON/TXT (one JSON per line)")

        with open(original_name, "wb") as f:
            f.write(original_bytes)
        with open(gen_name, "wb") as f:
            f.write(gen_bytes)

        original_df = pd.read_csv(original_name)
        generated_df = load_ndjson(gen_name)
        return original_df, generated_df, original_name, gen_name

    # dev mode
    original_path = input("Original CSV path: ").strip()
    generated_path = input("Generated NDJSON path: ").strip()
    original_df = pd.read_csv(original_path)
    generated_df = load_ndjson(generated_path)
    return original_df, generated_df, original_path, generated_path


def normalize_schema(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Some streams add ingestion_timestamp; keep it but not required.
    # Coerce numeric columns so comparisons work reliably.
    numeric_cols = [
        "year",
        "month",
        "arr_flights",
        "arr_del15",
        "arr_cancelled",
        "arr_diverted",
        "arr_delay",
    ] + CAUSE_CT_COLS + CAUSE_DELAY_COLS

    out = _coerce_numeric(out, numeric_cols)

    # Normalize string columns without turning NaN into the literal "nan".
    # This matters for accurate distinct counts and Power BI relationships.
    for c in ["carrier", "carrier_name", "airport", "airport_name"]:
        if c in out.columns:
            out[c] = out[c].astype("string")
            out[c] = out[c].str.strip()
            out.loc[out[c] == "", c] = pd.NA

    return out


def data_quality_checks(df: pd.DataFrame, label: str) -> CheckResult:
    errors: List[str] = []
    warnings: List[str] = []

    is_original = label.upper() == "ORIGINAL"

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        errors.append(f"{label}: Missing required columns: {missing}")
        return CheckResult(False, errors, warnings)

    # Null checks on required fields
    null_counts = df[REQUIRED_COLUMNS].isna().sum()
    null_required = null_counts[null_counts > 0]
    if len(null_required) > 0:
        msg = f"{label}: Nulls in required columns: {null_required.to_dict()}"
        # Original benchmark files can contain blanks; don't hard-fail.
        if label.upper() == "ORIGINAL":
            warnings.append(msg)
        else:
            errors.append(msg)

    # Basic range checks
    bad_month = df[(df["month"] < 1) | (df["month"] > 12)]
    if len(bad_month) > 0:
        errors.append(f"{label}: {len(bad_month)} records have invalid month")

    # In the demo pipeline we typically stream year=2025; warn if generated deviates.
    if label.upper() == "GENERATED" and "year" in df.columns:
        years = sorted({int(y) for y in df["year"].dropna().unique().tolist()})
        if years != [2025]:
            warnings.append(f"{label}: unexpected year values {years} (expected only 2025)")

    bad_flights = df[df["arr_flights"] <= 0]
    if len(bad_flights) > 0:
        msg = f"{label}: {len(bad_flights)} records have arr_flights <= 0"
        (warnings if is_original else errors).append(msg)

    invalid_del15 = df[df["arr_del15"] > df["arr_flights"]]
    if len(invalid_del15) > 0:
        msg = f"{label}: {len(invalid_del15)} records have arr_del15 > arr_flights"
        (warnings if is_original else errors).append(msg)

    # Non-negativity (matches original dataset constraints)
    num_cols = [
        "arr_del15",
        "arr_cancelled",
        "arr_diverted",
        "arr_delay",
    ] + CAUSE_CT_COLS + CAUSE_DELAY_COLS

    for c in num_cols:
        neg = df[df[c] < 0]
        if len(neg) > 0:
            msg = f"{label}: {len(neg)} records have negative {c}"
            if label.upper() == "ORIGINAL":
                warnings.append(msg)
            else:
                errors.append(msg)

    # Guardrails (Power BI + ClickHouse sanity)
    too_big_delay = df[df["arr_delay"] > MAX_ALLOWED_ARR_DELAY]
    if len(too_big_delay) > 0:
        warnings.append(f"{label}: {len(too_big_delay)} records have arr_delay > {MAX_ALLOWED_ARR_DELAY}")

    too_big_flights = df[df["arr_flights"] > MAX_ALLOWED_ARR_FLIGHTS]
    if len(too_big_flights) > 0:
        warnings.append(f"{label}: {len(too_big_flights)} records have arr_flights > {MAX_ALLOWED_ARR_FLIGHTS}")

    # Aggregated-record consistency: cause counts should not exceed delayed flights
    for c in CAUSE_CT_COLS:
        too_high = df[df[c] > df["arr_del15"]]
        if len(too_high) > 0:
            msg = f"{label}: {len(too_high)} records have {c} > arr_del15"
            (warnings if is_original else errors).append(msg)

    # Delay sum consistency: arr_delay == sum(component delays) within 1 minute
    calc = df[CAUSE_DELAY_COLS].sum(axis=1)
    mismatch = df[(df["arr_delay"] - calc).abs() > 1]
    if len(mismatch) > 0:
        msg = f"{label}: {len(mismatch)} records have delay sum mismatch > 1 minute"
        if label.upper() == "ORIGINAL":
            warnings.append(msg)
        else:
            errors.append(msg)

    return CheckResult(ok=(len(errors) == 0), errors=errors, warnings=warnings)


def domain_checks(original_df: pd.DataFrame, generated_df: pd.DataFrame) -> CheckResult:
    """Ensure generated categorical values exist in the original dataset.

    This protects Power BI relationships (airport/carrier dimensions) from drift.
    """

    errors: List[str] = []
    warnings: List[str] = []

    original_airports = set(original_df["airport"].dropna().astype(str).unique().tolist())
    original_carriers = set(original_df["carrier"].dropna().astype(str).unique().tolist())

    gen_airports = set(generated_df["airport"].dropna().astype(str).unique().tolist())
    gen_carriers = set(generated_df["carrier"].dropna().astype(str).unique().tolist())

    unknown_airports = sorted(gen_airports - original_airports)
    unknown_carriers = sorted(gen_carriers - original_carriers)

    if unknown_airports:
        errors.append(f"GENERATED: {len(unknown_airports)} airport codes not in original dataset (e.g. {unknown_airports[:10]})")
    if unknown_carriers:
        errors.append(f"GENERATED: {len(unknown_carriers)} carrier codes not in original dataset (e.g. {unknown_carriers[:10]})")

    # Optional: warn if names disagree for same code (can break Power BI grouping/labels)
    try:
        o_airport_name = (
            original_df[["airport", "airport_name"]]
            .dropna()
            .astype(str)
            .drop_duplicates()
            .groupby("airport")["airport_name"]
            .nunique()
        )
        g_airport_name = (
            generated_df[["airport", "airport_name"]]
            .dropna()
            .astype(str)
            .drop_duplicates()
            .groupby("airport")["airport_name"]
            .nunique()
        )
        noisy_airports = sorted((g_airport_name[g_airport_name > 1].index).tolist())
        if noisy_airports:
            warnings.append(
                f"GENERATED: {len(noisy_airports)} airports have multiple airport_name values (e.g. {noisy_airports[:10]})"
            )

        o_carrier_name = (
            original_df[["carrier", "carrier_name"]]
            .dropna()
            .astype(str)
            .drop_duplicates()
            .groupby("carrier")["carrier_name"]
            .nunique()
        )
        g_carrier_name = (
            generated_df[["carrier", "carrier_name"]]
            .dropna()
            .astype(str)
            .drop_duplicates()
            .groupby("carrier")["carrier_name"]
            .nunique()
        )
        noisy_carriers = sorted((g_carrier_name[g_carrier_name > 1].index).tolist())
        if noisy_carriers:
            warnings.append(
                f"GENERATED: {len(noisy_carriers)} carriers have multiple carrier_name values (e.g. {noisy_carriers[:10]})"
            )
    except Exception:
        # Name consistency is a nice-to-have; don't fail validation.
        pass

    return CheckResult(ok=(len(errors) == 0), errors=errors, warnings=warnings)


def _seasonal_index_profile(df: pd.DataFrame) -> pd.DataFrame:
    """Compute a monthly seasonal index profile for delay_rate.

    For ORIGINAL (multi-year):
    - compute delay_rate by (year, month)
    - normalize per-year by that year's mean (index)
    - average index across years for each month (weighted by flights)

    For GENERATED (typically single-year): this behaves similarly.
    """

    required = {"year", "month", "arr_flights", "arr_del15"}
    if not required.issubset(set(df.columns)):
        # Return empty profile; caller will warn.
        return pd.DataFrame({"month": list(range(1, 13)), "seasonal_index": [np.nan] * 12})

    g = (
        df.groupby(["year", "month"], as_index=False)
        .agg(arr_flights=("arr_flights", "sum"), arr_del15=("arr_del15", "sum"))
        .copy()
    )
    g["delay_rate"] = g["arr_del15"] / g["arr_flights"].replace(0, np.nan)
    year_mean = g.groupby("year", as_index=False).agg(year_mean=("delay_rate", "mean"))
    g = g.merge(year_mean, on="year", how="left")
    g["seasonal_index"] = g["delay_rate"] / g["year_mean"].replace(0, np.nan)
    g.loc[~np.isfinite(g["seasonal_index"].astype(float)), "seasonal_index"] = np.nan

    out = []
    for m in range(1, 13):
        gm = g[g["month"] == m]
        vals = gm["seasonal_index"].astype(float)
        w = gm["arr_flights"].astype(float)
        ok = vals.notna() & w.notna() & (w > 0)
        if int(ok.sum()) == 0:
            out.append({"month": m, "seasonal_index": np.nan})
        else:
            out.append({"month": m, "seasonal_index": float(np.average(vals[ok].to_numpy(), weights=w[ok].to_numpy()))})

    return pd.DataFrame(out)


def seasonality_checks(original_df_valid: pd.DataFrame, generated_df_valid: pd.DataFrame) -> Tuple[CheckResult, Dict[str, float], pd.DataFrame, pd.DataFrame]:
    """Compare monthly patterns using an averaged per-year seasonal index."""

    errors: List[str] = []
    warnings: List[str] = []

    orig_prof = _seasonal_index_profile(original_df_valid)
    gen_prof = _seasonal_index_profile(generated_df_valid)

    o = orig_prof["seasonal_index"].astype(float)
    g = gen_prof["seasonal_index"].astype(float)

    valid = o.notna() & g.notna()
    n = int(valid.sum())
    metrics: Dict[str, float] = {"seasonality_months_compared": float(n)}

    if n < 10:
        warnings.append(
            f"Monthly seasonality check: only {n}/12 months comparable; correlation may be unreliable"
        )
        return CheckResult(ok=True, errors=errors, warnings=warnings), metrics, orig_prof, gen_prof

    o_arr = o[valid].to_numpy()
    g_arr = g[valid].to_numpy()

    if np.all(o_arr == 0) or np.all(g_arr == 0):
        warnings.append("Monthly seasonality check: seasonal index is all zeros in one dataset")
        return CheckResult(ok=True, errors=errors, warnings=warnings), metrics, orig_prof, gen_prof

    corr = float(np.corrcoef(o_arr, g_arr)[0, 1])
    mad = float(np.mean(np.abs(o_arr - g_arr)))
    metrics.update({"seasonality_corr": corr, "seasonality_mad": mad})

    # Soft thresholds; we're ensuring dashboard seasonality looks plausible.
    if corr < 0.70:
        warnings.append(f"Monthly delay_rate seasonal-index correlation is low: corr={corr:.2f} (expected >= 0.70)")
    if mad > 0.10:
        warnings.append(f"Monthly delay_rate seasonal-index mean abs diff is high: {mad:.3f} (expected <= 0.10)")

    return CheckResult(ok=True, errors=errors, warnings=warnings), metrics, orig_prof, gen_prof


def compute_kpis(df: pd.DataFrame) -> Dict[str, float]:
    flights = float(df["arr_flights"].sum())
    delayed = float(df["arr_del15"].sum())
    cancelled = float(df["arr_cancelled"].sum())
    diverted = float(df["arr_diverted"].sum())
    delay_min = float(df["arr_delay"].sum())

    delay_rate = delayed / flights if flights > 0 else 0.0
    cancel_rate = cancelled / flights if flights > 0 else 0.0
    divert_rate = diverted / flights if flights > 0 else 0.0
    avg_delay_per_flight = delay_min / flights if flights > 0 else 0.0
    avg_delay_per_delayed = delay_min / delayed if delayed > 0 else 0.0

    cause_delay_sum = float(df[CAUSE_DELAY_COLS].sum().sum())
    cause_shares = {}
    if cause_delay_sum > 0:
        for c in CAUSE_DELAY_COLS:
            cause_shares[f"share_{c}"] = float(df[c].sum()) / cause_delay_sum

    return {
        "total_flights": flights,
        "total_delayed": delayed,
        "delay_rate": delay_rate,
        "cancel_rate": cancel_rate,
        "divert_rate": divert_rate,
        "avg_delay_per_flight": avg_delay_per_flight,
        "avg_delay_per_delayed": avg_delay_per_delayed,
        "unique_airports": float(df["airport"].nunique()),
        "unique_carriers": float(df["carrier"].nunique()),
        **cause_shares,
    }


def kpis_by_month(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("month", as_index=False).agg(
        arr_flights=("arr_flights", "sum"),
        arr_del15=("arr_del15", "sum"),
        arr_cancelled=("arr_cancelled", "sum"),
        arr_diverted=("arr_diverted", "sum"),
        arr_delay=("arr_delay", "sum"),
        carrier_delay=("carrier_delay", "sum"),
        weather_delay=("weather_delay", "sum"),
        nas_delay=("nas_delay", "sum"),
        security_delay=("security_delay", "sum"),
        late_aircraft_delay=("late_aircraft_delay", "sum"),
    )

    g["delay_rate"] = g["arr_del15"] / g["arr_flights"].replace(0, np.nan)
    g["cancel_rate"] = g["arr_cancelled"] / g["arr_flights"].replace(0, np.nan)
    g["divert_rate"] = g["arr_diverted"] / g["arr_flights"].replace(0, np.nan)
    g["avg_delay_per_flight"] = g["arr_delay"] / g["arr_flights"].replace(0, np.nan)

    cause_sum = g[CAUSE_DELAY_COLS].sum(axis=1).replace(0, np.nan)
    for c in CAUSE_DELAY_COLS:
        g[f"share_{c}"] = g[c] / cause_sum

    return g


def compare_kpis(original: Dict[str, float], generated: Dict[str, float]) -> CheckResult:
    errors: List[str] = []
    warnings: List[str] = []

    def _cmp_abs(name: str, tol: float, severity: str = "warn"):
        o = float(original.get(name, 0.0))
        g = float(generated.get(name, 0.0))
        diff = abs(g - o)
        if diff > tol:
            msg = f"KPI {name}: generated={g:.4f} vs original={o:.4f} (abs diff={diff:.4f} > {tol})"
            if severity == "error":
                errors.append(msg)
            else:
                warnings.append(msg)

    _cmp_abs("delay_rate", TOL_DELAY_RATE_ABS)
    _cmp_abs("cancel_rate", TOL_CANCEL_RATE_ABS)
    _cmp_abs("divert_rate", TOL_DIVERT_RATE_ABS)
    _cmp_abs("avg_delay_per_flight", TOL_AVG_DELAY_PER_FLIGHT_ABS_MIN)

    # Coverage expectations (Power BI richness)
    if generated.get("unique_airports", 0) < 50:
        warnings.append(f"Low airport coverage in generated: {int(generated['unique_airports'])}")

    if generated.get("unique_carriers", 0) < 10:
        warnings.append(f"Low carrier coverage in generated: {int(generated['unique_carriers'])}")

    return CheckResult(ok=(len(errors) == 0), errors=errors, warnings=warnings)


def plot_comparisons(
    orig_month: pd.DataFrame,
    gen_month: pd.DataFrame,
    orig_seasonal_profile: Optional[pd.DataFrame] = None,
    gen_seasonal_profile: Optional[pd.DataFrame] = None,
):
    # Lazy imports so the script still runs headless if needed.
    import matplotlib.pyplot as plt

    def _line(metric: str, title: str, yfmt: Optional[str] = None):
        plt.figure(figsize=(10, 4))
        plt.plot(orig_month["month"], orig_month[metric], marker="o", label="Original")
        plt.plot(gen_month["month"], gen_month[metric], marker="o", label="Generated")
        plt.title(title)
        plt.xlabel("Month")
        plt.grid(True, alpha=0.3)
        plt.legend()
        if yfmt == "pct":
            plt.gca().yaxis.set_major_formatter(lambda x, pos: f"{x*100:.0f}%")
        plt.tight_layout()
        plt.show()

    _line("delay_rate", "Monthly Delay Rate (seasonality)", yfmt="pct")
    _line("avg_delay_per_flight", "Monthly Avg Delay per Flight (minutes)")
    _line("divert_rate", "Monthly Divert Rate", yfmt="pct")

    # Relative seasonality (seasonal index)
    try:
        if orig_seasonal_profile is not None and gen_seasonal_profile is not None:
            plt.figure(figsize=(10, 4))
            plt.plot(
                orig_seasonal_profile["month"],
                orig_seasonal_profile["seasonal_index"],
                marker="o",
                label="Original (avg per-year index)",
            )
            plt.plot(
                gen_seasonal_profile["month"],
                gen_seasonal_profile["seasonal_index"],
                marker="o",
                label="Generated (per-year index)",
            )
            plt.title("Monthly Delay Rate Seasonal Index")
            plt.xlabel("Month")
            plt.grid(True, alpha=0.3)
            plt.legend()
            plt.tight_layout()
            plt.show()
        else:
            o = orig_month["delay_rate"].astype(float)
            g = gen_month["delay_rate"].astype(float)
            o_idx = o / (o.mean() if o.mean() else np.nan)
            g_idx = g / (g.mean() if g.mean() else np.nan)
            plt.figure(figsize=(10, 4))
            plt.plot(orig_month["month"], o_idx, marker="o", label="Original")
            plt.plot(gen_month["month"], g_idx, marker="o", label="Generated")
            plt.title("Monthly Delay Rate Seasonal Index (month / mean)")
            plt.xlabel("Month")
            plt.grid(True, alpha=0.3)
            plt.legend()
            plt.tight_layout()
            plt.show()
    except Exception:
        pass

    # Cause shares (minutes)
    import matplotlib.pyplot as plt

    months = orig_month["month"].values
    plt.figure(figsize=(12, 4))
    plt.stackplot(
        months,
        orig_month["share_carrier_delay"],
        orig_month["share_weather_delay"],
        orig_month["share_nas_delay"],
        orig_month["share_security_delay"],
        orig_month["share_late_aircraft_delay"],
        labels=["Carrier", "Weather", "NAS", "Security", "Late Aircraft"],
        alpha=0.85,
    )
    plt.title("Original: Cause Share of Delay Minutes by Month")
    plt.xlabel("Month")
    plt.ylabel("Share")
    plt.legend(loc="upper left", ncol=3)
    plt.tight_layout()
    plt.show()

    plt.figure(figsize=(12, 4))
    plt.stackplot(
        months,
        gen_month["share_carrier_delay"],
        gen_month["share_weather_delay"],
        gen_month["share_nas_delay"],
        gen_month["share_security_delay"],
        gen_month["share_late_aircraft_delay"],
        labels=["Carrier", "Weather", "NAS", "Security", "Late Aircraft"],
        alpha=0.85,
    )
    plt.title("Generated: Cause Share of Delay Minutes by Month")
    plt.xlabel("Month")
    plt.ylabel("Share")
    plt.legend(loc="upper left", ncol=3)
    plt.tight_layout()
    plt.show()


def main():
    print("🔍 PRO VALIDATOR: NiFi generated vs original")
    print("=" * 60)

    original_df, generated_df, original_name, generated_name = load_inputs_interactive()

    print(f"✅ Loaded ORIGINAL: {original_name} ({len(original_df):,} rows)")
    print(f"✅ Loaded GENERATED: {generated_name} ({len(generated_df):,} rows)")

    original_df = normalize_schema(original_df)
    generated_df = normalize_schema(generated_df)

    # Optional reference-year filtering (helps seasonality fairness)
    original_df, year_notes = filter_original_years(original_df)
    if year_notes:
        print("\n🗓️ ORIGINAL year filter:")
        for n in year_notes:
            print(" -", n)

    # Clean ORIGINAL reference (do not mutate generated)
    original_df, ref_notes = clean_reference_original(original_df)
    if ref_notes:
        print("\n🧹 ORIGINAL reference cleanup:")
        for n in ref_notes:
            print(" -", n)

    print("\n🔎 Data quality checks...")
    orig_q = data_quality_checks(original_df, "ORIGINAL")
    gen_q = data_quality_checks(generated_df, "GENERATED")

    if orig_q.errors:
        print("\n❌ ORIGINAL errors:")
        for e in orig_q.errors:
            print(" -", e)

    if orig_q.warnings:
        print("\n⚠️ ORIGINAL warnings:")
        for w in orig_q.warnings:
            print(" -", w)

    if gen_q.errors:
        print("\n❌ GENERATED errors:")
        for e in gen_q.errors:
            print(" -", e)

    if gen_q.warnings:
        print("\n⚠️ GENERATED warnings:")
        for w in gen_q.warnings:
            print(" -", w)

    # Only hard-stop on GENERATED errors.
    if gen_q.errors:
        print("\n❌ STOP: Fix GENERATED critical errors before Power BI.")
        return

    print("\n🔎 Domain checks (codes must exist in original)...")
    dom = domain_checks(original_df, generated_df)
    if dom.errors:
        print("\n❌ Domain errors:")
        for e in dom.errors:
            print(" -", e)
        print("\n❌ STOP: Generated contains values not present in original dataset.")
        return
    if dom.warnings:
        print("\n⚠️ Domain warnings:")
        for w in dom.warnings:
            print(" -", w)

    # Build/export canonical dimensions for Power BI
    print("\n🧩 Building canonical dimension tables (Power BI)...")
    dim_carrier, dim_airport = build_canonical_dimensions(original_df)
    dim_carrier_path = "dim_carrier.csv"
    dim_airport_path = "dim_airport.csv"
    dim_carrier.to_csv(dim_carrier_path, index=False)
    dim_airport.to_csv(dim_airport_path, index=False)
    print(f"- Wrote {dim_carrier_path} ({len(dim_carrier):,} rows)")
    print(f"- Wrote {dim_airport_path} ({len(dim_airport):,} rows)")
    if IN_COLAB:
        files.download(dim_carrier_path)  # type: ignore
        files.download(dim_airport_path)  # type: ignore

    name_chk = name_consistency_checks(generated_df, dim_carrier, dim_airport)
    if name_chk.warnings:
        print("\n⚠️ Name consistency warnings (Power BI grouping risk):")
        for w in name_chk.warnings:
            print(" -", w)

    canonical_notes: List[str] = []
    if WRITE_CANONICALIZED_GENERATED:
        print("\n🧾 Exporting canonicalized GENERATED outputs (Power BI friendly)...")
        gen_canon, canonical_notes = apply_canonical_names(generated_df, dim_carrier, dim_airport)
        gen_canon.to_csv(CANONICALIZED_GENERATED_CSV, index=False)
        write_ndjson(gen_canon, CANONICALIZED_GENERATED_NDJSON)
        print(f"- Wrote {CANONICALIZED_GENERATED_CSV} ({len(gen_canon):,} rows)")
        print(f"- Wrote {CANONICALIZED_GENERATED_NDJSON} ({len(gen_canon):,} lines)")
        for n in canonical_notes:
            print(" -", n)
        if IN_COLAB:
            files.download(CANONICALIZED_GENERATED_CSV)  # type: ignore
            files.download(CANONICALIZED_GENERATED_NDJSON)  # type: ignore

    print("\n📊 KPI comparison (overall)...")

    # Compute KPIs on a consistent 'valid' subset for fair comparison.
    orig_mask = valid_record_mask(original_df)
    gen_mask = valid_record_mask(generated_df)
    orig_valid = original_df[orig_mask].copy()
    gen_valid = generated_df[gen_mask].copy()

    dropped_orig = int((~orig_mask).sum())
    dropped_gen = int((~gen_mask).sum())
    if dropped_orig:
        print(f"⚠️ ORIGINAL: excluded {dropped_orig:,} rows from KPI/seasonality (invalid/outlier by guardrails)")
    if dropped_gen:
        print(f"⚠️ GENERATED: excluded {dropped_gen:,} rows from KPI/seasonality (should normally be 0)")

    kpi_o = compute_kpis(orig_valid)
    kpi_g = compute_kpis(gen_valid)

    comp = compare_kpis(kpi_o, kpi_g)
    for k in ["delay_rate", "cancel_rate", "divert_rate", "avg_delay_per_flight", "avg_delay_per_delayed"]:
        print(f"- {k}: original={kpi_o[k]:.4f} | generated={kpi_g[k]:.4f}")

    print(f"- unique_airports: original={int(kpi_o['unique_airports'])} | generated={int(kpi_g['unique_airports'])}")
    print(f"- unique_carriers: original={int(kpi_o['unique_carriers'])} | generated={int(kpi_g['unique_carriers'])}")

    if comp.warnings:
        print("\n⚠️ KPI warnings:")
        for w in comp.warnings:
            print(" -", w)

    if comp.errors:
        print("\n❌ KPI errors:")
        for e in comp.errors:
            print(" -", e)
        print("\n❌ STOP: KPI mismatches are too large for a 'realistic' demo.")
        return

    print("\n📈 Monthly comparison plots...")
    orig_m = kpis_by_month(orig_valid)
    gen_m = kpis_by_month(gen_valid)

    # Fill missing months (if any) so plots don't break
    all_months = pd.DataFrame({"month": list(range(1, 13))})
    orig_m = all_months.merge(orig_m, on="month", how="left")
    gen_m = all_months.merge(gen_m, on="month", how="left")

    seas, seas_metrics, orig_seas_prof, gen_seas_prof = seasonality_checks(orig_valid, gen_valid)
    if seas.warnings:
        print("\n⚠️ Seasonality warnings:")
        for w in seas.warnings:
            print(" -", w)

    plot_comparisons(orig_m, gen_m, orig_seas_prof, gen_seas_prof)

    # Export a compact report for sharing
    report = {
        "original": kpi_o,
        "generated": kpi_g,
        "seasonality": seas_metrics,
        "powerbi_outputs": {
            "dim_carrier_csv": dim_carrier_path,
            "dim_airport_csv": dim_airport_path,
            "canonicalized_generated_csv": CANONICALIZED_GENERATED_CSV if WRITE_CANONICALIZED_GENERATED else None,
            "canonicalized_generated_ndjson": CANONICALIZED_GENERATED_NDJSON if WRITE_CANONICALIZED_GENERATED else None,
            "canonicalization_notes": canonical_notes,
        },
        "generated_warnings": gen_q.warnings + dom.warnings + name_chk.warnings + comp.warnings + seas.warnings,
        "status": "PASS_WITH_WARNINGS"
        if (gen_q.warnings or dom.warnings or name_chk.warnings or comp.warnings or seas.warnings)
        else "PASS",
        "reference_notes": year_notes + ref_notes + orig_q.warnings,
    }

    out_path = "pro_validation_report.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print("\n✅ VALIDATION COMPLETE")
    print(f"- Status: {report['status']}")
    print(f"- Saved report: {out_path}")

    if IN_COLAB:
        files.download(out_path)  # type: ignore


if __name__ == "__main__":
    main()
