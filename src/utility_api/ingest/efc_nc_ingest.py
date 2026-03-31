#!/usr/bin/env python3
"""
UNC EFC North Carolina Water Rate Ingest

Purpose:
    Loads water rate data from the UNC Environmental Finance Center's
    North Carolina Water and Wastewater Rates Dashboard (2025 CSV export).

    The EFC CSV provides pre-computed bills at 500-gallon consumption
    increments rather than explicit tier structures. This module
    reverse-engineers tier breakpoints by detecting marginal rate changes
    in the bill curve, then converts to our standard tier schema.

    Filters to: residential, water, inside-city, non-seasonal rates.
    Normalizes all values to monthly equivalents (handles bimonthly/quarterly).

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - sqlalchemy (database)
    - loguru (logging)

Usage:
    # CLI
    ua-ingest efc-nc                    # Full ingest
    ua-ingest efc-nc --dry-run          # Preview without DB writes

    # Python
    from utility_api.ingest.efc_nc_ingest import run_efc_nc_ingest
    run_efc_nc_ingest(dry_run=True)

Notes:
    - Bill columns (total_bill_0 .. total_bill_15000, total_bill_50000) are
      per-billing-period at gallon consumption levels.
    - Tier limits are extracted in gallons per billing period, then converted
      to CCF/month (÷ 748.052, ÷ billing period divisor).
    - Tier rates are extracted in $/kgal, then converted to $/CCF (* 0.748052).
    - Allowances (included consumption at $0) are recorded but not counted
      as a volumetric tier.
    - Utilities with >4 detected tiers have tiers 4+ collapsed into tier 4
      (last rate, limit=NULL). A note is recorded.
    - Duplicate PWSIDs (different utilities sharing a PWSID) are flagged
      and only the first row is ingested.

Data Sources:
    - Input: data/raw/table_NC_cost_tables_2025.csv (UNC EFC dashboard export)
    - Input: utility.cws_boundaries (PWSID filter)
    - Output: utility.rate_schedules table (source_key=efc_nc_2025)

Configuration:
    - CSV file must be at data/raw/table_NC_cost_tables_2025.csv
    - Database connection via .env (DATABASE_URL)
"""

import csv
from datetime import date, datetime, timezone
from pathlib import Path

from loguru import logger
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine
from utility_api.ops.rate_schedule_helpers import water_rate_to_schedule, write_rate_schedule


# --- Constants ---

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_FILE = PROJECT_ROOT / "data" / "raw" / "table_NC_cost_tables_2025.csv"
SOURCE_TAG = "efc_nc_2025"

# Conversion factors
GAL_PER_CCF = 748.052  # 1 CCF = 748.052 gallons
KGAL_TO_CCF = 0.748052  # $/kgal * 0.748052 = $/CCF (inverse: 1 kgal / 748.052 gal * 100 cf)

# Bill column names at 500-gallon increments (per billing period)
BILL_COLS = [f"total_bill_{g}" for g in range(0, 15500, 500)]
BILL_GALS = list(range(0, 15500, 500))  # 0, 500, 1000, ..., 15000

# Billing period divisors (per-billing-period → monthly)
BILLING_DIVISOR = {
    "monthly": 1,
    "bimonthly": 2,
    "quarterly": 3,
}

# Rate structure mapping (EFC labels → our schema)
STRUCTURE_MAP = {
    "uniform_rate": "uniform",
    "increasing_block": "increasing_block",
    "decreasing_block": "decreasing_block",
    "increasing_decreasing_block": "increasing_block",  # dominant pattern
    "non_volumetric_flat_fee": "flat",
    "uniform_at_one_block_s_rate": "uniform",
}

# Tier detection tolerance: marginal rate changes smaller than this
# (in $/kgal) are considered floating-point noise, not a real tier break.
TIER_TOLERANCE_PER_KGAL = 0.05


def _safe_float(val: str) -> float | None:
    """Convert a CSV string to float, returning None for empty/NA."""
    if val is None or val == "" or val == "NA":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _get_nc_pwsids_in_db() -> set[str]:
    """Get NC PWSIDs that exist in cws_boundaries."""
    schema = settings.utility_schema
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT pwsid FROM {schema}.cws_boundaries
            WHERE pwsid LIKE 'NC%%'
        """)).fetchall()
    return {r[0] for r in rows}


def _extract_tiers(row: dict) -> list[dict]:
    """Extract tier structure from bill curve by detecting marginal rate changes.

    Computes the marginal rate ($/kgal) at each 500-gallon step, then groups
    consecutive steps with similar rates into tiers.

    Parameters
    ----------
    row : dict
        CSV row with total_bill_* columns.

    Returns
    -------
    list[dict]
        Each dict has:
        - 'start_gal': start of this tier (gallons, per billing period)
        - 'end_gal': end of this tier (gallons, per billing period; None for last)
        - 'rate_per_kgal': volumetric rate in $/kgal
        - 'is_allowance': True if rate is $0 (free consumption)
    """
    bills = []
    for col in BILL_COLS:
        val = _safe_float(row.get(col))
        if val is None:
            break
        bills.append(val)

    if len(bills) < 2:
        return []

    # Compute marginal rates at each 500-gal step
    marginal_rates = []
    for i in range(1, len(bills)):
        rate_per_kgal = (bills[i] - bills[i - 1]) / 0.5  # per 1000 gallons
        marginal_rates.append(rate_per_kgal)

    # Group consecutive similar rates into tiers
    tiers = []
    current_rate = marginal_rates[0]
    current_start_gal = BILL_GALS[0]  # 0

    for i in range(1, len(marginal_rates)):
        if abs(marginal_rates[i] - current_rate) > TIER_TOLERANCE_PER_KGAL:
            # Tier break detected
            tiers.append({
                "start_gal": current_start_gal,
                "end_gal": BILL_GALS[i],  # This step's start is previous tier's end
                "rate_per_kgal": current_rate,
                "is_allowance": current_rate < TIER_TOLERANCE_PER_KGAL,
            })
            current_rate = marginal_rates[i]
            current_start_gal = BILL_GALS[i]

    # Final tier (open-ended)
    tiers.append({
        "start_gal": current_start_gal,
        "end_gal": None,
        "rate_per_kgal": current_rate,
        "is_allowance": current_rate < TIER_TOLERANCE_PER_KGAL,
    })

    return tiers


def _tiers_to_schema(
    tiers: list[dict],
    divisor: int,
) -> dict:
    """Convert extracted tiers to water_rates schema columns.

    Filters out allowance tiers, converts gallons→CCF and $/kgal→$/CCF,
    normalizes to monthly, and collapses to max 4 tiers.

    Parameters
    ----------
    tiers : list[dict]
        Output from _extract_tiers().
    divisor : int
        Billing period divisor (1=monthly, 2=bimonthly, 3=quarterly).

    Returns
    -------
    dict
        Keys: tier_N_limit_ccf, tier_N_rate (N=1..4), plus overflow_note.
    """
    # Filter out allowance tiers (rate ≈ $0)
    volumetric = [t for t in tiers if not t["is_allowance"]]

    if not volumetric:
        return {
            "tier_1_limit_ccf": None, "tier_1_rate": None,
            "tier_2_limit_ccf": None, "tier_2_rate": None,
            "tier_3_limit_ccf": None, "tier_3_rate": None,
            "tier_4_limit_ccf": None, "tier_4_rate": None,
            "overflow_note": None,
        }

    overflow_note = None
    if len(volumetric) > 4:
        overflow_note = (
            f"EFC bill curve shows {len(volumetric)} volumetric tiers; "
            f"tiers 4+ collapsed into tier 4"
        )

    result = {}
    for i in range(1, 5):
        if i <= len(volumetric):
            tier = volumetric[i - 1]
            # For the last tier we store (or tier 4 when collapsing), limit = NULL
            if i == 4 and len(volumetric) > 4:
                # Collapsing: use the last tier's rate, limit = NULL
                last_tier = volumetric[-1]
                result[f"tier_{i}_limit_ccf"] = None
                result[f"tier_{i}_rate"] = round(
                    last_tier["rate_per_kgal"] * KGAL_TO_CCF, 4
                )
            elif tier["end_gal"] is None:
                # Open-ended final tier
                result[f"tier_{i}_limit_ccf"] = None
                result[f"tier_{i}_rate"] = round(
                    tier["rate_per_kgal"] * KGAL_TO_CCF, 4
                )
            else:
                # Tier limit: gallons per billing period → CCF per month
                limit_ccf_monthly = (tier["end_gal"] / GAL_PER_CCF) / divisor
                result[f"tier_{i}_limit_ccf"] = round(limit_ccf_monthly, 2)
                result[f"tier_{i}_rate"] = round(
                    tier["rate_per_kgal"] * KGAL_TO_CCF, 4
                )
        else:
            result[f"tier_{i}_limit_ccf"] = None
            result[f"tier_{i}_rate"] = None

    result["overflow_note"] = overflow_note
    return result


def _interpolate_bill(row: dict, target_gal: float) -> float | None:
    """Interpolate a bill amount at a non-standard gallon consumption level.

    Uses linear interpolation between the two nearest 500-gal bill columns.

    Parameters
    ----------
    row : dict
        CSV row with total_bill_* columns.
    target_gal : float
        Target consumption in gallons (per billing period).

    Returns
    -------
    float | None
        Interpolated bill amount (per billing period), or None if out of range.
    """
    if target_gal < 0:
        return None

    # Find bracketing columns
    lower_idx = int(target_gal // 500)
    if lower_idx >= len(BILL_GALS) - 1:
        # Beyond 15,000 gal — try extrapolating from last two points
        col_a = f"total_bill_{BILL_GALS[-2]}"
        col_b = f"total_bill_{BILL_GALS[-1]}"
        val_a = _safe_float(row.get(col_a))
        val_b = _safe_float(row.get(col_b))
        if val_a is not None and val_b is not None:
            rate_per_gal = (val_b - val_a) / 500.0
            overshoot = target_gal - BILL_GALS[-1]
            return val_b + rate_per_gal * overshoot
        return None

    gal_a = BILL_GALS[lower_idx]
    gal_b = BILL_GALS[lower_idx + 1]
    col_a = f"total_bill_{gal_a}"
    col_b = f"total_bill_{gal_b}"

    val_a = _safe_float(row.get(col_a))
    val_b = _safe_float(row.get(col_b))

    if val_a is None or val_b is None:
        return None

    # Linear interpolation
    frac = (target_gal - gal_a) / (gal_b - gal_a)
    return val_a + frac * (val_b - val_a)


def _compute_monthly_bill(row: dict, ccf: float, divisor: int) -> float | None:
    """Compute monthly bill at a given CCF/month consumption level.

    Accounts for billing period: converts monthly CCF to per-billing-period
    gallons, interpolates the bill curve, then divides by the billing period
    divisor to get the monthly equivalent.

    Parameters
    ----------
    row : dict
        CSV row with total_bill_* columns.
    ccf : float
        Monthly consumption in CCF.
    divisor : int
        Billing period divisor (1=monthly, 2=bimonthly, 3=quarterly).

    Returns
    -------
    float | None
        Monthly bill in dollars, or None if interpolation fails.
    """
    # Monthly CCF → gallons per billing period
    gal_per_period = ccf * GAL_PER_CCF * divisor
    bill_per_period = _interpolate_bill(row, gal_per_period)

    if bill_per_period is None:
        return None

    return round(bill_per_period / divisor, 2)


def _parse_effective_date(val: str) -> date | None:
    """Parse EFC effective_date_corrected (format: 'YYYY-M' or 'YYYY-MM').

    Parameters
    ----------
    val : str
        Date string like '2024-7' or '2024-12'.

    Returns
    -------
    date | None
        First of that month, or None if unparseable.
    """
    if not val or val == "NA":
        return None
    try:
        parts = val.split("-")
        year = int(parts[0])
        month = int(parts[1])
        return date(year, month, 1)
    except (ValueError, IndexError):
        return None


def _parse_row(row: dict, db_pwsids: set[str]) -> dict | None:
    """Parse a single EFC CSV row into a water_rates-compatible dict.

    Parameters
    ----------
    row : dict
        CSV row (from DictReader).
    db_pwsids : set[str]
        NC PWSIDs in our database.

    Returns
    -------
    dict | None
        Mapped record for DB insert, or None if row should be skipped.
        If skipped, returns None. Caller checks skip reason via row fields.
    """
    pwsid = row.get("pwsid", "").strip()

    # Skip non-NC or missing PWSIDs
    if not pwsid or pwsid == "NA":
        return None

    # Skip PWSIDs not in our database
    if pwsid not in db_pwsids:
        return None

    # Filter criteria (should already be applied, but defensive)
    if row.get("service_type") != "water":
        return None
    if row.get("rate_types") != "residential":
        return None
    if row.get("outside") != "0":
        return None
    if row.get("seasonal") != "0":
        return None

    # Billing period
    billing_period = row.get("billing_period", "monthly")
    divisor = BILLING_DIVISOR.get(billing_period, 1)

    # Rate structure
    raw_structure = row.get("volumetric_structure", "")
    structure_type = STRUCTURE_MAP.get(raw_structure)

    # Fixed charge — per billing period, normalize to monthly
    raw_base = _safe_float(row.get("base_charge"))
    fixed_charge_monthly = round(raw_base / divisor, 2) if raw_base is not None else None

    # Extract tiers from bill curve
    tiers = _extract_tiers(row)
    tier_data = _tiers_to_schema(tiers, divisor)

    # Compute bill snapshots (monthly equivalents)
    bill_5ccf = _compute_monthly_bill(row, 5.0, divisor)
    bill_10ccf = _compute_monthly_bill(row, 10.0, divisor)

    # Effective date
    eff_date = _parse_effective_date(row.get("effective_date_corrected", ""))

    # Confidence assessment
    has_tiers = tier_data.get("tier_1_rate") is not None
    has_bills = bill_5ccf is not None and bill_10ccf is not None
    if has_tiers and has_bills:
        confidence = "high"
    elif has_bills:
        confidence = "medium"
    else:
        confidence = "low"

    # Build notes
    notes_parts = []
    if tier_data.get("overflow_note"):
        notes_parts.append(tier_data["overflow_note"])
    if raw_structure and raw_structure not in STRUCTURE_MAP:
        notes_parts.append(f"Unmapped volumetric_structure: {raw_structure}")

    allowance = _safe_float(row.get("allowance"))
    if allowance is not None:
        notes_parts.append(f"Allowance: {allowance} kgal/period included in base")

    ownership = row.get("ownership_type", "")
    if ownership and ownership != "municipality":
        notes_parts.append(f"Ownership: {ownership}")

    return {
        "pwsid": pwsid,
        "utility_name": row.get("utility", "").strip() or None,
        "state_code": "NC",
        "county": None,
        "rate_effective_date": eff_date,
        "rate_structure_type": structure_type,
        "rate_class": "residential",
        "billing_frequency": billing_period,
        "fixed_charge_monthly": fixed_charge_monthly,
        "meter_size_inches": None,
        "tier_1_limit_ccf": tier_data.get("tier_1_limit_ccf"),
        "tier_1_rate": tier_data.get("tier_1_rate"),
        "tier_2_limit_ccf": tier_data.get("tier_2_limit_ccf"),
        "tier_2_rate": tier_data.get("tier_2_rate"),
        "tier_3_limit_ccf": tier_data.get("tier_3_limit_ccf"),
        "tier_3_rate": tier_data.get("tier_3_rate"),
        "tier_4_limit_ccf": tier_data.get("tier_4_limit_ccf"),
        "tier_4_rate": tier_data.get("tier_4_rate"),
        "bill_5ccf": bill_5ccf,
        "bill_10ccf": bill_10ccf,
        "bill_6ccf": None,
        "bill_9ccf": None,
        "bill_12ccf": None,
        "bill_24ccf": None,
        "source": SOURCE_TAG,
        "source_url": "https://dashboards.efc.sog.unc.edu/nc",
        "raw_text_hash": None,
        "parse_confidence": confidence,
        "parse_model": None,
        "parse_notes": "; ".join(notes_parts) if notes_parts else None,
    }


def run_efc_nc_ingest(dry_run: bool = False) -> dict:
    """Run the UNC EFC NC water rate ingest.

    Parameters
    ----------
    dry_run : bool
        If True, parse and report but don't write to DB.

    Returns
    -------
    dict
        Summary stats: total_csv_rows, filtered, matched, inserted,
        skipped_no_pwsid, skipped_not_in_db, duplicate_pwsids.
    """
    started = datetime.now(timezone.utc)
    logger.info("=== UNC EFC NC Rate Ingest Starting ===")

    # Validate data file
    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}")
        logger.info(
            "Download from: https://efc.sog.unc.edu/resource/north-carolina-rates-resources/"
        )
        return {"error": f"File not found: {DATA_FILE}"}

    logger.info(f"Source: {DATA_FILE.name} ({DATA_FILE.stat().st_size / 1024:.0f} KB)")

    # Load NC PWSIDs from database
    db_pwsids = _get_nc_pwsids_in_db()
    logger.info(f"NC PWSIDs in cws_boundaries: {len(db_pwsids)}")

    # Read and filter CSV
    with open(DATA_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)

    logger.info(f"Total CSV rows: {len(all_rows)}")

    # Pre-filter to target rows
    target_rows = [
        r for r in all_rows
        if r.get("service_type") == "water"
        and r.get("rate_types") == "residential"
        and r.get("outside") == "0"
        and r.get("seasonal") == "0"
    ]
    logger.info(f"After filter (residential/water/inside/non-seasonal): {len(target_rows)}")

    # Parse rows, track stats
    stats = {
        "total_csv_rows": len(all_rows),
        "filtered_rows": len(target_rows),
        "matched": 0,
        "inserted": 0,
        "skipped_no_pwsid": 0,
        "skipped_not_in_db": 0,
        "duplicate_pwsids": [],
    }

    records = []
    seen_pwsids = {}  # pwsid → utility_name (for dedup tracking)

    for row in target_rows:
        pwsid = row.get("pwsid", "").strip()

        # Skip missing PWSIDs
        if not pwsid or pwsid == "NA":
            stats["skipped_no_pwsid"] += 1
            continue

        # Skip PWSIDs not in DB
        if pwsid not in db_pwsids:
            stats["skipped_not_in_db"] += 1
            continue

        # Dedup: flag and skip duplicate PWSIDs
        utility_name = row.get("utility", "").strip()
        if pwsid in seen_pwsids:
            prev_name = seen_pwsids[pwsid]
            if utility_name != prev_name:
                stats["duplicate_pwsids"].append({
                    "pwsid": pwsid,
                    "kept": prev_name,
                    "skipped": utility_name,
                })
                logger.warning(
                    f"  Duplicate PWSID {pwsid}: "
                    f"keeping '{prev_name}', skipping '{utility_name}'"
                )
            continue
        seen_pwsids[pwsid] = utility_name

        record = _parse_row(row, db_pwsids)
        if record is not None:
            records.append(record)
            stats["matched"] += 1

    logger.info(f"Records to insert: {len(records)}")
    logger.info(f"Skipped (no PWSID): {stats['skipped_no_pwsid']}")
    logger.info(f"Skipped (not in DB): {stats['skipped_not_in_db']}")
    if stats["duplicate_pwsids"]:
        logger.info(f"Duplicate PWSIDs flagged: {len(stats['duplicate_pwsids'])}")
        for dup in stats["duplicate_pwsids"]:
            logger.info(f"  {dup['pwsid']}: kept='{dup['kept']}', skipped='{dup['skipped']}'")

    # Dry run: show samples and exit
    if dry_run:
        logger.info("[DRY RUN] Sample records:")
        for r in records[:8]:
            tier_str = ""
            for i in range(1, 5):
                rate = r.get(f"tier_{i}_rate")
                limit = r.get(f"tier_{i}_limit_ccf")
                if rate is not None:
                    limit_str = f"{limit:.1f}" if limit else "∞"
                    tier_str += f" T{i}:{limit_str}@${rate:.4f}"
            logger.info(
                f"  {r['pwsid']} | {(r['utility_name'] or '')[:30]:30s} | "
                f"{r['rate_structure_type']:18s} | "
                f"base=${r['fixed_charge_monthly'] or 0:.2f} | "
                f"@5ccf=${r['bill_5ccf'] or 0:.2f} | "
                f"@10ccf=${r['bill_10ccf'] or 0:.2f} | "
                f"[{r['parse_confidence']}] |{tier_str}"
            )
        if len(records) > 8:
            logger.info(f"  ... and {len(records) - 8} more")
        stats["inserted"] = 0
        return stats

    # Write to database
    schema = settings.utility_schema

    with engine.connect() as conn:
        # Delete existing EFC NC records (idempotent re-run)
        deleted = conn.execute(text(f"""
            DELETE FROM {schema}.rate_schedules
            WHERE source_key = :source
        """), {"source": SOURCE_TAG}).rowcount
        if deleted:
            logger.info(f"Cleared {deleted} existing {SOURCE_TAG} records from rate_schedules")

        # Batch insert into rate_schedules (Phase 3: direct write, no water_rates)
        for record in records:
            schedule = water_rate_to_schedule(record)
            write_rate_schedule(conn, schedule)

        conn.commit()
        stats["inserted"] = len(records)

    logger.info(f"Inserted {stats['inserted']} records")

    # Log pipeline run
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {schema}.pipeline_runs
                (step_name, started_at, finished_at, row_count, status, notes)
            VALUES (:step, :started, NOW(), :count, 'success', :notes)
        """), {
            "step": "efc-nc-ingest",
            "started": started,
            "count": stats["inserted"],
            "notes": (
                f"source={SOURCE_TAG}, filtered={stats['filtered_rows']}, "
                f"matched={stats['matched']}, inserted={stats['inserted']}, "
                f"skipped_no_pwsid={stats['skipped_no_pwsid']}, "
                f"skipped_not_in_db={stats['skipped_not_in_db']}, "
                f"duplicates={len(stats['duplicate_pwsids'])}"
            ),
        })
        conn.commit()

    logger.info(f"=== EFC NC Ingest Complete ({elapsed:.1f}s) ===")
    return stats
