#!/usr/bin/env python3
"""
UNC EFC Florida Water Rate Ingest (API-Based)

Purpose:
    Fetches water rate data from the UNC Environmental Finance Center's
    Florida Water and Wastewater Rates Dashboard via its JSON API, then
    ingests into the utility.water_rates table.

    The EFC API returns bill amounts at 500-gallon consumption increments
    (identical format to the NC EFC CSV). This module reuses the NC tier
    extraction logic to reverse-engineer tier breakpoints from the bill
    curve.

    This is the prototype for a generic EFC API client. The Topsail
    platform (Ruby on Rails) serves all 24 EFC state dashboards with the
    same JSON API pattern. Once proven here, the pattern generalizes.

    Dashboard: https://dashboards.efc.sog.unc.edu/fl (dashboard_id=15)
    Data vintage: Raftelis 2020 Florida Water and Wastewater Rate Survey
    Rate effective dates: ~October 2019

Author: AI-Generated
Created: 2026-03-25
Modified: 2026-03-25

Dependencies:
    - httpx (HTTP client)
    - sqlalchemy (database)
    - loguru (logging)

Usage:
    # CLI
    ua-ingest efc-fl                    # Fetch + ingest (uses cache if present)
    ua-ingest efc-fl --dry-run          # Fetch + preview without DB writes
    ua-ingest efc-fl --refresh          # Force re-fetch from API

    # Python
    from utility_api.ingest.efc_fl_ingest import run_efc_fl_ingest
    run_efc_fl_ingest(dry_run=True)

Notes:
    - API endpoint: /dashboards/15/chart_data.json (one call per utility)
    - 227 utilities, ~4 minutes at 1 req/sec with polite delay
    - Bill curve: 31 data points (0 to 15,000 gal in 500-gal steps)
    - Each utility's PWSID is in rate_structure.sdwis[].pwsid
    - Some utilities have no PWSID — these are skipped
    - Some utilities may share a PWSID — first encountered wins
    - Billing period is always "monthly" for FL (per-API response)
    - Tier extraction identical to efc_nc_ingest.py

Data Sources:
    - Input: EFC Topsail JSON API (fetched → cached at data/raw/efc_fl/)
    - Input: utility.cws_boundaries (PWSID filter)
    - Output: utility.water_rates table (source=efc_fl_2020)

Configuration:
    - API responses cached at data/raw/efc_fl/api_cache.json
    - Utility ID mapping at data/raw/efc_fl/fl_efc_utility_mapping.json
    - Database connection via .env (DATABASE_URL)
"""

import json
import time
from datetime import date, datetime, timezone
from pathlib import Path

import httpx
from loguru import logger
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine
from utility_api.ops.rate_schedule_helpers import water_rate_to_schedule, write_rate_schedule


# --- Constants ---

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = PROJECT_ROOT / "data" / "raw" / "efc_fl"
CACHE_FILE = DATA_DIR / "api_cache.json"
UTILITY_MAP_FILE = DATA_DIR / "fl_efc_utility_mapping.json"
SOURCE_TAG = "efc_fl_2020"
DASHBOARD_ID = 15

# API settings
API_BASE = "https://dashboards.efc.sog.unc.edu"
API_ENDPOINT = f"/dashboards/{DASHBOARD_ID}/chart_data.json"
REQUEST_DELAY_SECS = 1.0  # Polite delay between API calls
REQUEST_TIMEOUT_SECS = 30.0

# Conversion factors (same as NC ingest)
GAL_PER_CCF = 748.052  # 1 CCF = 748.052 gallons
KGAL_TO_CCF = 0.748052  # $/kgal * 0.748052 = $/CCF

# Bill curve gallon levels (0 to 15,000 in 500-gal steps)
BILL_GALS = list(range(0, 15500, 500))  # 31 values

# Billing period divisors (same as NC)
BILLING_DIVISOR = {
    "monthly": 1,
    "bimonthly": 2,
    "quarterly": 3,
}

# Tier detection tolerance (same as NC)
TIER_TOLERANCE_PER_KGAL = 0.05


# --- API Fetch ---


def _load_utility_ids() -> list[dict]:
    """Load FL utility rate_structure_ids from the mapping file.

    Returns
    -------
    list[dict]
        Each dict has 'rate_structure_id' and 'utility_name'.
    """
    if not UTILITY_MAP_FILE.exists():
        logger.error(f"Utility mapping not found: {UTILITY_MAP_FILE}")
        logger.info(
            "Run the dashboard HTML scraper first, or place "
            "fl_efc_utility_mapping.json in data/raw/efc_fl/"
        )
        return []

    with open(UTILITY_MAP_FILE) as f:
        data = json.load(f)

    # The mapping file is a dict: {"4450": "Aloha Gardens", ...}
    return [
        {"rate_structure_id": int(k), "utility_name": v}
        for k, v in data.items()
    ]


def _fetch_utility_data(
    client: httpx.Client,
    rate_structure_id: int,
) -> dict | None:
    """Fetch a single utility's data from the EFC API.

    Parameters
    ----------
    client : httpx.Client
        Reusable HTTP client.
    rate_structure_id : int
        The EFC rate_structure_id for this utility.

    Returns
    -------
    dict | None
        Full API response, or None on failure.
    """
    params = {
        "consumption_unit": "gal",
        "rate_structure_id": rate_structure_id,
        "usage_amount": 5000,
        "service_type": "water",
        "comparison_group": "all_rate_structures",
    }

    try:
        resp = client.get(
            f"{API_BASE}{API_ENDPOINT}",
            params=params,
            timeout=REQUEST_TIMEOUT_SECS,
        )
        resp.raise_for_status()
        return resp.json()
    except (httpx.HTTPError, json.JSONDecodeError) as e:
        logger.warning(f"  Failed to fetch rate_structure_id={rate_structure_id}: {e}")
        return None


def fetch_all_utilities(refresh: bool = False) -> dict:
    """Fetch all FL utility data from the EFC API.

    Results are cached to CACHE_FILE. If the cache exists and refresh=False,
    returns cached data without making API calls.

    Parameters
    ----------
    refresh : bool
        If True, re-fetch from API even if cache exists.

    Returns
    -------
    dict
        Mapping of rate_structure_id (str) → API response dict.
    """
    if CACHE_FILE.exists() and not refresh:
        logger.info(f"Loading cached API data from {CACHE_FILE.name}")
        with open(CACHE_FILE) as f:
            cache = json.load(f)
        logger.info(f"Cached: {len(cache)} utilities")
        return cache

    utilities = _load_utility_ids()
    if not utilities:
        return {}

    logger.info(f"Fetching {len(utilities)} FL utilities from EFC API...")
    logger.info(f"Estimated time: ~{len(utilities) * REQUEST_DELAY_SECS:.0f} seconds")

    cache = {}
    failures = 0

    with httpx.Client() as client:
        for i, util in enumerate(utilities):
            rs_id = util["rate_structure_id"]
            name = util["utility_name"]

            if (i + 1) % 25 == 0 or i == 0:
                logger.info(
                    f"  [{i + 1}/{len(utilities)}] Fetching {name} (id={rs_id})"
                )

            data = _fetch_utility_data(client, rs_id)
            if data is not None:
                cache[str(rs_id)] = data
            else:
                failures += 1

            # Polite delay
            if i < len(utilities) - 1:
                time.sleep(REQUEST_DELAY_SECS)

    logger.info(f"Fetched: {len(cache)} OK, {failures} failures")

    # Save cache
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)
    logger.info(f"Cached to {CACHE_FILE.name} ({CACHE_FILE.stat().st_size / 1024:.0f} KB)")

    return cache


# --- Tier Extraction (adapted from efc_nc_ingest.py) ---


def _extract_tiers_from_bill_curve(bill_water: dict) -> list[dict]:
    """Extract tier structure from bill curve by detecting marginal rate changes.

    This is the same algorithm as efc_nc_ingest._extract_tiers(), adapted
    to work with the API's dict format instead of CSV row format.

    Parameters
    ----------
    bill_water : dict
        Mapping of gallon level (str) → bill amount (float).
        Keys: "0", "500", "1000", ..., "15000".

    Returns
    -------
    list[dict]
        Each dict has: start_gal, end_gal, rate_per_kgal, is_allowance.
    """
    # Build bill array at 500-gal increments
    bills = []
    for gal in BILL_GALS:
        val = bill_water.get(str(gal))
        if val is None:
            break
        bills.append(float(val))

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
            tiers.append({
                "start_gal": current_start_gal,
                "end_gal": BILL_GALS[i],
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


def _tiers_to_schema(tiers: list[dict], divisor: int) -> dict:
    """Convert extracted tiers to water_rates schema columns.

    Identical logic to efc_nc_ingest._tiers_to_schema().

    Parameters
    ----------
    tiers : list[dict]
        Output from _extract_tiers_from_bill_curve().
    divisor : int
        Billing period divisor (1=monthly, 2=bimonthly, 3=quarterly).

    Returns
    -------
    dict
        Keys: tier_N_limit_ccf, tier_N_rate (N=1..4), plus overflow_note.
    """
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
            if i == 4 and len(volumetric) > 4:
                last_tier = volumetric[-1]
                result[f"tier_{i}_limit_ccf"] = None
                result[f"tier_{i}_rate"] = round(
                    last_tier["rate_per_kgal"] * KGAL_TO_CCF, 4
                )
            elif tier["end_gal"] is None:
                result[f"tier_{i}_limit_ccf"] = None
                result[f"tier_{i}_rate"] = round(
                    tier["rate_per_kgal"] * KGAL_TO_CCF, 4
                )
            else:
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


# --- Bill Computation ---


def _interpolate_bill(bill_water: dict, target_gal: float) -> float | None:
    """Interpolate a bill amount at any gallon consumption level.

    Parameters
    ----------
    bill_water : dict
        Mapping of gallon level (str) → bill amount (float).
    target_gal : float
        Target consumption in gallons (per billing period).

    Returns
    -------
    float | None
        Interpolated bill amount (per billing period), or None if out of range.
    """
    if target_gal < 0:
        return None

    lower_idx = int(target_gal // 500)
    if lower_idx >= len(BILL_GALS) - 1:
        # Beyond 15,000 gal — extrapolate from last two points
        gal_a = str(BILL_GALS[-2])
        gal_b = str(BILL_GALS[-1])
        val_a = bill_water.get(gal_a)
        val_b = bill_water.get(gal_b)
        if val_a is not None and val_b is not None:
            rate_per_gal = (float(val_b) - float(val_a)) / 500.0
            overshoot = target_gal - BILL_GALS[-1]
            return float(val_b) + rate_per_gal * overshoot
        return None

    gal_a = BILL_GALS[lower_idx]
    gal_b = BILL_GALS[lower_idx + 1]
    val_a = bill_water.get(str(gal_a))
    val_b = bill_water.get(str(gal_b))

    if val_a is None or val_b is None:
        return None

    frac = (target_gal - gal_a) / (gal_b - gal_a)
    return float(val_a) + frac * (float(val_b) - float(val_a))


def _compute_monthly_bill(
    bill_water: dict, ccf: float, divisor: int
) -> float | None:
    """Compute monthly bill at a given CCF/month consumption level.

    Parameters
    ----------
    bill_water : dict
        Bill curve from API response.
    ccf : float
        Monthly consumption in CCF.
    divisor : int
        Billing period divisor.

    Returns
    -------
    float | None
        Monthly bill in dollars.
    """
    gal_per_period = ccf * GAL_PER_CCF * divisor
    bill_per_period = _interpolate_bill(bill_water, gal_per_period)
    if bill_per_period is None:
        return None
    return round(bill_per_period / divisor, 2)


# --- Row Parsing ---


def _parse_effective_date(val: str | None) -> date | None:
    """Parse EFC effective date (format: 'MM/DD/YYYY').

    Parameters
    ----------
    val : str | None
        Date string like '10/01/2019'.

    Returns
    -------
    date | None
    """
    if not val:
        return None
    try:
        return datetime.strptime(val, "%m/%d/%Y").date()
    except (ValueError, TypeError):
        return None


def _get_fl_pwsids_in_db() -> set[str]:
    """Get FL PWSIDs that exist in cws_boundaries."""
    schema = settings.utility_schema
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT pwsid FROM {schema}.cws_boundaries
            WHERE pwsid LIKE 'FL%%'
        """)).fetchall()
    return {r[0] for r in rows}


def _parse_api_response(
    rs_id: str,
    data: dict,
    db_pwsids: set[str],
) -> list[dict]:
    """Parse one API response into water_rates records.

    A single API response may contain multiple PWSIDs (rate_structure.sdwis
    is a list). We create one record per PWSID.

    Parameters
    ----------
    rs_id : str
        Rate structure ID (string key from cache).
    data : dict
        Full API response.
    db_pwsids : set[str]
        FL PWSIDs in our database.

    Returns
    -------
    list[dict]
        Parsed records ready for DB insert. May be empty if no matching PWSIDs.
    """
    rate_struct = data.get("rate_structure", {})
    bill = data.get("bill", {})
    bill_water = bill.get("water", {})

    if not bill_water:
        return []

    # Extract PWSIDs
    sdwis_list = rate_struct.get("sdwis") or []
    pwsids = [
        s.get("pwsid", "").strip()
        for s in sdwis_list
        if s.get("pwsid", "").strip().startswith("FL")
    ]

    if not pwsids:
        return []

    # Filter to PWSIDs in our database
    matching_pwsids = [p for p in pwsids if p in db_pwsids]
    if not matching_pwsids:
        return []

    # Billing period
    billing_period = rate_struct.get("billing_period", "monthly")
    divisor = BILLING_DIVISOR.get(billing_period, 1)

    # Extract tiers from bill curve
    tiers = _extract_tiers_from_bill_curve(bill_water)
    tier_data = _tiers_to_schema(tiers, divisor)

    # Fixed charge: bill at 0 gallons is the base charge per billing period
    base_charge_per_period = bill_water.get("0")
    fixed_charge_monthly = None
    if base_charge_per_period is not None:
        fixed_charge_monthly = round(float(base_charge_per_period) / divisor, 2)

    # Bill snapshots (monthly equivalents)
    bill_5ccf = _compute_monthly_bill(bill_water, 5.0, divisor)
    bill_10ccf = _compute_monthly_bill(bill_water, 10.0, divisor)

    # Effective date
    eff_date = _parse_effective_date(rate_struct.get("first_effective_date"))

    # Determine rate structure type from tier count
    vol_tiers = [t for t in tiers if not t["is_allowance"]]
    if len(vol_tiers) == 0:
        structure_type = "flat"
    elif len(vol_tiers) == 1:
        structure_type = "uniform"
    else:
        # Check if rates are increasing or decreasing
        rates = [t["rate_per_kgal"] for t in vol_tiers]
        if all(rates[i] <= rates[i + 1] for i in range(len(rates) - 1)):
            structure_type = "increasing_block"
        elif all(rates[i] >= rates[i + 1] for i in range(len(rates) - 1)):
            structure_type = "decreasing_block"
        else:
            structure_type = "increasing_block"  # mixed → dominant pattern

    # Confidence
    has_tiers = tier_data.get("tier_1_rate") is not None
    has_bills = bill_5ccf is not None and bill_10ccf is not None
    if has_tiers and has_bills:
        confidence = "high"
    elif has_bills:
        confidence = "medium"
    else:
        confidence = "low"

    # Notes
    notes_parts = []
    if tier_data.get("overflow_note"):
        notes_parts.append(tier_data["overflow_note"])

    ownership = rate_struct.get("utility_ownership_type", "")
    if ownership and ownership.lower() != "municipality":
        notes_parts.append(f"Ownership: {ownership}")

    county = rate_struct.get("primary_county")

    utility_name = rate_struct.get("name", "").strip() or None

    # Build one record per matching PWSID
    records = []
    for pwsid in matching_pwsids:
        records.append({
            "pwsid": pwsid,
            "utility_name": utility_name,
            "state_code": "FL",
            "county": county,
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
            "source_url": f"https://dashboards.efc.sog.unc.edu/fl",
            "raw_text_hash": None,
            "parse_confidence": confidence,
            "parse_model": None,
            "parse_notes": "; ".join(notes_parts) if notes_parts else None,
        })

    return records


# --- Main Ingest ---


def run_efc_fl_ingest(
    dry_run: bool = False,
    refresh: bool = False,
) -> dict:
    """Run the UNC EFC FL water rate ingest.

    Parameters
    ----------
    dry_run : bool
        If True, fetch and parse but don't write to DB.
    refresh : bool
        If True, force re-fetch from API even if cache exists.

    Returns
    -------
    dict
        Summary stats.
    """
    started = datetime.now(timezone.utc)
    logger.info("=== UNC EFC FL Rate Ingest Starting ===")

    # Step 1: Fetch data (from cache or API)
    cache = fetch_all_utilities(refresh=refresh)
    if not cache:
        logger.error("No API data available. Check utility mapping file.")
        return {"error": "No data"}

    logger.info(f"API responses loaded: {len(cache)}")

    # Step 2: Load FL PWSIDs from database
    db_pwsids = _get_fl_pwsids_in_db()
    logger.info(f"FL PWSIDs in cws_boundaries: {len(db_pwsids)}")

    # Step 3: Parse all responses
    stats = {
        "total_api_responses": len(cache),
        "matched": 0,
        "inserted": 0,
        "skipped_no_pwsid": 0,
        "skipped_not_in_db": 0,
        "duplicate_pwsids": [],
    }

    records = []
    seen_pwsids = {}  # pwsid → utility_name (for dedup tracking)

    for rs_id, data in cache.items():
        rate_struct = data.get("rate_structure", {})
        sdwis_list = rate_struct.get("sdwis") or []

        # Check if utility has any PWSIDs at all
        pwsids_raw = [
            s.get("pwsid", "").strip()
            for s in sdwis_list
            if s.get("pwsid", "").strip()
        ]

        if not pwsids_raw:
            stats["skipped_no_pwsid"] += 1
            continue

        # Check if any PWSIDs are in our DB
        fl_pwsids = [p for p in pwsids_raw if p.startswith("FL")]
        if not any(p in db_pwsids for p in fl_pwsids):
            stats["skipped_not_in_db"] += 1
            continue

        parsed = _parse_api_response(rs_id, data, db_pwsids)

        for record in parsed:
            pwsid = record["pwsid"]
            utility_name = record["utility_name"] or ""

            # Dedup
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

        # Summary statistics
        if records:
            bills_10 = [r["bill_10ccf"] for r in records if r["bill_10ccf"] is not None]
            if bills_10:
                avg_bill = sum(bills_10) / len(bills_10)
                min_bill = min(bills_10)
                max_bill = max(bills_10)
                logger.info(
                    f"Bill @10CCF stats: avg=${avg_bill:.2f}, "
                    f"min=${min_bill:.2f}, max=${max_bill:.2f}"
                )

        stats["inserted"] = 0
        return stats

    # Step 4: Write to database
    schema = settings.utility_schema

    with engine.connect() as conn:
        # Delete existing records (idempotent re-run)
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
            "step": "efc-fl-ingest",
            "started": started,
            "count": stats["inserted"],
            "notes": (
                f"source={SOURCE_TAG}, api_responses={stats['total_api_responses']}, "
                f"matched={stats['matched']}, inserted={stats['inserted']}, "
                f"skipped_no_pwsid={stats['skipped_no_pwsid']}, "
                f"skipped_not_in_db={stats['skipped_not_in_db']}, "
                f"duplicates={len(stats['duplicate_pwsids'])}"
            ),
        })
        conn.commit()

    logger.info(f"=== EFC FL Ingest Complete ({elapsed:.1f}s) ===")
    return stats
