#!/usr/bin/env python3
"""
Generic UNC EFC Dashboard API Ingest

Purpose:
    Fetches water rate data from any UNC EFC state dashboard via the
    shared Topsail JSON API. All 24 EFC dashboards use the same platform
    at dashboards.efc.sog.unc.edu with identical API endpoints.

    For each state, the module:
    1. Fetches the dashboard HTML to extract rate_structure_ids
    2. Calls the JSON API for each utility (bill curves at 500-gal steps)
    3. Extracts tier structure from bill curve (marginal rate detection)
    4. Writes to utility.rate_schedules

    State configuration is in config/efc_dashboards.yaml.

Author: AI-Generated
Created: 2026-03-26
Modified: 2026-03-26

Dependencies:
    - httpx (HTTP client)
    - pyyaml (config)
    - sqlalchemy (database)
    - loguru (logging)

Usage:
    # CLI
    ua-ingest efc --state WI --dry-run
    ua-ingest efc --state GA
    ua-ingest efc --all --skip-ingested
    ua-ingest efc --list

    # Python
    from utility_api.ingest.efc_generic import run_efc_ingest
    run_efc_ingest(states=["WI"], dry_run=True)

Notes:
    - API: /dashboards/{id}/chart_data.json (one call per utility)
    - Bill curve: 31 data points (0–15,000 gal in 500-gal steps)
    - Some states have fewer bill points or empty water bills for some
      utilities (sewer-only entries). These are skipped gracefully.
    - PWSIDs are in rate_structure.sdwis[].pwsid — some utilities
      lack PWSIDs (skipped) or share PWSIDs (first wins).
    - Polite pacing: 0.5s between requests (configurable).

Data Sources:
    - Input: EFC Topsail JSON API
    - Input: utility.cws_boundaries (PWSID filter)
    - Output: utility.rate_schedules table
    - Config: config/efc_dashboards.yaml

Configuration:
    - API responses cached at data/raw/efc_{state}/api_cache.json
    - Dashboard config at config/efc_dashboards.yaml
    - Database connection via .env (DATABASE_URL)
"""

import json
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path

import httpx
import yaml
from loguru import logger
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine
from utility_api.ops.rate_schedule_helpers import water_rate_to_schedule, write_rate_schedule


# --- Constants ---

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_FILE = PROJECT_ROOT / "config" / "efc_dashboards.yaml"
DATA_BASE_DIR = PROJECT_ROOT / "data" / "raw"

# EFC API
API_BASE = "https://dashboards.efc.sog.unc.edu"
REQUEST_DELAY_SECS = 0.5
REQUEST_TIMEOUT_SECS = 30.0

# Unit conversions (same as FL/NC modules)
GAL_PER_CCF = 748.052
KGAL_TO_CCF = 0.748052

# Bill curve gallon levels (0 to 15,000 in 500-gal steps)
BILL_GALS = list(range(0, 15500, 500))

# Billing period divisors
BILLING_DIVISOR = {"monthly": 1, "bimonthly": 2, "quarterly": 3}

# Tier detection tolerance ($/kgal)
TIER_TOLERANCE_PER_KGAL = 0.05


# --- Config ---


def load_efc_config() -> dict:
    """Load EFC dashboard configuration from YAML.

    Returns
    -------
    dict
        Full config with 'states' key mapping state codes to config dicts.
    """
    with open(CONFIG_FILE) as f:
        return yaml.safe_load(f)


def get_state_config(state: str) -> dict | None:
    """Get config for a specific state.

    Parameters
    ----------
    state : str
        Two-letter state code (case-insensitive).

    Returns
    -------
    dict | None
        State config dict, or None if not found.
    """
    config = load_efc_config()
    return config.get("states", {}).get(state.lower())


# --- Dashboard HTML Extraction ---


def extract_utility_ids(state_lower: str, dashboard_id: int) -> list[dict]:
    """Fetch dashboard HTML and extract rate_structure_ids.

    Parameters
    ----------
    state_lower : str
        Lowercase state abbreviation (e.g., 'wi').
    dashboard_id : int
        EFC dashboard ID.

    Returns
    -------
    list[dict]
        Each dict has 'rate_structure_id' (int) and 'utility_name' (str).
    """
    url = f"{API_BASE}/dashboards/{dashboard_id}.html"
    logger.info(f"Fetching dashboard HTML: {url}")

    resp = httpx.get(url, timeout=REQUEST_TIMEOUT_SECS)
    resp.raise_for_status()
    html = resp.text

    # Extract <option value="NNN">Name</option> pairs
    options = re.findall(
        r'<option\s+value="(\d+)"[^>]*>([^<]+)</option>', html
    )

    utilities = []
    for rs_id, name in options:
        utilities.append({
            "rate_structure_id": int(rs_id),
            "utility_name": name.strip(),
        })

    logger.info(f"Extracted {len(utilities)} utility IDs from dashboard {dashboard_id}")
    return utilities


# --- API Fetch ---


def fetch_utility_data(
    client: httpx.Client,
    dashboard_id: int,
    rate_structure_id: int,
) -> dict | None:
    """Fetch a single utility's data from the EFC API.

    Parameters
    ----------
    client : httpx.Client
        Reusable HTTP client.
    dashboard_id : int
        EFC dashboard ID.
    rate_structure_id : int
        The utility's rate_structure_id.

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
            f"{API_BASE}/dashboards/{dashboard_id}/chart_data.json",
            params=params,
            timeout=REQUEST_TIMEOUT_SECS,
        )
        resp.raise_for_status()
        return resp.json()
    except (httpx.HTTPError, json.JSONDecodeError) as e:
        logger.warning(f"  Failed rs_id={rate_structure_id}: {e}")
        return None


def fetch_all_for_state(
    state_lower: str,
    dashboard_id: int,
    refresh: bool = False,
) -> dict:
    """Fetch all utility data for a state, with caching.

    Parameters
    ----------
    state_lower : str
        Lowercase state abbreviation.
    dashboard_id : int
        EFC dashboard ID.
    refresh : bool
        Force re-fetch even if cache exists.

    Returns
    -------
    dict
        Mapping of rate_structure_id (str) → API response dict.
    """
    cache_dir = DATA_BASE_DIR / f"efc_{state_lower}"
    cache_file = cache_dir / "api_cache.json"

    if cache_file.exists() and not refresh:
        logger.info(f"Loading cached data: {cache_file}")
        with open(cache_file) as f:
            cache = json.load(f)
        logger.info(f"Cached: {len(cache)} utilities")
        return cache

    # Extract utility IDs from dashboard HTML
    utilities = extract_utility_ids(state_lower, dashboard_id)
    if not utilities:
        return {}

    logger.info(
        f"Fetching {len(utilities)} {state_lower.upper()} utilities "
        f"(~{len(utilities) * REQUEST_DELAY_SECS:.0f}s at {REQUEST_DELAY_SECS}s/req)"
    )

    cache = {}
    failures = 0

    with httpx.Client() as client:
        for i, util in enumerate(utilities):
            rs_id = util["rate_structure_id"]

            if (i + 1) % 50 == 0 or i == 0:
                logger.info(
                    f"  [{i + 1}/{len(utilities)}] {util['utility_name']} (id={rs_id})"
                )

            data = fetch_utility_data(client, dashboard_id, rs_id)
            if data is not None:
                cache[str(rs_id)] = data
            else:
                failures += 1

            if i < len(utilities) - 1:
                time.sleep(REQUEST_DELAY_SECS)

    logger.info(f"Fetched: {len(cache)} OK, {failures} failures")

    # Save cache
    cache_dir.mkdir(parents=True, exist_ok=True)
    with open(cache_file, "w") as f:
        json.dump(cache, f)
    logger.info(f"Cached to {cache_file} ({cache_file.stat().st_size / 1024:.0f} KB)")

    return cache


# --- Tier Extraction (from FL/NC modules) ---


def _extract_tiers_from_bill_curve(bill_water: dict) -> list[dict]:
    """Extract tier structure from bill curve by detecting marginal rate changes.

    Handles variable gallon increments (500-gal, 1000-gal, or custom).
    Auto-detects the increment from the bill curve keys.

    Parameters
    ----------
    bill_water : dict
        Mapping of gallon level (str) → bill amount (float).

    Returns
    -------
    list[dict]
        Each dict: start_gal, end_gal, rate_per_kgal, is_allowance.
    """
    # Build sorted (gallon, bill) pairs from whatever keys are present
    points = []
    for k, v in bill_water.items():
        try:
            gal = int(k)
            bill = float(v)
            points.append((gal, bill))
        except (ValueError, TypeError):
            continue

    points.sort(key=lambda x: x[0])

    if len(points) < 2:
        return []

    gal_levels = [p[0] for p in points]
    bill_values = [p[1] for p in points]

    # Compute marginal rates between each consecutive pair
    marginal_rates = []
    marginal_gals = []  # start gallon for each marginal rate
    for i in range(1, len(points)):
        delta_gal = gal_levels[i] - gal_levels[i - 1]
        if delta_gal <= 0:
            continue
        rate_per_kgal = (bill_values[i] - bill_values[i - 1]) / (delta_gal / 1000.0)
        marginal_rates.append(rate_per_kgal)
        marginal_gals.append(gal_levels[i - 1])

    if not marginal_rates:
        return []

    # Group consecutive similar rates into tiers
    tiers = []
    current_rate = marginal_rates[0]
    current_start_gal = marginal_gals[0]

    for i in range(1, len(marginal_rates)):
        if abs(marginal_rates[i] - current_rate) > TIER_TOLERANCE_PER_KGAL:
            tiers.append({
                "start_gal": current_start_gal,
                "end_gal": marginal_gals[i],
                "rate_per_kgal": current_rate,
                "is_allowance": current_rate < TIER_TOLERANCE_PER_KGAL,
            })
            current_rate = marginal_rates[i]
            current_start_gal = marginal_gals[i]

    tiers.append({
        "start_gal": current_start_gal,
        "end_gal": None,
        "rate_per_kgal": current_rate,
        "is_allowance": current_rate < TIER_TOLERANCE_PER_KGAL,
    })

    return tiers


def _tiers_to_schema(tiers: list[dict], divisor: int) -> dict:
    """Convert extracted tiers to water_rates 4-tier schema columns.

    Parameters
    ----------
    tiers : list[dict]
        From _extract_tiers_from_bill_curve().
    divisor : int
        Billing period divisor.

    Returns
    -------
    dict
        tier_N_limit_ccf, tier_N_rate (N=1..4), overflow_note.
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
    """Interpolate bill at any gallon consumption level.

    Handles arbitrary gallon increments by finding the two bracketing
    data points and linearly interpolating between them.
    """
    if target_gal < 0:
        return None

    # Build sorted gallon→bill pairs
    points = []
    for k, v in bill_water.items():
        try:
            points.append((int(k), float(v)))
        except (ValueError, TypeError):
            continue
    points.sort()

    if not points:
        return None

    # Below range
    if target_gal <= points[0][0]:
        return points[0][1]

    # Beyond range — extrapolate from last two
    if target_gal > points[-1][0]:
        if len(points) >= 2:
            gal_a, val_a = points[-2]
            gal_b, val_b = points[-1]
            delta = gal_b - gal_a
            if delta > 0:
                rate = (val_b - val_a) / delta
                return val_b + rate * (target_gal - gal_b)
        return None

    # Find bracketing points
    for i in range(1, len(points)):
        if points[i][0] >= target_gal:
            gal_a, val_a = points[i - 1]
            gal_b, val_b = points[i]
            delta = gal_b - gal_a
            if delta <= 0:
                return val_a
            frac = (target_gal - gal_a) / delta
            return val_a + frac * (val_b - val_a)

    return None


def _compute_monthly_bill(bill_water: dict, ccf: float, divisor: int) -> float | None:
    """Compute monthly bill at a given CCF/month level."""
    gal_per_period = ccf * GAL_PER_CCF * divisor
    bill_per_period = _interpolate_bill(bill_water, gal_per_period)
    if bill_per_period is None:
        return None
    return round(bill_per_period / divisor, 2)


# --- Date Parsing ---


def _parse_effective_date(val: str | None) -> date | None:
    """Parse EFC effective date (MM/DD/YYYY)."""
    if not val:
        return None
    try:
        return datetime.strptime(val, "%m/%d/%Y").date()
    except (ValueError, TypeError):
        return None


# --- PWSID Lookup ---


def _get_state_pwsids_in_db(state_code: str) -> set[str]:
    """Get PWSIDs for a state that exist in cws_boundaries."""
    schema = settings.utility_schema
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT pwsid FROM {schema}.cws_boundaries
            WHERE pwsid LIKE :prefix
        """), {"prefix": f"{state_code}%"}).fetchall()
    return {r[0] for r in rows}


# --- Record Parsing ---


def _parse_api_response(
    data: dict,
    state_code: str,
    source_key: str,
    db_pwsids: set[str],
) -> list[dict]:
    """Parse one API response into water_rates records.

    Parameters
    ----------
    data : dict
        Full API response.
    state_code : str
        Two-letter state code (uppercase).
    source_key : str
        Source tag for DB records.
    db_pwsids : set[str]
        PWSIDs in our database for this state.

    Returns
    -------
    list[dict]
        Parsed records. May be empty.
    """
    rate_struct = data.get("rate_structure", {})
    bill = data.get("bill", {})
    bill_water = bill.get("water", {})

    # Skip if no water bill data
    if not bill_water or len(bill_water) < 2:
        return []

    # Extract PWSIDs
    sdwis_list = rate_struct.get("sdwis") or []
    pwsids = [
        s.get("pwsid", "").strip()
        for s in sdwis_list
        if s.get("pwsid", "").strip().startswith(state_code)
    ]

    if not pwsids:
        return []

    matching_pwsids = [p for p in pwsids if p in db_pwsids]
    if not matching_pwsids:
        return []

    # Billing period
    billing_period = rate_struct.get("billing_period") or "monthly"
    divisor = BILLING_DIVISOR.get(billing_period, 1)

    # Tiers
    tiers = _extract_tiers_from_bill_curve(bill_water)
    tier_data = _tiers_to_schema(tiers, divisor)

    # Fixed charge (bill at 0 gal)
    base_charge = bill_water.get("0")
    fixed_charge_monthly = None
    if base_charge is not None:
        fixed_charge_monthly = round(float(base_charge) / divisor, 2)

    # Bill snapshots
    bill_5ccf = _compute_monthly_bill(bill_water, 5.0, divisor)
    bill_10ccf = _compute_monthly_bill(bill_water, 10.0, divisor)

    # Effective date
    eff_date = _parse_effective_date(
        rate_struct.get("first_effective_date")
        or rate_struct.get("bill_effective_date")
    )

    # Structure type
    vol_tiers = [t for t in tiers if not t["is_allowance"]]
    if len(vol_tiers) == 0:
        structure_type = "flat"
    elif len(vol_tiers) == 1:
        structure_type = "uniform"
    else:
        rates = [t["rate_per_kgal"] for t in vol_tiers]
        if all(rates[i] <= rates[i + 1] for i in range(len(rates) - 1)):
            structure_type = "increasing_block"
        elif all(rates[i] >= rates[i + 1] for i in range(len(rates) - 1)):
            structure_type = "decreasing_block"
        else:
            structure_type = "increasing_block"

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

    utility_name = rate_struct.get("name", "").strip() or None
    county = rate_struct.get("primary_county")
    source_url = f"https://dashboards.efc.sog.unc.edu/{state_code.lower()}"

    records = []
    for pwsid in matching_pwsids:
        records.append({
            "pwsid": pwsid,
            "utility_name": utility_name,
            "state_code": state_code,
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
            "source": source_key,
            "source_url": source_url,
            "raw_text_hash": None,
            "parse_confidence": confidence,
            "parse_model": None,
            "parse_notes": "; ".join(notes_parts) if notes_parts else None,
        })

    return records


# --- Main Ingest ---


def run_efc_state_ingest(
    state: str,
    dry_run: bool = False,
    refresh: bool = False,
) -> dict:
    """Run EFC ingest for a single state.

    Parameters
    ----------
    state : str
        Two-letter state code (case-insensitive).
    dry_run : bool
        Parse and report without DB writes.
    refresh : bool
        Force re-fetch from API.

    Returns
    -------
    dict
        Summary stats.
    """
    state_lower = state.lower()
    state_upper = state.upper()
    started = datetime.now(timezone.utc)

    # Load config
    cfg = get_state_config(state_lower)
    if not cfg:
        logger.error(f"No config for state '{state_upper}' in {CONFIG_FILE}")
        return {"error": f"No config for {state_upper}"}

    dashboard_id = cfg["dashboard_id"]
    source_key = cfg["source_key"]
    state_code = cfg.get("state_code", state_upper)

    logger.info(f"=== EFC {state_upper} Ingest Starting (dashboard={dashboard_id}) ===")

    # Fetch data
    cache = fetch_all_for_state(state_lower, dashboard_id, refresh=refresh)
    if not cache:
        logger.error(f"No API data for {state_upper}")
        return {"error": "No data"}

    logger.info(f"API responses loaded: {len(cache)}")

    # Load PWSIDs
    db_pwsids = _get_state_pwsids_in_db(state_code)
    logger.info(f"{state_code} PWSIDs in cws_boundaries: {len(db_pwsids)}")

    # Parse
    stats = {
        "state": state_upper,
        "source_key": source_key,
        "total_api_responses": len(cache),
        "matched": 0,
        "inserted": 0,
        "skipped_no_water_bill": 0,
        "skipped_no_pwsid": 0,
        "skipped_not_in_db": 0,
        "duplicate_pwsids": 0,
    }

    records = []
    seen_pwsids = {}

    for rs_id, data in cache.items():
        bill = data.get("bill", {})
        bill_water = bill.get("water", {})

        if not bill_water or len(bill_water) < 2:
            stats["skipped_no_water_bill"] += 1
            continue

        rate_struct = data.get("rate_structure", {})
        sdwis_list = rate_struct.get("sdwis") or []
        pwsids_raw = [
            s.get("pwsid", "").strip()
            for s in sdwis_list
            if s.get("pwsid", "").strip()
        ]

        if not pwsids_raw:
            stats["skipped_no_pwsid"] += 1
            continue

        state_pwsids = [p for p in pwsids_raw if p.startswith(state_code)]
        if not any(p in db_pwsids for p in state_pwsids):
            stats["skipped_not_in_db"] += 1
            continue

        parsed = _parse_api_response(data, state_code, source_key, db_pwsids)

        for record in parsed:
            pwsid = record["pwsid"]
            if pwsid in seen_pwsids:
                stats["duplicate_pwsids"] += 1
                continue
            seen_pwsids[pwsid] = record["utility_name"]
            records.append(record)
            stats["matched"] += 1

    logger.info(f"Records to insert: {len(records)}")
    logger.info(
        f"Skipped: no_water_bill={stats['skipped_no_water_bill']}, "
        f"no_pwsid={stats['skipped_no_pwsid']}, "
        f"not_in_db={stats['skipped_not_in_db']}, "
        f"duplicates={stats['duplicate_pwsids']}"
    )

    # Dry run
    if dry_run:
        logger.info("[DRY RUN] Sample records:")
        for r in records[:5]:
            tier_str = ""
            for i in range(1, 5):
                rate = r.get(f"tier_{i}_rate")
                limit = r.get(f"tier_{i}_limit_ccf")
                if rate is not None:
                    limit_str = f"{limit:.1f}" if limit else "∞"
                    tier_str += f" T{i}:{limit_str}@${rate:.4f}"
            logger.info(
                f"  {r['pwsid']} | {(r['utility_name'] or '')[:30]:30s} | "
                f"base=${r['fixed_charge_monthly'] or 0:.2f} | "
                f"@5ccf=${r['bill_5ccf'] or 0:.2f} | "
                f"@10ccf=${r['bill_10ccf'] or 0:.2f} | "
                f"[{r['parse_confidence']}] |{tier_str}"
            )
        if len(records) > 5:
            logger.info(f"  ... and {len(records) - 5} more")

        if records:
            bills = [r["bill_10ccf"] for r in records if r["bill_10ccf"] is not None]
            if bills:
                logger.info(
                    f"Bill @10CCF: avg=${sum(bills)/len(bills):.2f}, "
                    f"min=${min(bills):.2f}, max=${max(bills):.2f}"
                )

        stats["inserted"] = 0
        return stats

    # Write to DB
    schema = settings.utility_schema

    with engine.connect() as conn:
        deleted = conn.execute(text(f"""
            DELETE FROM {schema}.rate_schedules WHERE source_key = :source
        """), {"source": source_key}).rowcount
        if deleted:
            logger.info(f"Cleared {deleted} existing {source_key} records from rate_schedules")

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
            "step": f"efc-{state_lower}-ingest",
            "started": started,
            "count": stats["inserted"],
            "notes": (
                f"source={source_key}, api_responses={stats['total_api_responses']}, "
                f"matched={stats['matched']}, inserted={stats['inserted']}, "
                f"no_water_bill={stats['skipped_no_water_bill']}, "
                f"no_pwsid={stats['skipped_no_pwsid']}, "
                f"not_in_db={stats['skipped_not_in_db']}, "
                f"duplicates={stats['duplicate_pwsids']}"
            ),
        })
        conn.commit()

    logger.info(f"=== EFC {state_upper} Ingest Complete ({elapsed:.1f}s) ===")
    return stats


def run_efc_ingest(
    states: list[str] | None = None,
    all_states: bool = False,
    skip_ingested: bool = False,
    dry_run: bool = False,
    refresh: bool = False,
) -> list[dict]:
    """Run EFC ingest for one or more states.

    Parameters
    ----------
    states : list[str] | None
        Specific states to ingest.
    all_states : bool
        If True, ingest all configured states.
    skip_ingested : bool
        Skip states marked as 'ingested' in config.
    dry_run : bool
        Parse and report without DB writes.
    refresh : bool
        Force re-fetch from API.

    Returns
    -------
    list[dict]
        Summary stats per state.
    """
    config = load_efc_config()
    all_cfg = config.get("states", {})

    if all_states:
        target_states = list(all_cfg.keys())
    elif states:
        target_states = [s.lower() for s in states]
    else:
        logger.error("Specify --state or --all")
        return []

    if skip_ingested:
        target_states = [
            s for s in target_states
            if all_cfg.get(s, {}).get("status") != "ingested"
        ]

    results = []
    for st in sorted(target_states):
        if st not in all_cfg:
            logger.warning(f"State '{st.upper()}' not in config — skipping")
            continue
        stats = run_efc_state_ingest(st, dry_run=dry_run, refresh=refresh)
        results.append(stats)

    # Summary
    total_inserted = sum(s.get("inserted", 0) for s in results)
    total_matched = sum(s.get("matched", 0) for s in results)
    logger.info(f"\n=== EFC INGEST SUMMARY ===")
    logger.info(f"States processed: {len(results)}")
    logger.info(f"Total matched: {total_matched}")
    logger.info(f"Total inserted: {total_inserted}")

    for s in results:
        st = s.get("state", "?")
        ins = s.get("inserted", s.get("matched", 0))
        logger.info(f"  {st}: {ins} records")

    return results


def list_efc_states() -> None:
    """Print a summary of all configured EFC states."""
    config = load_efc_config()
    states = config.get("states", {})

    logger.info(f"{'State':<6s} {'ID':>4s} {'Utils':>6s} {'Vintage':>8s} {'Status':>10s} {'Source Key'}")
    logger.info("-" * 65)
    for st, cfg in sorted(states.items()):
        logger.info(
            f"{st.upper():<6s} {cfg['dashboard_id']:>4d} "
            f"{cfg.get('utility_count', '?'):>6} "
            f"{cfg.get('vintage', '?'):>8s} "
            f"{cfg.get('status', 'pending'):>10s} "
            f"{cfg.get('source_key', '')}"
        )
