#!/usr/bin/env python3
"""
Duke NIEPS 10-State Water Affordability Data Ingest

Purpose:
    Ingests Duke/Nicholas Institute rate data into the canonical rate_schedules
    table for free-attributed distribution. This data is CC BY-NC-ND 4.0 and
    is served for free with full attribution — never behind a paywall.

    The Duke dataset provides FULL rate structures (fixed charges + volumetric
    tier breakpoints) for 5,371 PWSIDs across 10 states. This is richer than
    EFC dashboards (which only provide bill curves) and covers states/utilities
    not available from any other source.

    The existing duke_reference_ingest.py wrote to the internal-only
    duke_reference_rates table. This module supersedes it for production use,
    writing to rate_schedules with source_key="duke_nieps_10state" and
    tier="free_attributed".

Author: AI-Generated
Created: 2026-03-26
Modified: 2026-04-02

Dependencies:
    - openpyxl (Excel parsing)
    - sqlalchemy (database)
    - loguru (logging)

Usage:
    # CLI
    ua-ingest duke-nieps --state TX --dry-run
    ua-ingest duke-nieps --all --dry-run
    ua-ingest duke-nieps --all

    # Python
    from utility_api.ingest.duke_nieps_ingest import run_duke_nieps_ingest
    run_duke_nieps_ingest(states=["TX"], dry_run=True)

Notes:
    - Excel files use 'NA' as string for missing values
    - value_to = 1000000000 means unlimited (last tier)
    - meter_size = 0.625 is standard residential 5/8 inch
    - adjustment field is a multiplier, usually 1.0
    - bill_frequency varies — normalized to monthly for fixed charges
    - effective_date in rateTable is year integer, in ratesMetadata is datetime
    - Filters to rate_code='water' only (excludes sewer/storm/septic)
    - Filters to meter_size=0.625 for service charges (residential)
    - vol_unit can be 'gallons', 'cubic feet', or variants with trailing spaces
    - TX tends to use gallons; some NE states use cubic feet
    - Non-volume units (square feet, bedrooms, ERU) are stormwater — skipped
    - Deduplication: uses UPSERT keyed on (pwsid, source_key, vintage_date, customer_class)

Data Sources:
    - Input: data/duke_raw/data/rates_data/rates_{state}.xlsx
    - Input: utility.cws_boundaries (PWSID filter)
    - Output: utility.rate_schedules (source_key: duke_nieps_10state)
    - Output: utility.source_catalog (provenance seed)
    - Output: utility.pipeline_runs (audit trail)

Configuration:
    - Duke repo cloned at data/duke_raw/
    - Database connection via .env (DATABASE_URL)
"""

import json
import re
from datetime import date, datetime, timezone
from pathlib import Path

import openpyxl
from loguru import logger
from sqlalchemy import text

from utility_api.config import PROJECT_ROOT, settings
from utility_api.db import engine


# --- Constants ---

DUKE_DATA_DIR = PROJECT_ROOT / "data" / "duke_raw" / "data" / "rates_data"

ALL_STATES = ["tx", "ks", "pa", "wa", "nj", "nm", "or", "ca", "nc", "ct"]

SOURCE_KEY = "duke_nieps_10state"

GAL_PER_CCF = 748.052
GAL_PER_CUBIC_FOOT = 7.48052

# Billing frequency divisors (to monthly)
FREQ_DIVISOR = {
    "monthly": 1,
    "bi-monthly": 2,
    "quarterly": 3,
    "semi-annually": 6,
    "bi-annually": 6,  # CT uses this; same meaning as semi-annually
    "annually": 12,
}

UNLIMITED = 1_000_000_000

# Valid volumetric unit prefixes (after strip + lower)
VOLUME_UNITS_GALLONS = {"gallons", "gallon", "gal"}
VOLUME_UNITS_CUBIC_FEET = {"cubic feet", "cubic foot", "cf"}


# --- Helpers ---


def _safe_val(val):
    """Convert Excel cell value, treating 'NA' and None as None."""
    if val is None or str(val).strip() in ("NA", "None", ""):
        return None
    return val


def _safe_float(val) -> float | None:
    """Convert to float, treating NA/None as None."""
    v = _safe_val(val)
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _parse_effective_date(val) -> date | None:
    """Parse effective_date from ratesMetadata (datetime) or rateTable (year int).

    Parameters
    ----------
    val : any
        Raw cell value — may be datetime, date, year int, or string.

    Returns
    -------
    date | None
        Parsed date or None if unparseable.
    """
    if val is None or str(val).strip() in ("NA", "None", ""):
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    try:
        year = int(val)
        if 2000 <= year <= 2030:
            return date(year, 1, 1)
    except (ValueError, TypeError):
        pass
    return None


def _classify_vol_unit(raw_unit: str | None) -> str | None:
    """Classify a vol_unit value as 'gallons', 'cubic_feet', or None.

    Parameters
    ----------
    raw_unit : str | None
        Raw vol_unit from Excel (e.g., 'gallons', 'cubic feet ', 'NA').

    Returns
    -------
    str | None
        'gallons', 'cubic_feet', or None (non-volume unit, skip this row).
    """
    if raw_unit is None:
        return None
    cleaned = str(raw_unit).strip().lower()
    if cleaned in VOLUME_UNITS_GALLONS:
        return "gallons"
    if cleaned in VOLUME_UNITS_CUBIC_FEET:
        return "cubic_feet"
    return None  # square feet, bedrooms, ERU, etc. — not volumetric water


def _normalize_pwsid(raw: str, state: str) -> str | None:
    """Normalize Duke PWSID format to SDWIS format.

    Duke uses state-specific formats:
    - NC: '03-63-020' → 'NC0363020'
    - Most states: numeric only → prefix with state code
    - Some already have state prefix

    Parameters
    ----------
    raw : str
        Raw PWSID from Excel.
    state : str
        Two-letter state code (lowercase).

    Returns
    -------
    str | None
        Normalized PWSID (e.g., 'NC0363020') or None if unparseable.
    """
    raw = str(raw).strip()
    if not raw or raw in ("NA", "None"):
        return None

    st = state.upper()

    # Already has state prefix
    if raw.upper().startswith(st):
        return raw.upper().replace("-", "").replace(" ", "")

    # NC-style dashed format: NN-NN-NNN
    m = re.match(r"^(\d{2})-(\d{2,3})-(\d{3,4})$", raw)
    if m:
        return f"{st}{''.join(m.groups())}"

    # NJ-style: NNNNNNN (7 digits)
    m = re.match(r"^(\d{7})$", raw)
    if m:
        return f"{st}{raw}"

    # Generic: strip dashes, prefix with state
    cleaned = raw.replace("-", "").replace(" ", "")
    if cleaned.isdigit():
        return f"{st}{cleaned}"

    return raw.upper()


# --- Excel Reading ---


def _read_xlsx_sheet(filepath: Path, sheet_name: str) -> list[dict]:
    """Read an Excel sheet into a list of dicts.

    Parameters
    ----------
    filepath : Path
        Path to .xlsx file.
    sheet_name : str
        Sheet name.

    Returns
    -------
    list[dict]
        One dict per row, keys are column headers (stripped).
    """
    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb[sheet_name]

    rows_iter = ws.iter_rows(values_only=True)
    headers = [str(h).strip() if h else f"col_{i}" for i, h in enumerate(next(rows_iter))]

    result = []
    for row in rows_iter:
        d = {}
        for i, val in enumerate(row):
            if i < len(headers):
                d[headers[i]] = val
        result.append(d)

    wb.close()
    return result


# --- Rate Structure Extraction ---


def _extract_rate_structure(
    rate_rows: list[dict],
    bill_frequency_default: str = "monthly",
) -> dict:
    """Extract rate structure from rateTable rows for one PWSID.

    Handles unit normalization: tiers defined in gallons stay as-is,
    tiers defined in cubic feet are converted to gallons. Rates are
    always stored as $/1000 gallons.

    Parameters
    ----------
    rate_rows : list[dict]
        rateTable rows filtered to one PWSID + rate_code='water'.
    bill_frequency_default : str
        Default billing frequency if not specified in data.

    Returns
    -------
    dict
        Contains: fixed_charge_monthly, fixed_charges_jsonb, volumetric_tiers,
        bill_frequency, rate_structure_type, tier_count.
    """
    # Determine billing frequency (from first row that has it)
    bill_freq = bill_frequency_default
    for r in rate_rows:
        bf = _safe_val(r.get("bill_frequency"))
        if bf:
            bill_freq = str(bf).strip().lower()
            break
    divisor = FREQ_DIVISOR.get(bill_freq, 1)

    # --- Fixed charges (service_charge for 5/8 inch residential meter) ---
    fixed_charge = None
    fixed_charges_jsonb = []

    service_charges = [
        r for r in rate_rows
        if str(r.get("rate_type", "")).strip() == "service_charge"
    ]

    if service_charges:
        # Prefer meter_size = 0.625 (5/8 inch residential)
        target_row = None
        for r in service_charges:
            ms = _safe_float(r.get("meter_size"))
            if ms is not None and abs(ms - 0.625) < 0.01:
                target_row = r
                break

        # Fallback: smallest meter size
        if target_row is None:
            sized = [(r, _safe_float(r.get("meter_size")) or 999) for r in service_charges]
            sized.sort(key=lambda x: x[1])
            target_row = sized[0][0]

        if target_row:
            cost = _safe_float(target_row.get("cost"))
            adj = _safe_float(target_row.get("adjustment")) or 1.0
            if cost is not None:
                fixed_charge = round(cost * adj / divisor, 2)
                ms_val = _safe_float(target_row.get("meter_size"))
                fixed_charges_jsonb.append({
                    "name": "Service Charge",
                    "amount": fixed_charge,
                    "meter_size": f'{ms_val}"' if ms_val else "5/8\"",
                })

    # --- Volumetric tiers ---
    tiers = []
    vol_rows = [
        r for r in rate_rows
        if str(r.get("rate_type", "")).strip().startswith("commodity_charge")
        and str(r.get("volumetric", "")).strip().lower() == "yes"
    ]

    for r in vol_rows:
        val_from = _safe_float(r.get("value_from"))
        val_to = _safe_float(r.get("value_to"))
        vol_base = _safe_float(r.get("vol_base"))
        cost = _safe_float(r.get("cost"))
        adj = _safe_float(r.get("adjustment")) or 1.0
        raw_unit = _safe_val(r.get("vol_unit"))

        if cost is None or vol_base is None or vol_base <= 0:
            continue

        # Classify unit
        unit_class = _classify_vol_unit(raw_unit)
        if unit_class is None:
            # Non-volume unit (square feet, bedrooms, etc.) — skip
            continue

        if unit_class == "cubic_feet":
            # Convert tier boundaries from cubic feet to gallons
            gal_from = (val_from or 0) * GAL_PER_CUBIC_FOOT
            gal_to = (
                val_to * GAL_PER_CUBIC_FOOT
                if val_to and val_to < UNLIMITED
                else None
            )
            # Convert rate: cost is per vol_base cubic feet
            # → $/1000 gallons = (cost * adj) / (vol_base * GAL_PER_CUBIC_FOOT / 1000)
            rate_per_1000_gal = (cost * adj) / (vol_base * GAL_PER_CUBIC_FOOT / 1000.0)
        else:
            # Already in gallons
            gal_from = val_from or 0
            gal_to = val_to if val_to and val_to < UNLIMITED else None
            # cost is per vol_base gallons → $/1000 gallons
            rate_per_1000_gal = (cost * adj) / (vol_base / 1000.0)

        tiers.append({
            "min_gal": round(gal_from, 0),
            "max_gal": round(gal_to, 0) if gal_to else None,
            "rate_per_1000_gal": round(rate_per_1000_gal, 4),
        })

    # Sort by min_gal, deduplicate, make contiguous, assign tier numbers
    tiers.sort(key=lambda t: t["min_gal"])

    # Deduplicate tiers with identical boundaries + rate
    seen = set()
    unique_tiers = []
    for t in tiers:
        key = (t.get("min_gal"), t.get("max_gal"), t.get("rate_per_1000_gal"))
        if key not in seen:
            seen.add(key)
            unique_tiers.append(t)
    tiers = unique_tiers

    # Make tier boundaries contiguous (tier N+1 min = tier N max)
    for i in range(1, len(tiers)):
        prev_max = tiers[i - 1].get("max_gal")
        if prev_max is not None:
            tiers[i]["min_gal"] = prev_max

    for i, t in enumerate(tiers):
        t["tier"] = i + 1

    # Determine structure type
    if not tiers:
        structure_type = "flat"
    elif len(tiers) == 1:
        structure_type = "uniform"
    else:
        rates = [t["rate_per_1000_gal"] for t in tiers]
        if all(rates[i] <= rates[i + 1] for i in range(len(rates) - 1)):
            structure_type = "increasing_block"
        elif all(rates[i] >= rates[i + 1] for i in range(len(rates) - 1)):
            structure_type = "decreasing_block"
        else:
            structure_type = "increasing_block"  # mixed — default to increasing

    return {
        "fixed_charge_monthly": fixed_charge,
        "fixed_charges_jsonb": fixed_charges_jsonb if fixed_charges_jsonb else None,
        "volumetric_tiers": tiers if tiers else None,
        "bill_frequency": bill_freq,
        "rate_structure_type": structure_type,
        "tier_count": len(tiers),
    }


def _calculate_bill(
    fixed_charge: float | None,
    tiers: list[dict] | None,
    gallons: float,
) -> float | None:
    """Calculate monthly bill at a given gallon consumption.

    Parameters
    ----------
    fixed_charge : float | None
        Monthly fixed charge (already normalized to monthly).
    tiers : list[dict] | None
        Volumetric tiers with min_gal, max_gal, rate_per_1000_gal.
        Tier boundaries are in gallons.
    gallons : float
        Monthly consumption in gallons.

    Returns
    -------
    float | None
        Monthly bill in USD, or None if no rate structure available.
    """
    bill = fixed_charge or 0.0

    if not tiers:
        return round(bill, 2) if fixed_charge else None

    remaining = gallons
    for tier in tiers:
        if remaining <= 0:
            break
        min_gal = tier["min_gal"]
        max_gal = tier["max_gal"]
        rate = tier["rate_per_1000_gal"]

        if max_gal is not None:
            tier_width = max_gal - min_gal
            tier_gal = min(remaining, tier_width)
        else:
            tier_gal = remaining

        bill += (tier_gal / 1000.0) * rate
        remaining -= tier_gal

    return round(bill, 2)


def _conservation_signal(tiers: list[dict] | None) -> float | None:
    """Calculate conservation signal: ratio of highest to lowest tier rate.

    Parameters
    ----------
    tiers : list[dict] | None
        Volumetric tiers.

    Returns
    -------
    float | None
        Ratio (>1.0 = conservation pricing), or None if fewer than 2 tiers.
    """
    if not tiers or len(tiers) < 2:
        return None
    rates = [t["rate_per_1000_gal"] for t in tiers if t["rate_per_1000_gal"] > 0]
    if len(rates) < 2:
        return None
    return round(max(rates) / min(rates), 3)


def _assign_confidence(bill_10ccf: float | None, tier_count: int) -> str:
    """Assign nuanced confidence based on data quality signals.

    Parameters
    ----------
    bill_10ccf : float | None
        Monthly bill at 10 CCF.
    tier_count : int
        Number of volumetric tiers.

    Returns
    -------
    str
        'high', 'medium', or 'low'.
    """
    if bill_10ccf is None:
        return "low"
    if 10 <= bill_10ccf <= 200 and tier_count >= 2:
        return "high"
    if 5 <= bill_10ccf <= 500:
        return "medium"
    return "low"


# --- Main Ingest ---


def run_duke_nieps_ingest(
    states: list[str] | None = None,
    all_states: bool = False,
    dry_run: bool = False,
) -> dict:
    """Run Duke NIEPS 10-state data ingest into rate_schedules.

    Parameters
    ----------
    states : list[str] | None
        Specific states to ingest (e.g., ["TX", "NC"]).
    all_states : bool
        Ingest all 10 states.
    dry_run : bool
        Parse and report without DB writes.

    Returns
    -------
    dict
        Summary stats: states_processed, total_inserted, total_skipped, per_state.
    """
    started = datetime.now(timezone.utc)
    logger.info("=== Duke NIEPS 10-State Ingest Starting ===")
    logger.info(f"License: CC BY-NC-ND 4.0 — free_attributed tier (never paywalled)")
    logger.info(f"Target table: rate_schedules (source_key={SOURCE_KEY})")

    if all_states:
        target = ALL_STATES
    elif states:
        target = [s.lower() for s in states]
    else:
        logger.error("Specify --state or --all")
        return {"error": "no states"}

    # Load known PWSIDs from DB
    schema = settings.utility_schema
    with engine.connect() as conn:
        all_pwsids = set()
        for row in conn.execute(text(f"SELECT pwsid FROM {schema}.cws_boundaries")).fetchall():
            all_pwsids.add(row[0])
    logger.info(f"CWS boundaries: {len(all_pwsids):,} PWSIDs in database")

    total_stats = {
        "states_processed": 0,
        "total_upserted": 0,
        "total_skipped_no_cws": 0,
        "total_skipped_no_structure": 0,
        "per_state": {},
    }

    for state in target:
        filepath = DUKE_DATA_DIR / f"rates_{state}.xlsx"
        if not filepath.exists():
            logger.warning(f"  {state.upper()}: file not found at {filepath}")
            continue

        state_upper = state.upper()
        logger.info(f"\n--- {state_upper} ---")

        # Read metadata sheet
        metadata = _read_xlsx_sheet(filepath, "ratesMetadata")
        water_meta = [
            r for r in metadata
            if str(r.get("service_type", "")).strip().lower() == "water"
        ]
        logger.info(f"  Metadata: {len(metadata)} total rows, {len(water_meta)} water")

        # Read rate table sheet
        rate_table = _read_xlsx_sheet(filepath, "rateTable")
        water_rates = [
            r for r in rate_table
            if str(r.get("rate_code", "")).strip().lower() == "water"
        ]
        logger.info(f"  Rate table: {len(rate_table)} total rows, {len(water_rates)} water")

        # Group rate rows by normalized PWSID
        rate_by_pwsid: dict[str, list[dict]] = {}
        for r in water_rates:
            pwsid = _normalize_pwsid(str(r.get("pwsid", "")), state)
            if pwsid:
                rate_by_pwsid.setdefault(pwsid, []).append(r)

        # Build metadata lookup by normalized PWSID
        meta_by_pwsid: dict[str, dict] = {}
        for r in water_meta:
            pwsid = _normalize_pwsid(str(r.get("pwsid", "")), state)
            if pwsid:
                meta_by_pwsid[pwsid] = r

        # Process each PWSID
        records = []
        skipped_no_cws = 0
        skipped_no_structure = 0

        for pwsid in sorted(rate_by_pwsid.keys()):
            if pwsid not in all_pwsids:
                skipped_no_cws += 1
                continue

            rows = rate_by_pwsid[pwsid]
            meta = meta_by_pwsid.get(pwsid, {})

            # Extract rate structure with unit normalization
            rate_struct = _extract_rate_structure(rows)

            tiers = rate_struct["volumetric_tiers"]
            fixed = rate_struct["fixed_charge_monthly"]

            # Skip if no meaningful structure at all
            if fixed is None and not tiers:
                skipped_no_structure += 1
                continue

            # Calculate bills at standard CCF benchmarks
            bill_5 = _calculate_bill(fixed, tiers, 5.0 * GAL_PER_CCF)
            bill_10 = _calculate_bill(fixed, tiers, 10.0 * GAL_PER_CCF)
            bill_20 = _calculate_bill(fixed, tiers, 20.0 * GAL_PER_CCF)

            # Effective date from metadata
            eff_date = _parse_effective_date(meta.get("effective_date"))

            # Conservation signal
            cons_signal = _conservation_signal(tiers)

            # Source URL from metadata
            source_url = str(meta.get("website", "")).strip() or None

            records.append({
                "pwsid": pwsid,
                "source_key": SOURCE_KEY,
                "vintage_date": eff_date,
                "customer_class": "residential",
                "billing_frequency": rate_struct["bill_frequency"],
                "rate_structure_type": rate_struct["rate_structure_type"],
                "fixed_charges": json.dumps(rate_struct["fixed_charges_jsonb"]) if rate_struct["fixed_charges_jsonb"] else None,
                "volumetric_tiers": json.dumps(tiers) if tiers else None,
                "surcharges": None,
                "bill_5ccf": bill_5,
                "bill_10ccf": bill_10,
                "bill_20ccf": bill_20,
                "conservation_signal": cons_signal,
                "tier_count": rate_struct["tier_count"],
                "source_url": source_url,
                "confidence": _assign_confidence(
                    bill_10, rate_struct["tier_count"]
                ),
                "parse_notes": f"Duke NIEPS 10-state; {state_upper}; CC BY-NC-ND 4.0; free_attributed",
                "needs_review": False,
            })

        logger.info(
            f"  Records: {len(records)}, "
            f"skipped (no CWS match): {skipped_no_cws}, "
            f"skipped (no structure): {skipped_no_structure}"
        )

        state_stats = {
            "records": len(records),
            "skipped_no_cws": skipped_no_cws,
            "skipped_no_structure": skipped_no_structure,
        }

        if dry_run:
            # Show sample records
            for r in records[:5]:
                tiers_json = json.loads(r["volumetric_tiers"]) if r["volumetric_tiers"] else []
                logger.info(
                    f"  {r['pwsid']} | "
                    f"base=${r['bill_5ccf'] or 0:.2f}→${r['bill_10ccf'] or 0:.2f}→${r['bill_20ccf'] or 0:.2f} | "
                    f"tiers={r['tier_count']} | "
                    f"struct={r['rate_structure_type']} | "
                    f"freq={r['billing_frequency']}"
                )
            if records:
                bills = [r["bill_10ccf"] for r in records if r["bill_10ccf"] is not None]
                if bills:
                    logger.info(
                        f"  Bill @10CCF: n={len(bills)}, "
                        f"avg=${sum(bills)/len(bills):.2f}, "
                        f"min=${min(bills):.2f}, max=${max(bills):.2f}"
                    )
        else:
            # Write to DB via UPSERT
            with engine.connect() as conn:
                # Delete existing Duke NIEPS records for this state
                # (idempotent: safe to re-run)
                deleted = conn.execute(text(f"""
                    DELETE FROM {schema}.rate_schedules
                    WHERE source_key = :source_key
                      AND pwsid LIKE :state_prefix
                """), {
                    "source_key": SOURCE_KEY,
                    "state_prefix": f"{state_upper}%",
                }).rowcount
                if deleted:
                    logger.info(f"  Cleared {deleted} existing {state_upper} records")

                # Insert new records
                for record in records:
                    conn.execute(text(f"""
                        INSERT INTO {schema}.rate_schedules (
                            pwsid, source_key, vintage_date, customer_class,
                            billing_frequency, rate_structure_type,
                            fixed_charges, volumetric_tiers, surcharges,
                            bill_5ccf, bill_10ccf, bill_20ccf,
                            conservation_signal, tier_count,
                            source_url, confidence, parse_notes,
                            needs_review
                        ) VALUES (
                            :pwsid, :source_key, :vintage_date, :customer_class,
                            :billing_frequency, :rate_structure_type,
                            CAST(:fixed_charges AS jsonb), CAST(:volumetric_tiers AS jsonb), CAST(:surcharges AS jsonb),
                            :bill_5ccf, :bill_10ccf, :bill_20ccf,
                            :conservation_signal, :tier_count,
                            :source_url, :confidence, :parse_notes,
                            :needs_review
                        )
                    """), record)

                conn.commit()
                state_stats["upserted"] = len(records)
                logger.info(f"  Inserted {len(records)} records into rate_schedules")

        total_stats["states_processed"] += 1
        total_stats["total_upserted"] += state_stats.get("upserted", 0)
        total_stats["total_skipped_no_cws"] += skipped_no_cws
        total_stats["total_skipped_no_structure"] += skipped_no_structure
        total_stats["per_state"][state_upper] = state_stats

    # Log pipeline run
    if not dry_run:
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        with engine.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {schema}.pipeline_runs
                    (step_name, started_at, finished_at, row_count, status, notes)
                VALUES (:step, :started, NOW(), :count, 'success', :notes)
            """), {
                "step": "duke-nieps-ingest",
                "started": started,
                "count": total_stats["total_upserted"],
                "notes": (
                    f"source_key={SOURCE_KEY}, "
                    f"states={total_stats['states_processed']}, "
                    f"upserted={total_stats['total_upserted']}, "
                    f"skipped_no_cws={total_stats['total_skipped_no_cws']}, "
                    f"skipped_no_structure={total_stats['total_skipped_no_structure']}, "
                    f"tier=free_attributed, license=CC_BY-NC-ND_4.0, "
                    f"elapsed={elapsed:.1f}s"
                ),
            })
            conn.commit()

    logger.info(f"\n=== Duke NIEPS Ingest Complete ===")
    logger.info(
        f"States: {total_stats['states_processed']}, "
        f"Upserted: {total_stats['total_upserted']}, "
        f"Skipped (no CWS): {total_stats['total_skipped_no_cws']}, "
        f"Skipped (no structure): {total_stats['total_skipped_no_structure']}"
    )
    return total_stats


def seed_source_catalog(dry_run: bool = False) -> None:
    """Seed the source_catalog with Duke NIEPS provenance metadata.

    Parameters
    ----------
    dry_run : bool
        If True, log the entry without writing to DB.
    """
    schema = settings.utility_schema
    entry = {
        "source_key": SOURCE_KEY,
        "display_name": "Duke NIEPS Water Affordability Data Repository (10-State)",
        "source_type": "academic_research",
        "states_covered": "{TX,KS,PA,WA,NJ,NM,OR,CA,NC,CT}",
        "refresh_cadence": "one-time",
        "ingest_module": "utility_api.ingest.duke_nieps_ingest",
        "ingest_command": "ua-ingest duke-nieps --all",
        "notes": (
            "5,371 PWSIDs across 10 states with full rate structures. "
            "Repo last updated March 2022, data vintage 2019-2021. "
            "Supersedes duke_reference_rates (internal-only legacy table)."
        ),
        # Provenance fields (migration 016)
        "license_spdx": "CC-BY-NC-ND-4.0",
        "license_url": "https://creativecommons.org/licenses/by-nc-nd/4.0/",
        "license_summary": "Attribution required. Non-commercial use only. No derivatives.",
        "commercial_redistribution": False,
        "attribution_required": True,
        "attribution_text": (
            "Patterson, Lauren, Martin Doyle, Aislinn McLaughlin, and Sophia Bryson. 2021. "
            "Water Affordability Data Repository. Nicholas Institute for Environmental "
            "Policy Solutions at Duke University. "
            "https://github.com/NIEPS-Water-Program/water-affordability"
        ),
        "share_alike": False,
        "modifications_allowed": False,
        "tier": "free_attributed",
        "tier_rationale": (
            "CC BY-NC-ND requires non-commercial distribution. Data is served for free "
            "with full attribution. Premium product does not include this data behind "
            "a paywall. Free tier preserves NC requirement. Original rate values are "
            "preserved intact, satisfying ND requirement."
        ),
        "data_vintage": "2019-2021",
        "collection_date": "2020-2021",
        "upstream_sources": None,
        "transformation": "direct_ingest",
        "citation_doi": "10.5281/zenodo.5156654",
        "source_url": "https://github.com/NIEPS-Water-Program/water-affordability",
    }

    if dry_run:
        logger.info("=== Source Catalog Entry (dry run) ===")
        for k, v in entry.items():
            logger.info(f"  {k}: {v}")
        return

    with engine.connect() as conn:
        # UPSERT: insert or update on conflict
        conn.execute(text(f"""
            INSERT INTO {schema}.source_catalog (
                source_key, display_name, source_type, states_covered,
                refresh_cadence, ingest_module, ingest_command, notes,
                license_spdx, license_url, license_summary,
                commercial_redistribution, attribution_required, attribution_text,
                share_alike, modifications_allowed,
                tier, tier_rationale,
                data_vintage, collection_date,
                upstream_sources, transformation,
                citation_doi, source_url,
                last_ingested_at
            ) VALUES (
                :source_key, :display_name, :source_type, :states_covered,
                :refresh_cadence, :ingest_module, :ingest_command, :notes,
                :license_spdx, :license_url, :license_summary,
                :commercial_redistribution, :attribution_required, :attribution_text,
                :share_alike, :modifications_allowed,
                :tier, :tier_rationale,
                :data_vintage, :collection_date,
                :upstream_sources, :transformation,
                :citation_doi, :source_url,
                NOW()
            )
            ON CONFLICT (source_key) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                source_type = EXCLUDED.source_type,
                states_covered = EXCLUDED.states_covered,
                notes = EXCLUDED.notes,
                license_spdx = EXCLUDED.license_spdx,
                license_url = EXCLUDED.license_url,
                license_summary = EXCLUDED.license_summary,
                commercial_redistribution = EXCLUDED.commercial_redistribution,
                attribution_required = EXCLUDED.attribution_required,
                attribution_text = EXCLUDED.attribution_text,
                share_alike = EXCLUDED.share_alike,
                modifications_allowed = EXCLUDED.modifications_allowed,
                tier = EXCLUDED.tier,
                tier_rationale = EXCLUDED.tier_rationale,
                data_vintage = EXCLUDED.data_vintage,
                collection_date = EXCLUDED.collection_date,
                citation_doi = EXCLUDED.citation_doi,
                source_url = EXCLUDED.source_url,
                last_ingested_at = NOW(),
                updated_at = NOW()
        """), entry)
        conn.commit()
        logger.info(f"Source catalog entry upserted: {SOURCE_KEY}")
