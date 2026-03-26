#!/usr/bin/env python3
"""
Duke Water Affordability Reference Data Ingest

Purpose:
    Ingests Duke/Nicholas Institute rate data as INTERNAL REFERENCE ONLY.
    This data is CC BY-NC-ND 4.0 and CANNOT be redistributed commercially.

    Stored in a separate table (duke_reference_rates) that is never
    exposed through the API or bulk download.

    Used for:
    - Validating LLM-parsed rates against manually-collected rates
    - Gap analysis (PWSIDs with Duke data but no commercial source)
    - Coverage benchmarking

Author: AI-Generated
Created: 2026-03-26
Modified: 2026-03-26

Dependencies:
    - openpyxl (Excel parsing)
    - sqlalchemy (database)
    - loguru (logging)

Usage:
    # CLI
    ua-ingest duke-reference --state TX --dry-run
    ua-ingest duke-reference --all --dry-run
    ua-ingest duke-reference --all

    # Python
    from utility_api.ingest.duke_reference_ingest import run_duke_reference_ingest
    run_duke_reference_ingest(states=["TX"], dry_run=True)

Notes:
    - Excel files use 'NA' as string for missing values
    - value_to = 1000000000 means unlimited (last tier)
    - meter_size = 0.625 is standard residential 5/8 inch
    - adjustment field is a multiplier, usually 1.0
    - bill_frequency varies — normalized to monthly
    - effective_date in rateTable is year integer, in ratesMetadata is datetime
    - Filters to rate_code='water' only (excludes sewer/storm/septic)
    - Filters to meter_size=0.625 for service charges (residential)

Data Sources:
    - Input: data/duke_raw/data/rates_data/rates_{state}.xlsx
    - Input: utility.cws_boundaries (PWSID filter)
    - Output: utility.duke_reference_rates (INTERNAL ONLY)

Configuration:
    - Duke repo cloned at data/duke_raw/
    - Database connection via .env (DATABASE_URL)
"""

import json
from datetime import date, datetime, timezone
from pathlib import Path

import openpyxl
from loguru import logger
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine


# --- Constants ---

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DUKE_DATA_DIR = PROJECT_ROOT / "data" / "duke_raw" / "data" / "rates_data"

ALL_STATES = ["tx", "ks", "pa", "wa", "nj", "nm", "or", "ca", "nc", "ct"]
GAP_FILL_STATES = {"tx", "ks", "pa", "wa", "nj", "nm", "or"}

GAL_PER_CCF = 748.052

# Billing frequency divisors (to monthly)
FREQ_DIVISOR = {
    "monthly": 1,
    "bi-monthly": 2,
    "quarterly": 3,
    "semi-annually": 6,
    "annually": 12,
}

UNLIMITED = 1_000_000_000


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
    """Parse effective_date from ratesMetadata (datetime) or rateTable (year int)."""
    if val is None or str(val).strip() in ("NA", "None", ""):
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    # Try as year integer
    try:
        year = int(val)
        if 2000 <= year <= 2030:
            return date(year, 1, 1)
    except (ValueError, TypeError):
        pass
    return None


# --- Rate Extraction ---


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
        One dict per row, keys are column headers.
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


def _extract_rate_structure(
    rate_rows: list[dict],
    bill_frequency_default: str = "monthly",
) -> dict:
    """Extract rate structure from rateTable rows for one PWSID.

    Parameters
    ----------
    rate_rows : list[dict]
        rateTable rows filtered to one PWSID + rate_code='water'.
    bill_frequency_default : str
        Default billing frequency if not specified.

    Returns
    -------
    dict
        Contains: fixed_charge_monthly, volumetric_tiers, bill_frequency,
        rate_structure_type, tier_count.
    """
    # Determine billing frequency (from first row that has it)
    bill_freq = bill_frequency_default
    for r in rate_rows:
        bf = _safe_val(r.get("bill_frequency"))
        if bf:
            bill_freq = str(bf).strip().lower()
            break
    divisor = FREQ_DIVISOR.get(bill_freq, 1)

    # Fixed charge (service_charge for 5/8 inch meter)
    fixed_charge = None
    service_charges = [
        r for r in rate_rows
        if str(r.get("rate_type", "")).strip() == "service_charge"
    ]

    if service_charges:
        # Prefer meter_size = 0.625 (5/8 inch residential)
        for r in service_charges:
            ms = _safe_float(r.get("meter_size"))
            if ms is not None and abs(ms - 0.625) < 0.01:
                cost = _safe_float(r.get("cost"))
                adj = _safe_float(r.get("adjustment")) or 1.0
                if cost is not None:
                    fixed_charge = round(cost * adj / divisor, 2)
                break

        # Fallback: smallest meter size
        if fixed_charge is None:
            sized = [(r, _safe_float(r.get("meter_size")) or 999) for r in service_charges]
            sized.sort(key=lambda x: x[1])
            cost = _safe_float(sized[0][0].get("cost"))
            adj = _safe_float(sized[0][0].get("adjustment")) or 1.0
            if cost is not None:
                fixed_charge = round(cost * adj / divisor, 2)

    # Volumetric tiers
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
        vol_unit = str(r.get("vol_unit", "gallons")).strip().lower()

        if cost is None or vol_base is None or vol_base <= 0:
            continue

        # Convert to gallons if needed
        if "cubic" in vol_unit or vol_unit == "cf":
            # 1 cubic foot = 7.48052 gallons
            gal_from = (val_from or 0) * 7.48052
            gal_to = (val_to or UNLIMITED) * 7.48052 if val_to != UNLIMITED else None
            rate_per_1000_gal = (cost * adj) / (vol_base * 7.48052 / 1000.0)
        else:
            # Already in gallons
            gal_from = val_from or 0
            gal_to = val_to if val_to and val_to < UNLIMITED else None
            rate_per_1000_gal = (cost * adj) / (vol_base / 1000.0)

        # Normalize to monthly if needed (volumetric is per-billing-period)
        # Actually, volumetric rates are per unit of volume, not per billing period
        # The divisor applies to fixed charges, not volumetric rates

        tiers.append({
            "min_gal": round(gal_from, 0),
            "max_gal": round(gal_to, 0) if gal_to else None,
            "rate_per_1000_gal": round(rate_per_1000_gal, 4),
        })

    # Sort by min_gal and number
    tiers.sort(key=lambda t: t["min_gal"])
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
            structure_type = "increasing_block"

    return {
        "fixed_charge_monthly": fixed_charge,
        "volumetric_tiers": tiers if tiers else None,
        "bill_frequency": bill_freq,
        "rate_structure_type": structure_type,
        "tier_count": len(tiers),
    }


def _calculate_bill(fixed_charge: float | None, tiers: list[dict] | None, gallons: float) -> float | None:
    """Calculate monthly bill at a given gallon consumption.

    Parameters
    ----------
    fixed_charge : float | None
        Monthly fixed charge.
    tiers : list[dict] | None
        Volumetric tiers with min_gal, max_gal, rate_per_1000_gal.
    gallons : float
        Monthly consumption in gallons.

    Returns
    -------
    float | None
        Monthly bill in dollars.
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


# --- Main Ingest ---


def run_duke_reference_ingest(
    states: list[str] | None = None,
    all_states: bool = False,
    dry_run: bool = False,
) -> dict:
    """Run Duke reference data ingest.

    Parameters
    ----------
    states : list[str] | None
        Specific states to ingest.
    all_states : bool
        Ingest all 10 states.
    dry_run : bool
        Parse and report without DB writes.

    Returns
    -------
    dict
        Summary stats.
    """
    started = datetime.now(timezone.utc)
    logger.info("=== Duke Reference Rate Ingest Starting ===")
    logger.info("⚠ INTERNAL REFERENCE ONLY — CC BY-NC-ND 4.0 — NOT for commercial use")

    if all_states:
        target = ALL_STATES
    elif states:
        target = [s.lower() for s in states]
    else:
        logger.error("Specify --state or --all")
        return {"error": "no states"}

    # Load PWSIDs from DB
    schema = settings.utility_schema
    with engine.connect() as conn:
        all_pwsids = set()
        for row in conn.execute(text(f"SELECT pwsid FROM {schema}.cws_boundaries")).fetchall():
            all_pwsids.add(row[0])
    logger.info(f"CWS boundaries: {len(all_pwsids)} PWSIDs")

    total_stats = {
        "states_processed": 0,
        "total_inserted": 0,
        "total_skipped": 0,
        "per_state": {},
    }

    for state in target:
        filepath = DUKE_DATA_DIR / f"rates_{state}.xlsx"
        if not filepath.exists():
            logger.warning(f"  {state.upper()}: file not found at {filepath}")
            continue

        state_upper = state.upper()
        logger.info(f"\n--- {state_upper} ---")

        # Read metadata
        metadata = _read_xlsx_sheet(filepath, "ratesMetadata")
        water_meta = [
            r for r in metadata
            if str(r.get("service_type", "")).strip().lower() == "water"
        ]
        logger.info(f"  Metadata: {len(metadata)} rows, {len(water_meta)} water")

        # Read rate table
        rate_table = _read_xlsx_sheet(filepath, "rateTable")
        water_rates = [
            r for r in rate_table
            if str(r.get("rate_code", "")).strip().lower() == "water"
        ]
        logger.info(f"  Rate table: {len(rate_table)} rows, {len(water_rates)} water")

        # Normalize PWSIDs (NC uses dashed format: '03-63-020' → 'NC0363020')
        def _normalize_pwsid(raw: str, st: str) -> str | None:
            raw = raw.strip()
            if not raw or raw == "NA":
                return None
            if raw.upper().startswith(st.upper()):
                return raw.upper()
            # NC-style dashed format: NN-NN-NNN → STNNNNNN
            import re
            m = re.match(r"(\d{2})-(\d{2})-(\d{3})", raw)
            if m:
                return f"{st.upper()}{m.group(1)}{m.group(2)}{m.group(3)}"
            return raw.upper()

        # Group rate rows by PWSID
        rate_by_pwsid = {}
        for r in water_rates:
            pwsid = _normalize_pwsid(str(r.get("pwsid", "")), state)
            if pwsid:
                rate_by_pwsid.setdefault(pwsid, []).append(r)

        # Build metadata lookup
        meta_by_pwsid = {}
        for r in water_meta:
            pwsid = _normalize_pwsid(str(r.get("pwsid", "")), state)
            if pwsid:
                meta_by_pwsid[pwsid] = r

        # Process each PWSID
        records = []
        skipped_not_in_db = 0

        for pwsid in sorted(rate_by_pwsid.keys()):
            if pwsid not in all_pwsids:
                skipped_not_in_db += 1
                continue

            rows = rate_by_pwsid[pwsid]
            meta = meta_by_pwsid.get(pwsid, {})

            # Extract rate structure
            rate_struct = _extract_rate_structure(rows)

            # Calculate bills
            tiers = rate_struct["volumetric_tiers"]
            fixed = rate_struct["fixed_charge_monthly"]
            bill_5 = _calculate_bill(fixed, tiers, 5.0 * GAL_PER_CCF)
            bill_10 = _calculate_bill(fixed, tiers, 10.0 * GAL_PER_CCF)
            bill_20 = _calculate_bill(fixed, tiers, 20.0 * GAL_PER_CCF)

            # Effective date from metadata
            eff_date = _parse_effective_date(meta.get("effective_date"))

            # Notes
            notes_parts = []
            duke_notes = _safe_val(meta.get("notes"))
            if duke_notes:
                notes_parts.append(str(duke_notes)[:200])
            gap = "gap-fill" if state in GAP_FILL_STATES else "overlap"
            notes_parts.append(f"Duke/{gap}")

            records.append({
                "pwsid": pwsid,
                "state_code": state_upper,
                "utility_name": str(meta.get("utility_name", "")).strip() or None,
                "service_area": str(meta.get("service_area", "")).strip() or None,
                "effective_date": eff_date,
                "source_url": str(meta.get("website", "")).strip() or None,
                "bill_frequency": rate_struct["bill_frequency"],
                "rate_structure_type": rate_struct["rate_structure_type"],
                "fixed_charge_monthly": fixed,
                "volumetric_tiers": json.dumps(tiers) if tiers else None,
                "tier_count": rate_struct["tier_count"],
                "bill_5ccf": bill_5,
                "bill_10ccf": bill_10,
                "bill_20ccf": bill_20,
                "notes": "; ".join(notes_parts) if notes_parts else None,
            })

        logger.info(f"  Records: {len(records)}, skipped (not in DB): {skipped_not_in_db}")

        state_stats = {
            "records": len(records),
            "skipped_not_in_db": skipped_not_in_db,
        }

        if dry_run:
            for r in records[:3]:
                logger.info(
                    f"  {r['pwsid']} | {(r['utility_name'] or '')[:30]:30s} | "
                    f"base=${r['fixed_charge_monthly'] or 0:.2f} | "
                    f"tiers={r['tier_count']} | "
                    f"@5ccf=${r['bill_5ccf'] or 0:.2f} | "
                    f"@10ccf=${r['bill_10ccf'] or 0:.2f}"
                )
            if records:
                bills = [r["bill_10ccf"] for r in records if r["bill_10ccf"] is not None]
                if bills:
                    logger.info(
                        f"  Bill @10CCF: avg=${sum(bills)/len(bills):.2f}, "
                        f"min=${min(bills):.2f}, max=${max(bills):.2f}"
                    )
        else:
            # Write to DB
            with engine.connect() as conn:
                # Delete existing for this state (idempotent)
                deleted = conn.execute(text(f"""
                    DELETE FROM {schema}.duke_reference_rates
                    WHERE state_code = :state
                """), {"state": state_upper}).rowcount
                if deleted:
                    logger.info(f"  Cleared {deleted} existing {state_upper} records")

                for record in records:
                    conn.execute(text(f"""
                        INSERT INTO {schema}.duke_reference_rates (
                            pwsid, state_code, utility_name, service_area,
                            effective_date, source_url, bill_frequency,
                            rate_structure_type, fixed_charge_monthly,
                            volumetric_tiers, tier_count,
                            bill_5ccf, bill_10ccf, bill_20ccf, notes
                        ) VALUES (
                            :pwsid, :state_code, :utility_name, :service_area,
                            :effective_date, :source_url, :bill_frequency,
                            :rate_structure_type, :fixed_charge_monthly,
                            :volumetric_tiers, :tier_count,
                            :bill_5ccf, :bill_10ccf, :bill_20ccf, :notes
                        )
                    """), record)

                conn.commit()
                state_stats["inserted"] = len(records)
                logger.info(f"  Inserted {len(records)} records")

        total_stats["states_processed"] += 1
        total_stats["total_inserted"] += state_stats.get("inserted", 0)
        total_stats["total_skipped"] += skipped_not_in_db
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
                "step": "duke-reference-ingest",
                "started": started,
                "count": total_stats["total_inserted"],
                "notes": (
                    f"states={total_stats['states_processed']}, "
                    f"inserted={total_stats['total_inserted']}, "
                    f"skipped_not_in_db={total_stats['total_skipped']}, "
                    f"license=CC_BY-NC-ND_4.0_INTERNAL_ONLY"
                ),
            })
            conn.commit()

    logger.info(f"\n=== Duke Reference Ingest Complete ===")
    logger.info(f"States: {total_stats['states_processed']}, Inserted: {total_stats['total_inserted']}")
    return total_stats
