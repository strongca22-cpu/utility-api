#!/usr/bin/env python3
"""
OWRS (Open Water Rate Specification) Ingest

Purpose:
    Loads water rate data from the California Data Collaborative's OWRS project.
    Uses the pre-computed summary table from the OWRS-Analysis repo, which
    contains PWSIDs, effective dates, tier structures, and bill amounts for
    ~386 unique CA utilities (~492 OWRS YAML files across 433 utility directories).

    The summary table is the most efficient ingest path — it avoids parsing
    492 individual YAML files and already has PWSID crosswalk resolved.

    OWRS data is water-only (no sewer) and represents residential single-family
    rate structures. Effective dates range from 2002 to 2021.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - pandas
    - sqlalchemy
    - loguru

Usage:
    # CLI
    ua-ingest owrs                    # Ingest all OWRS records
    ua-ingest owrs --dry-run          # Preview without DB writes
    ua-ingest owrs --all-utilities    # Include utilities not in our CWS list

    # Python
    from utility_api.ingest.owrs_ingest import run_owrs_ingest
    run_owrs_ingest()

Notes:
    - OWRS summary table was downloaded from the OWRS-Analysis GitHub repo:
      https://github.com/California-Data-Collaborative/OWRS-Analysis
    - 61 of 419 records report rates in kgal (1000 gallons) instead of CCF.
      Tier limits are converted: kgal_limit × 1.337 = CCF (since 1 CCF = 748 gal).
      Tier prices are converted: $/kgal × 0.748 = $/CCF.
    - Bills in the summary table are computed at variable usage_ccf per utility
      (household-specific, 4-35 CCF). We recalculate bill_5ccf and bill_10ccf
      from the stored tier structure for consistency with other sources.
    - Some utilities have multiple OWRS files (different vintages). The summary
      table appears to use the most recent available.
    - 16 "Budget" type rates are ingested as rate_structure_type='budget_based'.
    - 3 records have anomalous bill_type values (formula strings) — skipped.
    - Billing frequency normalization: bimonthly service_charge ÷ 2, quarterly ÷ 3.

Data Sources:
    - Input: data/raw/owrs_summary_table.csv (from OWRS-Analysis GitHub repo)
    - Input: utility.cws_boundaries (PWSID filter for FK constraint)
    - Output: utility.water_rates table (source='owrs')

Configuration:
    - CSV file must be at data/raw/owrs_summary_table.csv
    - Database connection via .env (DATABASE_URL)
"""

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from loguru import logger
from sqlalchemy import text

from utility_api.config import PROJECT_ROOT, settings
from utility_api.db import engine


# --- Constants ---

DATA_FILE = PROJECT_ROOT / "data" / "raw" / "owrs_summary_table.csv"
SOURCE_TAG = "owrs"

# Unit conversion: 1 CCF = 748 gallons = 0.748 kgal
# So: kgal_limit × (1000/748) = CCF limit
#     $/kgal × (748/1000) = $/CCF
KGAL_TO_CCF_LIMIT = 1000.0 / 748.0  # 1.337
KGAL_TO_CCF_PRICE = 748.0 / 1000.0  # 0.748

# Bill type → rate_structure_type mapping
BILL_TYPE_MAP = {
    "Uniform": "uniform",
    "Tiered": "increasing_block",
    "Budget": "budget_based",
}

# Billing frequency normalization
BILLING_FREQ_MAP = {
    "Monthly": ("monthly", 1),
    "monthly": ("monthly", 1),
    "Bi-Monthly": ("bimonthly", 2),
    "bimonthly": ("bimonthly", 2),
    "Bimonthly": ("bimonthly", 2),
    "Quarterly": ("quarterly", 3),
    "Annually": ("annually", 12),
}


def _parse_tiers(tier_starts_str: str, tier_prices_str: str, is_kgal: bool) -> list[dict]:
    """Parse newline-delimited tier data into structured tiers.

    Parameters
    ----------
    tier_starts_str : str
        Newline-separated tier start values (e.g., "0\n10\n20").
    tier_prices_str : str
        Newline-separated tier price values (e.g., "2.32\n2.79\n3.45").
    is_kgal : bool
        If True, convert limits and prices from kgal to CCF.

    Returns
    -------
    list[dict]
        List of tier dicts with 'limit_ccf' and 'rate_ccf' keys.
        limit_ccf is the upper limit (next tier start), or None for the last tier.
    """
    if not tier_starts_str or not tier_prices_str:
        return []

    starts_raw = [x.strip() for x in str(tier_starts_str).split("\n") if x.strip()]
    prices_raw = [x.strip() for x in str(tier_prices_str).split("\n") if x.strip()]

    # Parse prices (always numeric)
    prices = []
    for p in prices_raw:
        try:
            prices.append(float(p))
        except ValueError:
            logger.debug(f"  Non-numeric tier price: '{p}', skipping tier set")
            return []

    # Parse starts — budget-based rates use non-numeric starts like "indoor", "100%"
    starts = []
    has_numeric_starts = True
    for s in starts_raw:
        try:
            starts.append(float(s))
        except ValueError:
            has_numeric_starts = False
            starts.append(None)

    if len(prices) == 0:
        return []

    # For budget-based (non-numeric starts): store prices without limits
    if not has_numeric_starts:
        tiers = []
        for price in prices:
            if is_kgal:
                price = price * KGAL_TO_CCF_PRICE
            tiers.append({
                "limit_ccf": None,
                "rate_ccf": round(price, 4),
            })
        return tiers

    if len(starts) != len(prices):
        logger.warning(f"Tier starts ({len(starts)}) != prices ({len(prices)}), truncating")
        n = min(len(starts), len(prices))
        starts = starts[:n]
        prices = prices[:n]

    tiers = []
    for i, (start, price) in enumerate(zip(starts, prices)):
        # Upper limit is the next tier's start, or None for the last tier
        upper = starts[i + 1] if i + 1 < len(starts) else None

        if is_kgal:
            upper = upper * KGAL_TO_CCF_LIMIT if upper is not None else None
            price = price * KGAL_TO_CCF_PRICE

        tiers.append({
            "limit_ccf": round(upper, 2) if upper is not None else None,
            "rate_ccf": round(price, 4),
        })

    return tiers


def _calculate_bill(fixed_monthly: float, tiers: list[dict], usage_ccf: float) -> float | None:
    """Calculate a monthly bill from tier structure.

    Parameters
    ----------
    fixed_monthly : float
        Monthly fixed/service charge.
    tiers : list[dict]
        Tier list from _parse_tiers.
    usage_ccf : float
        Monthly usage in CCF.

    Returns
    -------
    float | None
        Calculated monthly bill, or None if tier structure is missing.
    """
    if not tiers:
        return None

    commodity = 0.0
    remaining = usage_ccf

    for i, tier in enumerate(tiers):
        if remaining <= 0:
            break

        # How much usage falls in this tier?
        if tier["limit_ccf"] is not None and i == 0:
            # First tier: from 0 to limit
            tier_volume = min(remaining, tier["limit_ccf"])
        elif tier["limit_ccf"] is not None:
            # Middle tier: from previous limit to this limit
            prev_limit = tiers[i - 1]["limit_ccf"] if i > 0 and tiers[i - 1]["limit_ccf"] else 0
            tier_width = tier["limit_ccf"] - prev_limit
            tier_volume = min(remaining, tier_width)
        else:
            # Last tier (unlimited)
            tier_volume = remaining

        commodity += tier_volume * tier["rate_ccf"]
        remaining -= tier_volume

    return round((fixed_monthly or 0.0) + commodity, 2)


def _parse_owrs_row(row: pd.Series) -> dict | None:
    """Parse a single OWRS summary row into a water_rates record.

    Parameters
    ----------
    row : pd.Series
        One row from the OWRS summary table.

    Returns
    -------
    dict | None
        Record dict for insertion, or None if row should be skipped.
    """
    pwsid = str(row.get("pwsid", "")).strip()
    if not pwsid or pwsid == "nan":
        return None

    # Map bill type
    bill_type = str(row.get("bill_type", "")).strip()
    rate_structure = BILL_TYPE_MAP.get(bill_type)
    if rate_structure is None:
        # Skip anomalous bill_type values (formulas, "0")
        logger.debug(f"  Skipping {pwsid}: unrecognized bill_type '{bill_type}'")
        return None

    # Billing frequency
    freq_raw = str(row.get("bill_frequency", "Monthly")).strip()
    freq_norm, divisor = BILLING_FREQ_MAP.get(freq_raw, ("monthly", 1))

    # Unit check
    bill_unit = str(row.get("bill_unit", "ccf")).strip().lower()
    is_kgal = bill_unit == "kgal"

    # Service charge → monthly
    service_charge = row.get("service_charge")
    try:
        service_charge = float(service_charge)
        fixed_monthly = round(service_charge / divisor, 2)
    except (TypeError, ValueError):
        fixed_monthly = None

    # Parse tiers
    tier_starts = row.get("tier_starts")
    tier_prices = row.get("tier_prices")

    has_tier_data = (
        tier_starts is not None
        and str(tier_starts).strip() not in ("", "NA", "nan")
        and tier_prices is not None
        and str(tier_prices).strip() not in ("", "NA", "nan")
    )

    tiers = []
    if has_tier_data:
        tiers = _parse_tiers(str(tier_starts), str(tier_prices), is_kgal)

    # For uniform rates without tier starts, extract the flat rate from tier_prices
    # (tier_prices may have a single value even when tier_starts is NA)
    if rate_structure == "uniform" and not tiers:
        tp = row.get("tier_prices")
        tp_str = str(tp).strip() if tp is not None else ""
        if tp_str and tp_str not in ("NA", "nan"):
            try:
                flat_rate = float(tp_str.split("\n")[0].strip())
                if is_kgal:
                    flat_rate *= KGAL_TO_CCF_PRICE
                tiers = [{"limit_ccf": None, "rate_ccf": round(flat_rate, 4)}]
            except (ValueError, IndexError):
                pass
        # Fallback: if tier_prices is also missing, try deriving from commodity/usage
        if not tiers:
            commodity = row.get("commodity_charge")
            usage = row.get("usage_ccf")
            try:
                commodity_val = float(commodity)
                usage_val = float(usage)
                if usage_val > 0:
                    derived_rate = commodity_val / usage_val
                    if is_kgal:
                        derived_rate *= KGAL_TO_CCF_PRICE
                    tiers = [{"limit_ccf": None, "rate_ccf": round(derived_rate, 4)}]
            except (TypeError, ValueError):
                pass

    # Normalize tier limits for bimonthly/quarterly billing
    # Tier limits in the OWRS data are per-billing-period
    if divisor > 1 and tiers:
        for t in tiers:
            if t["limit_ccf"] is not None:
                t["limit_ccf"] = round(t["limit_ccf"] / divisor, 2)

    # Calculate bills at standard usage levels
    bill_5ccf = _calculate_bill(fixed_monthly, tiers, 5.0)
    bill_10ccf = _calculate_bill(fixed_monthly, tiers, 10.0)

    # Effective date
    effective_date = None
    date_str = str(row.get("effective_date", "")).strip()
    if date_str and date_str != "nan":
        try:
            effective_date = pd.to_datetime(date_str).date()
        except Exception:
            logger.debug(f"  {pwsid}: unparseable date '{date_str}'")

    # Confidence: high for tiered/uniform with complete data, medium for budget
    if rate_structure == "budget_based":
        confidence = "medium"
    elif bill_5ccf is not None and fixed_monthly is not None:
        confidence = "high"
    else:
        confidence = "medium"

    # Build record
    record = {
        "pwsid": pwsid,
        "utility_name": str(row.get("utility_name", "")).strip()[:255],
        "state_code": "CA",
        "county": None,  # OWRS doesn't include county
        "rate_effective_date": effective_date,
        "rate_structure_type": rate_structure,
        "rate_class": "residential",
        "billing_frequency": freq_norm,
        "fixed_charge_monthly": fixed_monthly,
        "meter_size_inches": 0.625,  # OWRS default: 5/8" meter
        # Tiers (up to 4)
        "tier_1_limit_ccf": tiers[0]["limit_ccf"] if len(tiers) > 0 else None,
        "tier_1_rate": tiers[0]["rate_ccf"] if len(tiers) > 0 else None,
        "tier_2_limit_ccf": tiers[1]["limit_ccf"] if len(tiers) > 1 else None,
        "tier_2_rate": tiers[1]["rate_ccf"] if len(tiers) > 1 else None,
        "tier_3_limit_ccf": tiers[2]["limit_ccf"] if len(tiers) > 2 else None,
        "tier_3_rate": tiers[2]["rate_ccf"] if len(tiers) > 2 else None,
        "tier_4_limit_ccf": tiers[3]["limit_ccf"] if len(tiers) > 3 else None,
        "tier_4_rate": tiers[3]["rate_ccf"] if len(tiers) > 3 else None,
        # Bill snapshots
        "bill_5ccf": bill_5ccf,
        "bill_10ccf": bill_10ccf,
        # eAR bill columns not applicable
        "bill_6ccf": None,
        "bill_9ccf": None,
        "bill_12ccf": None,
        "bill_24ccf": None,
        # Provenance
        "source": SOURCE_TAG,
        "source_url": "https://github.com/California-Data-Collaborative/Open-Water-Rate-Specification",
        "raw_text_hash": None,
        "parse_confidence": confidence,
        "parse_model": None,
        "parse_notes": f"OWRS summary table; bill_unit={bill_unit}; bill_freq={freq_raw}; bill_type={bill_type}",
    }

    return record


def run_owrs_ingest(
    dry_run: bool = False,
    all_utilities: bool = False,
) -> dict:
    """Run the OWRS rate data ingest.

    Parameters
    ----------
    dry_run : bool
        If True, parse and report but don't write to DB.
    all_utilities : bool
        If True, insert all OWRS records (including those not in cws_boundaries).
        If False (default), only insert PWSIDs that exist in cws_boundaries.

    Returns
    -------
    dict
        Summary stats: total, parsed, matched, inserted, skipped.
    """
    started = datetime.now(timezone.utc)
    logger.info("=== OWRS Rate Ingest Starting ===")

    if not DATA_FILE.exists():
        logger.error(f"OWRS summary table not found: {DATA_FILE}")
        logger.info(
            "Download from: https://raw.githubusercontent.com/"
            "California-Data-Collaborative/OWRS-Analysis/master/summary_table.csv"
        )
        return {"error": f"File not found: {DATA_FILE}"}

    # Load summary table
    df = pd.read_csv(DATA_FILE)
    logger.info(f"Loaded {len(df)} rows from OWRS summary table")

    # Get valid PWSIDs from our database
    schema = settings.utility_schema
    with engine.connect() as conn:
        valid_pwsids = set(
            r[0] for r in conn.execute(
                text(f"SELECT DISTINCT pwsid FROM {schema}.cws_boundaries")
            ).fetchall()
        )
    logger.info(f"Valid PWSIDs in cws_boundaries: {len(valid_pwsids)}")

    stats = {
        "total_rows": len(df),
        "parsed": 0,
        "matched_cws": 0,
        "new_utilities": 0,
        "inserted": 0,
        "skipped_bad_type": 0,
        "skipped_no_pwsid": 0,
        "skipped_fk": 0,
    }

    records = []
    for _, row in df.iterrows():
        record = _parse_owrs_row(row)
        if record is None:
            if not str(row.get("pwsid", "")).strip() or str(row.get("pwsid", "")) == "nan":
                stats["skipped_no_pwsid"] += 1
            else:
                stats["skipped_bad_type"] += 1
            continue

        stats["parsed"] += 1

        if record["pwsid"] in valid_pwsids:
            stats["matched_cws"] += 1
            records.append(record)
        elif all_utilities:
            stats["new_utilities"] += 1
            # Can't insert — FK constraint on cws_boundaries.pwsid
            logger.debug(f"  {record['pwsid']} not in cws_boundaries, skipping (FK constraint)")
            stats["skipped_fk"] += 1
        else:
            stats["skipped_fk"] += 1

    # Deduplicate: same PWSID + effective_date → keep first occurrence
    seen_keys = set()
    deduped = []
    dupes_removed = 0
    for r in records:
        key = (r["pwsid"], r["rate_effective_date"])
        if key in seen_keys:
            dupes_removed += 1
            continue
        seen_keys.add(key)
        deduped.append(r)
    records = deduped
    if dupes_removed:
        logger.info(f"Removed {dupes_removed} duplicate PWSID+date records (multi-district utilities)")

    logger.info(
        f"Parsed {stats['parsed']}/{stats['total_rows']} rows, "
        f"{stats['matched_cws']} match our CWS list, "
        f"{stats['skipped_bad_type']} skipped (bad bill_type), "
        f"{stats['skipped_fk']} skipped (not in cws_boundaries)"
    )

    if dry_run:
        logger.info(f"[DRY RUN] Would insert {len(records)} records")
        for r in records[:8]:
            logger.info(
                f"  {r['pwsid']} | {r['utility_name'][:35]:35s} | "
                f"{r['rate_structure_type']:15s} | "
                f"fixed=${r['fixed_charge_monthly'] or 0:7.2f} | "
                f"bill@5=${r['bill_5ccf'] or 0:7.2f} | "
                f"bill@10=${r['bill_10ccf'] or 0:7.2f} | "
                f"[{r['parse_confidence']}]"
            )
        if len(records) > 8:
            logger.info(f"  ... and {len(records) - 8} more")
        stats["inserted"] = 0
        return stats

    # Write to database
    with engine.connect() as conn:
        # Delete existing OWRS records (idempotent re-run)
        deleted = conn.execute(
            text(f"DELETE FROM {schema}.water_rates WHERE source = :source"),
            {"source": SOURCE_TAG},
        ).rowcount
        if deleted:
            logger.info(f"Cleared {deleted} existing OWRS records")

        # Batch insert
        for record in records:
            conn.execute(text(f"""
                INSERT INTO {schema}.water_rates (
                    pwsid, utility_name, state_code, county,
                    rate_effective_date, rate_structure_type, rate_class, billing_frequency,
                    fixed_charge_monthly, meter_size_inches,
                    tier_1_limit_ccf, tier_1_rate,
                    tier_2_limit_ccf, tier_2_rate,
                    tier_3_limit_ccf, tier_3_rate,
                    tier_4_limit_ccf, tier_4_rate,
                    bill_5ccf, bill_10ccf,
                    bill_6ccf, bill_9ccf, bill_12ccf, bill_24ccf,
                    source, source_url, raw_text_hash,
                    parse_confidence, parse_model, parse_notes
                ) VALUES (
                    :pwsid, :utility_name, :state_code, :county,
                    :rate_effective_date, :rate_structure_type, :rate_class, :billing_frequency,
                    :fixed_charge_monthly, :meter_size_inches,
                    :tier_1_limit_ccf, :tier_1_rate,
                    :tier_2_limit_ccf, :tier_2_rate,
                    :tier_3_limit_ccf, :tier_3_rate,
                    :tier_4_limit_ccf, :tier_4_rate,
                    :bill_5ccf, :bill_10ccf,
                    :bill_6ccf, :bill_9ccf, :bill_12ccf, :bill_24ccf,
                    :source, :source_url, :raw_text_hash,
                    :parse_confidence, :parse_model, :parse_notes
                )
            """), record)

        conn.commit()
        stats["inserted"] = len(records)

    # Log pipeline run
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {schema}.pipeline_runs (step_name, started_at, finished_at, row_count, status, notes)
            VALUES (:step, :started, :finished, :row_count, :status, :notes)
        """), {
            "step": "owrs_ingest",
            "started": started,
            "finished": datetime.now(timezone.utc),
            "row_count": stats["inserted"],
            "status": "success",
            "notes": (
                f"OWRS summary table: {stats['total_rows']} rows, "
                f"{stats['parsed']} parsed, {stats['matched_cws']} matched CWS, "
                f"{stats['inserted']} inserted"
            ),
        })
        conn.commit()

    logger.info(f"Inserted {stats['inserted']} OWRS records")
    logger.info("=== OWRS Rate Ingest Complete ===")
    return stats
