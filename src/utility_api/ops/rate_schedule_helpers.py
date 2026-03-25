#!/usr/bin/env python3
"""
Rate Schedule Helpers

Purpose:
    Conversion utilities for transforming between the legacy water_rates
    schema (fixed 4-tier columns) and the canonical rate_schedules schema
    (JSONB tiers). Used by:
    - scripts/migrate_to_rate_schedules.py (one-time migration)
    - Ingest modules (dual-write to both tables)
    - Best-estimate computation (reads canonical tiers)

    Unit convention:
    - water_rates stores tiers in CCF and $/CCF
    - rate_schedules stores in gallons and $/1000 gal
    - 1 CCF = 748 gallons
    - $/CCF × (1000/748) = $/1000 gal

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - sqlalchemy

Usage:
    from utility_api.ops.rate_schedule_helpers import (
        water_rate_to_schedule,
        compute_bill_at_gallons,
        compute_conservation_signal,
    )
"""

import json
from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine

# Unit conversions
CCF_TO_GAL = 748.0  # 1 CCF = 748 gallons
RATE_CCF_TO_PER_1000_GAL = 1000.0 / CCF_TO_GAL  # $/CCF → $/1000gal multiplier


def ccf_to_gal(ccf: float | None) -> float | None:
    """Convert CCF to gallons."""
    if ccf is None:
        return None
    return round(ccf * CCF_TO_GAL, 0)


def rate_ccf_to_per_1000_gal(rate_per_ccf: float | None) -> float | None:
    """Convert $/CCF to $/1000 gallons."""
    if rate_per_ccf is None:
        return None
    return round(rate_per_ccf * RATE_CCF_TO_PER_1000_GAL, 4)


def _is_valid(val) -> bool:
    """Check if a value is non-null and non-NaN."""
    if val is None:
        return False
    try:
        import math
        return not math.isnan(val)
    except (TypeError, ValueError):
        return val is not None


def build_volumetric_tiers(row: dict) -> list[dict] | None:
    """Convert fixed tier columns to JSONB tier array.

    Parameters
    ----------
    row : dict
        A water_rates record with tier_1_rate, tier_1_limit_ccf, etc.

    Returns
    -------
    list[dict] or None
        JSONB-compatible tier array, or None if no tiers present.
    """
    tiers = []
    prev_max = 0.0

    for i in range(1, 5):
        rate_ccf = row.get(f"tier_{i}_rate")
        if not _is_valid(rate_ccf):
            break

        limit_ccf = row.get(f"tier_{i}_limit_ccf")
        limit_valid = _is_valid(limit_ccf)

        tier = {
            "tier": i,
            "min_gal": int(round(prev_max)),
            "max_gal": int(ccf_to_gal(limit_ccf)) if limit_valid else None,
            "rate_per_1000_gal": rate_ccf_to_per_1000_gal(rate_ccf),
        }
        tiers.append(tier)

        if limit_valid:
            prev_max = ccf_to_gal(limit_ccf)
        else:
            break  # Unlimited tier = last tier

    return tiers if tiers else None


def build_fixed_charges(row: dict) -> list[dict] | None:
    """Convert fixed charge column to JSONB array.

    Parameters
    ----------
    row : dict
        A water_rates record with fixed_charge_monthly.

    Returns
    -------
    list[dict] or None
        JSONB-compatible fixed charges array.
    """
    fixed = row.get("fixed_charge_monthly")
    if not _is_valid(fixed) or fixed == 0:
        return None

    meter = row.get("meter_size_inches")
    charges = [{
        "name": "Service Charge",
        "amount": round(float(fixed), 2),
        "meter_size": str(meter) if _is_valid(meter) else None,
    }]
    return charges


def compute_conservation_signal(tiers: list[dict] | None) -> float | None:
    """Compute the conservation signal from tier rates.

    Conservation signal = highest tier rate / lowest tier rate.
    A value > 1 indicates conservation pricing (higher usage = higher cost).

    Returns
    -------
    float or None
        Conservation signal ratio, or None if < 2 tiers.
    """
    if not tiers or len(tiers) < 2:
        return None

    rates = [t["rate_per_1000_gal"] for t in tiers if t.get("rate_per_1000_gal")]
    if len(rates) < 2:
        return None

    lowest = min(rates)
    highest = max(rates)
    if lowest <= 0:
        return None

    return round(highest / lowest, 2)


def compute_bill_at_gallons(
    gallons: float,
    tiers: list[dict] | None,
    fixed_charges: list[dict] | None,
) -> float | None:
    """Compute monthly bill at a given consumption level.

    Parameters
    ----------
    gallons : float
        Monthly consumption in gallons.
    tiers : list[dict]
        Volumetric tier array from rate_schedules.
    fixed_charges : list[dict]
        Fixed charges array.

    Returns
    -------
    float or None
        Computed monthly bill in USD.
    """
    total = 0.0

    # Fixed charges
    if fixed_charges:
        for fc in fixed_charges:
            total += fc.get("amount", 0)

    # Volumetric charges
    if tiers:
        remaining = gallons
        for tier in sorted(tiers, key=lambda t: t.get("tier", 0)):
            min_gal = tier.get("min_gal", 0) or 0
            max_gal = tier.get("max_gal")
            rate = tier.get("rate_per_1000_gal", 0) or 0

            if remaining <= 0:
                break

            if max_gal is not None:
                tier_volume = min(remaining, max_gal - min_gal)
            else:
                tier_volume = remaining  # Unlimited tier

            total += (tier_volume / 1000.0) * rate
            remaining -= tier_volume

    return round(total, 2) if total > 0 else None


def water_rate_to_schedule(row: dict) -> dict:
    """Convert a water_rates record to a rate_schedules record.

    Parameters
    ----------
    row : dict
        A water_rates record (dict with column names as keys).

    Returns
    -------
    dict
        A rate_schedules record ready for insert.
    """
    tiers = build_volumetric_tiers(row)
    fixed = build_fixed_charges(row)
    conservation = compute_conservation_signal(tiers)
    tier_count = len(tiers) if tiers else 0

    # Compute bill_20ccf (14,960 gallons)
    bill_20ccf = compute_bill_at_gallons(14960, tiers, fixed)

    def _clean(val):
        """Convert NaN to None for DB insertion."""
        if not _is_valid(val):
            return None
        return val

    return {
        "pwsid": row["pwsid"],
        "source_key": row.get("source", "scraped_llm"),
        "vintage_date": _clean(row.get("rate_effective_date")),
        "customer_class": row.get("rate_class", "residential") or "residential",
        "billing_frequency": _clean(row.get("billing_frequency")),
        "rate_structure_type": _clean(row.get("rate_structure_type")),
        "fixed_charges": json.dumps(fixed) if fixed else None,
        "volumetric_tiers": json.dumps(tiers) if tiers else None,
        "surcharges": None,
        "bill_5ccf": _clean(row.get("bill_5ccf")),
        "bill_10ccf": _clean(row.get("bill_10ccf")),
        "bill_20ccf": _clean(bill_20ccf),
        "conservation_signal": _clean(conservation),
        "tier_count": tier_count,
        "source_url": _clean(row.get("source_url")),
        "scrape_timestamp": _clean(row.get("scraped_at")),
        "confidence": _clean(row.get("parse_confidence")),
        "raw_text_hash": _clean(row.get("raw_text_hash")),
        "parse_model": _clean(row.get("parse_model")),
        "parse_notes": _clean(row.get("parse_notes")),
        "needs_review": False,
        "review_reason": None,
    }


def write_rate_schedule(conn, record: dict) -> bool:
    """Insert or update a single rate_schedule record.

    Uses INSERT ON CONFLICT UPDATE on the unique constraint
    (pwsid, source_key, vintage_date, customer_class).

    Parameters
    ----------
    conn : sqlalchemy Connection
        Active database connection.
    record : dict
        Rate schedule record from water_rate_to_schedule().

    Returns
    -------
    bool
        True if inserted/updated, False if skipped.
    """
    schema = settings.utility_schema

    try:
        conn.execute(text(f"""
            INSERT INTO {schema}.rate_schedules (
                pwsid, source_key, vintage_date, customer_class,
                billing_frequency, rate_structure_type,
                fixed_charges, volumetric_tiers, surcharges,
                bill_5ccf, bill_10ccf, bill_20ccf,
                conservation_signal, tier_count,
                source_url, scrape_timestamp, confidence,
                raw_text_hash, parse_model, parse_notes,
                needs_review, review_reason
            ) VALUES (
                :pwsid, :source_key, :vintage_date, :customer_class,
                :billing_frequency, :rate_structure_type,
                CAST(:fixed_charges AS jsonb), CAST(:volumetric_tiers AS jsonb), CAST(:surcharges AS jsonb),
                :bill_5ccf, :bill_10ccf, :bill_20ccf,
                :conservation_signal, :tier_count,
                :source_url, :scrape_timestamp, :confidence,
                :raw_text_hash, :parse_model, :parse_notes,
                :needs_review, :review_reason
            )
            ON CONFLICT (pwsid, source_key, vintage_date, customer_class)
            DO UPDATE SET
                billing_frequency = EXCLUDED.billing_frequency,
                rate_structure_type = EXCLUDED.rate_structure_type,
                fixed_charges = EXCLUDED.fixed_charges,
                volumetric_tiers = EXCLUDED.volumetric_tiers,
                surcharges = EXCLUDED.surcharges,
                bill_5ccf = EXCLUDED.bill_5ccf,
                bill_10ccf = EXCLUDED.bill_10ccf,
                bill_20ccf = EXCLUDED.bill_20ccf,
                conservation_signal = EXCLUDED.conservation_signal,
                tier_count = EXCLUDED.tier_count,
                source_url = EXCLUDED.source_url,
                scrape_timestamp = EXCLUDED.scrape_timestamp,
                confidence = EXCLUDED.confidence,
                raw_text_hash = EXCLUDED.raw_text_hash,
                parse_model = EXCLUDED.parse_model,
                parse_notes = EXCLUDED.parse_notes,
                needs_review = EXCLUDED.needs_review,
                review_reason = EXCLUDED.review_reason
        """), record)
        return True
    except Exception as e:
        if "violates foreign key" in str(e):
            return False
        raise
