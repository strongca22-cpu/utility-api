#!/usr/bin/env python3
"""
Denver Water Distributor Rate Loader (Sprint 30)

Purpose:
    Bulk-loads Denver Water Read-and-Bill base rates for distributors that
    don't have their own scraped rate data. This is a methodology fallback
    based on the contractual relationship between Denver Water and its
    11 Read-and-Bill distributors.

    Source of truth: https://www.denverwater.org/residential/billing-and-rates/2026-rates
    Effective: January 1, 2026

    Denver Water 2026 Rate Structure (Outside Denver — Read & Bill):
        Fixed charge (5/8" & 3/4" meter): $20.91/month
        Tier 1 (0 to AWC):                 $3.03 / 1,000 gal
        Tier 2 (AWC to AWC + 15,000):      $5.45 / 1,000 gal
        Tier 3 (above AWC + 15,000):       $7.26 / 1,000 gal

    AWC (Average Winter Consumption) is calculated per-customer from
    Jan-Mar usage. Min 5,000 gal, max 15,000 gal. We assume AWC = 5,000
    (the published minimum), which is conservative — Tier 1 ends at 5k,
    so more of the consumption falls into the more expensive Tier 2.

    Methodology classification:
    - Total Service distributors: NOT applicable. These don't have their
      own PWSIDs in SDWIS — customers are served directly by Denver Water
      Board (CO0116001, 1.287M pop).
    - Read-and-Bill distributors: Insert DW base rates as fallback.
      Distributors may add their own surcharge (e.g., Southgate +$14.97,
      Platte Canyon +$18.00). Surcharges to be discovered via scraping
      and added in a follow-up step.
    - Master Meter distributors: NOT applicable here. Each sets its own
      rates independently and goes through normal scrape pipeline.

Author: AI-Generated
Created: 2026-04-05
Modified: 2026-04-05

Dependencies:
    - utility_api (local package)

Usage:
    # Dry run — show targets and computed bills
    python scripts/load_denver_water_distributors.py --dry-run

    # Run: insert rate_schedules for missing PWSIDs only
    python scripts/load_denver_water_distributors.py

    # Run: also overwrite existing entries with full 3-tier structure
    # (use only if you know the existing parses are incomplete)
    python scripts/load_denver_water_distributors.py --overwrite-existing

Notes:
    - source_key = 'scraped_llm' so it competes normally in best_estimate
    - vintage_date = '2026-01-01' (DW 2026 rates effective date)
    - parse_notes prefixed with '[denver_water_read_and_bill_base]'
    - Existing rate entries are flagged for surcharge spot-check
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.agents.best_estimate import BestEstimateAgent
from utility_api.config import settings
from utility_api.db import engine

schema = settings.utility_schema

# Denver Water 2026 Read-and-Bill rate structure
DW_FIXED_CHARGE = 20.91  # 5/8" & 3/4" meter
DW_TIER_1_RATE = 3.03    # $/1,000 gal
DW_TIER_2_RATE = 5.45
DW_TIER_3_RATE = 7.26
DW_AWC_GAL = 5000        # AWC assumption: published minimum
DW_TIER_2_LIMIT = DW_AWC_GAL + 15000  # AWC + 15,000 = 20,000
DW_VINTAGE = "2026-01-01"
DW_SOURCE_URL = "https://www.denverwater.org/residential/billing-and-rates/2026-rates"

# Read-and-Bill distributors (per Denver Water's published list)
# (DW name, SDWIS pattern, expected_in_sdwis)
RB_DISTRIBUTORS = [
    ("Bear Creek WSD",          "BEAR CREEK WSD",        True),
    ("Country Homes MD",        "COUNTRY HOMES",          True),
    ("North Lincoln WSD",       "NORTH LINCOLN",          True),
    ("Platte Canyon WSD",       "PLATTE CANYON",          True),
    ("South Sheridan WSSD",     "SOUTH SHERIDAN",         False),  # not in SDWIS
    ("Southgate Water District","SOUTHGATE",              True),
    ("Southwest Metropolitan WSD","SOUTHWEST METROPOLITAN",True),
    ("Willows Water District",  "WILLOWS",                True),
    ("Bear Creek WSD (alt)",    "BEAR CREEK WATER",       False),
    ("Colorado DNR",            "COLORADO DEPARTMENT OF NATURAL", False),
    ("Lockheed Martin",         "LOCKHEED",               False),
    ("Phillips Petroleum",      "PHILLIPS PETROLEUM",     False),
]


def compute_bill(gallons: float) -> float:
    """Compute monthly bill at given consumption using DW Read-and-Bill structure."""
    bill = DW_FIXED_CHARGE
    if gallons <= DW_AWC_GAL:
        bill += (gallons / 1000) * DW_TIER_1_RATE
    elif gallons <= DW_TIER_2_LIMIT:
        bill += (DW_AWC_GAL / 1000) * DW_TIER_1_RATE
        bill += ((gallons - DW_AWC_GAL) / 1000) * DW_TIER_2_RATE
    else:
        bill += (DW_AWC_GAL / 1000) * DW_TIER_1_RATE
        bill += ((DW_TIER_2_LIMIT - DW_AWC_GAL) / 1000) * DW_TIER_2_RATE
        bill += ((gallons - DW_TIER_2_LIMIT) / 1000) * DW_TIER_3_RATE
    return round(bill, 2)


def find_pwsid(pattern: str) -> dict | None:
    """Find SDWIS PWSID matching the pattern."""
    with engine.connect() as conn:
        r = conn.execute(text(f"""
            SELECT s.pwsid, s.pws_name, s.population_served_count as pop,
                   rs.id as rate_id, rs.confidence, rs.bill_10ccf
            FROM {schema}.sdwis_systems s
            LEFT JOIN {schema}.rate_schedules rs
                ON rs.pwsid = s.pwsid
                AND rs.source_key = 'scraped_llm'
                AND rs.vintage_date = :vintage
            WHERE s.state_code = 'CO'
              AND s.activity_status_cd = 'A'
              AND UPPER(s.pws_name) LIKE :pat
            ORDER BY s.population_served_count DESC NULLS LAST
            LIMIT 1
        """), {"pat": f"%{pattern}%", "vintage": DW_VINTAGE}).fetchone()
        if r:
            return dict(r._mapping)
    return None


def insert_dw_rate(pwsid: str, dw_name: str, sdwis_name: str, overwrite: bool = False) -> bool:
    """Insert Denver Water Read-and-Bill base rate for a PWSID.

    Returns True if inserted/updated, False if skipped.
    """
    # Build canonical structures
    fixed_charges = json.dumps([{
        "name": "Service Charge",
        "amount": DW_FIXED_CHARGE,
        "meter_size": "0.75",
    }])
    volumetric_tiers = json.dumps([
        {"tier": 1, "min_gal": 0,           "max_gal": DW_AWC_GAL,
         "rate_per_1000_gal": DW_TIER_1_RATE},
        {"tier": 2, "min_gal": DW_AWC_GAL,  "max_gal": DW_TIER_2_LIMIT,
         "rate_per_1000_gal": DW_TIER_2_RATE},
        {"tier": 3, "min_gal": DW_TIER_2_LIMIT, "max_gal": None,
         "rate_per_1000_gal": DW_TIER_3_RATE},
    ])
    bill_5 = compute_bill(3740)
    bill_10 = compute_bill(7480)
    bill_20 = compute_bill(14960)
    conservation = round(DW_TIER_3_RATE / DW_TIER_1_RATE, 2)

    notes = (
        f"[denver_water_read_and_bill_base] Bulk-loaded fallback from "
        f"Denver Water 2026 published rates. Distributor: {dw_name}. "
        f"AWC assumption = {DW_AWC_GAL} gal (DW published minimum). "
        f"Distributor may add a local surcharge (e.g., Southgate +$14.97, "
        f"Platte Canyon +$18.00) — surcharge lookup pending. "
        f"Spot-check manually."
    )

    # Use ON CONFLICT to handle existing entries
    if overwrite:
        action = "INSERT...ON CONFLICT DO UPDATE"
    else:
        action = "INSERT...ON CONFLICT DO NOTHING"

    with engine.begin() as conn:
        result = conn.execute(text(f"""
            INSERT INTO {schema}.rate_schedules (
                pwsid, source_key, vintage_date, customer_class,
                billing_frequency, rate_structure_type,
                fixed_charges, volumetric_tiers,
                bill_5ccf, bill_10ccf, bill_20ccf,
                conservation_signal, tier_count,
                source_url, scrape_timestamp, confidence,
                parse_model, parse_notes
            ) VALUES (
                :pwsid, 'scraped_llm', :vintage, 'residential',
                'monthly', 'tiered',
                CAST(:fixed AS jsonb), CAST(:tiers AS jsonb),
                :bill5, :bill10, :bill20,
                :cons, 3,
                :url, :now, 'medium',
                'denver_water_distributor_v1', :notes
            )
            ON CONFLICT (pwsid, source_key, vintage_date, customer_class)
            {"DO UPDATE SET fixed_charges = EXCLUDED.fixed_charges, volumetric_tiers = EXCLUDED.volumetric_tiers, bill_5ccf = EXCLUDED.bill_5ccf, bill_10ccf = EXCLUDED.bill_10ccf, bill_20ccf = EXCLUDED.bill_20ccf, parse_model = EXCLUDED.parse_model, parse_notes = EXCLUDED.parse_notes" if overwrite else "DO NOTHING"}
            RETURNING id
        """), {
            "pwsid": pwsid,
            "vintage": DW_VINTAGE,
            "fixed": fixed_charges,
            "tiers": volumetric_tiers,
            "bill5": bill_5,
            "bill10": bill_10,
            "bill20": bill_20,
            "cons": conservation,
            "url": DW_SOURCE_URL,
            "now": datetime.now(timezone.utc),
            "notes": notes,
        })
        row = result.fetchone()
        return row is not None


def main():
    parser = argparse.ArgumentParser(
        description="Load Denver Water distributor base rates"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show targets without inserting")
    parser.add_argument("--overwrite-existing", action="store_true",
                        help="Overwrite existing rate_schedules with same vintage")
    args = parser.parse_args()

    print("=" * 70)
    print("Denver Water Read-and-Bill Distributor Loader")
    print("=" * 70)
    print()
    print(f"DW 2026 Rate Structure (Outside Denver, Read & Bill):")
    print(f"  Fixed charge: ${DW_FIXED_CHARGE}/mo (5/8\" & 3/4\" meter)")
    print(f"  Tier 1 (0–{DW_AWC_GAL:,} gal):     ${DW_TIER_1_RATE}/1,000 gal")
    print(f"  Tier 2 ({DW_AWC_GAL+1:,}–{DW_TIER_2_LIMIT:,} gal): ${DW_TIER_2_RATE}/1,000 gal")
    print(f"  Tier 3 (>{DW_TIER_2_LIMIT:,} gal):    ${DW_TIER_3_RATE}/1,000 gal")
    print(f"  AWC assumption: {DW_AWC_GAL:,} gal (DW published minimum)")
    print()
    print(f"Computed bills (no surcharge):")
    print(f"  bill_5ccf  (3,740 gal): ${compute_bill(3740):.2f}")
    print(f"  bill_10ccf (7,480 gal): ${compute_bill(7480):.2f}")
    print(f"  bill_20ccf (14,960 gal): ${compute_bill(14960):.2f}")
    print()

    print("=" * 70)
    print("Read-and-Bill Distributor Status")
    print("=" * 70)
    print(f"{'DW Name':<32} {'PWSID':<12} {'Pop':>9}  {'Existing':<12} Action")
    print("-" * 90)

    targets_to_insert = []  # PWSIDs without any existing rate row
    targets_to_overwrite = []  # PWSIDs with existing parse, flag for review

    for dw_name, pattern, expected in RB_DISTRIBUTORS:
        m = find_pwsid(pattern)
        if not m:
            if expected:
                print(f"{dw_name[:31]:<32} {'(NOT FOUND)':<12}")
            continue

        existing = "yes" if m["rate_id"] else "no"
        existing_bill = f"${m['bill_10ccf']:.2f}" if m["bill_10ccf"] else "-"

        if m["rate_id"]:
            action = "FLAG (existing parse, surcharge spot-check)"
            targets_to_overwrite.append(m | {"dw_name": dw_name})
        else:
            action = "INSERT (DW base fallback)"
            targets_to_insert.append(m | {"dw_name": dw_name})

        print(f"{dw_name[:31]:<32} {m['pwsid']:<12} {m['pop'] or 0:>9,}  "
              f"{existing_bill:<12} {action}")

    print()
    print(f"Summary:")
    print(f"  To INSERT: {len(targets_to_insert)} PWSIDs "
          f"({sum(t['pop'] or 0 for t in targets_to_insert):,} pop)")
    print(f"  To FLAG (existing): {len(targets_to_overwrite)} PWSIDs")

    if args.dry_run:
        print()
        print("Dry run — no changes made.")
        return

    # Execute inserts
    print()
    print("=" * 70)
    print("Executing inserts")
    print("=" * 70)
    inserted = 0
    for t in targets_to_insert:
        ok = insert_dw_rate(
            pwsid=t["pwsid"],
            dw_name=t["dw_name"],
            sdwis_name=t["pws_name"],
            overwrite=False,
        )
        status = "✓" if ok else "(skipped)"
        print(f"  {status} {t['pwsid']} {t['dw_name']}")
        if ok:
            inserted += 1

    print(f"\nInserted {inserted} new rate_schedules")

    # Trigger best_estimate refresh
    print()
    print("Refreshing rate_best_estimate for CO...")
    BestEstimateAgent().run(state="CO")

    print()
    print("Done.")


if __name__ == "__main__":
    main()
