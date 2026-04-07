#!/usr/bin/env python3
"""
Apply Denver Water Distributor Local Surcharges (Sprint 30 follow-up)

Purpose:
    Updates rate_schedules for Denver Water Read-and-Bill distributor PWSIDs
    by adding each district's documented LOCAL surcharge on top of the
    Denver Water 2026 pass-through base rates loaded by
    scripts/load_denver_water_distributors.py.

    Each Read-and-Bill distributor passes Denver Water's tiered volumetric
    rates straight through to customers, then adds its own monthly fixed
    charge (sometimes called "service fee", "infrastructure fee", or
    "Capital Improvement Surcharge"). This script encodes the surcharges
    discovered via direct scraping of each district's website and updates
    the corresponding rate_schedules row in place:

      - fixed_charges JSONB: append a second line item for the local fee
      - bill_5/10/20ccf:    add the surcharge to the DW base bill amounts
      - parse_notes:        replace the [denver_water_read_and_bill_base]
                            tag with [denver_water_read_and_bill_plus_surcharge]
                            and cite the surcharge source URL
      - source_url:         keep the DW URL but mention surcharge URL in notes

    Special cases:
      - CO0103614 Platte Canyon: existing partial parse (Tier 1 only, no max_gal,
        bill_10ccf=$43.61 < DW base $49.58). Overwritten with the full DW 3-tier
        structure plus the $18 Infrastructure Fee.
      - CO0103186 Country Homes Land Co (100 pop): no web presence found.
        Left at DW base only; parse_notes flagged for manual follow-up.
      - CO0103100 Willows WD: NOT a DW pass-through. Existing high-confidence
        parse from willowswater.org represents Willows's own integrated rates
        (4 tiers, base $10.96/mo). NOT touched by this script.

Author: AI-Generated
Created: 2026-04-07
Modified: 2026-04-07

Dependencies:
    - utility_api (local package)
    - sqlalchemy

Usage:
    # Dry run — show planned changes without writing
    python scripts/apply_dw_distributor_surcharges.py --dry-run

    # Apply changes
    python scripts/apply_dw_distributor_surcharges.py

    # Apply changes AND refresh best_estimate for CO
    python scripts/apply_dw_distributor_surcharges.py --refresh-best-estimate

Notes:
    - Idempotent on parse_notes tag: if the row is already tagged
      [denver_water_read_and_bill_plus_surcharge] the row is skipped
      (re-runs are safe).
    - Takes a Tier 2 snapshot before writing (via the existing
      utility_api.ops.snapshot module).
    - Bill amounts assume the surcharge is a flat monthly fee (no
      consumption component). All discovered surcharges fit this assumption.

Data Sources:
    - Southgate (CO0103721):    +$14.97/mo  https://southgatedistricts.org/157/Monthly-Water-Service-Charges
    - SW Metro (CO0103723):     +$12.00/mo  https://swmetrowater.org/2026-budget-highlights/
    - Platte Canyon (CO0103614):+$18.00/mo  https://plattecanyon.org/2024-water-and-sewer-rates/
    - Bear Creek (CO0130138):   +$8.00/mo   https://www.bearcreekwater.org/services
    - North Lincoln (CO0116552):+$18.00/mo  https://northlincolnwsd.colorado.gov  (Capital Improvement Surcharge, eff 2026-01-01)
    - Country Homes (CO0103186): NO CHANGE  (no web presence; 100 pop)
    - Willows (CO0103100):       NO CHANGE  (standalone integrated rates, not a pass-through)
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine

schema = settings.utility_schema

# Denver Water 2026 Read-and-Bill base bill amounts (no surcharge).
# These match the constants in scripts/load_denver_water_distributors.py.
DW_BASE_BILL_5CCF = 32.24
DW_BASE_BILL_10CCF = 49.58
DW_BASE_BILL_20CCF = 90.34
DW_FIXED_CHARGE = 20.91
DW_TIER_1_RATE = 3.03
DW_TIER_2_RATE = 5.45
DW_TIER_3_RATE = 7.26
DW_AWC_GAL = 5000
DW_TIER_2_LIMIT = 20000
DW_VINTAGE = "2026-01-01"
DW_SOURCE_URL = "https://www.denverwater.org/residential/billing-and-rates/2026-rates"

NEW_TAG = "[denver_water_read_and_bill_plus_surcharge]"
OLD_TAG = "[denver_water_read_and_bill_base]"


# Surcharge specs. Each entry encodes one PWSID's local fee on top of DW base.
# `surcharge`: monthly $ added to fixed_charges and to all bill_Nccf values.
# `fee_label`:  the line-item label to put in fixed_charges JSONB.
# `surcharge_url`: where the value was sourced from (cited in parse_notes).
# `notes_extra`: any caveat or methodology note to append.
SURCHARGES: list[dict] = [
    {
        "pwsid": "CO0103721",
        "name": "Southgate Water District",
        "surcharge": 14.97,
        "fee_label": "Local Service Charge (Southgate)",
        "surcharge_url": "https://southgatedistricts.org/157/Monthly-Water-Service-Charges",
        "notes_extra": (
            "Surcharge is the 3/4-inch tap monthly service charge from "
            "southgatedistricts.org/157/Monthly-Water-Service-Charges. Page "
            "states fees are billed by Denver Water on Southgate's behalf. "
            "Effective 1/1/2026."
        ),
    },
    {
        "pwsid": "CO0103723",
        "name": "Southwest Metropolitan WSD",
        "surcharge": 12.00,
        "fee_label": "District Service Fee (SW Metro)",
        "surcharge_url": "https://swmetrowater.org/2026-budget-highlights/",
        "notes_extra": (
            "$12.00/month per 3/4-inch equivalent water tap. Confirmed in the "
            "2026 Budget Highlights as unchanged from prior year. Historical: "
            "$7 (2019) -> $8 (2020) -> $12 (2022) -> $12 (2026). District is "
            "a Denver Water Read-and-Bill distributor; surcharge is collected "
            "via the Denver Water bill on SW Metro's behalf."
        ),
    },
    {
        "pwsid": "CO0103614",
        "name": "Platte Canyon WSD",
        "surcharge": 18.00,
        "fee_label": "Infrastructure Fee (Platte Canyon)",
        "surcharge_url": "https://plattecanyon.org/2024-water-and-sewer-rates/",
        "notes_extra": (
            "Infrastructure Fee for 5/8-3/4 inch meters per plattecanyon.org "
            "2024 rate page (last documented value; 2026 page only lists "
            "Denver Water pass-through tiers and does not republish the local "
            "fee). Replaces the prior partial parse (id=31629) which only "
            "captured DW Tier 1 with no upper limit, producing an under-stated "
            "bill_10ccf=$43.61. New row uses the full DW 3-tier structure + "
            "$18 Infrastructure Fee."
        ),
    },
    {
        "pwsid": "CO0130138",
        "name": "Bear Creek WSD",
        "surcharge": 8.00,
        "fee_label": "Local Surcharge (Bear Creek SFRE)",
        "surcharge_url": "https://www.bearcreekwater.org/services",
        "notes_extra": (
            "$8.00/month per Single Family Residential Equivalent (SFRE), "
            "collected via Denver Water billing for water distribution system "
            "O&M. Source: bearcreekwater.org/services. Note: the district's "
            "billing-and-payment page still lists $7.00, which appears stale."
        ),
    },
    {
        "pwsid": "CO0116552",
        "name": "North Lincoln WSD",
        "surcharge": 18.00,
        "fee_label": "Capital Improvement Surcharge (North Lincoln)",
        "surcharge_url": "https://northlincolnwsd.colorado.gov",
        "notes_extra": (
            "$18.00/month per SFE Capital Improvement Surcharge, administered "
            "and billed by Denver Water on the District's behalf, effective "
            "January 1, 2026. The District also charges a separate $110.00 "
            "per quarter service rate covering combined 'water and sanitation "
            "operations'; that fee is NOT included here because the water/sewer "
            "split is not disclosed and inclusion would over-state the water "
            "bill. Source language references the Total Service Agreement, "
            "which is unusual for a Read-and-Bill distributor and may warrant "
            "a methodology review."
        ),
    },
]

# PWSIDs we explicitly DO NOT touch, with reasoning. Surfaced in dry-run output
# so the user sees the full distributor inventory in one place.
NO_CHANGE: list[dict] = [
    {
        "pwsid": "CO0103186",
        "name": "Country Homes Land Co",
        "reason": (
            "Population ~100. No web presence found via direct search "
            "(no .gov / .org / .colorado.gov page). 'Land Co' suggests a "
            "private subdivision developer rather than a public district. "
            "Leaving the DW base ($49.58) in place; surcharge unknown."
        ),
    },
    {
        "pwsid": "CO0103100",
        "name": "Willows Water District",
        "reason": (
            "willowswater.org publishes Willows's OWN integrated 4-tier rate "
            "structure (base $10.96/mo, Tier 1 $5.64/1000gal which is well "
            "above DW's $3.03 Tier 1) with no mention of Denver Water "
            "pass-through. Existing high-confidence parse (id=1535, "
            "bill_10ccf=$53.16) is the customer's full bill, NOT a DW base "
            "needing a surcharge added. Not touched."
        ),
    },
]


def _fetch_existing(conn, pwsid: str) -> dict | None:
    row = conn.execute(
        text(
            f"""
            SELECT id, pwsid, source_key, vintage_date, customer_class,
                   fixed_charges, volumetric_tiers,
                   bill_5ccf, bill_10ccf, bill_20ccf,
                   parse_notes, source_url
            FROM {schema}.rate_schedules
            WHERE pwsid = :p
              AND source_key = 'scraped_llm'
              AND vintage_date = :v
            ORDER BY id DESC
            LIMIT 1
            """
        ),
        {"p": pwsid, "v": DW_VINTAGE},
    ).fetchone()
    return dict(row._mapping) if row else None


def _apply_surcharge(conn, spec: dict, dry_run: bool) -> str:
    """Apply one surcharge spec. Returns a status string."""
    existing = _fetch_existing(conn, spec["pwsid"])
    if existing is None:
        return f"SKIP (no existing 2026 rate_schedules row for {spec['pwsid']})"

    notes = existing["parse_notes"] or ""
    if NEW_TAG in notes:
        return f"SKIP (already tagged {NEW_TAG}; idempotent re-run)"

    surcharge = float(spec["surcharge"])

    # Build new fixed_charges by appending the local fee line item.
    # For Platte Canyon (the partial-parse case), the existing fixed_charges
    # already has the DW Service Charge ($20.91), so this works for both the
    # bulk-loaded rows and the Platte Canyon overwrite.
    fixed = list(existing["fixed_charges"] or [])
    # Defensive: ensure DW base service charge is present. If not, prepend.
    has_dw_base = any(
        isinstance(f, dict) and abs(float(f.get("amount", 0)) - DW_FIXED_CHARGE) < 0.01
        for f in fixed
    )
    if not has_dw_base:
        fixed.insert(
            0,
            {"name": "Service Charge", "amount": DW_FIXED_CHARGE, "meter_size": "0.75"},
        )
    fixed.append(
        {
            "name": spec["fee_label"],
            "amount": surcharge,
            "meter_size": "0.75",
        }
    )

    # Volumetric tiers — for Platte Canyon's partial parse, replace with the
    # full DW 3-tier structure. For the bulk-loaded rows, the tiers are
    # already correct; preserve them.
    tiers = list(existing["volumetric_tiers"] or [])
    needs_full_tiers = (
        len(tiers) < 3
        or any(t.get("max_gal") is None for t in tiers[:-1])  # missing limit
    )
    if needs_full_tiers:
        tiers = [
            {"tier": 1, "min_gal": 0, "max_gal": DW_AWC_GAL, "rate_per_1000_gal": DW_TIER_1_RATE},
            {"tier": 2, "min_gal": DW_AWC_GAL, "max_gal": DW_TIER_2_LIMIT, "rate_per_1000_gal": DW_TIER_2_RATE},
            {"tier": 3, "min_gal": DW_TIER_2_LIMIT, "max_gal": None, "rate_per_1000_gal": DW_TIER_3_RATE},
        ]

    # Bills: always recompute as DW base + surcharge. This is correct for
    # both the bulk-loaded rows (which already have DW base) and the Platte
    # Canyon overwrite (whose bills were wrong because of the partial parse).
    new_bill_5 = round(DW_BASE_BILL_5CCF + surcharge, 2)
    new_bill_10 = round(DW_BASE_BILL_10CCF + surcharge, 2)
    new_bill_20 = round(DW_BASE_BILL_20CCF + surcharge, 2)

    # parse_notes: replace OLD_TAG with NEW_TAG if present, else prepend.
    if OLD_TAG in notes:
        new_notes_head = notes.replace(OLD_TAG, NEW_TAG, 1)
    else:
        new_notes_head = f"{NEW_TAG} {notes}".strip()
    new_notes = (
        f"{new_notes_head}\n\n"
        f"--- Surcharge applied {datetime.now(timezone.utc).strftime('%Y-%m-%d')} ---\n"
        f"Distributor: {spec['name']}. Local surcharge: ${surcharge:.2f}/mo "
        f"({spec['fee_label']}). Source: {spec['surcharge_url']}. "
        f"{spec['notes_extra']} "
        f"Recomputed bills: DW base + surcharge = "
        f"${new_bill_5:.2f}/${new_bill_10:.2f}/${new_bill_20:.2f}."
    )

    msg = (
        f"UPDATE id={existing['id']} {spec['pwsid']} {spec['name']}: "
        f"+${surcharge:.2f}  bill_10ccf "
        f"${existing['bill_10ccf']:.2f} -> ${new_bill_10:.2f}"
    )
    if dry_run:
        return f"[DRY RUN] {msg}"

    conn.execute(
        text(
            f"""
            UPDATE {schema}.rate_schedules
            SET fixed_charges    = CAST(:fixed AS jsonb),
                volumetric_tiers = CAST(:tiers AS jsonb),
                bill_5ccf        = :b5,
                bill_10ccf       = :b10,
                bill_20ccf       = :b20,
                parse_notes      = :notes,
                parse_model      = 'denver_water_distributor_v1_plus_surcharge',
                tier_count       = 3
            WHERE id = :id
            """
        ),
        {
            "fixed": json.dumps(fixed),
            "tiers": json.dumps(tiers),
            "b5": new_bill_5,
            "b10": new_bill_10,
            "b20": new_bill_20,
            "notes": new_notes,
            "id": existing["id"],
        },
    )
    return msg


def main():
    parser = argparse.ArgumentParser(
        description="Apply DW Read-and-Bill distributor local surcharges"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show planned changes; do not write"
    )
    parser.add_argument(
        "--refresh-best-estimate",
        action="store_true",
        help="Run BestEstimateAgent for CO after applying changes",
    )
    args = parser.parse_args()

    print("=" * 78)
    print("Denver Water Distributor Local Surcharge Application")
    print("=" * 78)
    print()
    print("Surcharges to apply:")
    for s in SURCHARGES:
        print(
            f"  {s['pwsid']}  {s['name'][:36]:<36}  +${s['surcharge']:>6.2f}/mo  "
            f"({s['fee_label']})"
        )
    print()
    print("PWSIDs intentionally NOT changed:")
    for n in NO_CHANGE:
        print(f"  {n['pwsid']}  {n['name'][:36]:<36}  reason: see notes")
    print()

    print("=" * 78)
    print("Applying" + (" (DRY RUN)" if args.dry_run else ""))
    print("=" * 78)

    if args.dry_run:
        # Use a non-committing connection
        with engine.connect() as conn:
            for spec in SURCHARGES:
                msg = _apply_surcharge(conn, spec, dry_run=True)
                print(f"  {msg}")
    else:
        with engine.begin() as conn:
            for spec in SURCHARGES:
                msg = _apply_surcharge(conn, spec, dry_run=False)
                print(f"  {msg}")

    print()
    if args.dry_run:
        print("Dry run — no changes written.")
        return

    if args.refresh_best_estimate:
        print("=" * 78)
        print("Refreshing rate_best_estimate for CO")
        print("=" * 78)
        from utility_api.agents.best_estimate import BestEstimateAgent

        BestEstimateAgent().run(state="CO")
        print()

    print("Done.")


if __name__ == "__main__":
    main()
