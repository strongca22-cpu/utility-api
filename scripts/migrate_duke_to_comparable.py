#!/usr/bin/env python3
"""
Duke NIEPS Comparability Migration

Purpose:
    One-time migration to patch existing Duke NIEPS records in rate_schedules
    so they are structurally comparable to scraped_llm records. Fixes:
    1. Strip `frequency` key from fixed_charges JSONB
    2. Make tier boundaries contiguous (no rounding gaps)
    3. Remove duplicate tiers
    4. Recalculate bills and conservation_signal after tier fixes
    5. Set nuanced confidence levels (not all "high")

    Targets ONLY rate_schedules WHERE source_key = 'duke_nieps_10state'.
    Does NOT touch water_rates, duke_reference_rates, or any other table.

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Dependencies:
    - sqlalchemy
    - loguru

Usage:
    python scripts/migrate_duke_to_comparable.py --dry-run   # Preview
    python scripts/migrate_duke_to_comparable.py              # Execute

Notes:
    - Idempotent: safe to re-run (patches are deterministic)
    - Imports bill calculation from duke_nieps_ingest.py
    - All changes logged to pipeline_runs table
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger
from sqlalchemy import text

# Add project to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine
from utility_api.ingest.duke_nieps_ingest import (
    GAL_PER_CCF,
    _calculate_bill,
    _conservation_signal,
)

SCHEMA = settings.utility_schema
SOURCE_KEY = "duke_nieps_10state"


def fix_fixed_charges(conn, dry_run: bool) -> int:
    """A1: Strip `frequency` key from fixed_charges JSONB.

    Scraped_llm fixed charges have {name, amount, meter_size} only.
    Duke adds {frequency: "monthly"} — remove it for consistency.

    Returns
    -------
    int
        Number of records updated.
    """
    if dry_run:
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM {SCHEMA}.rate_schedules
            WHERE source_key = :sk
              AND fixed_charges IS NOT NULL
              AND fixed_charges::text LIKE '%frequency%'
        """), {"sk": SOURCE_KEY})
        count = result.scalar()
        logger.info(f"  A1 [dry-run]: {count} records have frequency key in fixed_charges")
        return count

    result = conn.execute(text(f"""
        UPDATE {SCHEMA}.rate_schedules
        SET fixed_charges = (
            SELECT jsonb_agg(elem - 'frequency')
            FROM jsonb_array_elements(fixed_charges) AS elem
        )
        WHERE source_key = :sk
          AND fixed_charges IS NOT NULL
          AND fixed_charges::text LIKE '%frequency%'
    """), {"sk": SOURCE_KEY})
    count = result.rowcount
    logger.info(f"  A1: Stripped frequency from {count} records")
    return count


def fix_tiers_and_recalculate(conn, dry_run: bool) -> dict:
    """A2-A5: Fix tier boundaries, dedup, recalculate bills, set confidence.

    Returns
    -------
    dict
        Counts: contiguity_fixed, deduped, bills_recalculated, confidence_changed.
    """
    # Fetch all Duke records with volumetric tiers
    rows = conn.execute(text(f"""
        SELECT id, pwsid, volumetric_tiers, fixed_charges,
               bill_5ccf, bill_10ccf, bill_20ccf,
               tier_count, confidence, conservation_signal,
               rate_structure_type
        FROM {SCHEMA}.rate_schedules
        WHERE source_key = :sk
        ORDER BY pwsid
    """), {"sk": SOURCE_KEY}).fetchall()

    stats = {
        "total": len(rows),
        "contiguity_fixed": 0,
        "deduped": 0,
        "bills_changed": 0,
        "confidence_changed": 0,
        "flagged_review": 0,
    }

    updates = []

    for row in rows:
        record_id = row.id
        tiers = row.volumetric_tiers  # Already parsed as list[dict] by psycopg
        fixed_charges = row.fixed_charges

        # Parse fixed charge for bill calculation
        fixed_monthly = None
        if fixed_charges and len(fixed_charges) > 0:
            fixed_monthly = fixed_charges[0].get("amount")

        changed = False
        deduped_this = False
        contiguity_this = False

        if tiers and len(tiers) > 1:
            # --- A2: Make contiguous ---
            for i in range(1, len(tiers)):
                prev_max = tiers[i - 1].get("max_gal")
                curr_min = tiers[i].get("min_gal")
                if prev_max is not None and curr_min is not None and prev_max != curr_min:
                    tiers[i]["min_gal"] = prev_max
                    contiguity_this = True

            # --- A3: Deduplicate ---
            seen = set()
            unique_tiers = []
            for t in tiers:
                key = (t.get("min_gal"), t.get("max_gal"), t.get("rate_per_1000_gal"))
                if key not in seen:
                    seen.add(key)
                    unique_tiers.append(t)

            if len(unique_tiers) < len(tiers):
                tiers = unique_tiers
                deduped_this = True

            # Re-number tiers
            for i, t in enumerate(tiers):
                t["tier"] = i + 1

            if contiguity_this or deduped_this:
                changed = True

        if contiguity_this:
            stats["contiguity_fixed"] += 1
        if deduped_this:
            stats["deduped"] += 1

        # --- A4: Recalculate bills ---
        new_bill_5 = _calculate_bill(fixed_monthly, tiers, 5.0 * GAL_PER_CCF) if tiers else (
            round(fixed_monthly, 2) if fixed_monthly else None
        )
        new_bill_10 = _calculate_bill(fixed_monthly, tiers, 10.0 * GAL_PER_CCF) if tiers else (
            round(fixed_monthly, 2) if fixed_monthly else None
        )
        new_bill_20 = _calculate_bill(fixed_monthly, tiers, 20.0 * GAL_PER_CCF) if tiers else (
            round(fixed_monthly, 2) if fixed_monthly else None
        )
        new_cons = _conservation_signal(tiers)

        # Check if bills actually changed
        bills_shifted = (
            _val_differs(row.bill_5ccf, new_bill_5)
            or _val_differs(row.bill_10ccf, new_bill_10)
            or _val_differs(row.bill_20ccf, new_bill_20)
        )
        if bills_shifted:
            stats["bills_changed"] += 1
            changed = True

        # --- A5: Nuanced confidence ---
        tier_count = len(tiers) if tiers else 0
        review_reasons = []

        if deduped_this:
            review_reasons.append("duplicate tiers removed")

        # Check for identical bills at 5/10/20 CCF (tier boundaries above 20 CCF)
        if (
            tier_count > 1
            and new_bill_5 is not None
            and new_bill_10 is not None
            and new_bill_20 is not None
            and new_bill_5 == new_bill_10 == new_bill_20
            and row.rate_structure_type not in ("flat", "uniform")
        ):
            review_reasons.append("increasing_block but identical bills at 5/10/20 CCF")

        # Determine confidence
        if new_bill_10 is not None and 10 <= new_bill_10 <= 200 and tier_count >= 2 and not deduped_this:
            new_confidence = "high"
        elif new_bill_10 is not None and 5 <= new_bill_10 <= 500:
            new_confidence = "medium"
        elif new_bill_10 is None and fixed_monthly is not None:
            new_confidence = "low"
        elif new_bill_10 is not None:
            new_confidence = "low"
        else:
            new_confidence = "low"

        # Uniform (1 tier) → cap at medium
        if tier_count == 1 and new_confidence == "high":
            new_confidence = "medium"

        if new_confidence != row.confidence:
            stats["confidence_changed"] += 1
            changed = True

        needs_review = bool(review_reasons)
        if needs_review:
            stats["flagged_review"] += 1

        if changed or needs_review:
            updates.append({
                "id": record_id,
                "volumetric_tiers": json.dumps(tiers) if tiers else None,
                "tier_count": tier_count,
                "bill_5ccf": new_bill_5,
                "bill_10ccf": new_bill_10,
                "bill_20ccf": new_bill_20,
                "conservation_signal": new_cons,
                "confidence": new_confidence,
                "needs_review": needs_review,
                "review_reason": "; ".join(review_reasons) if review_reasons else None,
            })

    logger.info(f"  A2: Contiguity fixed: {stats['contiguity_fixed']}")
    logger.info(f"  A3: Deduped: {stats['deduped']}")
    logger.info(f"  A4: Bills recalculated: {stats['bills_changed']}")
    logger.info(f"  A5: Confidence changed: {stats['confidence_changed']}")
    logger.info(f"  A5: Flagged for review: {stats['flagged_review']}")
    logger.info(f"  Total records to update: {len(updates)}")

    if not dry_run and updates:
        for u in updates:
            conn.execute(text(f"""
                UPDATE {SCHEMA}.rate_schedules
                SET volumetric_tiers = CAST(:volumetric_tiers AS jsonb),
                    tier_count = :tier_count,
                    bill_5ccf = :bill_5ccf,
                    bill_10ccf = :bill_10ccf,
                    bill_20ccf = :bill_20ccf,
                    conservation_signal = :conservation_signal,
                    confidence = :confidence,
                    needs_review = :needs_review,
                    review_reason = :review_reason
                WHERE id = :id
            """), u)
        logger.info(f"  Updated {len(updates)} records in database")

    return stats


def _val_differs(old, new) -> bool:
    """Check if two numeric values differ (handling None)."""
    if old is None and new is None:
        return False
    if old is None or new is None:
        return True
    return round(float(old), 2) != round(float(new), 2)


def log_pipeline_run(conn, started: datetime, stats: dict, dry_run: bool) -> None:
    """A6: Log migration to pipeline_runs."""
    if dry_run:
        logger.info("  A6 [dry-run]: Would log to pipeline_runs")
        return

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    conn.execute(text(f"""
        INSERT INTO {SCHEMA}.pipeline_runs
            (step_name, started_at, finished_at, row_count, status, notes)
        VALUES (:step, :started, NOW(), :count, 'success', :notes)
    """), {
        "step": "duke_comparability_migration",
        "started": started,
        "count": stats.get("total", 0),
        "notes": (
            f"frequency_stripped={stats.get('frequency_stripped', 0)}, "
            f"contiguity_fixed={stats.get('contiguity_fixed', 0)}, "
            f"deduped={stats.get('deduped', 0)}, "
            f"bills_changed={stats.get('bills_changed', 0)}, "
            f"confidence_changed={stats.get('confidence_changed', 0)}, "
            f"flagged_review={stats.get('flagged_review', 0)}, "
            f"elapsed={elapsed:.1f}s"
        ),
    })
    logger.info("  A6: Logged to pipeline_runs")


def main():
    parser = argparse.ArgumentParser(description="Duke NIEPS comparability migration")
    parser.add_argument("--dry-run", action="store_true", help="Preview without DB writes")
    args = parser.parse_args()

    started = datetime.now(timezone.utc)
    logger.info("=== Duke NIEPS Comparability Migration ===")
    logger.info(f"Target: {SCHEMA}.rate_schedules WHERE source_key = '{SOURCE_KEY}'")
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    with engine.connect() as conn:
        # A1: Strip frequency from fixed_charges
        logger.info("\n--- A1: Strip frequency from fixed_charges ---")
        freq_count = fix_fixed_charges(conn, args.dry_run)

        # A2-A5: Fix tiers, recalculate, set confidence
        logger.info("\n--- A2-A5: Fix tiers, recalculate, set confidence ---")
        tier_stats = fix_tiers_and_recalculate(conn, args.dry_run)

        # A6: Log pipeline run
        logger.info("\n--- A6: Log pipeline run ---")
        all_stats = {**tier_stats, "frequency_stripped": freq_count}
        log_pipeline_run(conn, started, all_stats, args.dry_run)

        if not args.dry_run:
            conn.commit()
            logger.info("\nAll changes committed.")
        else:
            logger.info("\n[DRY RUN] No changes made.")

    logger.info("\n=== Migration Complete ===")


if __name__ == "__main__":
    main()
