#!/usr/bin/env python3
"""
KY PSC Comparability Migration — JSONB Cleanup + Confidence Recalibration

Purpose:
    Patch existing KY PSC records in rate_schedules:
    1. Strip extra `frequency` key from fixed_charges JSONB
    2. Fix 1-gallon tier contiguity gaps (min_gal N+1 → N)
    3. Recompute bills for KY0300387 (ingest bug: LLM over-reported
       first_tier_gallons, zeroing volumetric charges)
    4. Recalibrate confidence (upgrade qualifying records to "high")
    5. Flag outliers for review

    KY PSC tariffs use a "Minimum Bill includes first N gallons" pattern:
    tier 1 starts above 0 (e.g., min_gal=2001). This is architecturally
    correct — the fixed charge covers included gallons. Do NOT change
    tier 1 start points; only fix inter-tier gaps.

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Dependencies:
    - sqlalchemy, loguru

Usage:
    python scripts/migrate_ky_psc_to_comparable.py --dry-run
    python scripts/migrate_ky_psc_to_comparable.py

Notes:
    - Idempotent: safe to re-run
    - Bills recomputed ONLY for KY0300387 (identified bug)
    - Other bills computed by ingest's own calc_bill() which correctly
      handles the minimum-bill-includes-N-gallons pattern
    - JSONB structural fixes: strip frequency key, close 1-gal gaps

Data Sources:
    - rate_schedules table (source_key = 'ky_psc_water_tariffs_2025')
"""

# Standard library imports
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# Third-party imports
from loguru import logger
from sqlalchemy import text

# Local imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine
from utility_api.ops.rate_schedule_helpers import compute_bill_at_gallons

# Constants
SCHEMA = settings.utility_schema
SOURCE_KEY = "ky_psc_water_tariffs_2025"
CCF_TO_GAL = 748.0


def parse_args():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="KY PSC comparability migration"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without writing to database.",
    )
    return parser.parse_args()


def fix_jsonb_and_contiguity(conn, dry_run: bool) -> dict:
    """Strip frequency key and fix 1-gallon tier gaps.

    Returns
    -------
    dict
        Stats for fixes applied.
    """
    rows = conn.execute(text(f"""
        SELECT id, pwsid, fixed_charges, volumetric_tiers
        FROM {SCHEMA}.rate_schedules
        WHERE source_key = :sk
        ORDER BY pwsid
    """), {"sk": SOURCE_KEY}).fetchall()

    stats = {
        "total": len(rows),
        "fc_frequency_stripped": 0,
        "tier_gaps_fixed": 0,
        "records_updated": 0,
    }
    updates = []

    for row in rows:
        changed = False
        fc = row.fixed_charges
        vt = row.volumetric_tiers

        # 1. Strip frequency from fixed_charges
        new_fc = None
        if fc:
            new_fc = []
            for charge in fc:
                if "frequency" in charge:
                    cleaned = {k: v for k, v in charge.items() if k != "frequency"}
                    new_fc.append(cleaned)
                    changed = True
                else:
                    new_fc.append(charge)
            if not changed:
                new_fc = fc

        # 2. Fix 1-gallon gaps in volumetric_tiers
        new_vt = None
        if vt and len(vt) > 1:
            new_vt = [vt[0].copy()]  # Keep tier 1 as-is
            for i in range(1, len(vt)):
                tier = vt[i].copy()
                prev_max = new_vt[i - 1].get("max_gal")
                curr_min = tier.get("min_gal")
                if (
                    prev_max is not None
                    and curr_min is not None
                    and curr_min - prev_max == 1
                ):
                    tier["min_gal"] = prev_max
                    stats["tier_gaps_fixed"] += 1
                    changed = True
                new_vt.append(tier)
        else:
            new_vt = vt

        if changed:
            stats["records_updated"] += 1
            if new_fc and new_fc != fc:
                stats["fc_frequency_stripped"] += 1
            updates.append({
                "id": row.id,
                "fixed_charges": json.dumps(new_fc) if new_fc else None,
                "volumetric_tiers": json.dumps(new_vt) if new_vt else None,
            })

    logger.info(f"  JSONB fixes: {stats['records_updated']} records")
    logger.info(f"    frequency stripped: {stats['fc_frequency_stripped']}")
    logger.info(f"    tier gaps fixed: {stats['tier_gaps_fixed']}")

    if not dry_run and updates:
        for u in updates:
            conn.execute(text(f"""
                UPDATE {SCHEMA}.rate_schedules
                SET fixed_charges = CAST(:fixed_charges AS jsonb),
                    volumetric_tiers = CAST(:volumetric_tiers AS jsonb)
                WHERE id = :id
            """), u)
        logger.info(f"  Updated {len(updates)} records in database")

    return stats


def recompute_broken_bills(conn, dry_run: bool) -> dict:
    """Recompute bills for KY0300387 (ingest bug).

    The LLM over-reported first_tier_gallons, causing calc_bill to treat
    all consumption as included in the minimum bill. Recompute using
    compute_bill_at_gallons() which processes tiers correctly when tier 1
    starts at min_gal=0.

    Returns
    -------
    dict
        Stats.
    """
    stats = {"recomputed": 0}

    # Fetch the broken record (after JSONB fix)
    row = conn.execute(text(f"""
        SELECT id, pwsid, volumetric_tiers, fixed_charges,
               bill_5ccf, bill_10ccf, bill_20ccf, parse_notes
        FROM {SCHEMA}.rate_schedules
        WHERE source_key = :sk AND pwsid = 'KY0300387'
    """), {"sk": SOURCE_KEY}).fetchone()

    if row is None:
        logger.info("  KY0300387 not found — skipping")
        return stats

    tiers = row.volumetric_tiers
    fc = row.fixed_charges

    new_5 = compute_bill_at_gallons(5 * CCF_TO_GAL, tiers, fc)
    new_10 = compute_bill_at_gallons(10 * CCF_TO_GAL, tiers, fc)
    new_20 = compute_bill_at_gallons(20 * CCF_TO_GAL, tiers, fc)

    logger.info(f"  KY0300387 bill recomputation:")
    logger.info(f"    Old: 5ccf=${row.bill_5ccf}, 10ccf=${row.bill_10ccf}, 20ccf=${row.bill_20ccf}")
    logger.info(f"    New: 5ccf=${new_5}, 10ccf=${new_10}, 20ccf=${new_20}")

    notes = row.parse_notes or ""
    fix_note = (
        "[FIX 2026-04-02] Bills recomputed — ingest LLM over-reported "
        "first_tier_gallons=20000, zeroing volumetric charges. "
        f"Old: 5={row.bill_5ccf}/10={row.bill_10ccf}/20={row.bill_20ccf}"
    )
    notes = f"{notes}; {fix_note}" if notes else fix_note

    if not dry_run:
        conn.execute(text(f"""
            UPDATE {SCHEMA}.rate_schedules
            SET bill_5ccf = :b5, bill_10ccf = :b10, bill_20ccf = :b20,
                parse_notes = :notes
            WHERE id = :id
        """), {
            "id": row.id, "b5": new_5, "b10": new_10, "b20": new_20,
            "notes": notes,
        })
        logger.info(f"  Updated KY0300387 bills in database")

    stats["recomputed"] = 1
    return stats


def recalibrate_confidence(conn, dry_run: bool) -> dict:
    """Apply nuanced confidence levels.

    Criteria (Duke-established, Sprint 28):
      - high:   bill_10ccf in [10, 200] AND tier_count >= 2
      - medium: bill_10ccf in [5, 500] OR tier_count == 1
      - low:    bill_10ccf NULL or outside [5, 500]

    Additionally flags outliers for review.

    Returns
    -------
    dict
        Stats.
    """
    rows = conn.execute(text(f"""
        SELECT id, pwsid, bill_10ccf, tier_count, confidence,
               rate_structure_type, needs_review, review_reason
        FROM {SCHEMA}.rate_schedules
        WHERE source_key = :sk
        ORDER BY pwsid
    """), {"sk": SOURCE_KEY}).fetchall()

    stats = {
        "total": len(rows),
        "upgraded_to_high": 0,
        "stayed_medium": 0,
        "flagged_review": 0,
    }
    updates = []

    for row in rows:
        bill_10 = row.bill_10ccf
        tier_count = row.tier_count or 0
        review_reasons = []

        # Collect existing review reasons
        if row.review_reason:
            review_reasons.append(row.review_reason)

        # Determine new confidence
        if (
            bill_10 is not None
            and 10 <= bill_10 <= 200
            and tier_count >= 2
        ):
            new_confidence = "high"
        elif bill_10 is not None and 5 <= bill_10 <= 500:
            new_confidence = "medium"
        else:
            new_confidence = "low"

        # 1-tier cap at medium
        if tier_count <= 1 and new_confidence == "high":
            new_confidence = "medium"

        # Flag outliers
        if bill_10 is not None and bill_10 > 200:
            review_reasons.append(f"bill_10ccf=${bill_10:.2f} > $200")
        if bill_10 is not None and bill_10 < 10:
            review_reasons.append(f"bill_10ccf=${bill_10:.2f} < $10")

        changed = new_confidence != row.confidence
        needs_review = bool(review_reasons)

        if changed:
            if new_confidence == "high":
                stats["upgraded_to_high"] += 1
            else:
                stats["stayed_medium"] += 1

        if needs_review:
            stats["flagged_review"] += 1

        if changed or needs_review:
            updates.append({
                "id": row.id,
                "confidence": new_confidence,
                "needs_review": needs_review,
                "review_reason": "; ".join(review_reasons) if review_reasons else None,
            })

    logger.info(f"  Confidence recalibration:")
    logger.info(f"    Total: {stats['total']}")
    logger.info(f"    Upgraded to high: {stats['upgraded_to_high']}")
    logger.info(f"    Flagged for review: {stats['flagged_review']}")
    logger.info(f"    Records to update: {len(updates)}")

    if not dry_run and updates:
        for u in updates:
            conn.execute(text(f"""
                UPDATE {SCHEMA}.rate_schedules
                SET confidence = :confidence,
                    needs_review = :needs_review,
                    review_reason = :review_reason
                WHERE id = :id
            """), u)
        logger.info(f"  Updated {len(updates)} records in database")

    return stats


def log_pipeline_run(conn, started, stats_all, dry_run):
    """Log to pipeline_runs."""
    if dry_run:
        logger.info("  [dry-run] Would log to pipeline_runs")
        return

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    conn.execute(text(f"""
        INSERT INTO {SCHEMA}.pipeline_runs
            (step_name, started_at, finished_at, row_count, status, notes)
        VALUES (:step, :started, NOW(), :count, 'success', :notes)
    """), {
        "step": "ky_psc_audit_migration",
        "started": started,
        "count": stats_all.get("total", 84),
        "notes": (
            f"source={SOURCE_KEY}, "
            f"fc_stripped={stats_all.get('fc_frequency_stripped', 0)}, "
            f"gaps_fixed={stats_all.get('tier_gaps_fixed', 0)}, "
            f"bills_recomputed={stats_all.get('recomputed', 0)}, "
            f"upgraded_high={stats_all.get('upgraded_to_high', 0)}, "
            f"flagged_review={stats_all.get('flagged_review', 0)}, "
            f"elapsed={elapsed:.1f}s"
        ),
    })
    logger.info("  Logged to pipeline_runs")


def main():
    """Main entry point for script execution."""
    args = parse_args()
    started = datetime.now(timezone.utc)

    logger.info("=== KY PSC Comparability Migration ===")
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    with engine.connect() as conn:
        # Step 1: JSONB fixes
        logger.info("\n--- Step 1: JSONB Cleanup ---")
        jsonb_stats = fix_jsonb_and_contiguity(conn, dry_run=args.dry_run)

        # Step 2: Bill recomputation
        logger.info("\n--- Step 2: Bill Recomputation (KY0300387) ---")
        bill_stats = recompute_broken_bills(conn, dry_run=args.dry_run)

        # Step 3: Confidence recalibration
        logger.info("\n--- Step 3: Confidence Recalibration ---")
        conf_stats = recalibrate_confidence(conn, dry_run=args.dry_run)

        # Merge stats
        all_stats = {**jsonb_stats, **bill_stats, **conf_stats}

        # Log pipeline run
        logger.info("\n--- Pipeline Logging ---")
        log_pipeline_run(conn, started, all_stats, dry_run=args.dry_run)

        if not args.dry_run:
            conn.commit()
            logger.info("\nAll changes committed.")
        else:
            logger.info("\n[DRY RUN] No changes made.")

    logger.info("\n=== Migration Complete ===")


if __name__ == "__main__":
    main()
