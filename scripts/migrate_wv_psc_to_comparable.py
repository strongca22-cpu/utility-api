#!/usr/bin/env python3
"""
WV PSC Comparability Migration — Confidence Recalibration

Purpose:
    Patch existing WV PSC records in rate_schedules:
    1. Downgrade 1-tier records from "high" to "medium" (Duke criteria:
       high requires tier_count >= 2)
    2. Flag bill outliers for review

    JSONB is already clean — WV PSC uses water_rate_to_schedule() helper
    which writes canonical format. No structural fixes needed.

    WV PSC data is entirely single-tier or flat (no multi-tier records),
    so NO records qualify for "high" confidence under Duke criteria.

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Dependencies:
    - sqlalchemy, loguru

Usage:
    python scripts/migrate_wv_psc_to_comparable.py --dry-run
    python scripts/migrate_wv_psc_to_comparable.py

Notes:
    - Idempotent: safe to re-run
    - No JSONB fixes (already canonical via water_rate_to_schedule helper)
    - No bill recomputation (WV PSC bills computed from 2-point slope method)
    - Net effect: 155 records high → medium (1-tier cap)
"""

# Standard library imports
import argparse
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

# Constants
SCHEMA = settings.utility_schema
SOURCE_KEY = "wv_psc_2026"


def parse_args():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="WV PSC comparability migration — confidence recalibration"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without writing to database.",
    )
    return parser.parse_args()


def recalibrate_confidence(conn, dry_run: bool) -> dict:
    """Apply nuanced confidence levels.

    Criteria (Duke-established, Sprint 28):
      - high:   bill_10ccf in [10, 200] AND tier_count >= 2
      - medium: bill_10ccf in [5, 500] OR tier_count <= 1
      - low:    bill_10ccf NULL or outside [5, 500]

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
        "downgraded_to_medium": 0,
        "downgraded_to_low": 0,
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
        elif bill_10 is not None:
            new_confidence = "low"
        else:
            new_confidence = "low"

        # 1-tier cap at medium
        if tier_count <= 1 and new_confidence == "high":
            new_confidence = "medium"

        # Flag outliers
        if bill_10 is not None and bill_10 > 200:
            review_reasons.append(f"bill_10ccf=${bill_10:.2f} > $200")
        if bill_10 is not None and bill_10 < 5:
            review_reasons.append(f"bill_10ccf=${bill_10:.2f} < $5")

        changed = new_confidence != row.confidence
        needs_review = bool(review_reasons)

        if changed:
            if new_confidence == "medium":
                stats["downgraded_to_medium"] += 1
            elif new_confidence == "low":
                stats["downgraded_to_low"] += 1

        if needs_review:
            stats["flagged_review"] += 1

        if changed or needs_review:
            updates.append({
                "id": row.id,
                "confidence": new_confidence,
                "needs_review": needs_review,
                "review_reason": "; ".join(review_reasons) if review_reasons else None,
            })

    logger.info(f"  Total: {stats['total']}")
    logger.info(f"  Downgraded to medium: {stats['downgraded_to_medium']}")
    logger.info(f"  Downgraded to low: {stats['downgraded_to_low']}")
    logger.info(f"  Flagged for review: {stats['flagged_review']}")
    logger.info(f"  Records to update: {len(updates)}")

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


def log_pipeline_run(conn, started, stats, dry_run):
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
        "step": "wv_psc_audit_migration",
        "started": started,
        "count": stats.get("total", 0),
        "notes": (
            f"source={SOURCE_KEY}, "
            f"downgraded_medium={stats.get('downgraded_to_medium', 0)}, "
            f"downgraded_low={stats.get('downgraded_to_low', 0)}, "
            f"flagged_review={stats.get('flagged_review', 0)}, "
            f"elapsed={elapsed:.1f}s"
        ),
    })
    logger.info("  Logged to pipeline_runs")


def main():
    """Main entry point for script execution."""
    args = parse_args()
    started = datetime.now(timezone.utc)

    logger.info("=== WV PSC Comparability Migration ===")
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    with engine.connect() as conn:
        logger.info(f"Schema: {SCHEMA}.rate_schedules")
        logger.info(f"Source: {SOURCE_KEY}")

        logger.info("\n--- Confidence Recalibration ---")
        stats = recalibrate_confidence(conn, dry_run=args.dry_run)

        logger.info("\n--- Pipeline Logging ---")
        log_pipeline_run(conn, started, stats, dry_run=args.dry_run)

        if not args.dry_run:
            conn.commit()
            logger.info("\nAll changes committed.")
        else:
            logger.info("\n[DRY RUN] No changes made.")

    logger.info("\n=== Migration Complete ===")


if __name__ == "__main__":
    main()
