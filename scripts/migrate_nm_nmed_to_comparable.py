#!/usr/bin/env python3
"""
NM NMED Comparability Migration — bill_20ccf Backfill + Outlier Flagging

Purpose:
    Patch existing NM NMED records in rate_schedules:
    1. Backfill bill_20ccf from proportional model (2 × bill_10ccf)
    2. Flag bill outliers for review

    NMED reports a single bill at 6,000 gallons. The ingest already
    normalizes to CCF benchmarks using proportional scaling:
      rate_per_gal = bill_6000 / 6000
      bill_Xccf = rate_per_gal × X × 748

    This produces bill_10ccf = 2 × bill_5ccf (no fixed charge modeled).
    bill_20ccf extends the same proportional model.

    No JSONB fixes needed (empty arrays — bill_only source).
    Confidence stays at "medium" for all (correct for 0 tiers).

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Dependencies:
    - sqlalchemy, loguru

Usage:
    python scripts/migrate_nm_nmed_to_comparable.py --dry-run
    python scripts/migrate_nm_nmed_to_comparable.py

Notes:
    - Idempotent: safe to re-run
    - Proportional model: bill_20ccf = 2 × bill_10ccf = 4 × bill_5ccf
    - Original 6,000 gal bill preserved in parse_notes: "Bill @6000gal=$XX.XX"
    - 1 outlier: NM3561101 bill_10ccf=$263.21
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
SOURCE_KEY = "nm_nmed_rate_survey_2025"


def parse_args():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="NM NMED comparability migration — bill_20ccf backfill + outlier flagging"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without writing to database.",
    )
    return parser.parse_args()


def backfill_and_flag(conn, dry_run: bool) -> dict:
    """Backfill bill_20ccf and flag outliers.

    bill_20ccf = 2 × bill_10ccf (proportional model, consistent with
    the ingest's existing approach of bill_10ccf = 2 × bill_5ccf).

    Outlier flagging: bill_10ccf > $200 or < $5.

    Returns
    -------
    dict
        Stats.
    """
    rows = conn.execute(text(f"""
        SELECT id, pwsid, bill_5ccf, bill_10ccf, bill_20ccf,
               confidence, needs_review, review_reason, parse_notes
        FROM {SCHEMA}.rate_schedules
        WHERE source_key = :sk
        ORDER BY pwsid
    """), {"sk": SOURCE_KEY}).fetchall()

    stats = {
        "total": len(rows),
        "bill_20ccf_backfilled": 0,
        "bill_20ccf_already_set": 0,
        "outliers_flagged": 0,
    }
    updates = []

    for row in rows:
        new_b20 = row.bill_20ccf
        needs_update = False

        # Backfill bill_20ccf
        if row.bill_20ccf is None and row.bill_10ccf is not None:
            new_b20 = round(2 * row.bill_10ccf, 2)
            stats["bill_20ccf_backfilled"] += 1
            needs_update = True
        elif row.bill_20ccf is not None:
            stats["bill_20ccf_already_set"] += 1

        # Outlier detection
        review_reasons = []
        if row.review_reason:
            existing = [r.strip() for r in row.review_reason.split(";")
                        if "bill_10ccf" not in r]
            review_reasons.extend(existing)

        if row.bill_10ccf is not None and row.bill_10ccf > 200:
            review_reasons.append(f"bill_10ccf=${row.bill_10ccf:.2f} > $200")
            stats["outliers_flagged"] += 1
            needs_update = True
        if row.bill_10ccf is not None and row.bill_10ccf < 5:
            review_reasons.append(f"bill_10ccf=${row.bill_10ccf:.2f} < $5")
            stats["outliers_flagged"] += 1
            needs_update = True

        new_needs_review = bool(review_reasons)

        # Update parse_notes with bill_20ccf provenance if backfilling
        new_notes = row.parse_notes or ""
        if row.bill_20ccf is None and new_b20 is not None:
            if "bill_20ccf=" not in new_notes:
                new_notes = new_notes.rstrip() + "; bill_20ccf=extrapolated_proportional"

        if needs_update:
            updates.append({
                "id": row.id,
                "bill_20ccf": new_b20,
                "needs_review": new_needs_review,
                "review_reason": "; ".join(review_reasons) if review_reasons else None,
                "parse_notes": new_notes,
            })

    logger.info(f"  Total: {stats['total']}")
    logger.info(f"  bill_20ccf backfilled: {stats['bill_20ccf_backfilled']}")
    logger.info(f"  bill_20ccf already set: {stats['bill_20ccf_already_set']}")
    logger.info(f"  Outliers flagged: {stats['outliers_flagged']}")
    logger.info(f"  Records to update: {len(updates)}")

    if not dry_run and updates:
        for u in updates:
            conn.execute(text(f"""
                UPDATE {SCHEMA}.rate_schedules
                SET bill_20ccf = :bill_20ccf,
                    needs_review = :needs_review,
                    review_reason = :review_reason,
                    parse_notes = :parse_notes
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
        "step": "nm_nmed_audit_migration",
        "started": started,
        "count": stats.get("total", 0),
        "notes": (
            f"source={SOURCE_KEY}, "
            f"bill_20ccf_backfilled={stats.get('bill_20ccf_backfilled', 0)}, "
            f"outliers_flagged={stats.get('outliers_flagged', 0)}, "
            f"elapsed={elapsed:.1f}s"
        ),
    })
    logger.info("  Logged to pipeline_runs")


def main():
    """Main entry point for script execution."""
    args = parse_args()
    started = datetime.now(timezone.utc)

    logger.info("=== NM NMED Comparability Migration ===")
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    with engine.connect() as conn:
        logger.info(f"Schema: {SCHEMA}.rate_schedules")
        logger.info(f"Source: {SOURCE_KEY}")

        logger.info("\n--- bill_20ccf Backfill + Outlier Flagging ---")
        stats = backfill_and_flag(conn, dry_run=args.dry_run)

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
