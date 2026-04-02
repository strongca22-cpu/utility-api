#!/usr/bin/env python3
"""
TX TML Comparability Migration — Bill Normalization to CCF Benchmarks

Purpose:
    Patch existing TX TML records in rate_schedules:
    1. Normalize gallon-based bills to standard CCF benchmarks using
       2-point linear interpolation (F + R model)
    2. Compute bill_20ccf by extrapolation
    3. Flag bill outliers for review
    4. Preserve original gallon-based values in parse_notes

    TML reports bills at 5,000 and 10,000 gallons, NOT at 5/10 CCF:
      - 5,000 gal = 6.68 CCF  (our bill_5ccf = 5 CCF = 3,740 gal)
      - 10,000 gal = 13.37 CCF (our bill_10ccf = 10 CCF = 7,480 gal)

    The ingest stored gallon-based bills directly into CCF columns.
    This migration interpolates to true CCF values for cross-source
    comparability.

    Interpolation model (2-point linear):
      F (implied fixed charge) = 2 * bill_5000 - bill_10000
      R (volumetric rate/gal)  = (bill_10000 - bill_5000) / 5000
      bill_at_X_gal = F + R * X

    For 5 records with only bill_5000:
      bill_5ccf  = bill_5000 * (3740 / 5000)
      bill_10ccf = 2 * bill_5ccf  (extrapolated, assumes proportional)

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Dependencies:
    - sqlalchemy, loguru

Usage:
    python scripts/migrate_tx_tml_to_comparable.py --dry-run
    python scripts/migrate_tx_tml_to_comparable.py

Notes:
    - Idempotent: safe to re-run (original gallon values read from parse_notes
      if already migrated, or from current bill columns if not yet migrated)
    - No JSONB fixes (already empty arrays — bill_only source)
    - Confidence stays at "medium" for all (bill_only = 0 tiers < 2)
    - 23 records have negative implied F (increasing block) — interpolation
      still valid within [0, 10000] gal range
    - bill_20ccf is extrapolation beyond data range — flagged in parse_notes

Data Sources:
    - Input: utility.rate_schedules WHERE source_key = 'tx_tml_2023'
    - Output: same table, patched in place

Configuration:
    - CCF/gallon conversions: 1 CCF = 748 gallons
    - Outlier thresholds: bill_10ccf > $200 or < $5
"""

# Standard library imports
import argparse
import re
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
SOURCE_KEY = "tx_tml_2023"
GAL_PER_CCF = 748.0

# Target consumption levels in gallons
GAL_5CCF = 5 * GAL_PER_CCF     # 3,740 gal
GAL_10CCF = 10 * GAL_PER_CCF   # 7,480 gal
GAL_20CCF = 20 * GAL_PER_CCF   # 14,960 gal

# Source consumption levels (TML gallon benchmarks)
GAL_SRC_5K = 5000
GAL_SRC_10K = 10000


def parse_args():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="TX TML comparability migration — normalize gallon bills to CCF benchmarks"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without writing to database.",
    )
    return parser.parse_args()


def _extract_original_bills(parse_notes: str | None, bill_5ccf, bill_10ccf):
    """Extract original gallon-based bill values.

    If migration has already run, the originals are in parse_notes.
    Otherwise, the current bill columns hold the gallon-based values.

    Returns (bill_5000_gal, bill_10000_gal).
    """
    if parse_notes and "source_bill_5000gal=" in parse_notes:
        # Already migrated — extract originals from notes
        m5 = re.search(r"source_bill_5000gal=([\d.]+)", parse_notes)
        m10 = re.search(r"source_bill_10000gal=([\d.]+)", parse_notes)
        b5k = float(m5.group(1)) if m5 else None
        b10k = float(m10.group(1)) if m10 else None
        return b5k, b10k

    # Not yet migrated — current values are gallon-based
    return bill_5ccf, bill_10ccf


def normalize_bills(conn, dry_run: bool) -> dict:
    """Normalize gallon-based bills to CCF benchmarks via 2-point linear model.

    Model: bill(X) = F + R * X
      where F = 2 * bill_5000 - bill_10000
            R = (bill_10000 - bill_5000) / 5000

    For records with only bill_5000:
      bill_5ccf  = bill_5000 * (3740 / 5000)
      bill_10ccf = 2 * bill_5ccf

    Returns
    -------
    dict
        Stats: total, normalized_2pt, normalized_1pt, skipped, outliers.
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
        "normalized_2pt": 0,
        "normalized_1pt": 0,
        "skipped": 0,
        "negative_fixed": 0,
        "outliers_flagged": 0,
        "bill_20ccf_populated": 0,
    }
    updates = []

    for row in rows:
        # Extract original gallon-based values
        b5k, b10k = _extract_original_bills(
            row.parse_notes, row.bill_5ccf, row.bill_10ccf
        )

        new_b5 = None
        new_b10 = None
        new_b20 = None
        method = None
        implied_fixed = None

        if b5k is not None and b10k is not None:
            # 2-point linear model
            implied_fixed = 2 * b5k - b10k
            rate_per_gal = (b10k - b5k) / (GAL_SRC_10K - GAL_SRC_5K)

            new_b5 = implied_fixed + rate_per_gal * GAL_5CCF
            new_b10 = implied_fixed + rate_per_gal * GAL_10CCF
            new_b20 = implied_fixed + rate_per_gal * GAL_20CCF

            method = "interpolated_2pt_linear"
            stats["normalized_2pt"] += 1

            if implied_fixed < 0:
                stats["negative_fixed"] += 1

        elif b5k is not None:
            # Single-point proportional scaling
            new_b5 = b5k * (GAL_5CCF / GAL_SRC_5K)
            new_b10 = 2 * new_b5
            new_b20 = 4 * new_b5  # Proportional: 20 CCF = 4 × 5 CCF

            method = "interpolated_1pt_proportional"
            stats["normalized_1pt"] += 1

        else:
            stats["skipped"] += 1
            continue

        # Round to 2 decimal places
        new_b5 = round(new_b5, 2) if new_b5 is not None else None
        new_b10 = round(new_b10, 2) if new_b10 is not None else None
        new_b20 = round(new_b20, 2) if new_b20 is not None else None

        if new_b20 is not None:
            stats["bill_20ccf_populated"] += 1

        # Outlier detection on normalized values
        review_reasons = []
        if row.review_reason:
            # Preserve existing reasons, but strip old outlier flags
            existing = [r.strip() for r in row.review_reason.split(";")
                        if "bill_10ccf" not in r and "bill_5ccf" not in r]
            review_reasons.extend(existing)

        if new_b10 is not None and new_b10 > 200:
            review_reasons.append(f"normalized_bill_10ccf=${new_b10:.2f} > $200")
            stats["outliers_flagged"] += 1
        if new_b10 is not None and new_b10 < 5:
            review_reasons.append(f"normalized_bill_10ccf=${new_b10:.2f} < $5")
            stats["outliers_flagged"] += 1

        # Build updated parse_notes
        notes_parts = []

        # Preserve non-migration parts of existing notes
        if row.parse_notes:
            existing_notes = row.parse_notes
            # Strip old source_bill and ccf_bills markers if re-running
            existing_notes = re.sub(
                r"\s*\|\s*source_bill_\d+gal=[\d.]+", "", existing_notes
            )
            existing_notes = re.sub(
                r"\s*\|\s*ccf_bills=\S+", "", existing_notes
            )
            existing_notes = re.sub(
                r"\s*\|\s*bill_20ccf=\S+", "", existing_notes
            )
            existing_notes = re.sub(
                r"\s*\|\s*implied_fixed=[-\d.]+", "", existing_notes
            )
            notes_parts.append(existing_notes.strip())

        # Add original values and method
        if b5k is not None:
            notes_parts.append(f"source_bill_5000gal={b5k:.2f}")
        if b10k is not None:
            notes_parts.append(f"source_bill_10000gal={b10k:.2f}")
        notes_parts.append(f"ccf_bills={method}")
        if new_b20 is not None:
            notes_parts.append("bill_20ccf=extrapolated")
        if implied_fixed is not None:
            notes_parts.append(f"implied_fixed={implied_fixed:.2f}")

        new_notes = " | ".join(notes_parts)

        needs_review = bool(review_reasons)

        updates.append({
            "id": row.id,
            "bill_5ccf": new_b5,
            "bill_10ccf": new_b10,
            "bill_20ccf": new_b20,
            "needs_review": needs_review,
            "review_reason": "; ".join(review_reasons) if review_reasons else None,
            "parse_notes": new_notes,
        })

    # Log summary
    logger.info(f"  Total: {stats['total']}")
    logger.info(f"  2-point normalized: {stats['normalized_2pt']}")
    logger.info(f"  1-point normalized: {stats['normalized_1pt']}")
    logger.info(f"  Skipped (no bills): {stats['skipped']}")
    logger.info(f"  Negative implied fixed (increasing block): {stats['negative_fixed']}")
    logger.info(f"  Outliers flagged: {stats['outliers_flagged']}")
    logger.info(f"  bill_20ccf populated: {stats['bill_20ccf_populated']}")

    # Show sample transformations
    logger.info("\n  Sample transformations (first 10):")
    for u in updates[:10]:
        logger.info(
            f"    id={u['id']}: "
            f"b5ccf={u['bill_5ccf']}, b10ccf={u['bill_10ccf']}, "
            f"b20ccf={u['bill_20ccf']}"
        )

    if not dry_run and updates:
        for u in updates:
            conn.execute(text(f"""
                UPDATE {SCHEMA}.rate_schedules
                SET bill_5ccf = :bill_5ccf,
                    bill_10ccf = :bill_10ccf,
                    bill_20ccf = :bill_20ccf,
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
        "step": "tx_tml_audit_migration",
        "started": started,
        "count": stats.get("total", 0),
        "notes": (
            f"source={SOURCE_KEY}, "
            f"normalized_2pt={stats.get('normalized_2pt', 0)}, "
            f"normalized_1pt={stats.get('normalized_1pt', 0)}, "
            f"negative_fixed={stats.get('negative_fixed', 0)}, "
            f"outliers_flagged={stats.get('outliers_flagged', 0)}, "
            f"bill_20ccf_populated={stats.get('bill_20ccf_populated', 0)}, "
            f"elapsed={elapsed:.1f}s"
        ),
    })
    logger.info("  Logged to pipeline_runs")


def main():
    """Main entry point for script execution."""
    args = parse_args()
    started = datetime.now(timezone.utc)

    logger.info("=== TX TML Comparability Migration ===")
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    with engine.connect() as conn:
        logger.info(f"Schema: {SCHEMA}.rate_schedules")
        logger.info(f"Source: {SOURCE_KEY}")

        logger.info("\n--- Bill Normalization (gallon → CCF) ---")
        stats = normalize_bills(conn, dry_run=args.dry_run)

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
