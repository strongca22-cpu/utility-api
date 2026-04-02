#!/usr/bin/env python3
"""
CA eAR Comparability Migration — Confidence Recalibration

Purpose:
    Patch existing CA SWRCB eAR records in rate_schedules to apply nuanced
    confidence levels consistent with Duke/EFC-established criteria.

    Differences from EFC migration:
    - eAR uses bill_12ccf (not bill_10ccf) as the primary bill benchmark
    - eAR 2020 has NO pre-computed bills — confidence capped at "medium"
    - 1-tier (uniform) records capped at "medium" (per Duke audit precedent)
    - Flags records with NULL billing_frequency for review
    - Does NOT recalculate bills (state-reported bills are authoritative)
    - JSONB is already clean (Sprint 28 eAR audit confirmed no fixes needed)

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Dependencies:
    - sqlalchemy, loguru

Usage:
    # Dry-run (preview changes):
    python scripts/migrate_ear_to_comparable.py --dry-run

    # Specific source(s):
    python scripts/migrate_ear_to_comparable.py --source-keys swrcb_ear_2022 --dry-run

    # Execute (remove --dry-run):
    python scripts/migrate_ear_to_comparable.py

Notes:
    - Idempotent: safe to re-run
    - Does NOT recalculate bills (state-reported bills are authoritative)
    - Does NOT touch scraped_llm, water_rates, or any non-eAR records
    - JSONB structure already clean — no structural fixes needed
    - Inflation fix (fix_ear_tier_inflation.py) already applied 2026-03-24

Data Sources:
    - rate_schedules table (source_key LIKE 'swrcb_ear%')
    - 3 vintages: swrcb_ear_2020, swrcb_ear_2021, swrcb_ear_2022

Configuration:
    - DATABASE_URL from .env or config defaults
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
EAR_SOURCES = ["swrcb_ear_2020", "swrcb_ear_2021", "swrcb_ear_2022"]


def parse_args():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="CA eAR comparability migration — confidence recalibration"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without writing to database.",
    )
    parser.add_argument(
        "--source-keys", nargs="+", default=None,
        help="Specific eAR source_key(s). Default: all 3 vintages.",
    )
    return parser.parse_args()


def resolve_source_keys(args) -> list[str]:
    """Determine which eAR source_keys to process."""
    if args.source_keys:
        return args.source_keys
    return EAR_SOURCES


def recalibrate_confidence(conn, source_keys: list[str], dry_run: bool) -> dict:
    """Apply nuanced confidence levels to eAR records.

    Confidence criteria (adapted from Duke/EFC audit, Sprint 28):
      - high:   bill_12ccf in [10, 200] AND tier_count >= 2 AND has billing_frequency
      - medium: bill_12ccf in [5, 500] OR (tier_count == 1 and was "high")
                OR 2020 vintage (no pre-computed bills)
      - low:    bill_12ccf is NULL AND tier_count == 0
                OR bill_12ccf outside [5, 500]

    Special rules:
      - 1-tier (uniform) records capped at "medium" (never "high")
      - NULL billing_frequency flagged for review (doesn't downgrade)

    Returns
    -------
    dict
        Stats: total processed, confidence changes per source.
    """
    sk_list = ", ".join(f"'{sk}'" for sk in source_keys)

    rows = conn.execute(text(f"""
        SELECT id, pwsid, source_key, bill_12ccf,
               tier_count, confidence, rate_structure_type,
               billing_frequency, bill_6ccf, bill_24ccf,
               fixed_charges, volumetric_tiers,
               needs_review, review_reason
        FROM {SCHEMA}.rate_schedules
        WHERE source_key IN ({sk_list})
        ORDER BY source_key, pwsid
    """)).fetchall()

    stats = {
        "total": len(rows),
        "confidence_changed": 0,
        "flagged_review": 0,
        "uniform_capped": 0,
        "null_freq_flagged": 0,
    }
    per_source = {}
    updates = []

    for row in rows:
        source_key = row.source_key
        if source_key not in per_source:
            per_source[source_key] = {
                "total": 0, "changed": 0, "review": 0,
                "uniform_capped": 0,
            }
        per_source[source_key]["total"] += 1

        bill_12 = row.bill_12ccf
        tier_count = row.tier_count or 0
        has_fixed = row.fixed_charges is not None and len(row.fixed_charges) > 0
        has_tiers = row.volumetric_tiers is not None and len(row.volumetric_tiers) > 0

        review_reasons = []

        # Collect existing review reasons (don't overwrite)
        if row.review_reason:
            review_reasons.append(row.review_reason)

        # Check for identical bills across volumes for tiered structures
        if (
            tier_count > 1
            and row.bill_6ccf is not None
            and bill_12 is not None
            and row.bill_24ccf is not None
            and row.bill_6ccf == bill_12 == row.bill_24ccf
            and row.rate_structure_type not in ("flat", "uniform")
        ):
            review_reasons.append(
                "increasing_block but identical bills at 6/12/24 CCF"
            )

        # Flag NULL billing_frequency
        if row.billing_frequency is None:
            review_reasons.append("NULL billing_frequency")
            stats["null_freq_flagged"] += 1

        # Determine new confidence
        if (
            bill_12 is not None
            and 10 <= bill_12 <= 200
            and tier_count >= 2
            and row.billing_frequency is not None
        ):
            new_confidence = "high"
        elif bill_12 is not None and 5 <= bill_12 <= 500:
            new_confidence = "medium"
        elif bill_12 is None and has_tiers:
            # 2020 vintage: has tiers but no bills
            new_confidence = "medium"
        elif bill_12 is None and has_fixed and not has_tiers:
            new_confidence = "low"
        elif bill_12 is not None:
            # bill outside [5, 500]
            new_confidence = "low"
        else:
            new_confidence = "low"

        # Uniform (1 tier) → cap at medium
        if tier_count <= 1 and new_confidence == "high":
            new_confidence = "medium"
            stats["uniform_capped"] += 1
            per_source[source_key]["uniform_capped"] += 1

        needs_review = bool(review_reasons)
        changed = new_confidence != row.confidence

        if needs_review:
            stats["flagged_review"] += 1
            per_source[source_key]["review"] += 1

        if changed or needs_review:
            if changed:
                stats["confidence_changed"] += 1
                per_source[source_key]["changed"] += 1

            updates.append({
                "id": row.id,
                "confidence": new_confidence,
                "needs_review": needs_review,
                "review_reason": "; ".join(review_reasons) if review_reasons else None,
            })

    # Log summary
    logger.info(f"  Total records: {stats['total']}")
    logger.info(f"  Confidence changed: {stats['confidence_changed']}")
    logger.info(f"  Uniform capped (high→medium): {stats['uniform_capped']}")
    logger.info(f"  Flagged for review: {stats['flagged_review']}")
    logger.info(f"  NULL billing_frequency flagged: {stats['null_freq_flagged']}")
    logger.info(f"  Records to update: {len(updates)}")

    logger.info("\n  --- Per-Source Breakdown ---")
    for sk in sorted(per_source.keys()):
        ps = per_source[sk]
        logger.info(
            f"  {sk}: total={ps['total']} "
            f"confidence_changed={ps['changed']} "
            f"uniform_capped={ps['uniform_capped']} "
            f"review={ps['review']}"
        )

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


def log_pipeline_run(
    conn, started: datetime, source_keys: list[str],
    stats: dict, dry_run: bool,
) -> None:
    """Log migration to pipeline_runs."""
    if dry_run:
        logger.info("  [dry-run] Would log to pipeline_runs")
        return

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    conn.execute(text(f"""
        INSERT INTO {SCHEMA}.pipeline_runs
            (step_name, started_at, finished_at, row_count, status, notes)
        VALUES (:step, :started, NOW(), :count, 'success', :notes)
    """), {
        "step": "ear_confidence_recalibration",
        "started": started,
        "count": stats.get("total", 0),
        "notes": (
            f"sources={','.join(source_keys)}, "
            f"confidence_changed={stats.get('confidence_changed', 0)}, "
            f"uniform_capped={stats.get('uniform_capped', 0)}, "
            f"flagged_review={stats.get('flagged_review', 0)}, "
            f"null_freq={stats.get('null_freq_flagged', 0)}, "
            f"elapsed={elapsed:.1f}s"
        ),
    })
    logger.info("  Logged to pipeline_runs")


def main():
    """Main entry point for script execution."""
    args = parse_args()
    started = datetime.now(timezone.utc)

    logger.info("=== CA eAR Comparability Migration — Confidence Recalibration ===")
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")

    with engine.connect() as conn:
        source_keys = resolve_source_keys(args)
        logger.info(f"Target sources: {source_keys}")
        logger.info(f"Schema: {SCHEMA}.rate_schedules")

        # Recalibrate confidence
        logger.info("\n--- Confidence Recalibration ---")
        stats = recalibrate_confidence(conn, source_keys, dry_run=args.dry_run)

        # Log pipeline run
        logger.info("\n--- Pipeline Logging ---")
        log_pipeline_run(conn, started, source_keys, stats, dry_run=args.dry_run)

        if not args.dry_run:
            conn.commit()
            logger.info("\nAll changes committed.")
        else:
            logger.info("\n[DRY RUN] No changes made.")

    logger.info("\n=== Migration Complete ===")


if __name__ == "__main__":
    main()
