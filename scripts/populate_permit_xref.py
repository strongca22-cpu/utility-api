#!/usr/bin/env python3
"""
Populate Permit-Facility Cross-Reference

Purpose:
    Spatial cross-reference between state regulatory permits tagged as
    "Data Center" and canonical SS facility records. Produces two types
    of xref entries:
      - spatial_match: permit within 5km of a known SS facility
      - candidate: permit >5km from any SS facility, flagged as
        data_center_candidate for future validation

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - sqlalchemy

Usage:
    python scripts/populate_permit_xref.py
    python scripts/populate_permit_xref.py --threshold-km 5.0
    python scripts/populate_permit_xref.py --dry-run

Notes:
    - Idempotent: truncates and rebuilds xref table on each run
    - Cross-schema query: utility.permits ↔ public.facilities
    - Match confidence: high (<1km), medium (1-3km), low (3-5km)
    - Candidates are NOT confirmed data centers — they need validation
"""

import argparse
from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine


MATCH_THRESHOLD_KM = 5.0


def populate_xref(threshold_km: float = MATCH_THRESHOLD_KM, dry_run: bool = False) -> None:
    """Rebuild the permit-facility cross-reference table.

    Parameters
    ----------
    threshold_km : float
        Maximum distance in km for a spatial match (default 5.0).
    dry_run : bool
        If True, print results without writing to database.
    """
    started = datetime.now(timezone.utc)
    schema = settings.utility_schema

    with engine.connect() as conn:
        # Get all DC-tagged permits with nearest SS facility
        rows = conn.execute(text("""
            SELECT
                p.id AS permit_id,
                p.permit_number,
                p.facility_name,
                p.county,
                p.status,
                f.facility_id,
                f.name AS ss_name,
                f.operator_name,
                ST_Distance(p.geom::geography, f.geom::geography) / 1000.0 AS distance_km
            FROM utility.permits p
            LEFT JOIN LATERAL (
                SELECT facility_id, name, operator_name, geom
                FROM public.facilities
                WHERE region = 'VA'
                ORDER BY p.geom::geography <-> geom::geography
                LIMIT 1
            ) f ON TRUE
            WHERE p.source_category = 'Data Center'
              AND p.geom IS NOT NULL
        """)).fetchall()

        logger.info(f"Found {len(rows)} Data Center permits to cross-reference")

        if not dry_run:
            conn.execute(text(f"DELETE FROM {schema}.permit_facility_xref"))
            logger.info("Cleared existing xref rows")

        matched = 0
        candidates = 0

        insert_match = text(f"""
            INSERT INTO {schema}.permit_facility_xref
                (permit_id, facility_id, match_type, match_distance_km, match_confidence, notes)
            VALUES (:pid, :fid, 'spatial_match', :dist, :conf, :notes)
        """)

        insert_candidate = text(f"""
            INSERT INTO {schema}.permit_facility_xref
                (permit_id, facility_id, match_type, match_distance_km, match_confidence,
                 candidate_status, notes)
            VALUES (:pid, NULL, 'candidate', :dist, 'unmatched',
                    'data_center_candidate', :notes)
        """)

        for r in rows:
            permit_id = r[0]
            permit_num = r[1]
            deq_name = r[2]
            county = r[3]
            distance = r[8]
            facility_id = r[5]
            ss_name = r[6]
            operator = r[7]

            if distance is not None and distance < threshold_km:
                if distance < 1.0:
                    confidence = "high"
                elif distance < 3.0:
                    confidence = "medium"
                else:
                    confidence = "low"

                note = f"DEQ: {deq_name} -> SS: {ss_name or 'unnamed'} ({operator or 'no operator'})"
                logger.debug(f"  MATCH [{confidence}] {permit_num} -> {facility_id} ({distance:.2f}km) {note}")

                if not dry_run:
                    conn.execute(insert_match, {
                        "pid": permit_id,
                        "fid": facility_id,
                        "dist": round(distance, 3),
                        "conf": confidence,
                        "notes": note,
                    })
                matched += 1
            else:
                note = (
                    f"DEQ permit {permit_num}: {deq_name} in {county}. "
                    f"Nearest SS: {facility_id} at {distance:.1f}km"
                    if distance else f"DEQ permit {permit_num}: {deq_name}"
                )
                logger.info(f"  CANDIDATE {permit_num}: {deq_name} ({county}) — nearest {facility_id} at {distance:.1f}km")

                if not dry_run:
                    conn.execute(insert_candidate, {
                        "pid": permit_id,
                        "dist": round(distance, 3) if distance else None,
                        "notes": note,
                    })
                candidates += 1

        if not dry_run:
            # Log pipeline run
            conn.execute(
                text(f"""
                    INSERT INTO {schema}.pipeline_runs
                        (step_name, started_at, finished_at, row_count, status, notes)
                    VALUES (:step, :started, NOW(), :count, 'success', :notes)
                """),
                {
                    "step": "permit_xref",
                    "started": started,
                    "count": matched + candidates,
                    "notes": f"Matched: {matched}, Candidates: {candidates}, Threshold: {threshold_km}km",
                },
            )
            conn.commit()

        logger.info(f"Cross-reference {'(dry run) ' if dry_run else ''}complete: {matched} matched, {candidates} candidates")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate permit-facility cross-reference")
    parser.add_argument("--threshold-km", type=float, default=MATCH_THRESHOLD_KM,
                        help=f"Max match distance in km (default {MATCH_THRESHOLD_KM})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print results without writing to database")
    args = parser.parse_args()

    populate_xref(threshold_km=args.threshold_km, dry_run=args.dry_run)
