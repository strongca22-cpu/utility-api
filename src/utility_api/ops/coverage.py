#!/usr/bin/env python3
"""
Coverage Refresh Logic

Purpose:
    Recomputes derived columns in pwsid_coverage table while preserving
    mutable operational columns (scrape_status, priority_tier).

    Derived columns (recomputed):
    - has_rate_data, rate_source_count, rate_sources, last_rate_loaded_at
    - best_source, best_bill_10ccf, best_confidence
    - has_sdwis, population_served, primary_source_code, owner_type_code

    Mutable columns (preserved):
    - scrape_status
    - priority_tier

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - sqlalchemy
    - loguru

Usage:
    from utility_api.ops.coverage import refresh_coverage_derived, update_scrape_status
"""

from loguru import logger
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine


def refresh_coverage_derived() -> dict:
    """Recompute derived columns in pwsid_coverage.

    Updates rate coverage, best-estimate, and SDWIS columns from
    source tables. Does NOT overwrite scrape_status or priority_tier.

    Returns
    -------
    dict
        Summary: total, with_rates, with_sdwis counts.
    """
    schema = settings.utility_schema

    with engine.connect() as conn:
        # Ensure all CWS PWSIDs exist in the table
        conn.execute(text(f"""
            INSERT INTO {schema}.pwsid_coverage (pwsid, state_code, pws_name)
            SELECT c.pwsid, c.state_code, c.pws_name
            FROM {schema}.cws_boundaries c
            WHERE NOT EXISTS (
                SELECT 1 FROM {schema}.pwsid_coverage pc WHERE pc.pwsid = c.pwsid
            )
        """))

        # Update rate coverage columns (rate_schedules is sole source after Phase 4)
        conn.execute(text(f"""
            UPDATE {schema}.pwsid_coverage pc SET
                has_rate_data = (
                    EXISTS (SELECT 1 FROM {schema}.rate_schedules rs WHERE rs.pwsid = pc.pwsid)
                ),
                rate_source_count = COALESCE((
                    SELECT COUNT(DISTINCT rs.source_key)
                    FROM {schema}.rate_schedules rs WHERE rs.pwsid = pc.pwsid
                ), 0),
                rate_sources = (
                    SELECT STRING_AGG(DISTINCT rs.source_key, ',' ORDER BY rs.source_key)
                    FROM {schema}.rate_schedules rs WHERE rs.pwsid = pc.pwsid
                ),
                last_rate_loaded_at = (
                    SELECT MAX(rs.created_at) FROM {schema}.rate_schedules rs WHERE rs.pwsid = pc.pwsid
                )
        """))

        # Update best-estimate columns
        conn.execute(text(f"""
            UPDATE {schema}.pwsid_coverage pc SET
                best_source = be.selected_source,
                best_bill_10ccf = be.bill_estimate_10ccf,
                best_confidence = be.confidence
            FROM {schema}.rate_best_estimate be
            WHERE be.pwsid = pc.pwsid
        """))

        # Clear best-estimate for PWSIDs no longer in rate_best_estimate
        conn.execute(text(f"""
            UPDATE {schema}.pwsid_coverage pc SET
                best_source = NULL,
                best_bill_10ccf = NULL,
                best_confidence = NULL
            WHERE NOT EXISTS (
                SELECT 1 FROM {schema}.rate_best_estimate be WHERE be.pwsid = pc.pwsid
            )
            AND pc.best_source IS NOT NULL
        """))

        # Update SDWIS columns
        conn.execute(text(f"""
            UPDATE {schema}.pwsid_coverage pc SET
                has_sdwis = TRUE,
                population_served = s.population_served_count,
                primary_source_code = s.primary_source_code,
                owner_type_code = s.owner_type_code
            FROM {schema}.sdwis_systems s
            WHERE s.pwsid = pc.pwsid
        """))

        conn.execute(text(f"""
            UPDATE {schema}.pwsid_coverage pc SET
                has_sdwis = FALSE
            WHERE NOT EXISTS (
                SELECT 1 FROM {schema}.sdwis_systems s WHERE s.pwsid = pc.pwsid
            )
            AND pc.has_sdwis = TRUE
        """))

        conn.commit()

        # Get summary
        row = conn.execute(text(f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN has_rate_data THEN 1 ELSE 0 END) AS with_rates,
                SUM(CASE WHEN has_sdwis THEN 1 ELSE 0 END) AS with_sdwis
            FROM {schema}.pwsid_coverage
        """)).fetchone()

    stats = {
        "total": row.total,
        "with_rates": row.with_rates,
        "with_sdwis": row.with_sdwis,
    }
    logger.info(f"Coverage refreshed: {stats['total']:,} total, "
                f"{stats['with_rates']:,} rates, {stats['with_sdwis']:,} SDWIS")
    return stats


def update_scrape_status() -> dict:
    """Update pwsid_coverage.scrape_status from scrape_registry.

    Logic:
    - If PWSID has any entry with last_parse_result='success' → 'succeeded'
    - If PWSID has entries but all failed → 'attempted_failed'
    - If PWSID has entries with status='pending' → 'url_discovered'
    - If PWSID has no entries → 'not_attempted'

    Returns
    -------
    dict
        Counts per scrape_status value.
    """
    schema = settings.utility_schema

    with engine.connect() as conn:
        # succeeded: at least one successful parse
        conn.execute(text(f"""
            UPDATE {schema}.pwsid_coverage pc SET scrape_status = 'succeeded'
            WHERE EXISTS (
                SELECT 1 FROM {schema}.scrape_registry sr
                WHERE sr.pwsid = pc.pwsid AND sr.last_parse_result = 'success'
            )
        """))

        # attempted_failed: has entries but none succeeded
        conn.execute(text(f"""
            UPDATE {schema}.pwsid_coverage pc SET scrape_status = 'attempted_failed'
            WHERE EXISTS (
                SELECT 1 FROM {schema}.scrape_registry sr WHERE sr.pwsid = pc.pwsid
            )
            AND NOT EXISTS (
                SELECT 1 FROM {schema}.scrape_registry sr
                WHERE sr.pwsid = pc.pwsid AND sr.last_parse_result = 'success'
            )
            AND EXISTS (
                SELECT 1 FROM {schema}.scrape_registry sr
                WHERE sr.pwsid = pc.pwsid
                AND sr.last_fetch_at IS NOT NULL
                AND sr.last_http_status IS NOT NULL
            )
        """))

        # url_discovered: has pending entries but no fetch attempts
        conn.execute(text(f"""
            UPDATE {schema}.pwsid_coverage pc SET scrape_status = 'url_discovered'
            WHERE EXISTS (
                SELECT 1 FROM {schema}.scrape_registry sr
                WHERE sr.pwsid = pc.pwsid AND sr.status = 'pending'
            )
            AND NOT EXISTS (
                SELECT 1 FROM {schema}.scrape_registry sr
                WHERE sr.pwsid = pc.pwsid AND sr.last_fetch_at IS NOT NULL
            )
        """))

        # not_attempted: no entries at all (default, already set)
        conn.execute(text(f"""
            UPDATE {schema}.pwsid_coverage pc SET scrape_status = 'not_attempted'
            WHERE NOT EXISTS (
                SELECT 1 FROM {schema}.scrape_registry sr WHERE sr.pwsid = pc.pwsid
            )
            AND pc.scrape_status != 'not_attempted'
        """))

        conn.commit()

        # Get counts
        rows = conn.execute(text(f"""
            SELECT scrape_status, COUNT(*) AS cnt
            FROM {schema}.pwsid_coverage
            GROUP BY scrape_status
            ORDER BY cnt DESC
        """)).fetchall()

    stats = {r.scrape_status: r.cnt for r in rows}
    logger.info(f"Scrape status updated: {stats}")
    return stats
