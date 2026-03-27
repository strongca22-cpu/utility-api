#!/usr/bin/env python3
"""
Metro URL Importer

Purpose:
    Takes Research Agent output (list of {pwsid, url, confidence, notes}),
    validates PWSIDs against SDWIS, dedup against scrape_registry, and
    inserts with url_source='metro_research'.

Author: AI-Generated
Created: 2026-03-26
Modified: 2026-03-26

Dependencies:
    - sqlalchemy
    - utility_api (local package)

Usage:
    from scripts.metro_url_importer import import_research_results
    stats = import_research_results(results, metro_id="denver")

Notes:
    - Follows same pattern as import_guesser_state.py
    - ON CONFLICT (pwsid, url) DO NOTHING for idempotence
    - Updates pwsid_coverage.scrape_status to 'url_discovered' for new entries
    - Skips results where url is null or confidence is 'none'
"""

import sys
from pathlib import Path

from loguru import logger
from sqlalchemy import text

# Ensure project src is importable
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine

schema = settings.utility_schema


def import_research_results(
    results: list[dict],
    metro_id: str,
    dry_run: bool = False,
) -> dict:
    """Import Research Agent results into scrape_registry.

    Args:
        results: List of dicts from research_batch/research_metro.
        metro_id: Metro identifier for notes field.
        dry_run: If True, log what would be imported without writing.

    Returns:
        Stats dict with imported, skipped_no_url, skipped_exists, skipped_bad_pwsid.
    """
    stats = {
        "imported": 0,
        "skipped_no_url": 0,
        "skipped_exists": 0,
        "skipped_bad_pwsid": 0,
    }

    # Filter out results with no URL
    actionable = []
    for r in results:
        if not r.get("url") or r.get("confidence") == "none":
            stats["skipped_no_url"] += 1
            continue
        actionable.append(r)

    if not actionable:
        logger.info(f"No actionable URLs to import for {metro_id}")
        return stats

    if dry_run:
        logger.info(f"DRY RUN: would import {len(actionable)} URLs for {metro_id}")
        for r in actionable:
            url_display = r["url"][:65] if len(r["url"]) > 65 else r["url"]
            logger.info(
                f"  [{r.get('confidence', '?'):6s}] {r['pwsid']} → {url_display}"
            )
        return stats

    with engine.connect() as conn:
        for r in actionable:
            # Validate PWSID exists in SDWIS
            exists = conn.execute(
                text(f"SELECT 1 FROM {schema}.sdwis_systems WHERE pwsid = :pwsid"),
                {"pwsid": r["pwsid"]},
            ).fetchone()

            if not exists:
                logger.warning(f"  PWSID {r['pwsid']} not found in SDWIS — skipping")
                stats["skipped_bad_pwsid"] += 1
                continue

            # Insert to scrape_registry
            notes = (
                f"Metro scan: {metro_id} | "
                f"{r.get('confidence', 'unknown')} | "
                f"{r.get('notes', '')}"
            )

            row = conn.execute(
                text(f"""
                    INSERT INTO {schema}.scrape_registry
                        (pwsid, url, url_source, content_type, status, notes)
                    VALUES
                        (:pwsid, :url, 'metro_research', 'html', 'pending', :notes)
                    ON CONFLICT (pwsid, url) DO NOTHING
                    RETURNING id
                """),
                {
                    "pwsid": r["pwsid"],
                    "url": r["url"],
                    "notes": notes,
                },
            )

            if row.fetchone():
                stats["imported"] += 1
            else:
                stats["skipped_exists"] += 1

        # Update pwsid_coverage for newly imported PWSIDs
        if stats["imported"] > 0:
            conn.execute(
                text(f"""
                    UPDATE {schema}.pwsid_coverage pc
                    SET scrape_status = 'url_discovered'
                    WHERE pc.scrape_status = 'not_attempted'
                    AND EXISTS (
                        SELECT 1 FROM {schema}.scrape_registry sr
                        WHERE sr.pwsid = pc.pwsid
                        AND sr.status = 'pending'
                        AND sr.url_source = 'metro_research'
                    )
                """)
            )

        conn.commit()

    logger.info(
        f"Import complete for {metro_id}: "
        f"{stats['imported']} imported, "
        f"{stats['skipped_no_url']} no URL, "
        f"{stats['skipped_exists']} already exist, "
        f"{stats['skipped_bad_pwsid']} bad PWSID"
    )

    return stats
