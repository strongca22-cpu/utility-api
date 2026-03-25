#!/usr/bin/env python3
"""
Migrate Curated URLs to Scrape Registry

Purpose:
    One-time migration of config/rate_urls_*.yaml files into the
    scrape_registry table. Also backfills entries from existing
    water_rates records (scraped_llm source) that have source_url.

    YAML files are preserved as-is (backup/reference per CLAUDE.md rules).

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - pyyaml
    - sqlalchemy
    - loguru

Usage:
    python scripts/migrate_urls_to_registry.py
    python scripts/migrate_urls_to_registry.py --dry-run

Notes:
    - Idempotent: uses INSERT ON CONFLICT DO NOTHING
    - Sources: rate_urls_va.yaml (curated), rate_urls_ca.yaml (curated),
      rate_urls_ca_discovered.yaml (searxng), rate_urls_va_candidates.yaml (civicplus)
    - Also backfills from water_rates.source_url for scraped_llm records
    - Sets status based on whether the URL has been successfully parsed:
      'active' if parsed, 'pending' otherwise
"""

import argparse
import sys
from pathlib import Path

import yaml
from loguru import logger
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine

# YAML files to migrate, with their url_source classification
YAML_FILES = [
    {
        "path": "config/rate_urls_va.yaml",
        "url_source": "curated",
        "notes": "Sprint 3 v2 — SearXNG discovery + manual verification",
    },
    {
        "path": "config/rate_urls_ca.yaml",
        "url_source": "curated",
        "notes": "Sprint 3 — SearXNG discovery for CA utilities",
    },
    {
        "path": "config/rate_urls_ca_discovered.yaml",
        "url_source": "searxng",
        "notes": "Sprint 3 — auto-discovered CA URLs (unverified)",
    },
    {
        "path": "config/rate_urls_va_candidates.yaml",
        "url_source": "civicplus_crawler",
        "notes": "Sprint 4 — CivicPlus crawler discoveries for VA",
    },
]


def _detect_content_type(url: str) -> str:
    """Guess content type from URL."""
    url_lower = url.lower()
    if url_lower.endswith(".pdf"):
        return "pdf"
    if url_lower.endswith((".xlsx", ".xls", ".csv")):
        return "xlsx"
    return "html"


def migrate_yaml_files(dry_run: bool = False) -> dict:
    """Migrate YAML URL files into scrape_registry.

    Parameters
    ----------
    dry_run : bool
        Preview only, no DB writes.

    Returns
    -------
    dict
        Summary: {file: count} for each migrated file.
    """
    schema = settings.utility_schema
    summary = {}

    for yaml_spec in YAML_FILES:
        yaml_path = PROJECT_ROOT / yaml_spec["path"]
        if not yaml_path.exists():
            logger.warning(f"Skipping {yaml_spec['path']} (file not found)")
            summary[yaml_spec["path"]] = 0
            continue

        with open(yaml_path) as f:
            data = yaml.safe_load(f)

        if not data or not isinstance(data, dict):
            logger.warning(f"Skipping {yaml_spec['path']} (empty or invalid YAML)")
            summary[yaml_spec["path"]] = 0
            continue

        entries = []
        for pwsid, url in data.items():
            if not isinstance(url, str) or not url.startswith("http"):
                continue
            entries.append({
                "pwsid": str(pwsid),
                "url": url,
                "url_source": yaml_spec["url_source"],
                "content_type": _detect_content_type(url),
                "notes": yaml_spec["notes"],
            })

        logger.info(f"{yaml_spec['path']}: {len(entries)} URLs")

        if dry_run:
            for e in entries[:5]:
                logger.info(f"  {e['pwsid']}: {e['url'][:80]}")
            if len(entries) > 5:
                logger.info(f"  ... and {len(entries) - 5} more")
            summary[yaml_spec["path"]] = len(entries)
            continue

        # Insert into scrape_registry
        inserted = 0
        with engine.connect() as conn:
            for e in entries:
                result = conn.execute(text(f"""
                    INSERT INTO {schema}.scrape_registry (
                        pwsid, url, url_source, content_type, status, notes
                    ) VALUES (
                        :pwsid, :url, :url_source, :content_type, 'pending', :notes
                    )
                    ON CONFLICT (pwsid, url) DO NOTHING
                """), e)
                if result.rowcount > 0:
                    inserted += 1
            conn.commit()

        logger.info(f"  Inserted {inserted} new entries (skipped {len(entries) - inserted} duplicates)")
        summary[yaml_spec["path"]] = inserted

    return summary


def backfill_from_water_rates(dry_run: bool = False) -> int:
    """Backfill scrape_registry from existing water_rates.source_url.

    For scraped_llm records that have a source_url, create a registry
    entry with status='active' (since they were successfully parsed).

    Parameters
    ----------
    dry_run : bool
        Preview only, no DB writes.

    Returns
    -------
    int
        Number of entries backfilled.
    """
    schema = settings.utility_schema

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT DISTINCT pwsid, source_url, raw_text_hash, parse_confidence,
                   scraped_at, parsed_at
            FROM {schema}.water_rates
            WHERE source = 'scraped_llm'
              AND source_url IS NOT NULL
              AND source_url != ''
        """)).fetchall()

    logger.info(f"Found {len(rows)} scraped_llm records with source_url")

    if dry_run:
        for r in rows[:5]:
            logger.info(f"  {r.pwsid}: {r.source_url[:80]}")
        return len(rows)

    inserted = 0
    with engine.connect() as conn:
        for r in rows:
            result = conn.execute(text(f"""
                INSERT INTO {schema}.scrape_registry (
                    pwsid, url, url_source, content_type,
                    last_fetch_at, last_http_status, last_content_hash,
                    last_parse_at, last_parse_result, last_parse_confidence,
                    status, notes
                ) VALUES (
                    :pwsid, :url, 'curated', :content_type,
                    :last_fetch_at, 200, :content_hash,
                    :last_parse_at, 'success', :parse_confidence,
                    'active', 'Backfilled from water_rates scraped_llm records'
                )
                ON CONFLICT (pwsid, url) DO UPDATE SET
                    last_fetch_at = COALESCE(EXCLUDED.last_fetch_at, {schema}.scrape_registry.last_fetch_at),
                    last_http_status = 200,
                    last_content_hash = COALESCE(EXCLUDED.last_content_hash, {schema}.scrape_registry.last_content_hash),
                    last_parse_at = COALESCE(EXCLUDED.last_parse_at, {schema}.scrape_registry.last_parse_at),
                    last_parse_result = 'success',
                    last_parse_confidence = COALESCE(EXCLUDED.last_parse_confidence, {schema}.scrape_registry.last_parse_confidence),
                    status = 'active',
                    updated_at = NOW()
            """), {
                "pwsid": r.pwsid,
                "url": r.source_url,
                "content_type": _detect_content_type(r.source_url),
                "last_fetch_at": r.scraped_at,
                "content_hash": r.raw_text_hash,
                "last_parse_at": r.parsed_at,
                "parse_confidence": r.parse_confidence,
            })
            if result.rowcount > 0:
                inserted += 1
        conn.commit()

    logger.info(f"Backfilled {inserted} entries from water_rates")
    return inserted


def main():
    parser = argparse.ArgumentParser(description="Migrate YAML URLs to scrape_registry")
    parser.add_argument("--dry-run", action="store_true", help="Preview without DB writes")
    args = parser.parse_args()

    logger.info("=== Migrate URLs to Scrape Registry ===")

    # Step 1: YAML files
    logger.info("\n--- YAML File Migration ---")
    yaml_summary = migrate_yaml_files(dry_run=args.dry_run)

    # Step 2: Backfill from water_rates
    logger.info("\n--- Backfill from water_rates ---")
    backfilled = backfill_from_water_rates(dry_run=args.dry_run)

    # Summary
    logger.info("\n--- Summary ---")
    total = sum(yaml_summary.values()) + backfilled
    for path, count in yaml_summary.items():
        logger.info(f"  {path}: {count}")
    logger.info(f"  water_rates backfill: {backfilled}")
    logger.info(f"  Total: {total}")

    if args.dry_run:
        logger.info("[DRY RUN] No DB writes performed")

    logger.info("=== Migration Complete ===")


if __name__ == "__main__":
    main()
