#!/usr/bin/env python3
"""
Import Domain Guesser State CSV into Scrape Registry

Purpose:
    Takes a single state CSV from the domain guesser, applies best-URL
    selection (confidence tiers, no blanket .gov override), and imports
    one URL per PWSID into scrape_registry.

Author: AI-Generated
Created: 2026-03-25
Modified: 2026-03-25

Dependencies:
    - sqlalchemy
    - utility_api (local package)

Usage:
    python scripts/import_guesser_state.py data/guesser_incoming/guessed_domains_AL.csv
    python scripts/import_guesser_state.py data/guesser_incoming/guessed_domains_AL.csv --dry-run

Notes:
    - Idempotent: uses INSERT ON CONFLICT DO NOTHING
    - Prioritizes water-specific domains over generic .gov
    - Filters out bad redirects (domain totally changed)
    - Updates pwsid_coverage.scrape_status to 'url_discovered'
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

from loguru import logger
from sqlalchemy import text

# Ensure project src is importable
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.config import settings
from utility_api.db import engine

schema = settings.utility_schema


def pick_best_url(urls: list[dict]) -> str | None:
    """Pick the single best URL for a PWSID from domain guesser candidates.

    Priority: confidence tier ranking, NOT blanket .gov preference.
    Water-specific domains (name_water, or 'water' in domain) always win.
    Uses final_url (post-redirect) when available.
    Filters out obvious bad redirects (domain completely changed).
    """
    good = []
    for u in urls:
        guessed = u.get("guessed_url", "")
        final = u.get("final_url", "") or guessed

        if not guessed:
            continue

        guessed_domain = (urlparse(guessed).hostname or "").replace("www.", "")
        final_domain = (urlparse(final).hostname or "").replace("www.", "")

        # Filter: skip if redirect went to a completely unrelated domain
        if final_domain and guessed_domain and final_domain != guessed_domain:
            g_parts = set(guessed_domain.split(".")) - {
                "com", "org", "net", "gov", "us", "www",
            }
            f_parts = set(final_domain.split(".")) - {
                "com", "org", "net", "gov", "us", "www",
            }
            if not (g_parts & f_parts):
                continue  # e.g., spotsylvania.com -> telepathy.com

        good.append((u, final))

    if not good:
        return None

    # Prioritize water-specific domains
    water_specific = []
    for u, final in good:
        domain = u.get("domain", "").lower()
        tier = u.get("confidence_tier", "")
        if tier == "name_water" or "water" in domain:
            water_specific.append((u, final))

    if water_specific:
        water_specific.sort(
            key=lambda x: int(x[0].get("confidence_score", 0)), reverse=True
        )
        return water_specific[0][1]

    # Rank by confidence score (descending), .gov as tiebreaker
    def sort_key(item):
        u, final = item
        conf = int(u.get("confidence_score", 0))
        is_gov = 1 if ".gov" in final else 0
        return (-conf, -is_gov)

    good.sort(key=sort_key)
    return good[0][1]


def import_state(csv_path: str, dry_run: bool = False) -> dict:
    """Import a single state CSV into scrape_registry."""
    path = Path(csv_path)
    if not path.exists():
        logger.error(f"File not found: {csv_path}")
        return {"inserted": 0, "skipped": 0, "error": "file_not_found"}

    # Extract state code from filename
    state = path.stem.replace("guessed_domains_", "")

    # Read CSV
    with open(path) as f:
        rows = list(csv.DictReader(f))

    if not rows:
        logger.warning(f"{state}: empty CSV")
        return {"state": state, "inserted": 0, "skipped": 0}

    # Filter to HTTP 200 (live domains only)
    live = [r for r in rows if r.get("http_status") == "200"]
    logger.info(f"{state}: {len(rows)} total rows, {len(live)} live (HTTP 200)")

    # Group by PWSID
    by_pwsid = defaultdict(list)
    for r in live:
        by_pwsid[r["pwsid"]].append(r)

    logger.info(f"{state}: {len(by_pwsid)} unique PWSIDs with live URLs")

    if dry_run:
        water_count = 0
        for pwsid, urls in by_pwsid.items():
            best = pick_best_url(urls)
            if best and "water" in (urlparse(best).hostname or ""):
                water_count += 1
        logger.info(
            f"{state}: DRY RUN — would import {len(by_pwsid)} URLs "
            f"({water_count} water-specific)"
        )
        return {"state": state, "inserted": 0, "skipped": 0, "dry_run": True}

    # Select best URL per PWSID and import
    inserted = 0
    skipped = 0

    with engine.connect() as conn:
        for pwsid, urls in by_pwsid.items():
            best_url = pick_best_url(urls)
            if not best_url:
                skipped += 1
                continue

            # Get confidence tier of selected URL for notes
            selected = next(
                (
                    u
                    for u in urls
                    if (u.get("final_url") or u.get("guessed_url")) == best_url
                ),
                urls[0],
            )
            tier = selected.get("confidence_tier", "unknown")

            result = conn.execute(
                text(f"""
                INSERT INTO {schema}.scrape_registry
                    (pwsid, url, url_source, content_type, status, notes)
                VALUES
                    (:pwsid, :url, 'domain_guesser', 'html', 'pending',
                     :notes)
                ON CONFLICT (pwsid, url) DO NOTHING
            """),
                {
                    "pwsid": pwsid,
                    "url": best_url,
                    "notes": f"Domain guesser ({state}) | tier: {tier}",
                },
            )

            if result.rowcount > 0:
                inserted += 1
            else:
                skipped += 1

        # Update pwsid_coverage for newly imported PWSIDs
        conn.execute(
            text(f"""
            UPDATE {schema}.pwsid_coverage pc
            SET scrape_status = 'url_discovered'
            WHERE pc.scrape_status = 'not_attempted'
            AND EXISTS (
                SELECT 1 FROM {schema}.scrape_registry sr
                WHERE sr.pwsid = pc.pwsid
                AND sr.status = 'pending'
                AND sr.url_source = 'domain_guesser'
            )
        """)
        )

        conn.commit()

    logger.info(f"{state}: {inserted} inserted, {skipped} skipped")
    return {
        "state": state,
        "inserted": inserted,
        "skipped": skipped,
        "total_live": len(live),
        "total_pwsids": len(by_pwsid),
    }


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/import_guesser_state.py <csv_path> [--dry-run]")
        sys.exit(1)

    dry_run = "--dry-run" in sys.argv
    csv_path = [a for a in sys.argv[1:] if not a.startswith("--")][0]
    result = import_state(csv_path, dry_run=dry_run)
    print(f"Result: {result}")
