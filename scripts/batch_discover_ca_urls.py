#!/usr/bin/env python3
"""
Batch CA Rate URL Discovery

Purpose:
    Discover rate page URLs for CA MDWD utilities in batches to avoid
    SearXNG rate limiting. Outputs curated YAML for pipeline input.
    Prioritizes by population (largest first).

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - httpx, sqlalchemy, pyyaml

Usage:
    python scripts/batch_discover_ca_urls.py [--batch-size 40] [--batch-delay 60]

Notes:
    - SearXNG rate-limits after ~60 queries; batches of 40 with 60s pause
    - Verifies URLs with HTTP HEAD before including
    - Outputs config/rate_urls_ca.yaml
"""

import argparse
import sys
import time
from pathlib import Path

import httpx
import yaml

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from utility_api.config import settings
from utility_api.db import engine
from utility_api.ingest.rate_discovery import _search_searxng, discover_rate_url
from sqlalchemy import text

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}


def verify_url(url: str) -> dict:
    """HTTP HEAD check."""
    try:
        with httpx.Client(headers=HEADERS, timeout=10, follow_redirects=True) as c:
            r = c.head(url)
            if r.status_code == 405:
                r = c.get(url, follow_redirects=True)
            return {"ok": r.status_code < 400, "status": r.status_code,
                    "is_pdf": "pdf" in r.headers.get("content-type", "").lower() or url.lower().endswith(".pdf")}
    except Exception as e:
        return {"ok": False, "status": None, "is_pdf": False, "error": str(e)[:60]}


def get_targets() -> list[dict]:
    """Get CA MDWD utilities ordered by population."""
    schema = settings.utility_schema
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT c.pwsid, c.pws_name, c.county_served, m.population
            FROM {schema}.cws_boundaries c
            JOIN {schema}.mdwd_financials m ON m.pwsid = c.pwsid
            WHERE c.state_code = 'CA'
            ORDER BY m.population DESC NULLS LAST
        """)).fetchall()

    # Skip any that already have good rates
    with engine.connect() as conn:
        existing = conn.execute(text(f"""
            SELECT DISTINCT pwsid FROM {schema}.rate_schedules
            WHERE confidence IN ('high', 'medium')
        """)).fetchall()
        existing_set = {r[0] for r in existing}

    return [
        {"pwsid": r[0], "name": r[1], "county": r[2], "population": r[3]}
        for r in rows if r[0] not in existing_set
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=40)
    parser.add_argument("--batch-delay", type=int, default=90, help="Seconds between batches")
    parser.add_argument("--search-delay", type=float, default=2.5, help="Seconds between searches")
    parser.add_argument("--max-batches", type=int, default=None, help="Stop after N batches")
    args = parser.parse_args()

    targets = get_targets()
    print(f"CA MDWD utilities to discover: {len(targets)}")

    results = {}
    failed = []
    batch_num = 0

    for i in range(0, len(targets), args.batch_size):
        batch_num += 1
        if args.max_batches and batch_num > args.max_batches:
            print(f"\nStopping after {args.max_batches} batches")
            break

        batch = targets[i:i + args.batch_size]
        print(f"\n=== Batch {batch_num} ({len(batch)} utilities, starting at #{i+1}) ===")

        empty_count = 0  # Track consecutive empty results to detect rate limiting

        for j, t in enumerate(batch):
            pwsid = t["pwsid"]
            name = t["name"]
            county = t["county"] or ""
            pop = t["population"] or 0

            result = discover_rate_url(pwsid, name, "CA", county)

            if result.best_url:
                vr = verify_url(result.best_url)
                if vr["ok"]:
                    results[pwsid] = {
                        "url": result.best_url,
                        "name": name,
                        "county": county,
                        "population": pop,
                        "is_pdf": vr["is_pdf"],
                        "score": result.candidates[0].score if result.candidates else 0,
                    }
                    tag = "[PDF]" if vr["is_pdf"] else "[HTML]"
                    print(f"  [{i+j+1}] ✓ {pwsid} {name[:35]} {tag} score={results[pwsid]['score']:.0f}")
                    empty_count = 0
                else:
                    failed.append({"pwsid": pwsid, "name": name, "reason": f"HTTP {vr['status']}"})
                    print(f"  [{i+j+1}] ✗ {pwsid} {name[:35]} — HTTP {vr['status']}")
                    empty_count = 0
            else:
                failed.append({"pwsid": pwsid, "name": name, "reason": "no results"})
                empty_count += 1
                if empty_count >= 5:
                    print(f"  [{i+j+1}] ⚠ 5 consecutive empty results — SearXNG likely rate-limited")
                    print(f"  Stopping batch early. Discovered {len(results)} so far.")
                    break

            time.sleep(args.search_delay)

        # Check if we should stop due to rate limiting
        if empty_count >= 5:
            remaining = len(targets) - i - len(batch)
            print(f"\nRate limit detected. {remaining} utilities remaining.")
            print(f"Re-run with --batch-delay to resume (already-found URLs will be preserved)")
            break

        # Pause between batches
        if i + args.batch_size < len(targets):
            print(f"\nBatch complete. Pausing {args.batch_delay}s for rate limit cooldown...")
            time.sleep(args.batch_delay)

    # Write YAML
    yaml_out = Path(__file__).parents[1] / "config" / "rate_urls_ca.yaml"
    with open(yaml_out, "w") as f:
        f.write("# CA Rate URL Candidates — auto-discovered via SearXNG\n")
        f.write(f"# Generated: 2026-03-24\n")
        f.write(f"# Discovered: {len(results)} | Failed: {len(failed)}\n\n")
        for pwsid in sorted(results.keys()):
            r = results[pwsid]
            tag = "PDF" if r["is_pdf"] else "HTML"
            f.write(f'# {r["name"]} ({r["county"]}, pop={r["population"]:,}) [{tag}]\n')
            f.write(f'{pwsid}: "{r["url"]}"\n\n')

    print(f"\n=== Summary ===")
    print(f"Discovered: {len(results)}/{len(targets)}")
    print(f"Failed: {len(failed)}")
    print(f"PDFs: {sum(1 for r in results.values() if r['is_pdf'])}")
    print(f"YAML written: {yaml_out}")


if __name__ == "__main__":
    main()
