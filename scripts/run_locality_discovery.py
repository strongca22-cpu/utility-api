#!/usr/bin/env python3
"""
Locality Discovery — Batch Runner

Purpose:
    Runs LocalityDiscoveryAgent on gap PWSIDs where the standard discovery
    pipeline failed. Extracts municipality names from formal PWSID system
    names and searches using reformulated queries.

    Primary use: fallback discovery for PWSIDs that went through the standard
    pipeline (Serper discovery → scrape → parse) and all attempts failed.

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Dependencies:
    - sqlalchemy
    - utility_api (local package)

Usage:
    # Dry run — preview municipality names and queries (no API calls)
    python scripts/run_locality_discovery.py --state NY --dry-run

    # Execute for a single state
    python scripts/run_locality_discovery.py --state NY

    # Execute for specific PWSIDs
    python scripts/run_locality_discovery.py --pwsids NY4600070,NY5903435

    # Limit by population
    python scripts/run_locality_discovery.py --state NY --min-pop 10000

    # With diagnostics (log near-miss URLs)
    python scripts/run_locality_discovery.py --state NY --diagnostic

Notes:
    - Always run --dry-run first to preview municipality extractions
    - Gap criteria: has scrape_registry entries, no scraped_llm rate, pop >= threshold
    - Cost: ~4 Serper queries per PWSID (~$0.004/PWSID)
    - Private companies and institutions are automatically skipped
"""

import argparse
import sys
from pathlib import Path

from loguru import logger
from sqlalchemy import text

# Ensure project src is importable
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.agents.locality_discovery import (
    LocalityDiscoveryAgent,
    extract_municipality,
)
from utility_api.config import settings
from utility_api.db import engine

# Add file logging
LOG_PATH = Path("/var/log/uapi/locality_discovery.log")
if LOG_PATH.parent.exists():
    logger.add(str(LOG_PATH), rotation="10 MB", retention="30 days")

schema = settings.utility_schema


def get_gap_pwsids(
    state_code: str | None = None,
    min_pop: int = 3000,
    pwsid_list: list[str] | None = None,
) -> list[dict]:
    """Find PWSIDs that qualify for locality discovery.

    Criteria:
        1. Has entries in scrape_registry (been through standard pipeline)
        2. No scraped_llm rate in rate_schedules
        3. Population >= min_pop
        4. Optionally filtered by state

    Parameters
    ----------
    state_code : str, optional
        2-letter state code filter.
    min_pop : int
        Minimum population threshold (default 3,000).
    pwsid_list : list[str], optional
        Explicit list of PWSIDs (overrides state/pop filters).

    Returns
    -------
    list[dict]
        Dicts with pwsid, pws_name, population, county, state.
    """
    with engine.connect() as conn:
        if pwsid_list:
            # Explicit PWSID list — no filtering
            placeholders = ", ".join(f"'{p}'" for p in pwsid_list)
            result = conn.execute(text(f"""
                SELECT cb.pwsid, cb.pws_name, cb.population_served,
                       cb.county_served, cb.state_code
                FROM {schema}.cws_boundaries cb
                WHERE cb.pwsid IN ({placeholders})
                ORDER BY cb.population_served DESC
            """))
        else:
            # Gap query: in scrape_registry, no scraped_llm rate
            state_filter = f"AND cb.state_code = '{state_code}'" if state_code else ""
            result = conn.execute(text(f"""
                SELECT cb.pwsid, cb.pws_name, cb.population_served,
                       cb.county_served, cb.state_code
                FROM {schema}.cws_boundaries cb
                WHERE cb.population_served >= :min_pop
                  {state_filter}
                  AND EXISTS (
                      SELECT 1 FROM {schema}.scrape_registry sr
                      WHERE sr.pwsid = cb.pwsid
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM {schema}.rate_schedules rs
                      WHERE rs.pwsid = cb.pwsid
                        AND rs.source_key = 'scraped_llm'
                  )
                ORDER BY cb.population_served DESC
            """), {"min_pop": min_pop})

        rows = result.fetchall()

    return [
        {
            "pwsid": r.pwsid,
            "pws_name": r.pws_name,
            "population": r.population_served,
            "county": r.county_served,
            "state": r.state_code,
        }
        for r in rows
    ]


def run_dry_run(gap_pwsids: list[dict]) -> None:
    """Preview municipality name extractions and queries without API calls.

    Prints a formatted report showing:
    - Which PWSIDs produce valid municipality names
    - Which are private companies (skipped)
    - Which are institutions (skipped)
    - Sample queries for each valid municipality
    """
    valid = []
    private = []
    institutional = []
    unextractable = []

    for p in gap_pwsids:
        municipality = extract_municipality(p["pws_name"], p["county"])
        entry = {**p, "municipality": municipality}

        if municipality is None:
            # Categorize skip reason
            name_upper = p["pws_name"].upper()
            if any(kw in name_upper for kw in [
                "VEOLIA", "AQUA", "LIBERTY", "SUEZ", "AMERICAN WATER",
                "CAL AM", "INC.", "INC,", "LLC", "CORP",
            ]):
                private.append(entry)
            elif any(kw in name_upper for kw in [
                "UNIVERSITY", "COLLEGE", "CORRECTIONAL", "PRISON",
                "MILITARY", "FORT DRUM", "NATIONAL LAB", "U.S.M.A.",
            ]):
                institutional.append(entry)
            else:
                unextractable.append(entry)
        else:
            valid.append(entry)

    # --- Report ---
    total = len(gap_pwsids)
    total_pop = sum(p["population"] or 0 for p in gap_pwsids)

    print(f"\n{'=' * 80}")
    print(f"LOCALITY DISCOVERY — DRY RUN REPORT")
    print(f"{'=' * 80}")
    print(f"Total gap PWSIDs:        {total:>5}  ({total_pop:>12,} pop)")
    print(f"Valid municipalities:     {len(valid):>5}  ({sum(p['population'] or 0 for p in valid):>12,} pop)")
    print(f"Private companies:       {len(private):>5}  ({sum(p['population'] or 0 for p in private):>12,} pop)")
    print(f"Institutions:            {len(institutional):>5}  ({sum(p['population'] or 0 for p in institutional):>12,} pop)")
    print(f"Unextractable:           {len(unextractable):>5}  ({sum(p['population'] or 0 for p in unextractable):>12,} pop)")
    print()

    estimated_queries = len(valid) * 4
    estimated_cost = estimated_queries * 0.001
    print(f"Estimated Serper queries: {estimated_queries}")
    print(f"Estimated cost:           ${estimated_cost:.2f}")
    print()

    # --- Valid municipalities ---
    if valid:
        print(f"--- VALID MUNICIPALITIES ({len(valid)}) ---")
        print(f"{'PWSID':<15} {'Pop':>7} {'County':<18} {'Name':<40} → Municipality")
        print("-" * 120)
        for p in valid:
            print(
                f"{p['pwsid']:<15} {(p['population'] or 0):>7,} "
                f"{(p['county'] or '?'):<18} {p['pws_name'][:38]:<40} → "
                f"{p['municipality']}"
            )
        print()

        # Show sample queries for top 5
        print(f"--- SAMPLE QUERIES (top 5 by pop) ---")
        from utility_api.agents.locality_discovery import build_locality_queries
        for p in valid[:5]:
            queries = build_locality_queries(p["municipality"], p["state"])
            print(f"\n  {p['pwsid']} — {p['pws_name']} → \"{p['municipality']}\"")
            for i, q in enumerate(queries, 1):
                print(f"    Q{i}: {q}")
        print()

    # --- Skipped: Private companies ---
    if private:
        print(f"--- SKIPPED: PRIVATE COMPANIES ({len(private)}) ---")
        for p in private:
            print(f"  {p['pwsid']:<15} {(p['population'] or 0):>7,}  {p['pws_name']}")
        print()

    # --- Skipped: Institutions ---
    if institutional:
        print(f"--- SKIPPED: INSTITUTIONS ({len(institutional)}) ---")
        for p in institutional:
            print(f"  {p['pwsid']:<15} {(p['population'] or 0):>7,}  {p['pws_name']}")
        print()

    # --- Unextractable ---
    if unextractable:
        print(f"--- UNEXTRACTABLE ({len(unextractable)}) ---")
        for p in unextractable:
            print(f"  {p['pwsid']:<15} {(p['population'] or 0):>7,}  {p['pws_name']}")
        print()


def run_live(
    gap_pwsids: list[dict],
    diagnostic: bool = False,
) -> dict:
    """Execute locality discovery for gap PWSIDs.

    Parameters
    ----------
    gap_pwsids : list[dict]
        PWSIDs from get_gap_pwsids().
    diagnostic : bool
        Log near-miss URLs.

    Returns
    -------
    dict
        Summary with totals and per-PWSID details.
    """
    agent = LocalityDiscoveryAgent()
    total_written = 0
    total_found = 0
    skipped = 0
    errors = 0
    details = []

    for i, p in enumerate(gap_pwsids, 1):
        logger.info(
            f"[{i}/{len(gap_pwsids)}] {p['pwsid']} — "
            f"{p['pws_name'][:35]} (pop {(p['population'] or 0):,})"
        )
        try:
            result = agent.run(
                pwsid=p["pwsid"],
                diagnostic=diagnostic,
            )
            if result.get("skip_reason"):
                skipped += 1
            else:
                total_written += result.get("urls_written", 0)
                total_found += result.get("urls_found", 0)
            details.append(result)
        except Exception as e:
            logger.error(f"  → Failed: {e}")
            errors += 1
            details.append({"pwsid": p["pwsid"], "error": str(e)})

    # --- Summary ---
    successful = [d for d in details if d.get("urls_written", 0) > 0]
    no_hits = [
        d for d in details
        if not d.get("skip_reason") and not d.get("error") and d.get("urls_written", 0) == 0
    ]

    print(f"\n{'=' * 60}")
    print(f"LOCALITY DISCOVERY — RESULTS")
    print(f"{'=' * 60}")
    print(f"Total processed:   {len(gap_pwsids)}")
    print(f"Skipped:           {skipped}")
    print(f"Errors:            {errors}")
    print(f"URLs found:        {total_found}")
    print(f"URLs written:      {total_written}")
    print(f"PWSIDs with URLs:  {len(successful)}")
    print(f"PWSIDs no hits:    {len(no_hits)}")
    print()

    if successful:
        print(f"--- PWSIDs WITH NEW URLs ({len(successful)}) ---")
        print(f"{'PWSID':<15} {'Municipality':<25} {'Written':>7} Top URL")
        print("-" * 100)
        for d in successful:
            top_url = d["top_candidates"][0]["url"][:50] if d.get("top_candidates") else "?"
            print(
                f"{d['pwsid']:<15} {(d.get('municipality') or '?'):<25} "
                f"{d['urls_written']:>7} {top_url}"
            )
        print()

    # Domain distribution
    if successful:
        from collections import Counter
        from urllib.parse import urlparse

        domains = Counter()
        for d in successful:
            for c in d.get("top_candidates", []):
                hostname = urlparse(c["url"]).hostname or "?"
                # Group by root domain (drop subdomains)
                parts = hostname.split(".")
                root = ".".join(parts[-2:]) if len(parts) >= 2 else hostname
                domains[root] += 1

        print(f"--- DOMAIN DISTRIBUTION ---")
        for domain, count in domains.most_common(20):
            print(f"  {domain:<40} {count:>4}")
        print()

    return {
        "total": len(gap_pwsids),
        "skipped": skipped,
        "errors": errors,
        "urls_found": total_found,
        "urls_written": total_written,
        "pwsids_with_urls": len(successful),
        "details": details,
    }


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Run locality discovery on gap PWSIDs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_locality_discovery.py --state NY --dry-run
  python scripts/run_locality_discovery.py --state NY
  python scripts/run_locality_discovery.py --pwsids NY4600070,NY5903435
  python scripts/run_locality_discovery.py --state NY --min-pop 10000
        """,
    )
    parser.add_argument("--state", help="2-letter state code filter")
    parser.add_argument("--pwsids", help="Comma-separated PWSID list")
    parser.add_argument("--min-pop", type=int, default=3000, help="Min population (default: 3000)")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, no API calls")
    parser.add_argument("--diagnostic", action="store_true", help="Log near-miss URLs")

    args = parser.parse_args()

    if not args.state and not args.pwsids:
        parser.error("Must specify --state or --pwsids")

    # Parse PWSID list if provided
    pwsid_list = None
    if args.pwsids:
        pwsid_list = [p.strip() for p in args.pwsids.split(",")]

    # Get gap PWSIDs
    logger.info("Querying gap PWSIDs...")
    gap = get_gap_pwsids(
        state_code=args.state,
        min_pop=args.min_pop,
        pwsid_list=pwsid_list,
    )
    logger.info(f"Found {len(gap)} gap PWSIDs")

    if not gap:
        print("No gap PWSIDs found matching criteria.")
        return

    if args.dry_run:
        run_dry_run(gap)
    else:
        run_live(gap, diagnostic=args.diagnostic)


if __name__ == "__main__":
    main()
