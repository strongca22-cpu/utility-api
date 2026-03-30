#!/usr/bin/env python3
"""
Sprint 24 — Serper Bulk Discovery Sweep

Purpose:
    Run large-scale Serper-powered URL discovery for gap-state PWSIDs.
    Replaces the slow SearXNG discovery batches (10 PWSIDs/hour) with
    Serper throughput (~100+ PWSIDs/hour at 0.2s delay).

    Targets PWSIDs in states with <20% bulk coverage, sorted by
    population descending. Respects free-tier budget guards and
    logs every query for billing audit.

Author: AI-Generated
Created: 2026-03-29
Modified: 2026-03-29

Dependencies:
    - utility_api (installed in dev mode)
    - SERPER_API_KEY in .env
    - PostgreSQL with utility schema (migration 019 applied)

Usage:
    # Dry run — see what would be searched
    python scripts/serper_bulk_discovery.py --max-pwsids 625 --dry-run

    # Validate on free tier (25 PWSIDs = 100 queries)
    python scripts/serper_bulk_discovery.py --max-pwsids 25

    # Full free-tier sweep
    python scripts/serper_bulk_discovery.py --max-pwsids 625

    # Specific state
    python scripts/serper_bulk_discovery.py --state NY --pop-min 5000

    # Check usage
    python scripts/serper_bulk_discovery.py --usage

    # Run in tmux (recommended for large sweeps):
    tmux new-session -d -s serper_sweep \
        "python3 scripts/serper_bulk_discovery.py --max-pwsids 625 2>&1 | tee logs/serper_sweep.log"

Notes:
    - Budget guard: refuses to exceed 2,500 queries unless SERPER_PAID_MODE=true
    - Each PWSID uses 4 Serper queries (optimized for Google quality)
    - Top 3 URLs per PWSID written with discovery_rank tagging
    - search_queries table logs every API call (billing audit trail)
    - search_log table logs per-PWSID funnel summary with ranked URLs
    - Run --dry-run FIRST to preview before committing API budget

Data Sources:
    - Input: utility.pwsid_coverage + utility.sdwis_systems (gap states, pop >= min)
    - Output: utility.scrape_registry (url_source='serper', discovery_rank=1/2/3)
"""

import argparse
import sys
import time
from pathlib import Path

# Ensure project root is on path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from loguru import logger
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine


def get_target_pwsids(
    scope: str,
    state: str | None,
    pop_min: int,
    max_pwsids: int | None,
    cooldown_days: int = 30,
) -> list[dict]:
    """Select PWSIDs to search based on scope and filters.

    Parameters
    ----------
    scope : str
        "gap_states" | "all_uncovered" | "specific_state"
    state : str, optional
        Two-letter state code (required for specific_state scope).
    pop_min : int
        Minimum population served.
    max_pwsids : int, optional
        Hard cap on number of PWSIDs returned.
    cooldown_days : int
        Skip PWSIDs searched within this many days.

    Returns
    -------
    list[dict]
        Each dict: pwsid, pws_name, state_code, city, county, population, owner_type.
    """
    schema = settings.utility_schema

    # Build the state filter based on scope
    if scope == "specific_state":
        if not state:
            raise ValueError("--state is required for specific_state scope")
        state_filter = f"AND s.state_code = '{state}'"
    elif scope == "all_uncovered":
        state_filter = ""  # no state restriction
    else:
        # gap_states: states with <20% bulk coverage
        state_filter = f"""
            AND s.state_code IN (
                SELECT state_code FROM (
                    SELECT state_code,
                           round(100.0 * count(*) FILTER (WHERE has_rate_data)
                                 / NULLIF(count(*), 0), 1) as pct
                    FROM {schema}.pwsid_coverage
                    GROUP BY state_code
                ) sub WHERE pct < 20 OR pct IS NULL
            )
        """

    limit_clause = f"LIMIT {max_pwsids}" if max_pwsids else ""

    query = f"""
        SELECT pc.pwsid, s.pws_name, s.state_code, s.city,
               c.county_served, s.population_served_count, s.owner_type_code
        FROM {schema}.pwsid_coverage pc
        JOIN {schema}.sdwis_systems s ON s.pwsid = pc.pwsid
        LEFT JOIN {schema}.cws_boundaries c ON c.pwsid = pc.pwsid
        WHERE pc.has_rate_data = FALSE
          AND (pc.search_attempted_at IS NULL
               OR pc.search_attempted_at < NOW() - INTERVAL '{cooldown_days} days')
          AND s.population_served_count >= :pop_min
          AND s.pws_type_code = 'CWS'
          {state_filter}
        ORDER BY s.population_served_count DESC NULLS LAST
        {limit_clause}
    """

    with engine.connect() as conn:
        rows = conn.execute(text(query), {"pop_min": pop_min}).fetchall()

    return [
        {
            "pwsid": r.pwsid,
            "pws_name": r.pws_name,
            "state_code": r.state_code,
            "city": r.city,
            "county": r.county_served,
            "population": r.population_served_count,
            "owner_type": r.owner_type_code,
        }
        for r in rows
    ]


def show_usage():
    """Display current Serper API usage stats."""
    from utility_api.search.serper_client import SerperSearchClient

    try:
        client = SerperSearchClient()
        usage = client.usage
        print(f"\nSerper.dev Usage")
        print("=" * 40)
        print(f"  Total queries:       {usage['queries_total']:>6,}")
        print(f"  Today:               {usage['queries_today']:>6,}")
        print(f"  This week:           {usage['queries_this_week']:>6,}")
        print(f"  Estimated cost:      {usage['estimated_cost']}")
        print(f"  Free tier remaining: {usage['free_tier_remaining']:>6,}")
        print("=" * 40)
    except Exception as e:
        print(f"Error reading usage: {e}")
        print("(Is SERPER_API_KEY set in .env?)")


def run_bulk_discovery(args):
    """Main bulk discovery loop."""
    from utility_api.agents.discovery import DiscoveryAgent

    queries_per = 4  # Serper uses 4 queries per PWSID

    # 1. Show current usage
    try:
        from utility_api.search.serper_client import SerperSearchClient
        client = SerperSearchClient()
        usage = client.usage
        total_used = usage["queries_total"]
        logger.info(
            f"Current Serper usage: {total_used:,} total, "
            f"cost: {usage['estimated_cost']}, "
            f"free remaining: {usage['free_tier_remaining']:,}"
        )
    except Exception as e:
        logger.error(f"Cannot initialize Serper client: {e}")
        return

    # 2. Select PWSIDs
    logger.info(f"Selecting PWSIDs (scope={args.scope}, pop>={args.pop_min:,})")
    pwsids = get_target_pwsids(
        scope=args.scope,
        state=args.state,
        pop_min=args.pop_min,
        max_pwsids=args.max_pwsids,
        cooldown_days=args.cooldown_days,
    )

    if not pwsids:
        logger.info("No PWSIDs match the selection criteria.")
        return

    # 3. Apply query budget cap
    total_queries_needed = len(pwsids) * queries_per
    max_queries = args.max_queries

    if max_queries and total_queries_needed > max_queries:
        capped = max_queries // queries_per
        logger.warning(
            f"Capping from {len(pwsids)} to {capped} PWSIDs to stay "
            f"within {max_queries:,} query budget"
        )
        pwsids = pwsids[:capped]
        total_queries_needed = len(pwsids) * queries_per

    logger.info(
        f"Targeting {len(pwsids)} PWSIDs x {queries_per} queries = "
        f"{total_queries_needed:,} total queries"
    )

    # 4. Dry run — preview only
    if args.dry_run:
        print(f"\n{'='*80}")
        print(f"DRY RUN — No API calls will be made")
        print(f"{'='*80}")
        print(f"\n  PWSIDs: {len(pwsids)}")
        print(f"  Queries: {total_queries_needed:,}")
        print(f"  Estimated cost: ${total_queries_needed / 1000:.2f}")
        print(f"\n  Top 20 targets:")
        print(f"  {'PWSID':12s} {'Name':35s} {'Pop':>10s} {'State':>6s}")
        print(f"  {'─'*12} {'─'*35} {'─'*10} {'─'*6}")
        for p in pwsids[:20]:
            pop = f"{p['population']:>10,}" if p["population"] else "     N/A"
            print(
                f"  {p['pwsid']:12s} {(p['pws_name'] or '?')[:35]:35s} "
                f"{pop} {p['state_code']:>6s}"
            )
        if len(pwsids) > 20:
            print(f"  ... and {len(pwsids) - 20} more")

        # State breakdown
        state_counts: dict[str, int] = {}
        for p in pwsids:
            state_counts[p["state_code"]] = state_counts.get(p["state_code"], 0) + 1
        print(f"\n  State breakdown:")
        for st, cnt in sorted(state_counts.items(), key=lambda x: -x[1]):
            print(f"    {st}: {cnt:,}")

        print(f"\n{'='*80}")
        return

    # 5. Cost confirmation for large sweeps
    if total_queries_needed > 2500:
        cost = total_queries_needed / 1000
        confirm = input(
            f"\nThis will use {total_queries_needed:,} Serper queries "
            f"(est. ${cost:.2f}). Continue? [y/N] "
        )
        if confirm.lower() != "y":
            logger.info("Aborted by user.")
            return

    # 6. Discovery loop
    agent = DiscoveryAgent()
    stats = {
        "searched": 0,
        "urls_found": 0,
        "urls_written": 0,
        "no_results": 0,
        "errors": 0,
    }

    delay_between = args.delay_between

    for pwsid_meta in pwsids:
        pwsid = pwsid_meta["pwsid"]

        try:
            result = agent.run(
                pwsid=pwsid,
                utility_name=pwsid_meta["pws_name"],
                state=pwsid_meta["state_code"],
                search_delay=args.delay,
                skip_domain_guess=True,  # domain guesser is separate pipeline
                diagnostic=args.diagnostic,
            )

            stats["searched"] += 1
            stats["urls_written"] += result["urls_written"]
            if result["urls_written"] > 0:
                stats["urls_found"] += 1
            else:
                stats["no_results"] += 1

        except Exception as e:
            logger.error(f"  {pwsid} error: {e}")
            stats["errors"] += 1
            stats["searched"] += 1

        # Progress logging every 25 PWSIDs
        if stats["searched"] % 25 == 0:
            try:
                current_usage = client.usage
                logger.info(
                    f"  Progress: {stats['searched']}/{len(pwsids)} searched, "
                    f"{stats['urls_found']} with URLs, "
                    f"{current_usage['queries_total']:,} total Serper queries"
                )
            except Exception:
                logger.info(
                    f"  Progress: {stats['searched']}/{len(pwsids)} searched, "
                    f"{stats['urls_found']} with URLs"
                )

        # Delay between PWSIDs (polite pacing)
        if delay_between > 0:
            time.sleep(delay_between)

    # 7. Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"Serper Bulk Discovery Complete")
    logger.info(f"{'='*60}")
    logger.info(f"  PWSIDs searched:  {stats['searched']}")
    if stats["searched"] > 0:
        hit_pct = 100 * stats["urls_found"] / stats["searched"]
        logger.info(f"  With URLs (>50):  {stats['urls_found']} ({hit_pct:.0f}%)")
    logger.info(f"  URLs written:     {stats['urls_written']}")
    logger.info(f"  No results:       {stats['no_results']}")
    logger.info(f"  Errors:           {stats['errors']}")

    try:
        final_usage = client.usage
        logger.info(f"  Serper usage:     {final_usage}")
    except Exception:
        pass

    logger.info(f"\nNext steps:")
    logger.info(f"  ua-ops serper-status              # Check results")
    logger.info(f"  ua-ops process-backlog --max 50   # Parse discovered URLs")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Serper.dev bulk discovery sweep for gap-state PWSIDs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --usage                           # Check Serper query budget
  %(prog)s --max-pwsids 625 --dry-run        # Preview gap-state sweep
  %(prog)s --max-pwsids 25                   # Small validation (100 queries)
  %(prog)s --max-pwsids 625                  # Full free-tier sweep
  %(prog)s --state NY --pop-min 5000         # Specific state
  %(prog)s --scope all_uncovered --pop-min 10000 --max-queries 5000
        """,
    )

    parser.add_argument(
        "--scope",
        choices=["gap_states", "all_uncovered", "specific_state"],
        default="gap_states",
        help="Which PWSIDs to target (default: gap_states)",
    )
    parser.add_argument(
        "--state",
        type=str,
        default=None,
        help="Two-letter state code (required for specific_state scope)",
    )
    parser.add_argument(
        "--pop-min",
        type=int,
        default=3000,
        help="Minimum population served (default: 3000)",
    )
    parser.add_argument(
        "--max-pwsids",
        type=int,
        default=None,
        help="Hard cap on PWSIDs to search",
    )
    parser.add_argument(
        "--max-queries",
        type=int,
        default=2400,
        help="Hard cap on Serper queries (default: 2400, free tier safety margin)",
    )
    parser.add_argument(
        "--cooldown-days",
        type=int,
        default=30,
        help="Skip PWSIDs searched within this many days (default: 30)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="Seconds between Serper queries within a PWSID (default: 0.2)",
    )
    parser.add_argument(
        "--delay-between",
        type=float,
        default=1.0,
        help="Seconds between PWSIDs (default: 1.0)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be searched, no API calls",
    )
    parser.add_argument(
        "--usage",
        action="store_true",
        help="Show current Serper usage stats and exit",
    )
    parser.add_argument(
        "--diagnostic",
        action="store_true",
        help="Log near-miss URLs for threshold tuning",
    )

    args = parser.parse_args()

    # Handle --state implying specific_state scope
    if args.state and args.scope == "gap_states":
        args.scope = "specific_state"

    if args.usage:
        show_usage()
        return

    run_bulk_discovery(args)


if __name__ == "__main__":
    main()
