#!/usr/bin/env python3
"""
Targeted PWSID Research Pipeline

Purpose:
    Lightweight orchestrator that takes a list of PWSIDs and runs
    DiscoveryAgent → process_pwsid → rebuild best_estimate end-to-end.

    Designed for ad-hoc priority processing of specific PWSIDs (e.g.,
    top-25 uncovered by population) without the full metro scan overhead.

    Uses the existing Serper-based DiscoveryAgent for URL discovery
    (not Claude's web_search tool) and the cascade process pipeline
    for scrape/parse. All pipeline improvements (45k text cap, section
    extraction, rate_structure_type normalization, bill computation fix)
    apply automatically.

Author: AI-Generated
Created: 2026-03-31
Modified: 2026-03-31

Dependencies:
    - pyyaml
    - sqlalchemy
    - loguru
    - utility_api (local package)

Usage:
    # Run a named batch from config
    python scripts/run_targeted_research.py --batch top25_duke_sourced

    # Run specific PWSIDs from CLI
    python scripts/run_targeted_research.py --pwsids TX1010013 TX2200012

    # Discovery only (find URLs, don't parse)
    python scripts/run_targeted_research.py --batch top25_duke_sourced --discovery-only

    # Process only (parse existing URLs, skip discovery)
    python scripts/run_targeted_research.py --batch top25_duke_sourced --process-only

    # Dry run (preview, no API calls)
    python scripts/run_targeted_research.py --batch top25_duke_sourced --dry-run

    # Force re-discovery even if URLs exist
    python scripts/run_targeted_research.py --batch top25_duke_sourced --force

Notes:
    - DiscoveryAgent writes directly to scrape_registry — no intermediate YAML
    - process_pwsid() handles cascade parse + reactive deep crawl
    - Best estimate rebuild is batched per-state at the end (not per-PWSID)
    - Does NOT skip PWSIDs with existing rate data (e.g., Duke) — always
      attempts discovery+parse to find better scraped_llm replacement
"""

import argparse
import sys
import time
from pathlib import Path

import yaml
from loguru import logger
from sqlalchemy import text

# Ensure project src is importable
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utility_api.agents.discovery import DiscoveryAgent
from utility_api.config import settings
from utility_api.db import engine
from utility_api.ops.best_estimate import run_best_estimate
from utility_api.pipeline.process import process_pwsid


# --- Constants ---

CONFIG_PATH = PROJECT_ROOT / "config" / "targeted_research.yaml"
SERPER_QUERIES_PER_PWSID = 4
SERPER_COST_PER_QUERY = 0.001  # Approximate, paid mode


def load_batch(batch_name: str) -> list[str]:
    """Load a named PWSID batch from config/targeted_research.yaml.

    Parameters
    ----------
    batch_name : str
        Key under 'batches' in the YAML config.

    Returns
    -------
    list[str]
        List of PWSID strings.

    Raises
    ------
    SystemExit
        If batch not found.
    """
    if not CONFIG_PATH.exists():
        logger.error(f"Config not found: {CONFIG_PATH}")
        sys.exit(1)

    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    batches = config.get("batches", {})
    if batch_name not in batches:
        available = ", ".join(batches.keys())
        logger.error(f"Batch '{batch_name}' not found. Available: {available}")
        sys.exit(1)

    batch = batches[batch_name]
    pwsids = batch.get("pwsids", [])
    logger.info(f"Batch '{batch_name}': {batch.get('description', '')}")
    logger.info(f"  {len(pwsids)} PWSIDs loaded")
    return pwsids


def has_existing_urls(pwsid: str) -> bool:
    """Check if a PWSID already has URLs in scrape_registry.

    Parameters
    ----------
    pwsid : str
        EPA PWSID.

    Returns
    -------
    bool
        True if at least one URL exists in scrape_registry for this PWSID.
    """
    schema = settings.utility_schema
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM {schema}.scrape_registry
            WHERE pwsid = :pwsid
        """), {"pwsid": pwsid})
        count = result.scalar()
    return count > 0


def get_pwsid_metadata(pwsids: list[str]) -> dict[str, dict]:
    """Fetch population, utility name, and state for a list of PWSIDs.

    Parameters
    ----------
    pwsids : list[str]
        List of EPA PWSIDs.

    Returns
    -------
    dict[str, dict]
        Keyed by PWSID with fields: pws_name, state_code, population.
    """
    schema = settings.utility_schema
    meta = {}
    with engine.connect() as conn:
        for pwsid in pwsids:
            row = conn.execute(text(f"""
                SELECT s.pws_name, s.state_code, c.population_served
                FROM {schema}.sdwis_systems s
                LEFT JOIN {schema}.cws_boundaries c ON s.pwsid = c.pwsid
                WHERE s.pwsid = :pwsid
            """), {"pwsid": pwsid}).first()

            if row:
                meta[pwsid] = {
                    "pws_name": row.pws_name or pwsid,
                    "state_code": row.state_code or pwsid[:2],
                    "population": row.population_served or 0,
                }
            else:
                meta[pwsid] = {
                    "pws_name": pwsid,
                    "state_code": pwsid[:2],
                    "population": 0,
                }
    return meta


def run_discovery(
    pwsids: list[str],
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, dict]:
    """Run DiscoveryAgent for each PWSID.

    Parameters
    ----------
    pwsids : list[str]
        Target PWSIDs.
    force : bool
        Re-discover even if URLs already exist.
    dry_run : bool
        Preview only, no API calls.

    Returns
    -------
    dict[str, dict]
        Keyed by PWSID with discovery results from DiscoveryAgent.run().
    """
    agent = DiscoveryAgent()
    results = {}
    skipped = 0

    for i, pwsid in enumerate(pwsids, 1):
        # Check for existing URLs
        if not force and has_existing_urls(pwsid):
            logger.info(
                f"  [{i}/{len(pwsids)}] {pwsid} — skipping discovery "
                f"(URLs exist, use --force to re-discover)"
            )
            skipped += 1
            results[pwsid] = {"pwsid": pwsid, "urls_written": 0, "skipped": True}
            continue

        if dry_run:
            logger.info(f"  [{i}/{len(pwsids)}] {pwsid} — would run discovery")
            results[pwsid] = {"pwsid": pwsid, "urls_written": 0, "dry_run": True}
            continue

        logger.info(f"  [{i}/{len(pwsids)}] {pwsid} — running discovery...")
        try:
            result = agent.run(pwsid=pwsid, search_delay=0.3)
            results[pwsid] = result
            logger.info(
                f"    → {result['urls_written']} URLs written "
                f"({result['urls_found']} candidates found)"
            )
        except Exception as e:
            logger.error(f"    → Discovery failed: {e}")
            results[pwsid] = {"pwsid": pwsid, "urls_written": 0, "error": str(e)}

    if skipped:
        logger.info(f"  Skipped {skipped}/{len(pwsids)} (URLs already exist)")

    return results


def run_processing(
    pwsids: list[str],
    dry_run: bool = False,
) -> list[dict]:
    """Run process_pwsid() for each PWSID.

    Parameters
    ----------
    pwsids : list[str]
        Target PWSIDs.
    dry_run : bool
        Preview only, no API calls.

    Returns
    -------
    list[dict]
        Per-PWSID results with parse_success, winning_url, etc.
    """
    results = []

    for i, pwsid in enumerate(pwsids, 1):
        if dry_run:
            logger.info(f"  [{i}/{len(pwsids)}] {pwsid} — would run process_pwsid")
            results.append({"pwsid": pwsid, "parse_success": False, "dry_run": True})
            continue

        logger.info(f"  [{i}/{len(pwsids)}] {pwsid} — processing...")
        try:
            result = process_pwsid(pwsid, skip_best_estimate=True)
            result["pwsid"] = pwsid
            results.append(result)

            if result["parse_success"]:
                logger.info(
                    f"    → ✓ Parsed: {result.get('winning_url', '?')[:70]}"
                )
            else:
                logger.info(
                    f"    → ✗ No parse success after "
                    f"{result.get('parse_attempts', '?')} attempts"
                )
        except Exception as e:
            logger.error(f"    → Processing failed: {e}")
            results.append({
                "pwsid": pwsid,
                "parse_success": False,
                "error": str(e),
            })

    return results


def rebuild_best_estimates(states: set[str], dry_run: bool = False) -> None:
    """Rebuild rate_best_estimate for affected states.

    Parameters
    ----------
    states : set[str]
        State codes to rebuild.
    dry_run : bool
        Preview only.
    """
    if not states:
        logger.info("  No states to rebuild (no parse successes).")
        return

    for state in sorted(states):
        if dry_run:
            logger.info(f"  Would rebuild best_estimate for {state}")
            continue
        logger.info(f"  Rebuilding best_estimate for {state}...")
        try:
            run_best_estimate(state_filter=state)
            logger.info(f"    → {state} done")
        except Exception as e:
            logger.error(f"    → {state} rebuild failed: {e}")


def print_summary(
    pwsids: list[str],
    metadata: dict[str, dict],
    discovery_results: dict[str, dict],
    process_results: list[dict],
    batch_name: str | None = None,
) -> None:
    """Print a formatted summary report.

    Parameters
    ----------
    pwsids : list[str]
        All target PWSIDs.
    metadata : dict[str, dict]
        PWSID metadata (name, state, pop).
    discovery_results : dict[str, dict]
        Per-PWSID discovery results.
    process_results : list[dict]
        Per-PWSID processing results.
    batch_name : str, optional
        Batch name if loaded from config.
    """
    # Build lookup for process results
    process_by_pwsid = {r["pwsid"]: r for r in process_results}

    # Count stats
    disc_searched = sum(
        1 for r in discovery_results.values()
        if not r.get("skipped") and not r.get("dry_run")
    )
    disc_found = sum(
        1 for r in discovery_results.values()
        if r.get("urls_written", 0) > 0
    )

    parse_attempted = sum(
        1 for r in process_results if not r.get("dry_run")
    )
    parse_success = sum(
        1 for r in process_results if r.get("parse_success")
    )

    # State breakdown
    success_states = {}
    for r in process_results:
        if r.get("parse_success"):
            st = metadata.get(r["pwsid"], {}).get("state_code", "??")
            success_states[st] = success_states.get(st, 0) + 1

    pop_gained = sum(
        metadata.get(r["pwsid"], {}).get("population", 0)
        for r in process_results
        if r.get("parse_success")
    )

    # Print report
    label = f"Batch: {batch_name}" if batch_name else "Ad-hoc"
    print(f"\n{'=' * 65}")
    print(f"  Targeted Research Results — {label} ({len(pwsids)} PWSIDs)")
    print(f"{'=' * 65}")
    print(f"  Discovery: {disc_searched} searched, {disc_found} URLs found")
    if parse_attempted:
        print(
            f"  Parse:     {parse_success}/{parse_attempted} succeeded"
        )
    if success_states:
        state_str = ", ".join(
            f"{st}({n})" for st, n in sorted(success_states.items())
        )
        print(f"  States:    {state_str}")
    print()

    # Per-PWSID detail
    if process_results and not process_results[0].get("dry_run"):
        successes = [r for r in process_results if r.get("parse_success")]
        failures = [r for r in process_results if not r.get("parse_success") and not r.get("dry_run")]

        if successes:
            print("  Successes:")
            for r in successes:
                m = metadata.get(r["pwsid"], {})
                url = r.get("winning_url", "?")[:50]
                print(
                    f"    {r['pwsid']}  {m.get('pws_name', '?')[:25]:25s}  "
                    f"{m.get('population', 0):>10,}  ✓  {url}"
                )
            print()

        if failures:
            print("  Failures:")
            for r in failures:
                m = metadata.get(r["pwsid"], {})
                reason = r.get("error", f"{r.get('parse_attempts', '?')} attempts, no success")
                print(
                    f"    {r['pwsid']}  {m.get('pws_name', '?')[:25]:25s}  "
                    f"{m.get('population', 0):>10,}  ✗  {reason}"
                )
            print()

    if pop_gained:
        print(f"  Population covered: {pop_gained:,} (new scraped_llm)")

    print(f"{'=' * 65}\n")


def main():
    """Main entry point for targeted research pipeline."""
    parser = argparse.ArgumentParser(
        description=(
            "Run targeted URL discovery + parse for specific PWSIDs. "
            "Uses Serper-based DiscoveryAgent and the cascade process pipeline."
        )
    )

    # Target selection (mutually exclusive)
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument(
        "--batch",
        metavar="NAME",
        help="Load PWSIDs from config/targeted_research.yaml batch",
    )
    target_group.add_argument(
        "--pwsids",
        nargs="+",
        metavar="PWSID",
        help="Specify PWSIDs directly on command line",
    )

    # Mode flags
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview targets and cost estimate, no API calls",
    )
    parser.add_argument(
        "--discovery-only",
        action="store_true",
        help="Run URL discovery only, skip parse processing",
    )
    parser.add_argument(
        "--process-only",
        action="store_true",
        help="Skip discovery, process existing URLs in scrape_registry",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-discover URLs even if they already exist in scrape_registry",
    )
    parser.add_argument(
        "--skip-best-estimate",
        action="store_true",
        help="Skip rate_best_estimate rebuild after processing",
    )

    args = parser.parse_args()

    # Load targets
    batch_name = None
    if args.batch:
        batch_name = args.batch
        pwsids = load_batch(args.batch)
    else:
        pwsids = args.pwsids

    if not pwsids:
        logger.error("No PWSIDs to process.")
        sys.exit(1)

    # Fetch metadata for all targets
    logger.info(f"\nFetching metadata for {len(pwsids)} PWSIDs...")
    metadata = get_pwsid_metadata(pwsids)

    # Dry-run preview
    if args.dry_run:
        est_queries = len(pwsids) * SERPER_QUERIES_PER_PWSID
        est_cost = est_queries * SERPER_COST_PER_QUERY

        print(f"\n{'=' * 65}")
        print(f"  DRY RUN — Targeted Research Preview")
        print(f"{'=' * 65}")
        print(f"  Batch: {batch_name or 'ad-hoc'}")
        print(f"  PWSIDs: {len(pwsids)}")
        print(f"  Est. Serper queries: {est_queries}")
        print(f"  Est. Serper cost: ${est_cost:.2f}")
        print(f"  Est. parse cost: $1-2 (direct API, per-PWSID)")
        print()

        for pwsid in pwsids:
            m = metadata.get(pwsid, {})
            has_urls = has_existing_urls(pwsid)
            url_status = "URLs exist" if has_urls else "no URLs"
            print(
                f"    {pwsid}  {m.get('pws_name', '?')[:30]:30s}  "
                f"{m.get('state_code', '??'):>2s}  "
                f"{m.get('population', 0):>10,}  "
                f"[{url_status}]"
            )

        print(f"\n{'=' * 65}\n")
        return

    # Phase 1: Discovery
    discovery_results = {}
    if not args.process_only:
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Phase 1: URL Discovery ({len(pwsids)} PWSIDs)")
        logger.info(f"{'=' * 60}")
        discovery_results = run_discovery(
            pwsids, force=args.force, dry_run=False
        )

    # Phase 2: Process
    process_results = []
    if not args.discovery_only:
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Phase 2: Cascade Parse ({len(pwsids)} PWSIDs)")
        logger.info(f"{'=' * 60}")
        process_results = run_processing(pwsids, dry_run=False)

    # Phase 3: Rebuild best estimates
    if not args.discovery_only and not args.skip_best_estimate:
        affected_states = set()
        for r in process_results:
            if r.get("parse_success"):
                st = metadata.get(r["pwsid"], {}).get("state_code")
                if st:
                    affected_states.add(st)

        if affected_states:
            logger.info(f"\n{'=' * 60}")
            logger.info(f"Phase 3: Rebuild Best Estimate ({', '.join(sorted(affected_states))})")
            logger.info(f"{'=' * 60}")
            rebuild_best_estimates(affected_states)

    # Summary
    print_summary(
        pwsids=pwsids,
        metadata=metadata,
        discovery_results=discovery_results,
        process_results=process_results,
        batch_name=batch_name,
    )


if __name__ == "__main__":
    main()
