#!/usr/bin/env python3
"""
Metro Scan Orchestrator

Purpose:
    End-to-end metro area rate page discovery + pipeline processing.
    Ties together: metro config → template generator → DiscoveryAgent (Serper)
    → process_pwsid cascade pipeline.

    Sprint 26: Migrated from Claude web_search (metro_research_agent.py) to
    Serper-based DiscoveryAgent. Eliminates Claude API dependency for URL
    discovery, reduces cost ~97% ($0.15/batch → $0.004/utility), and gains
    rank tagging, diagnostics, domain guessing, and service-area handling.

Author: AI-Generated
Created: 2026-03-26
Modified: 2026-03-31 (Sprint 26: Serper migration via DiscoveryAgent)

Dependencies:
    - pyyaml
    - sqlalchemy
    - utility_api (local package)

Usage:
    python scripts/run_metro_scan.py --metro denver              # scan one metro
    python scripts/run_metro_scan.py --metro denver --dry-run     # preview only
    python scripts/run_metro_scan.py --metro denver --large-only  # pop >= 10K only
    python scripts/run_metro_scan.py --top 5                      # top 5 priority
    python scripts/run_metro_scan.py --all                        # all pending metros
    python scripts/run_metro_scan.py --list                       # show metro status

Notes:
    - Always run --dry-run first to preview cost and utility counts
    - Cost confirmation prompt for metros with >50 utilities
    - Uses --force to skip confirmation prompt
    - Logs to /var/log/uapi/metro_scan.log if directory exists
"""

import argparse
import sys
from pathlib import Path

import yaml
from loguru import logger

# Ensure project src is importable
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Add scripts dir to path so sibling script imports work
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from metro_template_generator import generate_metro_context

from utility_api.agents.discovery import DiscoveryAgent

# Legacy imports — preserved for batch-collect of in-flight batches.
# These are no longer used for new discovery runs (Serper replaces web_search).
try:
    from metro_research_agent import (
        check_batch_status as legacy_check_batch_status,
        collect_batch_results as legacy_collect_batch_results,
        list_batches as legacy_list_batches,
    )
    from metro_url_importer import import_research_results as legacy_import_results
    _LEGACY_AVAILABLE = True
except ImportError:
    _LEGACY_AVAILABLE = False

# Add file logging
LOG_PATH = Path("/var/log/uapi/metro_scan.log")
if LOG_PATH.parent.exists():
    logger.add(str(LOG_PATH), rotation="10 MB", retention="30 days")

CONFIG_PATH = PROJECT_ROOT / "config" / "metro_targets.yaml"


def load_all_metros() -> list[dict]:
    """Load all metro configs from YAML."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return config.get("metros", [])


def load_metro_config(metro_id: str) -> dict | None:
    """Load a single metro config by ID."""
    metros = load_all_metros()
    return next((m for m in metros if m["id"] == metro_id), None)


def update_metro_status(metro_id: str, new_status: str) -> None:
    """Update a metro's status in the YAML config file.

    Reads the file, finds the metro entry, updates status, writes back.
    """
    with open(CONFIG_PATH) as f:
        raw = f.read()

    # Simple text replacement for the status field of this metro
    # Find the line "    status: pending" after "  - id: {metro_id}"
    lines = raw.split("\n")
    found_metro = False
    for i, line in enumerate(lines):
        if line.strip() == f"- id: {metro_id}":
            found_metro = True
        elif found_metro and line.strip().startswith("status:"):
            indent = line[: len(line) - len(line.lstrip())]
            lines[i] = f"{indent}status: {new_status}"
            break
        elif found_metro and line.strip().startswith("- id:"):
            # Moved past our metro without finding status
            break

    with open(CONFIG_PATH, "w") as f:
        f.write("\n".join(lines))


def discover_metro_urls(
    utilities: list[dict],
    metro_name: str = "",
    dry_run: bool = False,
) -> dict:
    """Run DiscoveryAgent for each utility in a metro context.

    Replaces the old research_metro() → import_research_results() two-step.
    DiscoveryAgent writes directly to scrape_registry with rank tagging,
    so no separate import step is needed.

    Parameters
    ----------
    utilities : list[dict]
        Utility dicts from generate_metro_context(), each with
        pwsid, pws_name, population, state, etc.
    metro_name : str
        Metro name for logging.
    dry_run : bool
        Preview only, no API calls.

    Returns
    -------
    dict
        Summary with urls_found, urls_written, errors, per-utility details.
    """
    agent = DiscoveryAgent()
    total_written = 0
    total_found = 0
    errors = 0
    details = []

    for i, u in enumerate(utilities, 1):
        pwsid = u["pwsid"]

        if dry_run:
            details.append({"pwsid": pwsid, "dry_run": True})
            continue

        logger.info(
            f"  [{i}/{len(utilities)}] {pwsid} — "
            f"{u.get('pws_name', '?')[:35]} (pop {u.get('population', 0):,})"
        )
        try:
            result = agent.run(pwsid=pwsid, search_delay=0.3)
            total_written += result.get("urls_written", 0)
            total_found += result.get("urls_found", 0)
            details.append(result)
        except Exception as e:
            logger.error(f"    → Discovery failed for {pwsid}: {e}")
            errors += 1
            details.append({"pwsid": pwsid, "error": str(e)})

    logger.info(
        f"  Discovery complete: {total_found} candidates found, "
        f"{total_written} URLs written, {errors} errors"
    )

    return {
        "urls_found": total_found,
        "urls_written": total_written,
        "errors": errors,
        "details": details,
    }


def list_metros() -> None:
    """Print metro status table."""
    metros = load_all_metros()

    print(f"\n{'ID':<18} {'Name':<42} {'Priority':>8} {'Coverage':>9} {'Status':<12}")
    print("-" * 95)
    for m in metros:
        coverage = f"{m.get('current_coverage_pct', 0):.1f}%"
        print(
            f"{m['id']:<18} {m['name']:<42} "
            f"{m.get('priority', '?'):>8} {coverage:>9} {m.get('status', '?'):<12}"
        )
    print()


def run_metro(
    metro_id: str,
    dry_run: bool = False,
    large_only: bool = False,
    force: bool = False,
    process: bool = True,
    budget_remaining: float | None = None,
) -> dict | None:
    """Run the full metro scan pipeline for a single metro.

    Args:
        metro_id: Metro identifier from config.
        dry_run: Preview without API calls or DB writes.
        large_only: Skip utilities with population < 10,000.
        force: Skip cost confirmation prompt.
        process: Run through scrape/parse pipeline after import.
        budget_remaining: If set, cap utilities so estimated cost stays within budget.

    Returns:
        Summary dict, or None if metro not found.
    """
    config = load_metro_config(metro_id)
    if not config:
        logger.error(f"Metro '{metro_id}' not found in {CONFIG_PATH}")
        return None

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Metro Scan: {config['name']} ({', '.join(config['states'])})")
    logger.info(f"{'=' * 60}")

    # Step 1: Generate context from SDWIS
    context = generate_metro_context(config)
    stats = context["stats"]

    logger.info(f"  CWS in area:      {stats['total_cws_in_area']}")
    logger.info(f"  Already covered:   {stats['already_covered']}")
    logger.info(f"  Pending URLs:      {stats['has_pending_url']}")
    logger.info(
        f"  Need research:     {stats['needs_url']} "
        f"(L:{stats['large']} M:{stats['medium']} S:{stats['small']})"
    )

    if stats["needs_url"] == 0:
        logger.info("  All utilities covered or have pending URLs. Done.")
        update_metro_status(metro_id, "complete")
        return {"metro_id": metro_id, "status": "already_complete"}

    # Optional: filter to large/medium only
    utilities = context["utilities"]
    if large_only:
        utilities = [u for u in utilities if u["tier"] in ("large", "medium")]
        logger.info(f"  --large-only: {len(utilities)} utilities remaining (skipped small)")

    if not utilities:
        logger.info("  No utilities to research after filtering. Done.")
        return {"metro_id": metro_id, "status": "nothing_to_research"}

    # Budget enforcement: truncate utility list to fit remaining budget
    if budget_remaining is not None:
        max_batches = max(1, int(budget_remaining / 0.75))
        max_utilities = max_batches * 10
        if len(utilities) > max_utilities:
            logger.info(
                f"  Budget cap: ${budget_remaining:.2f} remaining → "
                f"truncating from {len(utilities)} to {max_utilities} utilities"
            )
            utilities = utilities[:max_utilities]
        elif budget_remaining < 0.75:
            logger.info(f"  Budget exhausted (${budget_remaining:.2f} remaining). Skipping.")
            return {"metro_id": metro_id, "status": "budget_exhausted", "estimated_cost": 0}

    # Cost estimate — Serper-based (4 queries per PWSID, ~$0.001/query)
    estimated_queries = len(utilities) * 4
    estimated_cost = estimated_queries * 0.001
    logger.info(
        f"  Estimated discovery cost: ${estimated_cost:.2f} "
        f"({estimated_queries} Serper queries, {len(utilities)} utilities)"
    )

    if dry_run:
        logger.info(f"\n  DRY RUN — showing what would be researched:\n")
        for u in utilities[:30]:
            logger.info(
                f"    {u['pwsid']} | {u['pws_name'][:35]:35s} | "
                f"pop {u['population']:>10,} | {u['tier']}"
            )
        if len(utilities) > 30:
            logger.info(f"    ... +{len(utilities) - 30} more")
        return {
            "metro_id": metro_id,
            "status": "dry_run",
            "utilities_count": len(utilities),
            "estimated_cost": estimated_cost,
        }

    # Cost confirmation for large metros
    if len(utilities) > 50 and not force:
        response = input(
            f"  This will make {batches_needed} API calls "
            f"(est. ${estimated_cost:.2f}). Continue? [y/N] "
        )
        if response.lower() != "y":
            logger.info("  Cancelled by user.")
            return {"metro_id": metro_id, "status": "cancelled"}

    # Step 2: Discover URLs via Serper (DiscoveryAgent writes directly to scrape_registry)
    update_metro_status(metro_id, "processing")
    logger.info(f"  Discovering URLs for {len(utilities)} utilities via Serper...")

    discovery = discover_metro_urls(utilities, metro_name=config["name"])

    # Step 3: Optionally process through cascade pipeline
    parse_successes = 0
    parse_failures = 0
    if process and discovery["urls_written"] > 0:
        logger.info(
            f"  Processing {discovery['urls_written']} URLs through cascade pipeline..."
        )
        from utility_api.pipeline.process import process_pwsid

        for u in utilities:
            try:
                result = process_pwsid(u["pwsid"], skip_best_estimate=True)
                if result.get("parse_success"):
                    parse_successes += 1
                else:
                    parse_failures += 1
            except Exception as e:
                logger.warning(f"  Process failed for {u['pwsid']}: {e}")
                parse_failures += 1

        # Rebuild best estimate per state
        from utility_api.ops.best_estimate import run_best_estimate

        if parse_successes > 0:
            for state in config["states"]:
                try:
                    run_best_estimate(state_filter=state)
                except Exception as e:
                    logger.warning(f"  Best estimate rebuild failed for {state}: {e}")

    # Step 4: Update status and report
    update_metro_status(metro_id, "complete")

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Metro Scan Complete: {config['name']}")
    logger.info(f"{'=' * 60}")
    logger.info(f"  Utilities discovered:   {len(utilities)}")
    logger.info(f"  URLs written:           {discovery['urls_written']}")
    logger.info(f"  Discovery errors:       {discovery['errors']}")
    if process and discovery["urls_written"] > 0:
        logger.info(f"  Parse successes:        {parse_successes}")
        logger.info(f"  Parse failures:         {parse_failures}")
    logger.info(f"{'=' * 60}")

    return {
        "metro_id": metro_id,
        "status": "complete",
        "utilities_count": len(utilities),
        "urls_written": discovery["urls_written"],
        "parse_successes": parse_successes,
        "parse_failures": parse_failures,
        "estimated_cost": estimated_cost,
    }


def batch_submit(
    metro_ids: list[str],
    large_only: bool = False,
) -> str | None:
    """LEGACY: Submit metros to Claude Batch API for web_search discovery.

    Preserved for collecting in-flight batches submitted before Serper migration.
    New discovery runs should use the immediate Serper path (--metro or --top).

    Args:
        metro_ids: List of metro IDs to research.
        large_only: Skip utilities with population < 10,000.

    Returns:
        Batch ID string, or None if nothing to submit.
    """
    if not _LEGACY_AVAILABLE:
        logger.error(
            "Legacy batch imports not available. "
            "Use --metro for Serper-based discovery instead."
        )
        return None

    from metro_research_agent import build_batch_requests, submit_batch_request

    metro_contexts = []

    for metro_id in metro_ids:
        config = load_metro_config(metro_id)
        if not config:
            logger.warning(f"Metro '{metro_id}' not found — skipping")
            continue

        context = generate_metro_context(config)
        stats = context["stats"]

        logger.info(
            f"  {metro_id}: {stats['total_cws_in_area']} CWS, "
            f"{stats['already_covered']} covered, "
            f"{stats['needs_url']} need research "
            f"(L:{stats['large']} M:{stats['medium']} S:{stats['small']})"
        )

        if stats["needs_url"] == 0:
            logger.info(f"  {metro_id}: all covered — skipping")
            update_metro_status(metro_id, "complete")
            continue

        utilities = context["utilities"]
        if large_only:
            utilities = [u for u in utilities if u["tier"] in ("large", "medium")]
            if not utilities:
                logger.info(f"  {metro_id}: no large/medium utilities — skipping")
                continue

        metro_contexts.append({
            "metro_id": metro_id,
            "metro_name": config["name"],
            "utilities": utilities,
        })

    if not metro_contexts:
        logger.info("No utilities to research across selected metros.")
        return None

    # Build batch requests
    requests = build_batch_requests(metro_contexts)
    total_utilities = sum(len(ctx["utilities"]) for ctx in metro_contexts)

    logger.info(f"\nBatch summary:")
    logger.info(f"  Metros: {len(metro_contexts)}")
    logger.info(f"  Utilities: {total_utilities}")
    logger.info(f"  API requests: {len(requests)}")
    logger.info(f"  Est. cost: ${len(requests) * 0.38:.2f} (batch rate, 50% off)")

    # Submit
    metro_id_list = [ctx["metro_id"] for ctx in metro_contexts]
    batch_id = submit_batch_request(requests, metro_id_list)

    # Mark metros as processing
    for ctx in metro_contexts:
        update_metro_status(ctx["metro_id"], "batch_submitted")

    return batch_id


def batch_collect(batch_id: str) -> None:
    """LEGACY: Collect results from a completed Claude Batch API submission.

    Preserved for collecting in-flight batches submitted before Serper migration.

    Args:
        batch_id: The batch ID from batch_submit().
    """
    if not _LEGACY_AVAILABLE:
        logger.error(
            "Legacy batch imports not available. "
            "This command is only for collecting batches submitted "
            "before the Serper migration."
        )
        return

    # Check status
    status = legacy_check_batch_status(batch_id)
    if status["processing_status"] != "ended":
        logger.info(
            f"Batch not ready yet. Status: {status['processing_status']}. "
            f"Check again later with: "
            f"python scripts/run_metro_scan.py --batch-status {batch_id}"
        )
        return

    # Collect and parse results
    results_by_metro = legacy_collect_batch_results(batch_id)

    if not results_by_metro:
        logger.warning("No results to import.")
        return

    # Import each metro's results
    for metro_id, results in results_by_metro.items():
        found = sum(1 for r in results if r.get("url"))
        logger.info(f"\n  {metro_id}: {found}/{len(results)} URLs found")

        if results:
            import_stats = legacy_import_results(results, metro_id)
            logger.info(
                f"  {metro_id}: {import_stats['imported']} imported, "
                f"{import_stats['skipped_no_url']} no URL, "
                f"{import_stats['skipped_exists']} already exist"
            )

        update_metro_status(metro_id, "complete")

    logger.info(f"\nBatch collection complete for {len(results_by_metro)} metros.")


def main():
    parser = argparse.ArgumentParser(
        description="Metro area water rate page discovery pipeline"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--metro", help="Scan a single metro by ID")
    group.add_argument(
        "--top", type=int, metavar="N",
        help="Scan top N priority pending metros",
    )
    group.add_argument(
        "--all", action="store_true",
        help="Scan all pending metros",
    )
    group.add_argument(
        "--list", action="store_true",
        help="Show metro status table",
    )
    group.add_argument(
        "--batch-submit", nargs="*", metavar="METRO",
        help="Submit metros to Batch API (50%% cheaper). Use metro IDs or 'all'/'top:N'",
    )
    group.add_argument(
        "--batch-status", metavar="BATCH_ID",
        help="Check status of a submitted batch",
    )
    group.add_argument(
        "--batch-collect", metavar="BATCH_ID",
        help="Collect results from a completed batch and import to DB",
    )
    group.add_argument(
        "--batch-list", action="store_true",
        help="List all submitted batches",
    )

    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview without API calls or DB writes",
    )
    parser.add_argument(
        "--large-only", action="store_true",
        help="Skip utilities with population < 10,000",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Skip cost confirmation prompt",
    )
    parser.add_argument(
        "--no-process", action="store_true",
        help="Skip pipeline processing after import",
    )
    parser.add_argument(
        "--max-cost", type=float, default=None, metavar="USD",
        help="Maximum total API spend across all metros (e.g., 5.00)",
    )

    args = parser.parse_args()

    if args.list:
        list_metros()
        return

    if args.batch_list:
        if not _LEGACY_AVAILABLE:
            logger.error("Legacy batch functions not available.")
            return
        legacy_list_batches()
        return

    if args.batch_status:
        if not _LEGACY_AVAILABLE:
            logger.error("Legacy batch functions not available.")
            return
        legacy_check_batch_status(args.batch_status)
        return

    if args.batch_collect:
        batch_collect(args.batch_collect)
        return

    if args.batch_submit is not None:
        # Parse metro IDs: explicit list, "all", or "top:N"
        metro_args = args.batch_submit
        if not metro_args or "all" in metro_args:
            metros = load_all_metros()
            metro_ids = [
                m["id"] for m in metros if m.get("status") == "pending"
            ]
        elif any(a.startswith("top:") for a in metro_args):
            n = int(next(a for a in metro_args if a.startswith("top:")).split(":")[1])
            metros = load_all_metros()
            pending = [m for m in metros if m.get("status") == "pending"]
            pending.sort(key=lambda m: m.get("priority", 999))
            metro_ids = [m["id"] for m in pending[:n]]
        else:
            metro_ids = metro_args

        if not metro_ids:
            logger.info("No pending metros to submit.")
            return

        batch_submit(metro_ids, large_only=args.large_only)
        return

    process = not args.no_process

    budget = args.max_cost

    if args.metro:
        run_metro(
            args.metro,
            dry_run=args.dry_run,
            large_only=args.large_only,
            force=args.force,
            process=process,
            budget_remaining=budget,
        )

    elif args.top or args.all:
        metros = load_all_metros()
        pending = [m for m in metros if m.get("status") == "pending"]
        pending.sort(key=lambda m: m.get("priority", 999))

        if args.top:
            pending = pending[: args.top]

        if not pending:
            logger.info("No pending metros to process.")
            return

        logger.info(
            f"Running {len(pending)} metros: "
            f"{', '.join(m['id'] for m in pending)}"
        )
        if budget is not None:
            logger.info(f"Budget cap: ${budget:.2f}")

        total_spent = 0.0
        for metro in pending:
            remaining = (budget - total_spent) if budget is not None else None
            if remaining is not None and remaining < 0.75:
                logger.info(
                    f"Budget exhausted (${remaining:.2f} remaining). "
                    f"Stopping before {metro['id']}."
                )
                break

            result = run_metro(
                metro["id"],
                dry_run=args.dry_run,
                large_only=args.large_only,
                force=args.force,
                process=process,
                budget_remaining=remaining,
            )

            if result and result.get("estimated_cost"):
                total_spent += result["estimated_cost"]

        if budget is not None:
            logger.info(f"\nTotal estimated spend: ${total_spent:.2f} / ${budget:.2f} budget")


if __name__ == "__main__":
    main()
