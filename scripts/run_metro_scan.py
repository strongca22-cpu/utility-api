#!/usr/bin/env python3
"""
Metro Scan Orchestrator

Purpose:
    End-to-end metro area rate page discovery + pipeline processing.
    Ties together: metro config → template generator → research agent →
    URL importer → existing scrape/parse pipeline.

Author: AI-Generated
Created: 2026-03-26
Modified: 2026-03-26

Dependencies:
    - pyyaml
    - sqlalchemy
    - anthropic
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
import math
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
from metro_research_agent import research_metro
from metro_url_importer import import_research_results

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
) -> dict | None:
    """Run the full metro scan pipeline for a single metro.

    Args:
        metro_id: Metro identifier from config.
        dry_run: Preview without API calls or DB writes.
        large_only: Skip utilities with population < 10,000.
        force: Skip cost confirmation prompt.
        process: Run through scrape/parse pipeline after import.

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

    # Cost estimate
    batches_needed = math.ceil(len(utilities) / 10)
    estimated_cost = batches_needed * 0.18
    logger.info(
        f"  Estimated research cost: ${estimated_cost:.2f} "
        f"({batches_needed} API calls × ~$0.18)"
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

    # Step 2: Research URLs via Claude API
    update_metro_status(metro_id, "processing")
    logger.info(f"  Researching {len(utilities)} utilities...")

    research_context = {
        "metro_name": config["name"],
        "utilities": utilities,
    }
    results = research_metro(research_context)

    found = [r for r in results if r.get("url")]
    not_found = [r for r in results if not r.get("url")]
    logger.info(f"  Found {len(found)}/{len(results)} URLs")

    # Step 3: Import to scrape_registry
    import_stats = import_research_results(results, metro_id)

    # Step 4: Optionally process through pipeline
    if process and import_stats["imported"] > 0:
        logger.info(
            f"  Processing {import_stats['imported']} URLs through pipeline..."
        )
        try:
            from process_guesser_batch import process_batch

            for state in config["states"]:
                process_batch(
                    max_count=import_stats["imported"],
                    state=state,
                    url_source="metro_research",
                )
        except Exception as e:
            logger.warning(
                f"  Pipeline processing failed: {e}. "
                f"URLs are in scrape_registry — process manually with: "
                f"python scripts/process_guesser_batch.py --url-source metro_research"
            )

    # Step 5: Update status and report
    update_metro_status(metro_id, "complete")

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Metro Scan Complete: {config['name']}")
    logger.info(f"{'=' * 60}")
    logger.info(f"  Utilities researched: {len(results)}")
    logger.info(f"  URLs found:           {len(found)}")
    logger.info(f"  Imported to registry:  {import_stats['imported']}")
    logger.info(f"  Skipped (no URL):      {import_stats['skipped_no_url']}")
    logger.info(f"  Skipped (exists):      {import_stats['skipped_exists']}")
    logger.info(f"  Skipped (bad PWSID):   {import_stats['skipped_bad_pwsid']}")
    logger.info(f"{'=' * 60}")

    return {
        "metro_id": metro_id,
        "status": "complete",
        "researched": len(results),
        "found": len(found),
        "imported": import_stats["imported"],
    }


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

    args = parser.parse_args()

    if args.list:
        list_metros()
        return

    process = not args.no_process

    if args.metro:
        run_metro(
            args.metro,
            dry_run=args.dry_run,
            large_only=args.large_only,
            force=args.force,
            process=process,
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

        for metro in pending:
            run_metro(
                metro["id"],
                dry_run=args.dry_run,
                large_only=args.large_only,
                force=args.force,
                process=process,
            )


if __name__ == "__main__":
    main()
