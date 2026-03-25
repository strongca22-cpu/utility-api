#!/usr/bin/env python3
"""
Orchestrator CLI

Purpose:
    Generates a task queue from database state and optionally executes
    tasks by dispatching to the appropriate agent. This is the entry
    point for autonomous rate data acquisition.

    Sprint 14 adds --batch mode for Anthropic Batch API routing.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-25

Dependencies:
    - typer

Usage:
    ua-run-orchestrator                           # print task queue
    ua-run-orchestrator --execute 10              # execute top 10 tasks
    ua-run-orchestrator --execute 5 --state VA    # VA only
    ua-run-orchestrator --execute 25 --state VA --batch  # batch mode
    ua-run-orchestrator --dry-run                 # same as no flag
"""

import time

import typer
from loguru import logger

app = typer.Typer(help="Generate and execute the orchestrator task queue.")


@app.callback(invoke_without_command=True)
def main(
    execute: int = typer.Option(0, "--execute", "-n", help="Execute top N tasks from the queue"),
    state: str = typer.Option(None, "--state", "-s", help="Limit to a single state code"),
    batch_size: int = typer.Option(50, "--batch-size", help="Max discovery tasks to generate"),
    batch: bool = typer.Option(False, "--batch", help="Use Batch API for parse tasks (cheaper, async)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print queue without executing"),
    search_delay: float = typer.Option(2.0, "--search-delay", help="Seconds between SearXNG queries"),
    scrape_delay: float = typer.Option(1.5, "--scrape-delay", help="Seconds between URL fetches"),
    no_llm: bool = typer.Option(False, "--no-llm", help="Disable LLM scoring in discovery"),
):
    """Generate task queue and optionally execute top N tasks."""
    from utility_api.agents.orchestrator import OrchestratorAgent

    # Generate task queue
    orchestrator = OrchestratorAgent()
    result = orchestrator.run(batch_size=batch_size, state_filter=state)
    tasks = result["tasks"]
    summary = result["summary"]

    typer.echo(f"\n{'=' * 60}")
    typer.echo(f"  Task Queue: {len(tasks)} tasks")
    typer.echo(f"  Bulk checks: {summary['bulk_checks']}  |  "
               f"Discoveries: {summary['new_discoveries']}  |  "
               f"Retries: {summary['retries']}  |  "
               f"Change detect: {summary['change_detections']}")
    if batch:
        typer.echo(f"  Mode: BATCH (parse tasks will be submitted async)")
    typer.echo(f"{'=' * 60}\n")

    # Print top tasks
    display_count = min(len(tasks), 20)
    for i, task in enumerate(tasks[:display_count]):
        typer.echo(
            f"  [{i + 1:3d}] {task.task_type:22s} "
            f"pri={task.priority:<3d} "
            f"{'pwsid=' + task.pwsid if task.pwsid else 'source=' + (task.source_key or '?'):20s} "
            f"{task.notes or ''}"
        )
    if len(tasks) > display_count:
        typer.echo(f"  ... and {len(tasks) - display_count} more")

    if dry_run or execute == 0:
        typer.echo("\n(Use --execute N to run tasks)")
        return

    # Execute tasks
    typer.echo(f"\n{'─' * 60}")
    typer.echo(f"  Executing top {execute} tasks{' (batch mode)' if batch else ''}")
    typer.echo(f"{'─' * 60}\n")

    from utility_api.agents.discovery import DiscoveryAgent
    from utility_api.agents.scrape import ScrapeAgent
    from utility_api.agents.parse import ParseAgent

    discovery = DiscoveryAgent()
    scrape = ScrapeAgent()
    parse = ParseAgent()

    executed = 0
    succeeded = 0
    total_cost = 0.0

    # In batch mode, collect parse tasks instead of running them immediately
    pending_parse_tasks = []

    for task in tasks[:execute]:
        try:
            typer.echo(f"\n── Task {executed + 1}: {task.task_type} ──")

            if task.task_type == "discover_and_scrape":
                typer.echo(f"  PWSID: {task.pwsid} — {task.utility_name}")

                # Step 1: Discover URLs
                disc_result = discovery.run(
                    pwsid=task.pwsid,
                    utility_name=task.utility_name,
                    state=task.state_code,
                    use_llm=not no_llm,
                    search_delay=search_delay,
                )

                if disc_result["urls_written"] == 0:
                    typer.echo(f"  No URLs found — skipping")
                    executed += 1
                    continue

                # Step 2: Scrape discovered URLs
                time.sleep(scrape_delay)
                scrape_result = scrape.run(pwsid=task.pwsid)

                if not scrape_result.get("raw_texts"):
                    typer.echo(f"  Scrape failed — no content")
                    executed += 1
                    continue

                # Step 3: Parse (live or collect for batch)
                if batch:
                    # Take the first scraped text for batch parsing
                    text_entry = scrape_result["raw_texts"][0]
                    pending_parse_tasks.append({
                        "pwsid": task.pwsid,
                        "raw_text": text_entry["text"],
                        "content_type": text_entry["content_type"],
                        "source_url": text_entry["url"],
                        "registry_id": text_entry["registry_id"],
                    })
                    typer.echo(f"  Scraped {text_entry['char_count']:,} chars — queued for batch parse")
                else:
                    for text_entry in scrape_result["raw_texts"]:
                        parse_result = parse.run(
                            pwsid=task.pwsid,
                            raw_text=text_entry["text"],
                            content_type=text_entry["content_type"],
                            source_url=text_entry["url"],
                            registry_id=text_entry["registry_id"],
                        )
                        total_cost += parse_result.get("cost_usd", 0)
                        if parse_result.get("success"):
                            succeeded += 1
                            typer.echo(
                                f"  ✓ bill@10CCF=${parse_result.get('bill_10ccf', 0):.2f} "
                                f"[{parse_result.get('confidence')}] "
                                f"cost=${parse_result.get('cost_usd', 0):.4f}"
                            )
                            break  # One successful parse per PWSID is enough

            elif task.task_type == "retry_scrape":
                typer.echo(f"  Retry: registry_id={task.registry_id} — {task.notes}")
                scrape_result = scrape.run(registry_id=task.registry_id)

                if scrape_result.get("raw_texts"):
                    if batch:
                        text_entry = scrape_result["raw_texts"][0]
                        pending_parse_tasks.append({
                            "pwsid": task.pwsid,
                            "raw_text": text_entry["text"],
                            "content_type": text_entry["content_type"],
                            "source_url": text_entry["url"],
                            "registry_id": text_entry["registry_id"],
                        })
                        typer.echo(f"  Scraped — queued for batch parse")
                    else:
                        for text_entry in scrape_result["raw_texts"]:
                            parse_result = parse.run(
                                pwsid=task.pwsid,
                                raw_text=text_entry["text"],
                                content_type=text_entry["content_type"],
                                source_url=text_entry["url"],
                                registry_id=text_entry["registry_id"],
                            )
                            total_cost += parse_result.get("cost_usd", 0)
                            if parse_result.get("success"):
                                succeeded += 1

            elif task.task_type == "change_detection":
                typer.echo(f"  Change detection: registry_id={task.registry_id}")
                scrape_result = scrape.run(registry_id=task.registry_id)
                for text_entry in scrape_result.get("raw_texts", []):
                    if text_entry.get("content_changed"):
                        typer.echo(f"  Content changed — re-parsing")
                        if batch:
                            pending_parse_tasks.append({
                                "pwsid": task.pwsid,
                                "raw_text": text_entry["text"],
                                "content_type": text_entry["content_type"],
                                "source_url": text_entry["url"],
                                "registry_id": text_entry["registry_id"],
                            })
                        else:
                            parse_result = parse.run(
                                pwsid=task.pwsid,
                                raw_text=text_entry["text"],
                                content_type=text_entry["content_type"],
                                source_url=text_entry["url"],
                                registry_id=text_entry["registry_id"],
                            )
                            total_cost += parse_result.get("cost_usd", 0)
                    else:
                        typer.echo(f"  No change detected")

            elif task.task_type == "check_bulk_source":
                from utility_api.agents.source_checker import SourceChecker
                checker = SourceChecker()
                check_result = checker.run(source_key=task.source_key)
                if check_result.get("new_data_available"):
                    typer.echo(f"  ⚠ NEW DATA: {check_result.get('details', '')}")
                else:
                    typer.echo(f"  No change: {check_result.get('details', '')}")

            executed += 1

        except Exception as e:
            typer.echo(f"  ✗ Task failed: {e}")
            executed += 1
            continue

    # If batch mode: submit all collected parse tasks
    if batch and pending_parse_tasks:
        typer.echo(f"\n{'─' * 60}")
        typer.echo(f"  Submitting {len(pending_parse_tasks)} parse tasks to Batch API")
        typer.echo(f"{'─' * 60}\n")

        from utility_api.agents.batch import BatchAgent
        batch_agent = BatchAgent()
        batch_result = batch_agent.submit(
            parse_tasks=pending_parse_tasks,
            state_filter=state,
        )

        if batch_result.get("batch_id"):
            typer.echo(f"  ✓ Batch submitted: {batch_result['batch_id']}")
            typer.echo(f"    Tasks: {batch_result['task_count']}")
            typer.echo(f"\n  Check results with: ua-ops batch-status {batch_result['batch_id']}")
            typer.echo(f"  Process results with: ua-ops process-batches")
        else:
            typer.echo(f"  ✗ Batch submission failed: {batch_result.get('error', 'unknown')}")

    typer.echo(f"\n{'=' * 60}")
    typer.echo(f"  Executed: {executed}/{execute}")
    if not batch:
        typer.echo(f"  Succeeded: {succeeded}")
        typer.echo(f"  Total API cost: ${total_cost:.4f}")
    else:
        typer.echo(f"  Parse tasks queued: {len(pending_parse_tasks)}")
    typer.echo(f"{'=' * 60}")
    typer.echo(f"\nRun 'ua-ops refresh-coverage' to update coverage stats.")


if __name__ == "__main__":
    app()
