#!/usr/bin/env python3
"""
Utility Operations CLI

Purpose:
    Command-and-control CLI for the data operations layer.
    Provides status reporting, coverage analysis, materialized view
    refresh, and best-estimate building.

    This is the "what do we have, what do we need, what should we do next"
    interface. Every acquisition session should start here.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - typer
    - sqlalchemy
    - loguru

Usage:
    ua-ops status                         # State-of-the-world summary
    ua-ops source-catalog                 # List all known data sources
    ua-ops coverage-report                # Coverage by state, source, freshness
    ua-ops refresh-coverage               # Refresh the pwsid_coverage mat view
    ua-ops build-best-estimate            # Build best estimates (all states)
    ua-ops build-best-estimate --state CA # CA only
    ua-ops build-best-estimate --dry-run  # Preview
"""

import typer
from loguru import logger
from sqlalchemy import text

app = typer.Typer(help="Data operations: status, coverage, best-estimate.")


@app.command()
def status():
    """Quick state-of-the-world summary.

    Shows source catalog, table row counts, rate coverage stats,
    and scrape registry summary. This is the first command to run
    at the start of any acquisition session.
    """
    from utility_api.config import settings
    from utility_api.db import engine

    schema = settings.utility_schema

    with engine.connect() as conn:
        typer.echo("=" * 70)
        typer.echo("  UAPI — State of the World")
        typer.echo("=" * 70)

        # --- Table Row Counts ---
        typer.echo("\n── Table Sizes ──")
        tables = [
            "cws_boundaries", "sdwis_systems", "mdwd_financials",
            "water_rates", "rate_schedules", "rate_best_estimate",
            "permits", "source_catalog", "scrape_registry",
            "pwsid_coverage", "ingest_log", "pipeline_runs",
        ]
        for table in tables:
            try:
                count = conn.execute(
                    text(f"SELECT COUNT(*) FROM {schema}.{table}")
                ).scalar()
                typer.echo(f"  {table:30s} {count:>8,}")
            except Exception:
                typer.echo(f"  {table:30s}      N/A")

        # --- Source Catalog ---
        typer.echo("\n── Source Catalog ──")
        try:
            rows = conn.execute(text(f"""
                SELECT source_key, display_name, source_type,
                       states_covered, pwsid_count, last_ingested_at
                FROM {schema}.source_catalog
                ORDER BY source_type, source_key
            """)).fetchall()
            for r in rows:
                states = ",".join(r.states_covered) if r.states_covered else "?"
                count = r.pwsid_count or 0
                last = r.last_ingested_at.strftime("%Y-%m-%d") if r.last_ingested_at else "never"
                typer.echo(
                    f"  {r.source_key:25s} [{r.source_type:16s}] "
                    f"states={states:8s} pwsids={count:>5d}  last={last}"
                )
        except Exception as e:
            typer.echo(f"  (source_catalog not available: {e})")

        # --- Rate Coverage Summary ---
        typer.echo("\n── Rate Coverage by State ──")
        try:
            rows = conn.execute(text(f"""
                SELECT
                    c.state_code,
                    COUNT(*) AS total_cws,
                    COUNT(CASE WHEN EXISTS (
                        SELECT 1 FROM {schema}.water_rates wr WHERE wr.pwsid = c.pwsid
                    ) THEN 1 END) AS with_rates,
                    ROUND(
                        100.0 * COUNT(CASE WHEN EXISTS (
                            SELECT 1 FROM {schema}.water_rates wr WHERE wr.pwsid = c.pwsid
                        ) THEN 1 END) / NULLIF(COUNT(*), 0), 1
                    ) AS pct
                FROM {schema}.cws_boundaries c
                GROUP BY c.state_code
                HAVING COUNT(CASE WHEN EXISTS (
                    SELECT 1 FROM {schema}.water_rates wr WHERE wr.pwsid = c.pwsid
                ) THEN 1 END) > 0
                ORDER BY pct DESC
            """)).fetchall()
            typer.echo(f"  {'State':8s} {'CWS':>8s} {'With Rates':>12s} {'Coverage':>10s}")
            typer.echo(f"  {'─'*8} {'─'*8} {'─'*12} {'─'*10}")
            for r in rows:
                typer.echo(f"  {r.state_code:8s} {r.total_cws:>8,} {r.with_rates:>12,} {r.pct:>9.1f}%")
        except Exception as e:
            typer.echo(f"  (coverage query failed: {e})")

        # --- Scrape Registry Summary ---
        typer.echo("\n── Scrape Registry ──")
        try:
            rows = conn.execute(text(f"""
                SELECT status, COUNT(*) AS cnt
                FROM {schema}.scrape_registry
                GROUP BY status
                ORDER BY cnt DESC
            """)).fetchall()
            if rows:
                for r in rows:
                    typer.echo(f"  {r.status:20s} {r.cnt:>6,}")
            else:
                typer.echo("  (empty — run migrate_urls_to_registry.py to seed)")
        except Exception:
            typer.echo("  (scrape_registry not available)")

        # --- Recent Pipeline Runs ---
        typer.echo("\n── Recent Pipeline Runs (last 10) ──")
        try:
            rows = conn.execute(text(f"""
                SELECT step_name, started_at, row_count, status
                FROM {schema}.pipeline_runs
                ORDER BY started_at DESC
                LIMIT 10
            """)).fetchall()
            for r in rows:
                ts = r.started_at.strftime("%Y-%m-%d %H:%M") if r.started_at else "?"
                typer.echo(f"  {ts}  {r.step_name:25s} rows={r.row_count or 0:>6,}  [{r.status}]")
        except Exception:
            typer.echo("  (pipeline_runs not available)")

        typer.echo("\n" + "=" * 70)


@app.command("source-catalog")
def source_catalog():
    """List all known data sources with full detail."""
    from utility_api.config import settings
    from utility_api.db import engine

    schema = settings.utility_schema

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT * FROM {schema}.source_catalog
            ORDER BY source_type, source_key
        """)).fetchall()

        if not rows:
            typer.echo("Source catalog is empty. Run: python scripts/seed_source_catalog.py")
            return

        for r in rows:
            typer.echo(f"\n{'─' * 50}")
            typer.echo(f"  Key:       {r.source_key}")
            typer.echo(f"  Name:      {r.display_name}")
            typer.echo(f"  Type:      {r.source_type}")
            states = ",".join(r.states_covered) if r.states_covered else "?"
            typer.echo(f"  States:    {states}")
            typer.echo(f"  PWSIDs:    {r.pwsid_count or '(not counted)'}")
            if r.vintage_start or r.vintage_end:
                typer.echo(f"  Vintage:   {r.vintage_start} → {r.vintage_end}")
            typer.echo(f"  Cadence:   {r.refresh_cadence or 'unknown'}")
            last = r.last_ingested_at.strftime("%Y-%m-%d %H:%M") if r.last_ingested_at else "never"
            typer.echo(f"  Last run:  {last}")
            if r.next_check_date:
                typer.echo(f"  Next chk:  {r.next_check_date}")
            if r.ingest_command:
                typer.echo(f"  Command:   {r.ingest_command}")
            if r.notes:
                typer.echo(f"  Notes:     {r.notes[:120]}")


@app.command("coverage-report")
def coverage_report():
    """Detailed coverage analysis: by state, by source, by freshness.

    Pulls from the pwsid_coverage materialized view (run refresh-coverage
    if data looks stale).
    """
    from utility_api.config import settings
    from utility_api.db import engine

    schema = settings.utility_schema

    with engine.connect() as conn:
        typer.echo("=" * 70)
        typer.echo("  UAPI — Coverage Report")
        typer.echo("=" * 70)

        # --- Coverage by State (from mat view) ---
        typer.echo("\n── Coverage by State ──")
        try:
            rows = conn.execute(text(f"""
                SELECT
                    state_code,
                    COUNT(*) AS total,
                    SUM(CASE WHEN has_rate_data THEN 1 ELSE 0 END) AS with_rates,
                    SUM(CASE WHEN has_sdwis THEN 1 ELSE 0 END) AS with_sdwis,
                    ROUND(100.0 * SUM(CASE WHEN has_rate_data THEN 1 ELSE 0 END)
                          / NULLIF(COUNT(*), 0), 1) AS rate_pct,
                    ROUND(100.0 * SUM(CASE WHEN has_sdwis THEN 1 ELSE 0 END)
                          / NULLIF(COUNT(*), 0), 1) AS sdwis_pct
                FROM {schema}.pwsid_coverage
                GROUP BY state_code
                ORDER BY total DESC
            """)).fetchall()

            typer.echo(f"  {'State':6s} {'Total':>7s} {'Rates':>7s} {'Rate%':>7s} {'SDWIS':>7s} {'SDWIS%':>7s}")
            typer.echo(f"  {'─'*6} {'─'*7} {'─'*7} {'─'*7} {'─'*7} {'─'*7}")

            total_cws = 0
            total_rates = 0
            total_sdwis = 0
            for r in rows:
                total_cws += r.total
                total_rates += r.with_rates
                total_sdwis += r.with_sdwis
                # Only show states with data or large CWS counts
                if r.with_rates > 0 or r.total >= 500:
                    typer.echo(
                        f"  {r.state_code:6s} {r.total:>7,} {r.with_rates:>7,} "
                        f"{r.rate_pct:>6.1f}% {r.with_sdwis:>7,} {r.sdwis_pct:>6.1f}%"
                    )

            typer.echo(f"  {'─'*6} {'─'*7} {'─'*7} {'─'*7} {'─'*7} {'─'*7}")
            rate_pct = 100.0 * total_rates / total_cws if total_cws > 0 else 0
            sdwis_pct = 100.0 * total_sdwis / total_cws if total_cws > 0 else 0
            typer.echo(
                f"  {'TOTAL':6s} {total_cws:>7,} {total_rates:>7,} "
                f"{rate_pct:>6.1f}% {total_sdwis:>7,} {sdwis_pct:>6.1f}%"
            )
        except Exception as e:
            typer.echo(f"  (pwsid_coverage not available — run migration 009: {e})")
            return

        # --- Coverage by Source ---
        typer.echo("\n── Rate Records by Source ──")
        rows = conn.execute(text(f"""
            SELECT source, state_code,
                   COUNT(*) AS records,
                   COUNT(DISTINCT pwsid) AS pwsids,
                   MIN(rate_effective_date) AS earliest,
                   MAX(rate_effective_date) AS latest
            FROM {schema}.water_rates
            GROUP BY source, state_code
            ORDER BY source, state_code
        """)).fetchall()

        typer.echo(f"  {'Source':25s} {'State':6s} {'Records':>8s} {'PWSIDs':>8s} {'Earliest':>12s} {'Latest':>12s}")
        typer.echo(f"  {'─'*25} {'─'*6} {'─'*8} {'─'*8} {'─'*12} {'─'*12}")
        for r in rows:
            earliest = str(r.earliest) if r.earliest else "?"
            latest = str(r.latest) if r.latest else "?"
            typer.echo(
                f"  {r.source:25s} {r.state_code or '?':6s} {r.records:>8,} "
                f"{r.pwsids:>8,} {earliest:>12s} {latest:>12s}"
            )

        # --- Freshness ---
        typer.echo("\n── Rate Data Freshness ──")
        rows = conn.execute(text(f"""
            SELECT
                CASE
                    WHEN rate_effective_date >= CURRENT_DATE - INTERVAL '1 year' THEN '<1 year'
                    WHEN rate_effective_date >= CURRENT_DATE - INTERVAL '2 years' THEN '1-2 years'
                    WHEN rate_effective_date >= CURRENT_DATE - INTERVAL '5 years' THEN '2-5 years'
                    WHEN rate_effective_date IS NOT NULL THEN '>5 years'
                    ELSE 'no date'
                END AS age_bucket,
                COUNT(*) AS records,
                COUNT(DISTINCT pwsid) AS pwsids
            FROM {schema}.water_rates
            GROUP BY age_bucket
            ORDER BY age_bucket
        """)).fetchall()
        for r in rows:
            typer.echo(f"  {r.age_bucket:12s}  {r.records:>6,} records  {r.pwsids:>6,} PWSIDs")

        # --- Top Gaps ---
        typer.echo("\n── Largest Uncovered States (top 10) ──")
        rows = conn.execute(text(f"""
            SELECT
                state_code,
                COUNT(*) AS total,
                SUM(CASE WHEN has_rate_data THEN 1 ELSE 0 END) AS with_rates,
                COUNT(*) - SUM(CASE WHEN has_rate_data THEN 1 ELSE 0 END) AS gap
            FROM {schema}.pwsid_coverage
            GROUP BY state_code
            ORDER BY gap DESC
            LIMIT 10
        """)).fetchall()
        typer.echo(f"  {'State':6s} {'Total':>7s} {'Gap':>7s} {'With Rates':>12s}")
        typer.echo(f"  {'─'*6} {'─'*7} {'─'*7} {'─'*12}")
        for r in rows:
            typer.echo(f"  {r.state_code:6s} {r.total:>7,} {r.gap:>7,} {r.with_rates:>12,}")

        typer.echo("\n" + "=" * 70)


@app.command("refresh-coverage")
def refresh_coverage():
    """Refresh derived columns in pwsid_coverage table.

    Recomputes rate coverage, best-estimate, and SDWIS columns from source
    tables. Also updates scrape_status from scrape_registry. Does NOT
    overwrite priority_tier (manually set).

    Run this after any ingest, best-estimate build, or SDWIS expansion.
    """
    from utility_api.ops.coverage import refresh_coverage_derived, update_scrape_status

    typer.echo("Refreshing pwsid_coverage derived columns...")
    stats = refresh_coverage_derived()
    typer.echo(f"  {stats['total']:,} PWSIDs, {stats['with_rates']:,} rates, {stats['with_sdwis']:,} SDWIS")

    typer.echo("Updating scrape_status from scrape_registry...")
    scrape_stats = update_scrape_status()
    for status, cnt in sorted(scrape_stats.items(), key=lambda x: -x[1]):
        typer.echo(f"  {status:20s} {cnt:>7,}")

    typer.echo("Done.")


@app.command("build-best-estimate")
def build_best_estimate(
    state: str = typer.Option(None, "--state", "-s", help="Limit to a single state code"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview without DB writes"),
    csv: bool = typer.Option(False, "--csv", help="Also write CSV output"),
):
    """Build source-prioritized best-estimate rates for all PWSIDs.

    Reads config/source_priority.yaml for source ranking. Handles all states.
    For CA: multi-source reconciliation with eAR anchor.
    For NC/VA: single-source, simpler selection.

    After running, use 'ua-ops refresh-coverage' to update the mat view.
    """
    from utility_api.ops.best_estimate import run_best_estimate

    stats = run_best_estimate(
        state_filter=state,
        dry_run=dry_run,
        write_csv=csv,
    )

    if not dry_run and stats.get("inserted", 0) > 0:
        typer.echo("\nTip: Run 'ua-ops refresh-coverage' to update the coverage mat view.")


@app.command("scrape-status")
def scrape_status(
    state: str = typer.Option(None, "--state", "-s", help="Filter by state code"),
):
    """Show scrape registry status breakdown.

    Summarizes URL statuses (pending, active, failed, dead, stale) and
    parse outcomes from scrape_registry. Use --state to filter.
    """
    from utility_api.config import settings
    from utility_api.db import engine

    schema = settings.utility_schema

    with engine.connect() as conn:
        state_filter = ""
        params = {}
        if state:
            state_filter = "WHERE sr.pwsid LIKE :prefix"
            params["prefix"] = f"{state.upper()}%"

        typer.echo("=" * 60)
        typer.echo(f"  Scrape Registry Status{' — ' + state.upper() if state else ''}")
        typer.echo("=" * 60)

        # Status breakdown
        typer.echo("\n── URL Status ──")
        rows = conn.execute(text(f"""
            SELECT sr.status, COUNT(*) AS cnt
            FROM {schema}.scrape_registry sr
            {state_filter}
            GROUP BY sr.status
            ORDER BY cnt DESC
        """), params).fetchall()
        total = sum(r.cnt for r in rows)
        for r in rows:
            typer.echo(f"  {r.status:20s} {r.cnt:>6,}")
        typer.echo(f"  {'─'*20} {'─'*6}")
        typer.echo(f"  {'TOTAL':20s} {total:>6,}")

        # Parse outcome breakdown
        typer.echo("\n── Parse Outcomes ──")
        rows = conn.execute(text(f"""
            SELECT
                COALESCE(sr.last_parse_result, 'not_parsed') AS result,
                COALESCE(sr.last_parse_confidence, '-') AS confidence,
                COUNT(*) AS cnt
            FROM {schema}.scrape_registry sr
            {state_filter}
            GROUP BY sr.last_parse_result, sr.last_parse_confidence
            ORDER BY cnt DESC
        """), params).fetchall()
        for r in rows:
            typer.echo(f"  {r.result:15s} [{r.confidence:6s}] {r.cnt:>6,}")

        # HTTP status breakdown
        typer.echo("\n── HTTP Status Codes ──")
        rows = conn.execute(text(f"""
            SELECT
                COALESCE(sr.last_http_status::text, 'no fetch') AS http_status,
                COUNT(*) AS cnt
            FROM {schema}.scrape_registry sr
            {state_filter}
            GROUP BY sr.last_http_status
            ORDER BY cnt DESC
            LIMIT 10
        """), params).fetchall()
        for r in rows:
            typer.echo(f"  {r.http_status:12s} {r.cnt:>6,}")

        # Top failing URLs
        typer.echo("\n── Recent Failures (last 5) ──")
        fail_where = "WHERE sr.status IN ('failed', 'dead', 'blocked')"
        if state:
            fail_where += " AND sr.pwsid LIKE :prefix"
        rows = conn.execute(text(f"""
            SELECT sr.pwsid, sr.url, sr.status, sr.notes
            FROM {schema}.scrape_registry sr
            {fail_where}
            ORDER BY sr.updated_at DESC NULLS LAST
            LIMIT 5
        """), params).fetchall()
        if rows:
            for r in rows:
                typer.echo(f"  {r.pwsid} [{r.status}] {(r.notes or '')[:60]}")
                typer.echo(f"    {r.url[:80]}")
        else:
            typer.echo("  (no failures)")

        typer.echo("\n" + "=" * 60)


@app.command("sync-rate-schedules")
def sync_rate_schedules(
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview without DB writes"),
):
    """Sync water_rates → rate_schedules.

    Finds water_rates records not yet in rate_schedules and converts
    them to the canonical JSONB schema. Run this after any ingest
    to keep rate_schedules current.

    This replaces inline dual-write in each ingest module — simpler
    and less invasive. Sprint 12 agents will write directly to
    rate_schedules, making this sync unnecessary for new data.
    """
    from utility_api.config import settings
    from utility_api.db import engine
    from utility_api.ops.rate_schedule_helpers import (
        water_rate_to_schedule,
        write_rate_schedule,
    )

    schema = settings.utility_schema

    with engine.connect() as conn:
        # Find water_rates records not yet in rate_schedules
        rows = conn.execute(text(f"""
            SELECT
                wr.pwsid, wr.source, wr.utility_name, wr.state_code,
                wr.rate_effective_date, wr.rate_structure_type, wr.rate_class,
                wr.billing_frequency, wr.fixed_charge_monthly, wr.meter_size_inches,
                wr.tier_1_limit_ccf, wr.tier_1_rate,
                wr.tier_2_limit_ccf, wr.tier_2_rate,
                wr.tier_3_limit_ccf, wr.tier_3_rate,
                wr.tier_4_limit_ccf, wr.tier_4_rate,
                wr.bill_5ccf, wr.bill_10ccf,
                wr.bill_6ccf, wr.bill_9ccf, wr.bill_12ccf, wr.bill_24ccf,
                wr.source_url, wr.raw_text_hash, wr.parse_confidence,
                wr.parse_model, wr.parse_notes, wr.scraped_at, wr.parsed_at
            FROM {schema}.water_rates wr
            WHERE NOT EXISTS (
                SELECT 1 FROM {schema}.rate_schedules rs
                WHERE rs.pwsid = wr.pwsid
                  AND rs.source_key = wr.source
                  AND rs.vintage_date IS NOT DISTINCT FROM wr.rate_effective_date
                  AND rs.customer_class = COALESCE(wr.rate_class, 'residential')
            )
        """)).mappings().all()

    if not rows:
        typer.echo("rate_schedules is in sync — no new records to convert.")
        return

    typer.echo(f"Found {len(rows)} water_rates records not yet in rate_schedules.")

    if dry_run:
        for r in list(rows)[:5]:
            typer.echo(f"  {r['pwsid']} [{r['source']}] date={r['rate_effective_date']}")
        if len(rows) > 5:
            typer.echo(f"  ... and {len(rows) - 5} more")
        return

    inserted = 0
    skipped = 0
    with engine.connect() as conn:
        for r in rows:
            schedule = water_rate_to_schedule(dict(r))
            try:
                if write_rate_schedule(conn, schedule):
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                typer.echo(f"  Error on {r['pwsid']}: {e}")
                skipped += 1
                conn.rollback()
        conn.commit()

    typer.echo(f"Synced {inserted} records to rate_schedules ({skipped} skipped).")


@app.command("check-sources")
def check_sources():
    """Check bulk data sources for new data availability.

    Finds all sources in source_catalog with next_check_date <= today
    and a configured source_url. Fetches the URL, checks for content
    changes or new data vintages, and updates the catalog.

    New data findings are logged to source_catalog.notes and ingest_log.
    No auto-ingest — human decides whether to run.
    """
    from utility_api.agents.source_checker import SourceChecker

    checker = SourceChecker()
    result = checker.run_all_due()

    typer.echo(f"\nSources checked: {result['sources_checked']}")

    if result["new_data_found"]:
        typer.echo(f"\n⚠ New data available for:")
        for key in result["new_data_found"]:
            typer.echo(f"  - {key}")
        typer.echo("\nReview source_catalog notes and decide whether to re-ingest.")
    else:
        typer.echo("No new data detected.")

    if result["no_change"]:
        typer.echo(f"\nUnchanged: {', '.join(result['no_change'])}")


@app.command("pipeline-health")
def pipeline_health():
    """Pipeline health report: last runs, registry status, recent activity, errors.

    Summarizes operational state of the acquisition pipeline. Use this
    to check that cron jobs are running, batches are processing, and
    the scrape/parse pipeline is healthy.
    """
    from datetime import datetime, timezone

    from utility_api.config import settings
    from utility_api.db import engine

    schema = settings.utility_schema
    now = datetime.now(timezone.utc)

    with engine.connect() as conn:
        typer.echo(f"\nPipeline Health Report — {now.strftime('%Y-%m-%d %H:%M UTC')}")
        typer.echo("=" * 65)

        # --- Last Agent Runs ---
        typer.echo("\n── Last Agent Runs ──")
        rows = conn.execute(text(f"""
            SELECT DISTINCT ON (agent_name)
                agent_name, completed_at, status, notes
            FROM {schema}.ingest_log
            ORDER BY agent_name, completed_at DESC
        """)).fetchall()
        if rows:
            for r in rows:
                if r.completed_at:
                    age = now - r.completed_at
                    age_str = f"{age.days}d {age.seconds // 3600}h ago"
                    status_icon = "✓" if r.status == "success" else "✗" if r.status == "failed" else "~"
                else:
                    age_str = "never"
                    status_icon = "?"
                typer.echo(
                    f"  {r.agent_name:20s} {status_icon} {r.completed_at.strftime('%Y-%m-%d %H:%M') if r.completed_at else 'never':20s} ({age_str})"
                )
        else:
            typer.echo("  No agent runs recorded yet.")

        # --- Batch Jobs ---
        typer.echo("\n── Batch Jobs ──")
        rows = conn.execute(text(f"""
            SELECT status, COUNT(*) AS cnt, MAX(submitted_at) AS latest
            FROM {schema}.batch_jobs
            GROUP BY status
            ORDER BY status
        """)).fetchall()
        if rows:
            for r in rows:
                latest = r.latest.strftime("%Y-%m-%d %H:%M") if r.latest else "?"
                typer.echo(f"  {r.status:15s} {r.cnt:>3} jobs  (latest: {latest})")
        else:
            typer.echo("  No batch jobs yet.")

        # --- Scrape Registry ---
        typer.echo("\n── Scrape Registry ──")
        rows = conn.execute(text(f"""
            SELECT status, COUNT(*) AS cnt
            FROM {schema}.scrape_registry
            GROUP BY status
            ORDER BY cnt DESC
        """)).fetchall()
        total = 0
        for r in rows:
            total += r.cnt
            typer.echo(f"  {r.status:20s} {r.cnt:>6,}")
        typer.echo(f"  {'TOTAL':20s} {total:>6,}")

        # --- Last 7 Days Activity ---
        typer.echo("\n── Last 7 Days ──")
        activity = conn.execute(text(f"""
            SELECT
                (SELECT COUNT(*) FROM {schema}.scrape_registry
                 WHERE created_at >= NOW() - INTERVAL '7 days') AS urls_discovered,
                (SELECT COUNT(*) FROM {schema}.scrape_registry
                 WHERE last_fetch_at >= NOW() - INTERVAL '7 days') AS urls_fetched,
                (SELECT COUNT(*) FROM {schema}.scrape_registry
                 WHERE last_parse_at >= NOW() - INTERVAL '7 days'
                   AND last_parse_result = 'success') AS parses_succeeded,
                (SELECT COUNT(*) FROM {schema}.scrape_registry
                 WHERE last_parse_at >= NOW() - INTERVAL '7 days'
                   AND last_parse_result = 'failed') AS parses_failed,
                (SELECT COALESCE(SUM(last_parse_cost_usd), 0) FROM {schema}.scrape_registry
                 WHERE last_parse_at >= NOW() - INTERVAL '7 days') AS total_cost
        """)).fetchone()
        typer.echo(f"  URLs discovered:    {activity.urls_discovered:>6}")
        typer.echo(f"  URLs fetched:       {activity.urls_fetched:>6}")
        typer.echo(f"  Parses succeeded:   {activity.parses_succeeded:>6}")
        typer.echo(f"  Parses failed:      {activity.parses_failed:>6}")
        typer.echo(f"  Total API cost:     ${float(activity.total_cost):>.4f}")

        # --- Recent Errors ---
        typer.echo("\n── Recent Errors (last 7 days) ──")
        rows = conn.execute(text(f"""
            SELECT completed_at, agent_name, notes
            FROM {schema}.ingest_log
            WHERE status = 'failed'
              AND completed_at >= NOW() - INTERVAL '7 days'
            ORDER BY completed_at DESC
            LIMIT 10
        """)).fetchall()
        if rows:
            for r in rows:
                ts = r.completed_at.strftime("%Y-%m-%d %H:%M") if r.completed_at else "?"
                notes = (r.notes or "")[:80]
                typer.echo(f"  {ts}  {r.agent_name:15s}  {notes}")
        else:
            typer.echo("  No errors in the last 7 days.")

        # --- Source Catalog Freshness ---
        typer.echo("\n── Source Catalog Check Schedule ──")
        rows = conn.execute(text(f"""
            SELECT source_key, next_check_date, last_content_hash IS NOT NULL AS has_hash
            FROM {schema}.source_catalog
            WHERE next_check_date IS NOT NULL
            ORDER BY next_check_date ASC
        """)).fetchall()
        for r in rows:
            overdue = "⚠ OVERDUE" if r.next_check_date and r.next_check_date <= now.date() else ""
            typer.echo(f"  {r.source_key:25s} next={r.next_check_date}  {overdue}")

        typer.echo("\n" + "=" * 65)


@app.command("batch-status")
def batch_status(
    batch_id: str = typer.Argument(None, help="Specific batch ID to check (default: all pending)"),
):
    """Check status of Batch API jobs.

    Queries Anthropic for current processing status of pending batches
    and updates the local batch_jobs table. Shows all batches if no
    ID is given.
    """
    from utility_api.agents.batch import BatchAgent
    from utility_api.config import settings
    from utility_api.db import engine

    schema = settings.utility_schema

    if batch_id:
        batch_agent = BatchAgent()
        results = batch_agent.check_status(batch_id=batch_id)
    else:
        # Show all batches from DB
        with engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT batch_id, submitted_at, task_count, status,
                       completed_at, processed_at, results_summary, state_filter
                FROM {schema}.batch_jobs
                ORDER BY submitted_at DESC
                LIMIT 20
            """)).fetchall()

        if not rows:
            typer.echo("No batch jobs found.")
            return

        typer.echo(f"\n{'=' * 70}")
        typer.echo(f"  Batch Jobs")
        typer.echo(f"{'=' * 70}\n")

        for r in rows:
            submitted = r.submitted_at.strftime("%Y-%m-%d %H:%M") if r.submitted_at else "?"
            typer.echo(f"  {r.batch_id}")
            typer.echo(f"    Status:    {r.status:12s}  Tasks: {r.task_count}")
            typer.echo(f"    Submitted: {submitted}  State: {r.state_filter or 'all'}")
            if r.completed_at:
                typer.echo(f"    Completed: {r.completed_at.strftime('%Y-%m-%d %H:%M')}")
            if r.processed_at:
                typer.echo(f"    Processed: {r.processed_at.strftime('%Y-%m-%d %H:%M')}")
            if r.results_summary:
                s = r.results_summary
                typer.echo(f"    Results:   {s.get('succeeded', 0)} succeeded, {s.get('failed', 0)} failed, ${s.get('total_cost', 0):.4f}")
            typer.echo()

        # Check pending batches against Anthropic
        pending = [r for r in rows if r.status in ('pending', 'in_progress')]
        if pending:
            typer.echo("Checking pending batches against Anthropic API...")
            batch_agent = BatchAgent()
            results = batch_agent.check_status()
            for r in results:
                if "error" in r:
                    typer.echo(f"  {r['batch_id']}: error — {r['error']}")
                else:
                    typer.echo(f"  {r['batch_id']}: {r['api_status']} ({r.get('succeeded', 0)} succeeded, {r.get('errored', 0)} errored)")

        return

    # Single batch status display
    if not results:
        typer.echo(f"Batch {batch_id} not found.")
        return

    for r in results:
        if "error" in r:
            typer.echo(f"  {r['batch_id']}: error — {r['error']}")
        else:
            typer.echo(f"  Batch:   {r['batch_id']}")
            typer.echo(f"  Status:  {r['api_status']} (local: {r['local_status']})")
            typer.echo(f"  Tasks:   {r['task_count']}")
            typer.echo(f"  OK: {r.get('succeeded', 0)}  Errors: {r.get('errored', 0)}")


@app.command("process-batches")
def process_batches():
    """Process all completed Batch API jobs.

    Checks for completed batches, downloads results from Anthropic,
    validates parse output, writes to rate_schedules, and updates
    scrape_registry and best estimates.
    """
    from utility_api.agents.batch import BatchAgent

    batch_agent = BatchAgent()
    result = batch_agent.process_all_pending()

    typer.echo(f"\nBatches checked:   {result['batches_checked']}")
    typer.echo(f"Batches processed: {result['batches_processed']}")
    typer.echo(f"Total succeeded:   {result['total_succeeded']}")
    typer.echo(f"Total failed:      {result['total_failed']}")

    if result['batches_processed'] > 0:
        typer.echo("\nRun 'ua-ops refresh-coverage' to update coverage stats.")


@app.command("create-api-key")
def create_api_key(
    name: str = typer.Option(..., "--name", "-n", help="Human-readable key name"),
    tier: str = typer.Option("free", "--tier", "-t", help="Rate limit tier: free, basic, premium"),
):
    """Create a new API key for the Utility Intelligence API.

    Generates a random API key, stores its SHA-256 hash in the database,
    and prints the plaintext key ONCE. The plaintext is never stored.

    Tiers: free (100 req/day), basic (1000 req/day), premium (10000 req/day).
    """
    import hashlib
    import secrets

    from utility_api.config import settings
    from utility_api.db import engine

    if tier not in ("free", "basic", "premium"):
        typer.echo(f"Invalid tier '{tier}'. Must be: free, basic, or premium.")
        raise typer.Exit(1)

    # Generate key: ua-key-<32 random hex chars>
    raw_key = f"ua-key-{secrets.token_hex(16)}"
    key_hash = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    schema = settings.utility_schema
    from sqlalchemy import text
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO {schema}.api_keys (key_hash, name, tier)
            VALUES (:key_hash, :name, :tier)
        """), {"key_hash": key_hash, "name": name, "tier": tier})
        conn.commit()

    typer.echo(f"\nAPI Key Created")
    typer.echo("=" * 50)
    typer.echo(f"  Name:  {name}")
    typer.echo(f"  Tier:  {tier}")
    typer.echo(f"  Key:   {raw_key}")
    typer.echo(f"\n  Store this key securely — it cannot be recovered.")
    typer.echo(f"  Usage: curl -H 'X-API-Key: {raw_key}' http://localhost:8000/resolve?lat=38.85&lng=-77.35")


@app.command("iou-map")
def iou_map(
    state: str = typer.Option(None, "--state", "-s", help="Limit to a single state code"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview matches without writing"),
    no_yaml: bool = typer.Option(False, "--no-yaml", help="Skip YAML file generation"),
):
    """Map investor-owned utility PWSIDs to known corporate rate page URLs.

    Scans sdwis_systems for name patterns matching major IOUs (American Water,
    Aqua/Essential, CalWater, SJW, Middlesex, Artesian, Aquarion, CSWR, Nexus).
    Writes matched URLs to scrape_registry and per-state YAML config files.

    Expects ~1,000-1,500 matches nationally. Zero search queries needed.
    """
    from utility_api.ops.iou_mapper import run_iou_mapping

    result = run_iou_mapping(
        state_filter=state,
        dry_run=dry_run,
        write_yaml=not no_yaml,
    )

    typer.echo(f"\nIOU Mapping Results")
    typer.echo("=" * 50)
    typer.echo(f"  Total matched:        {result['total_matched']:>6,}")
    typer.echo(f"  Registry writes:      {result['urls_written_registry']:>6,}")
    typer.echo(f"  YAML entries written:  {result['urls_written_yaml']:>6,}")

    typer.echo(f"\n  By parent company:")
    for parent, count in sorted(result["by_parent"].items(), key=lambda x: -x[1]):
        typer.echo(f"    {parent:40s} {count:>5,}")

    typer.echo(f"\n  By state:")
    for st, count in sorted(result["by_state"].items()):
        typer.echo(f"    {st:6s} {count:>5,}")

    if dry_run:
        typer.echo("\n  (dry run — no writes performed)")
        # Show first 10 matches
        for m in result.get("matches", [])[:10]:
            typer.echo(f"    {m['pwsid']} {m['pws_name'][:40]:40s} → {m['parent']}")
        remaining = result["total_matched"] - 10
        if remaining > 0:
            typer.echo(f"    ... and {remaining:,} more")
    else:
        typer.echo(f"\nRun 'ua-ops refresh-coverage' to update coverage stats.")
        typer.echo(f"Run 'ua-run-orchestrator --execute 10' to start scraping IOU URLs.")


@app.command("ingest-ccr-links")
def ingest_ccr_links(
    csv_file: str = typer.Argument(..., help="Path to CSV file with pwsid,ccr_url columns"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview without writing"),
):
    """Ingest CCR (Consumer Confidence Report) links and derive candidate rate URLs.

    Accepts a CSV with columns: pwsid, ccr_url
    Extracts base domains from CCR URLs and generates candidate rate page
    URLs (e.g., /water/rates, /utilities, /rates). Writes candidates to
    scrape_registry for the ScrapeAgent to validate.

    The CSV is produced manually from the EPA CCR search at
    https://sdwis.epa.gov/fylccr — automation of that APEX form is not
    currently supported.
    """
    from utility_api.ops.ccr_ingester import ingest_ccr_csv

    result = ingest_ccr_csv(csv_file, dry_run=dry_run)

    typer.echo(f"\nCCR Link Ingestion Results")
    typer.echo("=" * 50)
    typer.echo(f"  CSV rows read:          {result['rows_read']:>6,}")
    typer.echo(f"  Valid base domains:     {result['domains_extracted']:>6,}")
    typer.echo(f"  Candidate URLs generated: {result['candidates_generated']:>6,}")
    typer.echo(f"  Registry writes:        {result['urls_written']:>6,}")

    if dry_run:
        typer.echo("\n  (dry run — no writes performed)")
        for c in result.get("candidates", [])[:10]:
            typer.echo(f"    {c['pwsid']} → {c['url']}")


if __name__ == "__main__":
    app()
