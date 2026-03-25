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
            "water_rates", "rate_best_estimate", "permits",
            "source_catalog", "scrape_registry", "pipeline_runs",
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
    """Refresh the pwsid_coverage materialized view.

    Run this after any ingest, best-estimate build, or SDWIS expansion
    to update coverage statistics.
    """
    from utility_api.config import settings
    from utility_api.db import engine

    schema = settings.utility_schema

    typer.echo("Refreshing pwsid_coverage materialized view...")
    with engine.connect() as conn:
        conn.execute(text(
            f"REFRESH MATERIALIZED VIEW CONCURRENTLY {schema}.pwsid_coverage"
        ))
        conn.commit()

        # Report new stats
        row = conn.execute(text(f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN has_rate_data THEN 1 ELSE 0 END) AS with_rates,
                SUM(CASE WHEN has_sdwis THEN 1 ELSE 0 END) AS with_sdwis
            FROM {schema}.pwsid_coverage
        """)).fetchone()

    typer.echo(f"Done. {row.total:,} PWSIDs total, "
               f"{row.with_rates:,} with rates, {row.with_sdwis:,} with SDWIS.")


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


if __name__ == "__main__":
    app()
