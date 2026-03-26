#!/usr/bin/env python3
"""
Utility Ingest CLI

Purpose:
    Typer CLI for running data ingest steps.
    Each step downloads (if needed) and loads data into the utility schema.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - typer

Usage:
    ua-ingest cws        # Load EPA CWS boundaries
    ua-ingest sdwis      # Load SDWIS system data (VA + CA)
    ua-ingest mdwd       # Load MDWD financial data
    ua-ingest aqueduct   # Load Aqueduct 4.0 polygons
    ua-ingest all        # Run all steps in order
"""

import typer

app = typer.Typer(help="Utility enrichment data ingest pipeline.")


@app.command()
def cws():
    """Download and load EPA CWS service area boundaries."""
    from utility_api.ingest.cws import run_cws_ingest

    run_cws_ingest()


@app.command()
def sdwis():
    """Download and load SDWIS system data.

    Loads from EPA ECHO bulk download (~200MB ZIP). State scope is
    controlled by config/sources.yaml sdwis_states key. Set to "ALL"
    for all 50 states, or a list of state codes for targeted loading.

    Note: Full 50-state load produces ~44K records and takes several
    minutes. Consider running in tmux for long operations.
    """
    from utility_api.ingest.sdwis import run_sdwis_ingest

    run_sdwis_ingest()


@app.command()
def mdwd():
    """Download and load MDWD financial data."""
    from utility_api.ingest.mdwd import run_mdwd_ingest

    run_mdwd_ingest()


@app.command()
def aqueduct():
    """Load Aqueduct 4.0 watershed risk polygons into PostGIS."""
    from utility_api.ingest.aqueduct import run_aqueduct_ingest

    run_aqueduct_ingest()


@app.command("tiger-county")
def tiger_county():
    """Download and load Census TIGER county boundaries + spatial join."""
    from utility_api.ingest.tiger_county import run_tiger_county_ingest

    run_tiger_county_ingest()


@app.command("va-deq")
def va_deq():
    """Download and load VA DEQ permits (VWP + VPDES) from EDMA MapServer."""
    from utility_api.ingest.va_deq import run_va_deq_ingest

    run_va_deq_ingest()


@app.command("ca-ewrims")
def ca_ewrims():
    """Download and load CA SWRCB eWRIMS water rights (targeted load)."""
    from utility_api.ingest.ca_ewrims import run_ca_ewrims_ingest

    run_ca_ewrims_ingest()


@app.command()
def ear(
    year: list[int] = typer.Option(None, "--year", "-y", help="Year(s) to ingest: 2020, 2021, 2022"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Parse and report, no DB writes"),
):
    """Ingest CA SWRCB eAR bulk rate data from HydroShare Excel files.

    Loads state-reported water rate structures and bill amounts for CA
    utilities that match our existing database. Source: HydroShare processed
    eAR data (2020-2022).

    Example: ua-ingest ear --year 2022 --dry-run
    """
    from utility_api.ingest.ear_ingest import run_ear_ingest

    run_ear_ingest(
        years=year or None,
        dry_run=dry_run,
    )


@app.command("civicplus-crawl")
def civicplus_crawl(
    domain: list[str] = typer.Option(None, "--domain", "-d", help="CivicPlus domain(s) to crawl"),
    min_score: float = typer.Option(2.0, "--min-score", help="Minimum relevance score for candidates"),
):
    """Search CivicPlus DocumentCenter sites for water rate PDFs.

    Uses Playwright to render CivicPlus site search and scores results
    for water rate relevance. Outputs ranked candidate URLs.

    Example: ua-ingest civicplus-crawl --domain fredericksburgva.gov
    """
    import asyncio

    from utility_api.ingest.civicplus_crawler import crawl_civicplus_search

    if not domain:
        typer.echo("Provide at least one --domain")
        raise typer.Exit(1)

    async def run():
        for d in domain:
            base_url = f"https://www.{d}" if not d.startswith("http") else d
            typer.echo(f"\n=== {d} ===")
            result = await crawl_civicplus_search(base_url, min_score=min_score)
            typer.echo(f"Results: {result.total_results}, Candidates: {len(result.candidates)}")
            for c in result.candidates[:10]:
                dc = " [DC]" if c.is_document_center else ""
                typer.echo(f"  [{c.relevance_score:+.1f}]{dc} {c.title}")
                typer.echo(f"         {c.url}")

    asyncio.run(run())


@app.command()
def owrs(
    dry_run: bool = typer.Option(False, "--dry-run", help="Parse and report, no DB writes"),
    all_utilities: bool = typer.Option(False, "--all-utilities", help="Include utilities not in CWS list"),
):
    """Ingest CA OWRS rate data from the California Data Collaborative.

    Loads pre-computed rate structures and bill amounts for ~386 CA utilities
    from the OWRS-Analysis summary table. Source: OpenEI / GitHub.

    Example: ua-ingest owrs --dry-run
    """
    from utility_api.ingest.owrs_ingest import run_owrs_ingest

    run_owrs_ingest(
        dry_run=dry_run,
        all_utilities=all_utilities,
    )


@app.command("efc-nc")
def efc_nc(
    dry_run: bool = typer.Option(False, "--dry-run", help="Parse and report, no DB writes"),
):
    """Ingest NC water rates from UNC EFC dashboard (2025 CSV).

    Loads pre-computed bill curves for ~400 NC utilities, reverse-engineers
    tier structures, and normalizes to monthly equivalents.

    Source: https://dashboards.efc.sog.unc.edu/nc

    Example: ua-ingest efc-nc --dry-run
    """
    from utility_api.ingest.efc_nc_ingest import run_efc_nc_ingest

    run_efc_nc_ingest(dry_run=dry_run)


@app.command("efc-fl")
def efc_fl(
    dry_run: bool = typer.Option(False, "--dry-run", help="Parse and report, no DB writes"),
    refresh: bool = typer.Option(False, "--refresh", help="Force re-fetch from API (ignore cache)"),
):
    """Ingest FL water rates from UNC EFC dashboard (2020 Raftelis survey).

    Fetches bill curves for ~227 FL utilities via the EFC JSON API,
    reverse-engineers tier structures, and normalizes to monthly equivalents.
    API responses are cached locally for re-runs.

    Source: https://dashboards.efc.sog.unc.edu/fl

    Example: ua-ingest efc-fl --dry-run
    """
    from utility_api.ingest.efc_fl_ingest import run_efc_fl_ingest

    run_efc_fl_ingest(dry_run=dry_run, refresh=refresh)


@app.command("wv-psc")
def wv_psc(
    dry_run: bool = typer.Option(False, "--dry-run", help="Parse and report, no DB writes"),
    refresh: bool = typer.Option(False, "--refresh", help="Force re-fetch from PSC website"),
):
    """Ingest WV water rates from Public Service Commission rankings.

    Scrapes cost rankings at 3,400 and 4,000 gallon consumption levels
    for ~325 WV utilities. Derives volumetric rates from the two data
    points and fuzzy-matches utility names to SDWIS PWSIDs.

    Source: https://www.psc.state.wv.us/scripts/Utilities/rptWaterRankings4000.cfm

    Example: ua-ingest wv-psc --dry-run
    """
    from utility_api.ingest.wv_psc_ingest import run_wv_psc_ingest

    run_wv_psc_ingest(dry_run=dry_run, refresh=refresh)


@app.command()
def efc(
    state: list[str] = typer.Option(None, "--state", "-s", help="State(s) to ingest (e.g., WI, GA)"),
    all_states: bool = typer.Option(False, "--all", help="Ingest all configured EFC states"),
    skip_ingested: bool = typer.Option(False, "--skip-ingested", help="Skip states already ingested"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Parse and report, no DB writes"),
    refresh: bool = typer.Option(False, "--refresh", help="Force re-fetch from API (ignore cache)"),
    list_states: bool = typer.Option(False, "--list", help="List all configured states and exit"),
):
    """Generic EFC dashboard ingest — works for any of the 20+ EFC states.

    Fetches bill curves from the UNC EFC Topsail JSON API, extracts
    tier structures, and writes to water_rates. All states share the
    same API pattern.

    Examples:
        ua-ingest efc --list
        ua-ingest efc --state WI --dry-run
        ua-ingest efc --state GA --state OH
        ua-ingest efc --all --skip-ingested
    """
    from utility_api.ingest.efc_generic import list_efc_states, run_efc_ingest

    if list_states:
        list_efc_states()
        return

    if not state and not all_states:
        typer.echo("Specify --state or --all. Use --list to see available states.")
        raise typer.Exit(1)

    run_efc_ingest(
        states=state or None,
        all_states=all_states,
        skip_ingested=skip_ingested,
        dry_run=dry_run,
        refresh=refresh,
    )


@app.command("duke-reference")
def duke_reference(
    state: list[str] = typer.Option(None, "--state", "-s", help="State(s) to ingest"),
    all_states: bool = typer.Option(False, "--all", help="Ingest all 10 Duke states"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Parse and report, no DB writes"),
):
    """[LEGACY] Ingest Duke data as INTERNAL REFERENCE.

    Superseded by 'duke-nieps' which writes to rate_schedules as free_attributed.
    This command writes to the legacy duke_reference_rates table.

    Examples:
        ua-ingest duke-reference --state TX --dry-run
        ua-ingest duke-reference --all
    """
    from utility_api.ingest.duke_reference_ingest import run_duke_reference_ingest

    if not state and not all_states:
        typer.echo("Specify --state or --all")
        raise typer.Exit(1)

    run_duke_reference_ingest(
        states=state or None,
        all_states=all_states,
        dry_run=dry_run,
    )


@app.command("ky-psc")
def ky_psc(
    dry_run: bool = typer.Option(False, "--dry-run", help="Parse and report, no DB writes"),
    refresh: bool = typer.Option(False, "--refresh", help="Force re-download PDFs"),
    limit: int = typer.Option(None, "--limit", "-n", help="Process at most N utilities"),
):
    """Ingest Kentucky PSC water tariff directory.

    Crawls IIS directory (~136 utilities), downloads Tariff.pdf for each,
    extracts rate structures via Claude API, fuzzy-matches to SDWIS PWSIDs.

    Examples:
        ua-ingest ky-psc --dry-run --limit 5
        ua-ingest ky-psc --dry-run
        ua-ingest ky-psc
    """
    from utility_api.ingest.ky_psc_ingest import run_ky_psc_ingest

    run_ky_psc_ingest(dry_run=dry_run, refresh=refresh, limit=limit)


@app.command("in-iurc")
def in_iurc(
    dry_run: bool = typer.Option(False, "--dry-run", help="Parse and report, no DB writes"),
    refresh: bool = typer.Option(False, "--refresh", help="Force re-download PDF"),
):
    """Ingest Indiana IURC water billing survey (2024).

    Parses PDF with monthly bills at 4,000 gal for ~78 IURC-regulated
    utilities. Fuzzy-matches utility names to SDWIS PWSIDs.

    Examples:
        ua-ingest in-iurc --dry-run
        ua-ingest in-iurc
    """
    from utility_api.ingest.in_iurc_ingest import run_in_iurc_ingest

    run_in_iurc_ingest(dry_run=dry_run, refresh=refresh)


@app.command("duke-nieps")
def duke_nieps(
    state: list[str] = typer.Option(None, "--state", "-s", help="State(s) to ingest (e.g., TX, NC)"),
    all_states: bool = typer.Option(False, "--all", help="Ingest all 10 Duke states"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Parse and report, no DB writes"),
    seed_catalog: bool = typer.Option(False, "--seed-catalog", help="Seed source_catalog provenance entry"),
):
    """Ingest Duke NIEPS 10-state rate data into rate_schedules.

    CC BY-NC-ND 4.0 license — free_attributed tier (free with attribution,
    never paywalled). Full rate structures: fixed charges + volumetric tier
    breakpoints for 5,371 PWSIDs across 10 states.

    Use --seed-catalog on first run to create the source_catalog entry
    with full provenance metadata.

    Examples:
        ua-ingest duke-nieps --state TX --dry-run
        ua-ingest duke-nieps --all --seed-catalog --dry-run
        ua-ingest duke-nieps --all --seed-catalog
    """
    from utility_api.ingest.duke_nieps_ingest import (
        run_duke_nieps_ingest,
        seed_source_catalog,
    )

    if seed_catalog:
        seed_source_catalog(dry_run=dry_run)

    if not state and not all_states:
        typer.echo("Specify --state or --all")
        raise typer.Exit(1)

    run_duke_nieps_ingest(
        states=state or None,
        all_states=all_states,
        dry_run=dry_run,
    )


@app.command()
def rates(
    state: list[str] = typer.Option(None, "--state", "-s", help="Filter to state(s): VA, CA"),
    pwsid: list[str] = typer.Option(None, "--pwsid", "-p", help="Specific PWSID(s) to process"),
    limit: int = typer.Option(None, "--limit", "-n", help="Max utilities to process"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Discover + scrape only, no API calls"),
    search_delay: float = typer.Option(2.0, "--search-delay", help="Seconds between web searches"),
    scrape_delay: float = typer.Option(1.5, "--scrape-delay", help="Seconds between page scrapes"),
    url_file: str = typer.Option(None, "--url-file", "-f", help="YAML file with curated pwsid→url mappings"),
    max_cost: float = typer.Option(None, "--max-cost", help="Hard cap on API cost in USD"),
):
    """Discover, scrape, and parse water rates via Claude API.

    Pipeline: URL discovery → web scrape → Claude API parse → bill calculation → DB store.
    Requires ANTHROPIC_API_KEY in environment or .env file.

    Use --url-file to provide curated URLs (skips web search for those PWSIDs).
    Example: ua-ingest rates --url-file config/rate_urls_va.yaml
    """
    from utility_api.ingest.rates import run_rate_ingest

    run_rate_ingest(
        pwsids=pwsid or None,
        state_filter=state or None,
        limit=limit,
        search_delay=search_delay,
        scrape_delay=scrape_delay,
        dry_run=dry_run,
        url_file=url_file,
        max_cost_usd=max_cost,
    )


@app.command()
def all():
    """Run all ingest steps in dependency order."""
    typer.echo("=== Step 1/7: CWS Boundaries ===")
    cws()
    typer.echo("=== Step 2/7: Aqueduct Polygons ===")
    aqueduct()
    typer.echo("=== Step 3/7: SDWIS Systems (+ county enrichment) ===")
    sdwis()
    typer.echo("=== Step 4/7: MDWD Financials ===")
    mdwd()
    typer.echo("=== Step 5/7: TIGER County Boundaries (+ spatial join) ===")
    tiger_county()
    typer.echo("=== Step 6/7: VA DEQ Permits ===")
    va_deq()
    typer.echo("=== Step 7/7: CA eWRIMS Water Rights ===")
    ca_ewrims()
    typer.echo("=== All ingest steps complete ===")


if __name__ == "__main__":
    app()
