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
    """Download and load SDWIS system data for VA + CA."""
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


@app.command()
def all():
    """Run all ingest steps in dependency order."""
    typer.echo("=== Step 1/5: CWS Boundaries ===")
    cws()
    typer.echo("=== Step 2/5: Aqueduct Polygons ===")
    aqueduct()
    typer.echo("=== Step 3/5: SDWIS Systems (+ county enrichment) ===")
    sdwis()
    typer.echo("=== Step 4/5: MDWD Financials ===")
    mdwd()
    typer.echo("=== Step 5/5: TIGER County Boundaries (+ spatial join) ===")
    tiger_county()
    typer.echo("=== All ingest steps complete ===")


if __name__ == "__main__":
    app()
