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
