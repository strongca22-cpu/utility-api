#!/usr/bin/env python3
"""
Utility API Application

Purpose:
    FastAPI application factory for the utility intelligence API.
    Serves /resolve endpoint for geographic utility lookup.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - fastapi

Usage:
    ua-api                  # Launch via CLI
    uvicorn utility_api.api.app:app --port 8000  # Direct launch
"""

from fastapi import FastAPI

from utility_api.api.routers.resolve import router as resolve_router

app = FastAPI(
    title="Utility Intelligence API",
    description=(
        "Water utility enrichment API. Resolves geographic coordinates to "
        "water utility identity, regulatory context, financial health, and "
        "water stress risk. Powered by EPA CWS boundaries, SDWIS, MDWD, "
        "and WRI Aqueduct 4.0."
    ),
    version="0.1.0",
)

app.include_router(resolve_router)


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "ok", "version": "0.1.0"}
