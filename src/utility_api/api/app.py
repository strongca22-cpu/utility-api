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

from fastapi import Depends, FastAPI
from sqlalchemy import text
from sqlalchemy.orm import Session

from utility_api.api.dependencies import get_db
from utility_api.api.routers.resolve import router as resolve_router
from utility_api.config import settings

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


VINTAGE_QUERY = text("""
    SELECT
        step_name,
        finished_at,
        row_count,
        status,
        notes
    FROM utility.pipeline_runs
    WHERE (step_name, finished_at) IN (
        SELECT step_name, MAX(finished_at)
        FROM utility.pipeline_runs
        WHERE status = 'success'
        GROUP BY step_name
    )
    ORDER BY step_name
""")


@app.get("/health")
def health(db: Session = Depends(get_db)):
    """Health check with data vintage information.

    Returns the last successful pipeline run for each data layer,
    including timestamp, row count, and any notes.
    """
    data_vintage = {}
    try:
        rows = db.execute(VINTAGE_QUERY).mappings().all()
        for row in rows:
            data_vintage[row["step_name"]] = {
                "last_updated": row["finished_at"].isoformat() if row["finished_at"] else None,
                "row_count": row["row_count"],
                "notes": row["notes"],
            }
    except Exception:
        data_vintage = {"error": "Could not query pipeline_runs"}

    return {
        "status": "ok",
        "version": "0.1.0",
        "data_vintage": data_vintage,
    }
