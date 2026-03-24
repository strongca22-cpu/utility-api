#!/usr/bin/env python3
"""
Utility API Server Launcher

Purpose:
    Launch the FastAPI utility API via uvicorn.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Usage:
    ua-api
"""

import uvicorn

from utility_api.config import settings


def main():
    """Launch the utility API server."""
    uvicorn.run(
        "utility_api.api.app:app",
        host="0.0.0.0",
        port=settings.api_port,
        reload=False,
    )


if __name__ == "__main__":
    main()
