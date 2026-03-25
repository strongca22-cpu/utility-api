#!/usr/bin/env python3
"""
Scrape Registry Writer

Purpose:
    Write-only instrumentation for the scraping pipeline. Records
    discovery, fetch, and parse outcomes to scrape_registry without
    affecting pipeline control flow.

    Sprint 12: Write-only. Sprint 13 orchestrator reads.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - sqlalchemy
    - loguru

Usage:
    from utility_api.ops.registry_writer import log_discovery, log_fetch, log_parse

Notes:
    - All functions are wrapped in try/except — never break the pipeline
    - If a registry write fails, the scrape/parse result still gets stored
    - Uses UPSERT on (pwsid, url) unique constraint
"""

from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine


def _detect_content_type(url: str) -> str:
    """Guess content type from URL."""
    url_lower = url.lower()
    if url_lower.endswith(".pdf"):
        return "pdf"
    if url_lower.endswith((".xlsx", ".xls", ".csv")):
        return "xlsx"
    return "html"


def log_discovery(
    pwsid: str,
    url: str,
    url_source: str = "searxng",
    discovery_query: str | None = None,
) -> None:
    """Record a URL discovery in scrape_registry.

    Called after rate_discovery.py finds a URL via search or curated list.
    """
    schema = settings.utility_schema
    try:
        with engine.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {schema}.scrape_registry
                    (pwsid, url, url_source, discovery_query, content_type, status)
                VALUES
                    (:pwsid, :url, :url_source, :query, :ctype, 'pending')
                ON CONFLICT (pwsid, url) DO UPDATE SET
                    url_source = COALESCE(EXCLUDED.url_source, {schema}.scrape_registry.url_source),
                    discovery_query = COALESCE(EXCLUDED.discovery_query, {schema}.scrape_registry.discovery_query),
                    updated_at = NOW()
            """), {
                "pwsid": pwsid,
                "url": url,
                "url_source": url_source,
                "query": discovery_query,
                "ctype": _detect_content_type(url),
            })
            conn.commit()
    except Exception as e:
        logger.debug(f"Registry write (discovery) failed for {pwsid}: {e}")


def log_fetch(
    pwsid: str,
    url: str,
    http_status: int | None = None,
    content_hash: str | None = None,
    content_length: int | None = None,
    error: str | None = None,
) -> None:
    """Record a fetch attempt in scrape_registry.

    Called after rate_scraper.py fetches a URL.
    """
    schema = settings.utility_schema
    status = "active" if http_status and 200 <= http_status < 400 else "failed"
    if error:
        status = "failed"

    try:
        with engine.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {schema}.scrape_registry
                    (pwsid, url, last_fetch_at, last_http_status,
                     last_content_hash, last_content_length, status, notes)
                VALUES
                    (:pwsid, :url, :now, :http_status,
                     :hash, :length, :status, :notes)
                ON CONFLICT (pwsid, url) DO UPDATE SET
                    last_fetch_at = EXCLUDED.last_fetch_at,
                    last_http_status = EXCLUDED.last_http_status,
                    last_content_hash = EXCLUDED.last_content_hash,
                    last_content_length = EXCLUDED.last_content_length,
                    status = EXCLUDED.status,
                    notes = CASE WHEN EXCLUDED.notes IS NOT NULL THEN EXCLUDED.notes
                                 ELSE {schema}.scrape_registry.notes END,
                    updated_at = NOW()
            """), {
                "pwsid": pwsid,
                "url": url,
                "now": datetime.now(timezone.utc),
                "http_status": http_status,
                "hash": content_hash,
                "length": content_length,
                "status": status,
                "notes": error,
            })
            conn.commit()
    except Exception as e:
        logger.debug(f"Registry write (fetch) failed for {pwsid}: {e}")


def log_parse(
    pwsid: str,
    url: str,
    parse_result: str | None = None,
    parse_confidence: str | None = None,
    parse_cost_usd: float | None = None,
    parse_model: str | None = None,
) -> None:
    """Record a parse outcome in scrape_registry.

    Called after rate_parser.py extracts rate data via Claude API.
    """
    schema = settings.utility_schema
    status = "active" if parse_result == "success" else "failed"

    try:
        with engine.connect() as conn:
            conn.execute(text(f"""
                UPDATE {schema}.scrape_registry SET
                    last_parse_at = :now,
                    last_parse_result = :result,
                    last_parse_confidence = :confidence,
                    last_parse_cost_usd = :cost,
                    status = :status,
                    updated_at = NOW()
                WHERE pwsid = :pwsid AND url = :url
            """), {
                "now": datetime.now(timezone.utc),
                "result": parse_result,
                "confidence": parse_confidence,
                "cost": parse_cost_usd,
                "status": status,
                "pwsid": pwsid,
                "url": url,
            })
            conn.commit()
    except Exception as e:
        logger.debug(f"Registry write (parse) failed for {pwsid}: {e}")
