#!/usr/bin/env python3
"""
Source Freshness Checker

Purpose:
    Checks bulk data sources for new data availability. Fetches the source URL,
    hashes the content, and compares to the stored hash in source_catalog.
    Source-specific checks (eAR year detection, EFC survey year) supplement
    the generic hash-based approach.

    This agent does NOT use an LLM. It is HTTP + hashing + SQL.

Author: AI-Generated
Created: 2026-03-25
Modified: 2026-03-25

Dependencies:
    - requests
    - sqlalchemy

Usage:
    from utility_api.agents.source_checker import SourceChecker
    result = SourceChecker().run(source_key='efc_nc_2025')
    # Or check all due sources:
    result = SourceChecker().run_all_due()

Notes:
    - Sprint 14 scope: detect changes and log them for human review
    - Does NOT auto-trigger ingest (Sprint 15+)
    - Updates source_catalog.last_content_hash and next_check_date
    - Source-specific checks look for new vintage years in page content
"""

import hashlib
import re
from datetime import date, datetime, timedelta, timezone

import requests
from loguru import logger
from sqlalchemy import text

from utility_api.agents.base import BaseAgent
from utility_api.config import settings
from utility_api.db import engine


# Timeout for HTTP requests (seconds)
REQUEST_TIMEOUT = 30


def _fetch_page(url: str) -> tuple[str | None, int | None]:
    """Fetch a URL and return (text_content, status_code)."""
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT, headers={
            "User-Agent": "UAPI-SourceChecker/1.0 (data freshness monitor)",
        })
        return response.text, response.status_code
    except requests.RequestException as e:
        logger.warning(f"  Fetch failed for {url}: {e}")
        return None, None


def _hash_content(content: str) -> str:
    """SHA-256 hash of page content."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _check_ear_freshness(content: str, vintage_end: date | None) -> dict:
    """Check HydroShare eAR page for new year files.

    Looks for year patterns (e.g., '2023', '2024') in the page content
    that are newer than the current vintage_end year.
    """
    if not vintage_end:
        return {"new_data_available": False, "details": "No vintage_end set"}

    current_year = vintage_end.year
    # Find all 4-digit years in the content
    years_found = set(int(y) for y in re.findall(r"\b(20[12]\d)\b", content))
    newer_years = sorted(y for y in years_found if y > current_year)

    if newer_years:
        return {
            "new_data_available": True,
            "details": f"New year(s) found: {newer_years} (current vintage: {current_year})",
        }
    return {
        "new_data_available": False,
        "details": f"Latest year on page: {max(years_found) if years_found else '?'} (current vintage: {current_year})",
    }


def _check_efc_freshness(content: str, vintage_end: date | None) -> dict:
    """Check EFC dashboard for updated survey year.

    Looks for year mentions in the dashboard content that indicate a
    newer data vintage than what we have.
    """
    if not vintage_end:
        return {"new_data_available": False, "details": "No vintage_end set"}

    current_year = vintage_end.year
    # EFC dashboard typically mentions the survey year prominently
    years_found = set(int(y) for y in re.findall(r"\b(20[12]\d)\b", content))
    newer_years = sorted(y for y in years_found if y > current_year)

    if newer_years:
        return {
            "new_data_available": True,
            "details": f"Newer survey year(s): {newer_years} (current: {current_year})",
        }
    return {
        "new_data_available": False,
        "details": f"Years found: {sorted(years_found) if years_found else '?'} (current: {current_year})",
    }


def _check_generic_freshness(content: str, stored_hash: str | None) -> dict:
    """Generic check: has the page content changed since last check?"""
    new_hash = _hash_content(content)
    if stored_hash is None:
        return {
            "new_data_available": False,
            "details": "First check — baseline hash recorded",
            "content_hash": new_hash,
        }

    changed = new_hash != stored_hash
    return {
        "new_data_available": changed,
        "details": "Content changed" if changed else "No change detected",
        "content_hash": new_hash,
    }


class SourceChecker(BaseAgent):
    """Checks bulk data sources for new data availability."""

    agent_name = "source_checker"

    def run(self, source_key: str, **kwargs) -> dict:
        """Check a single source for new data.

        Parameters
        ----------
        source_key : str
            Key from source_catalog.

        Returns
        -------
        dict
            source_key, new_data_available, details, http_status.
        """
        schema = settings.utility_schema
        logger.info(f"SourceChecker: checking {source_key}")

        # Load source info
        with engine.connect() as conn:
            row = conn.execute(text(f"""
                SELECT source_key, display_name, source_url, source_type,
                       last_content_hash, vintage_end, check_interval_days
                FROM {schema}.source_catalog
                WHERE source_key = :key
            """), {"key": source_key}).fetchone()

        if not row:
            msg = f"Source '{source_key}' not found in source_catalog"
            logger.error(msg)
            return {"source_key": source_key, "error": msg}

        if not row.source_url:
            msg = f"No source_url configured for '{source_key}'"
            logger.warning(f"  {msg}")
            self.log_run(status="failed", source_key=source_key, notes=msg)
            return {"source_key": source_key, "new_data_available": False, "details": msg}

        # Fetch the page
        content, http_status = _fetch_page(row.source_url)
        if content is None:
            msg = f"Failed to fetch {row.source_url} (status={http_status})"
            logger.warning(f"  {msg}")
            self.log_run(status="failed", source_key=source_key, notes=msg)
            return {"source_key": source_key, "new_data_available": False,
                    "http_status": http_status, "details": msg}

        logger.info(f"  Fetched {len(content):,} chars (HTTP {http_status})")

        # Source-specific checks
        if source_key.startswith("swrcb_ear"):
            check_result = _check_ear_freshness(content, row.vintage_end)
        elif source_key.startswith("efc_"):
            check_result = _check_efc_freshness(content, row.vintage_end)
        else:
            check_result = _check_generic_freshness(content, row.last_content_hash)

        # Always compute hash for storage
        content_hash = check_result.get("content_hash") or _hash_content(content)

        # Update source_catalog: hash + next_check_date
        interval = row.check_interval_days or 90
        next_check = date.today() + timedelta(days=interval)

        with engine.connect() as conn:
            conn.execute(text(f"""
                UPDATE {schema}.source_catalog SET
                    last_content_hash = :hash,
                    next_check_date = :next_check,
                    updated_at = :now
                WHERE source_key = :key
            """), {
                "hash": content_hash,
                "next_check": next_check,
                "now": datetime.now(timezone.utc),
                "key": source_key,
            })

            # Log the finding if new data is available
            if check_result.get("new_data_available"):
                ts = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                prefix = f"[{ts}] NEW DATA: {check_result['details']}\n"
                conn.execute(text(f"""
                    UPDATE {schema}.source_catalog SET
                        notes = :prefix || COALESCE(notes, '')
                    WHERE source_key = :key
                """), {
                    "prefix": prefix,
                    "key": source_key,
                })

            conn.commit()

        status = "new_data" if check_result.get("new_data_available") else "no_change"
        logger.info(f"  Result: {status} — {check_result.get('details', '')}")
        logger.info(f"  Next check: {next_check}")

        self.log_run(
            status="success",
            source_key=source_key,
            notes=f"{status}: {check_result.get('details', '')}",
        )

        return {
            "source_key": source_key,
            "new_data_available": check_result.get("new_data_available", False),
            "details": check_result.get("details", ""),
            "http_status": http_status,
            "next_check_date": str(next_check),
        }

    def run_all_due(self) -> dict:
        """Check all sources with next_check_date <= today.

        Returns
        -------
        dict
            sources_checked, new_data_found (list), no_change (list).
        """
        schema = settings.utility_schema
        logger.info("=== SourceChecker: checking all due sources ===")

        with engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT source_key, display_name
                FROM {schema}.source_catalog
                WHERE next_check_date IS NOT NULL
                  AND next_check_date <= CURRENT_DATE
                  AND source_url IS NOT NULL
                ORDER BY next_check_date ASC
            """)).fetchall()

        if not rows:
            logger.info("  No sources due for checking")
            return {"sources_checked": 0, "new_data_found": [], "no_change": []}

        new_data = []
        no_change = []

        for row in rows:
            result = self.run(source_key=row.source_key)
            if result.get("new_data_available"):
                new_data.append(row.source_key)
            else:
                no_change.append(row.source_key)

        logger.info(f"  Checked {len(rows)} sources: {len(new_data)} new, {len(no_change)} unchanged")

        return {
            "sources_checked": len(rows),
            "new_data_found": new_data,
            "no_change": no_change,
        }
