#!/usr/bin/env python3
"""
Scrape Agent

Purpose:
    Fetches content from URLs in the scrape_registry. Wraps existing
    rate_scraper.py functionality with registry coordination — reads
    pending URLs, fetches content, updates registry with outcomes.

    This agent does NOT use an LLM. No `anthropic` import.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - sqlalchemy
    - requests, playwright, pymupdf (via rate_scraper.py)

Usage:
    from utility_api.agents.scrape import ScrapeAgent
    result = ScrapeAgent().run(pwsid='VA4760100')

Notes:
    - Reads from scrape_registry (Sprint 13 change — Sprint 12 only wrote)
    - Returns raw text in memory for ParseAgent to consume
    - Retry logic: 403 → exponential backoff, 404 → dead, 5xx → 6h retry
    - Reuses existing rate_scraper.py functions — does not rewrite them
"""

from datetime import datetime, timedelta, timezone

from loguru import logger
from sqlalchemy import text

from utility_api.agents.base import BaseAgent
from utility_api.config import settings
from utility_api.db import engine


class ScrapeAgent(BaseAgent):
    """Fetches URLs from scrape_registry and returns raw text."""

    agent_name = "scrape"

    def run(
        self,
        registry_id: int | None = None,
        pwsid: str | None = None,
        **kwargs,
    ) -> dict:
        """Fetch content from URLs in scrape_registry.

        Parameters
        ----------
        registry_id : int, optional
            Fetch a specific registry entry by ID.
        pwsid : str, optional
            Fetch all pending URLs for this PWSID.

        Returns
        -------
        dict
            urls_fetched, succeeded, failed, raw_texts (list of dicts).
        """
        from utility_api.ingest.rate_scraper import scrape_rate_page

        schema = settings.utility_schema

        # Get URLs to fetch
        with engine.connect() as conn:
            if registry_id:
                rows = conn.execute(text(f"""
                    SELECT id, pwsid, url, content_type, last_content_hash, retry_count
                    FROM {schema}.scrape_registry
                    WHERE id = :id
                """), {"id": registry_id}).fetchall()
            elif pwsid:
                rows = conn.execute(text(f"""
                    SELECT id, pwsid, url, content_type, last_content_hash, retry_count
                    FROM {schema}.scrape_registry
                    WHERE pwsid = :pwsid
                      AND status IN ('pending', 'pending_retry')
                    ORDER BY created_at ASC
                """), {"pwsid": pwsid}).fetchall()
            else:
                logger.warning("ScrapeAgent: no registry_id or pwsid specified")
                return {"urls_fetched": 0, "succeeded": 0, "failed": 0, "raw_texts": []}

        if not rows:
            logger.info("ScrapeAgent: no pending URLs to fetch")
            return {"urls_fetched": 0, "succeeded": 0, "failed": 0, "raw_texts": []}

        logger.info(f"ScrapeAgent: {len(rows)} URLs to fetch")

        succeeded = 0
        failed = 0
        raw_texts = []

        for row in rows:
            url = row.url
            logger.info(f"  Fetching: {url[:80]}")

            try:
                scrape_result = scrape_rate_page(url)
            except Exception as e:
                logger.warning(f"  Scrape exception: {e}")
                self._update_registry_failure(row.id, row.retry_count, str(e))
                failed += 1
                continue

            if scrape_result.error and not scrape_result.text:
                logger.warning(f"  Scrape failed: {scrape_result.error}")
                http_status = getattr(scrape_result, "status_code", None)
                self._update_registry_failure(
                    row.id, row.retry_count, scrape_result.error, http_status
                )
                failed += 1
                continue

            # Success — update registry
            content_hash = getattr(scrape_result, "text_hash", None)
            content_changed = (
                content_hash != row.last_content_hash
                if row.last_content_hash and content_hash
                else True
            )
            char_count = len(scrape_result.text) if scrape_result.text else 0

            with engine.connect() as conn:
                conn.execute(text(f"""
                    UPDATE {schema}.scrape_registry SET
                        last_fetch_at = :now,
                        last_http_status = :status,
                        last_content_hash = :hash,
                        last_content_length = :length,
                        status = 'active',
                        notes = NULL,
                        updated_at = :now
                    WHERE id = :id
                """), {
                    "now": datetime.now(timezone.utc),
                    "status": getattr(scrape_result, "status_code", 200),
                    "hash": content_hash,
                    "length": char_count,
                    "id": row.id,
                })
                conn.commit()

            raw_texts.append({
                "registry_id": row.id,
                "pwsid": row.pwsid,
                "url": url,
                "text": scrape_result.text,
                "content_type": "pdf" if getattr(scrape_result, "is_pdf", False) else "html",
                "content_changed": content_changed,
                "char_count": char_count,
            })
            succeeded += 1
            logger.info(f"  ✓ {char_count:,} chars, changed={content_changed}")

        self.log_run(
            status="success" if succeeded > 0 else "failed",
            rows_affected=succeeded,
            notes=f"Fetched {succeeded}/{len(rows)}, failed {failed}",
        )

        return {
            "urls_fetched": len(rows),
            "succeeded": succeeded,
            "failed": failed,
            "raw_texts": raw_texts,
        }

    def _update_registry_failure(
        self, registry_id: int, retry_count: int,
        error: str, http_status: int | None = None,
    ) -> None:
        """Update scrape_registry on fetch failure with retry logic."""
        schema = settings.utility_schema
        now = datetime.now(timezone.utc)

        if http_status == 404:
            status = "dead"
            retry_after = None
        elif http_status == 403:
            new_retry_count = retry_count + 1
            if new_retry_count > 5:
                status = "dead"
                retry_after = None
            else:
                status = "pending_retry"
                retry_after = now + timedelta(days=min(2 ** new_retry_count, 30))
        elif http_status and http_status >= 500:
            status = "pending_retry"
            retry_after = now + timedelta(hours=6)
        else:
            status = "pending_retry"
            retry_after = now + timedelta(days=1)

        try:
            with engine.connect() as conn:
                conn.execute(text(f"""
                    UPDATE {schema}.scrape_registry SET
                        last_fetch_at = :now,
                        last_http_status = :http_status,
                        status = :status,
                        retry_after = :retry_after,
                        retry_count = retry_count + 1,
                        notes = :error,
                        updated_at = :now
                    WHERE id = :id
                """), {
                    "now": now,
                    "http_status": http_status,
                    "status": status,
                    "retry_after": retry_after,
                    "error": error[:500] if error else None,
                    "id": registry_id,
                })
                conn.commit()
        except Exception as e:
            logger.debug(f"Registry failure update failed: {e}")
