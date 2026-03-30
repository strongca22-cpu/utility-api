#!/usr/bin/env python3
"""
Serper.dev API Client

Purpose:
    Thin wrapper around the Serper.dev Google Search API with usage tracking,
    cost awareness, and free-tier budget guards. Every query is logged to the
    search_queries table for billing audit. The client refuses to exceed the
    free-tier limit (2,500 queries) unless SERPER_PAID_MODE is set.

Author: AI-Generated
Created: 2026-03-29
Modified: 2026-03-29

Dependencies:
    - requests
    - sqlalchemy
    - loguru

Usage:
    from utility_api.search.serper_client import SerperSearchClient

    client = SerperSearchClient()
    results = client.search("Portland water rates OR")
    # Returns: [{"url": "...", "title": "...", "snippet": "..."}]

    print(client.usage)
    # {"queries_total": 47, "queries_today": 12, "estimated_cost": "$0.05"}

Configuration:
    - SERPER_API_KEY: API key from serper.dev (required)
    - SERPER_PAID_MODE: Set to "true" to bypass free-tier budget guard (default: false)

Notes:
    - Free tier: 2,500 queries. Budget guard triggers at 2,400 (warning) and 2,500 (hard stop).
    - Serper returns Google results via POST to https://google.serper.dev/search
    - Rate limit: ~300 queries/sec on paid; conservatively ~3-5/sec on free tier.
    - HTTP 429 = rate limited (retry with backoff). HTTP 401 = invalid key (fail fast).
"""

import time
from datetime import date, datetime, timezone

import requests
from loguru import logger
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine


# --- Exceptions ---

class SerperError(Exception):
    """Base exception for Serper client errors."""
    pass


class BudgetExceededError(SerperError):
    """Raised when free-tier query budget is exhausted."""
    pass


class SerperAuthError(SerperError):
    """Raised when the API key is invalid (HTTP 401)."""
    pass


# --- Constants ---

FREE_TIER_LIMIT = 2500
FREE_TIER_WARNING = 2400
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2.0  # seconds


class SerperSearchClient:
    """Serper.dev Google Search API client with usage tracking and budget guards."""

    BASE_URL = "https://google.serper.dev/search"

    def __init__(self, paid_mode: bool | None = None):
        """Initialize the Serper client.

        Parameters
        ----------
        paid_mode : bool, optional
            Override for paid mode. If None, reads from SERPER_PAID_MODE env var.
        """
        self.api_key = getattr(settings, "serper_utility_api_key", "") or ""
        if not self.api_key:
            raise SerperError(
                "SERPER_UTILITY_API_KEY not configured. Add it to .env file."
            )

        if paid_mode is not None:
            self._paid_mode = paid_mode
        else:
            self._paid_mode = getattr(settings, "serper_paid_mode", False)

        # In-memory daily counter (supplementary to DB-based tracking)
        self._daily_count = 0
        self._daily_date = date.today()

    def search(
        self,
        query: str,
        num_results: int = 10,
        pwsid: str | None = None,
    ) -> list[dict]:
        """Execute a single search query. Returns normalized results.

        Parameters
        ----------
        query : str
            The search query string.
        num_results : int
            Number of results to request (max 100, default 10).
        pwsid : str, optional
            PWSID associated with this query (for audit trail).

        Returns
        -------
        list[dict]
            Each dict has keys: url, title, snippet.

        Raises
        ------
        BudgetExceededError
            If free-tier budget is exhausted and paid_mode is False.
        SerperAuthError
            If the API key is invalid.
        SerperError
            On other API errors after retries.
        """
        # Budget guard (check before spending a query)
        self._check_budget()

        # Execute with retry logic for rate limits
        start_time = time.monotonic()
        response_data = self._execute_with_retry(query, num_results)
        elapsed_ms = int((time.monotonic() - start_time) * 1000)

        # Normalize results
        organic = response_data.get("organic", [])
        results = [
            {
                "url": r.get("link", ""),
                "title": r.get("title", ""),
                "snippet": r.get("snippet", ""),
            }
            for r in organic
        ]

        # Log to search_queries (billing audit trail)
        self._log_query(
            query=query,
            pwsid=pwsid,
            result_count=len(results),
            response_time_ms=elapsed_ms,
        )

        return results

    def _execute_with_retry(
        self, query: str, num_results: int
    ) -> dict:
        """POST to Serper API with retry on 429 (rate limit).

        Returns the parsed JSON response.
        """
        last_error = None

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(
                    self.BASE_URL,
                    headers={"X-API-KEY": self.api_key},
                    json={"q": query, "num": num_results},
                    timeout=10,
                )

                if response.status_code == 401:
                    raise SerperAuthError(
                        "Invalid Serper API key. Check SERPER_API_KEY in .env."
                    )

                if response.status_code == 429:
                    wait = RETRY_BACKOFF_BASE * (2 ** attempt)
                    logger.warning(
                        f"Serper rate limited (429). Retrying in {wait:.1f}s "
                        f"(attempt {attempt + 1}/{MAX_RETRIES})"
                    )
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                return response.json()

            except (SerperAuthError, BudgetExceededError):
                raise
            except requests.exceptions.RequestException as e:
                last_error = e
                if attempt < MAX_RETRIES - 1:
                    wait = RETRY_BACKOFF_BASE * (2 ** attempt)
                    logger.warning(
                        f"Serper request error: {e}. Retrying in {wait:.1f}s"
                    )
                    time.sleep(wait)

        raise SerperError(
            f"Serper API failed after {MAX_RETRIES} attempts: {last_error}"
        )

    def _check_budget(self) -> None:
        """Enforce free-tier budget guard. Raises BudgetExceededError if over."""
        if self._paid_mode:
            return

        total_used = self._get_total_usage()

        if total_used >= FREE_TIER_LIMIT:
            raise BudgetExceededError(
                f"Free tier exhausted ({total_used:,}/{FREE_TIER_LIMIT:,} used). "
                f"Purchase credits at serper.dev and set SERPER_PAID_MODE=true in .env."
            )

        if total_used >= FREE_TIER_WARNING:
            logger.warning(
                f"Approaching free tier limit: {total_used:,}/{FREE_TIER_LIMIT:,} "
                f"queries used ({FREE_TIER_LIMIT - total_used} remaining)"
            )

    def _get_total_usage(self) -> int:
        """Get total Serper query count from search_queries table."""
        schema = settings.utility_schema
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT count(*) FROM {schema}.search_queries
                    WHERE search_engine = 'serper'
                """)).scalar()
                return result or 0
        except Exception as e:
            logger.debug(f"Could not read search_queries for budget check: {e}")
            return 0

    def _log_query(
        self,
        query: str,
        pwsid: str | None,
        result_count: int,
        response_time_ms: int,
    ) -> None:
        """Log a single API call to search_queries (billing audit trail)."""
        schema = settings.utility_schema

        # Update in-memory daily counter
        if date.today() != self._daily_date:
            self._daily_count = 0
            self._daily_date = date.today()
        self._daily_count += 1

        try:
            with engine.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO {schema}.search_queries
                        (pwsid, query, search_engine, result_count,
                         response_time_ms, queried_at)
                    VALUES
                        (:pwsid, :query, 'serper', :result_count,
                         :response_time_ms, :queried_at)
                """), {
                    "pwsid": pwsid,
                    "query": query,
                    "result_count": result_count,
                    "response_time_ms": response_time_ms,
                    "queried_at": datetime.now(timezone.utc),
                })
                conn.commit()
        except Exception as e:
            logger.warning(f"Failed to log Serper query: {e}")

    @property
    def usage(self) -> dict:
        """Current usage stats from search_queries.

        Returns dict with: queries_total, queries_today, queries_this_week,
        estimated_cost, free_tier_remaining.
        """
        schema = settings.utility_schema
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT
                        count(*) as total,
                        count(*) FILTER (
                            WHERE queried_at > CURRENT_DATE
                        ) as today,
                        count(*) FILTER (
                            WHERE queried_at > CURRENT_DATE - INTERVAL '7 days'
                        ) as week
                    FROM {schema}.search_queries
                    WHERE search_engine = 'serper'
                """)).fetchone()

            total = result.total or 0
            return {
                "queries_total": total,
                "queries_today": result.today or 0,
                "queries_this_week": result.week or 0,
                "estimated_cost": f"${total / 1000:.2f}",
                "free_tier_remaining": max(0, FREE_TIER_LIMIT - total),
            }
        except Exception as e:
            logger.warning(f"Could not read Serper usage: {e}")
            return {
                "queries_total": -1,
                "queries_today": -1,
                "queries_this_week": -1,
                "estimated_cost": "unknown",
                "free_tier_remaining": -1,
            }
