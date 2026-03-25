#!/usr/bin/env python3
"""
Scrape Registry Model

Purpose:
    Per-URL tracking of search attempts, fetch results, parse outcomes,
    and retry scheduling. This is the "chip tracker" for the scraping
    pipeline — every interaction with an external URL is recorded here.

    Lifecycle: pending → active → dead/blocked/stale/pending_retry
    Every scraping session starts by querying this table to decide
    what to attempt next.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - sqlalchemy

Usage:
    from utility_api.models.scrape_registry import ScrapeRegistry

Notes:
    - Composite unique constraint on (pwsid, url)
    - pwsid is nullable for discovery-phase entries (URL found but
      not yet assigned to a utility)
    - Sprint 10: table + YAML migration. Write-only logging.
    - Sprint 12: BaseAgent + ScrapeAgent wire read/write.
    - Sprint 13: OrchestratorAgent reads registry for task planning.

Data Sources:
    - Seeded from config/rate_urls_va.yaml and config/rate_urls_ca.yaml
    - Updated by rate_discovery.py, rate_scraper.py, rate_parser.py
"""

from sqlalchemy import (
    DateTime,
    Float,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from utility_api.models.base import SCHEMA, Base


class ScrapeRegistry(Base):
    """Per-URL tracking for the scraping pipeline."""

    __tablename__ = "scrape_registry"
    __table_args__ = (
        UniqueConstraint("pwsid", "url", name="uq_scrape_registry_pwsid_url"),
        {"schema": SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Identity
    pwsid: Mapped[str | None] = mapped_column(
        String(12), index=True,
        comment="PWSID this URL is associated with (nullable for discovery-phase)",
    )
    url: Mapped[str] = mapped_column(
        Text,
        comment="The URL being tracked",
    )
    url_source: Mapped[str | None] = mapped_column(
        String(30),
        comment="How this URL was found: searxng | curated | civicplus_crawler | manual",
    )
    discovery_query: Mapped[str | None] = mapped_column(
        Text,
        comment="The search query that found this URL (if discovered via search)",
    )
    content_type: Mapped[str | None] = mapped_column(
        String(20),
        comment="html | pdf | xlsx | unknown",
    )

    # Fetch tracking
    last_fetch_at = mapped_column(
        DateTime(timezone=True), nullable=True,
        comment="When this URL was last fetched",
    )
    last_http_status: Mapped[int | None] = mapped_column(
        Integer,
        comment="HTTP status code from last fetch (200, 403, 404, 500, etc.)",
    )
    last_content_hash: Mapped[str | None] = mapped_column(
        String(64),
        comment="SHA-256 of fetched content (for change detection)",
    )
    last_content_length: Mapped[int | None] = mapped_column(
        Integer,
        comment="Content length in bytes from last fetch",
    )

    # Parse tracking
    last_parse_at = mapped_column(
        DateTime(timezone=True), nullable=True,
        comment="When content was last sent to parse agent",
    )
    last_parse_result: Mapped[str | None] = mapped_column(
        String(20),
        comment="success | failed | partial | skipped",
    )
    last_parse_confidence: Mapped[str | None] = mapped_column(
        String(10),
        comment="high | medium | low",
    )
    last_parse_cost_usd: Mapped[float | None] = mapped_column(
        Float,
        comment="API cost in USD for this parse attempt",
    )

    # Status and retry
    status: Mapped[str] = mapped_column(
        String(20), default="pending",
        comment="active | dead | blocked | stale | pending | pending_retry",
    )
    retry_after = mapped_column(
        DateTime(timezone=True), nullable=True,
        comment="When to retry (for pending_retry status)",
    )
    retry_count: Mapped[int] = mapped_column(
        Integer, default=0,
        comment="Number of retry attempts so far",
    )
    notes: Mapped[str | None] = mapped_column(
        Text,
        comment="Free text — why it failed, what was tried",
    )

    # Timestamps
    created_at = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
