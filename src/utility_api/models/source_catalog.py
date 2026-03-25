#!/usr/bin/env python3
"""
Source Catalog Model

Purpose:
    Registry of all known bulk data sources, their state coverage,
    vintage, refresh cadence, and ingest status. This is the operational
    complement to config/sources.yaml (which holds static metadata like
    URLs and DOIs). The DB table tracks mutable operational state:
    when we last ingested, how many PWSIDs it covers, when to check next.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - sqlalchemy

Usage:
    from utility_api.models.source_catalog import SourceCatalog

Notes:
    - source_key is the primary key and matches water_rates.source values
    - states_covered uses PostgreSQL TEXT[] array type
    - Seeded by scripts/seed_source_catalog.py after migration
    - Queried by ua-ops CLI for status reporting

Data Sources:
    - Populated from existing water_rates.source values + manual entries
"""

from sqlalchemy import (
    Date,
    DateTime,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from utility_api.models.base import SCHEMA, Base


class SourceCatalog(Base):
    """Registry entry for a known data source."""

    __tablename__ = "source_catalog"
    __table_args__ = {"schema": SCHEMA}

    source_key: Mapped[str] = mapped_column(
        String(50), primary_key=True,
        comment="Unique key matching water_rates.source (e.g., swrcb_ear_2022, efc_nc_2025)",
    )
    display_name: Mapped[str] = mapped_column(
        String(255),
        comment="Human-readable source name",
    )
    source_type: Mapped[str] = mapped_column(
        String(30),
        comment="bulk_government | bulk_survey | scraped | curated",
    )
    states_covered = mapped_column(
        ARRAY(String(2)),
        comment="Array of 2-letter state codes this source covers",
    )
    pwsid_count: Mapped[int | None] = mapped_column(
        Integer,
        comment="Number of PWSIDs this source covers (updated after ingest)",
    )
    vintage_start: Mapped[str | None] = mapped_column(
        Date,
        comment="Earliest data vintage date",
    )
    vintage_end: Mapped[str | None] = mapped_column(
        Date,
        comment="Latest data vintage date",
    )
    refresh_cadence: Mapped[str | None] = mapped_column(
        String(30),
        comment="annual | semi-annual | one-time | continuous",
    )
    last_ingested_at = mapped_column(
        DateTime(timezone=True), nullable=True,
        comment="When this source was last ingested into water_rates",
    )
    next_check_date = mapped_column(
        Date, nullable=True,
        comment="When to check for new data from this source",
    )
    ingest_module: Mapped[str | None] = mapped_column(
        String(100),
        comment="Python module path (e.g., utility_api.ingest.ear_ingest)",
    )
    ingest_command: Mapped[str | None] = mapped_column(
        String(100),
        comment="CLI command to run (e.g., ua-ingest ear --year 2022)",
    )
    notes: Mapped[str | None] = mapped_column(Text)

    created_at = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
