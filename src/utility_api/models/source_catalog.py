#!/usr/bin/env python3
"""
Source Catalog Model

Purpose:
    Registry of all known bulk data sources, their state coverage,
    vintage, refresh cadence, ingest status, AND source provenance
    (licensing, distribution tier, attribution requirements).

    This is the single source of truth for "where did this data come
    from and what can we do with it?" Every rate record FK's to this
    table via source_key. The API uses the tier field to decide access
    control and the attribution fields for display.

    Tier definitions:
    - free_open:       No restrictions (EPA, Census)
    - free_attributed: Free with attribution, never paywalled (CC BY-NC-ND)
    - premium:         Commercial redistribution, subscription required
    - internal_only:   Reference/validation, never redistributed

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-26

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
    Boolean,
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

    # --- Source Provenance (migration 016) ---

    # Licensing
    license_spdx: Mapped[str | None] = mapped_column(
        String(80),
        comment="SPDX identifier (e.g., CC-BY-NC-ND-4.0) or LicenseRef-*",
    )
    license_url: Mapped[str | None] = mapped_column(
        Text, comment="URL to license text",
    )
    license_summary: Mapped[str | None] = mapped_column(
        Text, comment="Human-readable license summary",
    )

    # Redistribution flags
    commercial_redistribution: Mapped[bool | None] = mapped_column(
        Boolean,
        comment="Can records from this source appear in a paid product?",
    )
    attribution_required: Mapped[bool | None] = mapped_column(
        Boolean,
        comment="Must we credit the source when serving records?",
    )
    attribution_text: Mapped[str | None] = mapped_column(
        Text, comment="Required attribution string for display",
    )
    share_alike: Mapped[bool | None] = mapped_column(
        Boolean,
        comment="Does redistribution require the same license?",
    )
    modifications_allowed: Mapped[bool | None] = mapped_column(
        Boolean,
        comment="Can we compute derived values from this data?",
    )

    # Distribution tier
    tier: Mapped[str | None] = mapped_column(
        String(20),
        comment="free_open | free_attributed | premium | internal_only",
    )
    tier_rationale: Mapped[str | None] = mapped_column(
        Text, comment="Why this source is assigned to this tier",
    )

    # Temporal provenance
    data_vintage: Mapped[str | None] = mapped_column(
        String(30),
        comment="When the rates were effective (e.g., 2019-2021)",
    )
    collection_date: Mapped[str | None] = mapped_column(
        String(30),
        comment="When the source collected/published the data",
    )

    # Provenance chain
    upstream_sources: Mapped[str | None] = mapped_column(
        Text,
        comment="Comma-separated source_keys this was derived from",
    )
    transformation: Mapped[str | None] = mapped_column(
        String(30),
        comment="direct_ingest | geospatial_join | rate_computation | fuzzy_match | llm_extraction",
    )

    # Academic citation
    citation_doi: Mapped[str | None] = mapped_column(
        String(100), comment="DOI for citation",
    )
    source_url: Mapped[str | None] = mapped_column(
        Text, comment="Primary URL for the source",
    )

    created_at = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
