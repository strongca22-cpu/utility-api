#!/usr/bin/env python3
"""
Add source provenance columns to source_catalog

Purpose:
    Extends source_catalog with licensing, distribution tier, and attribution
    metadata. Every rate record FK's to source_catalog via source_key; these
    new columns let the API decide at query time whether a record is free,
    attributed, premium, or internal-only — and what license governs it.

    Tier definitions:
    - free_open:       No restrictions (EPA, Census)
    - free_attributed: Free with attribution, never paywalled (CC BY-NC-ND academic data)
    - premium:         Commercial redistribution, subscription required
    - internal_only:   Reference/validation, never redistributed

Author: AI-Generated
Created: 2026-03-26
Modified: 2026-03-26

Revision ID: 016
Revises: 015
"""

from alembic import op
import sqlalchemy as sa

revision = "016"
down_revision = "015"
branch_labels = None
depends_on = None

SCHEMA = "utility"


def upgrade():
    # Licensing fields
    op.add_column(
        "source_catalog",
        sa.Column(
            "license_spdx", sa.String(80), nullable=True,
            comment="SPDX identifier (e.g., CC-BY-NC-ND-4.0) or LicenseRef-*",
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "source_catalog",
        sa.Column("license_url", sa.Text, nullable=True, comment="URL to license text"),
        schema=SCHEMA,
    )
    op.add_column(
        "source_catalog",
        sa.Column(
            "license_summary", sa.Text, nullable=True,
            comment="Human-readable license summary",
        ),
        schema=SCHEMA,
    )

    # Redistribution flags
    op.add_column(
        "source_catalog",
        sa.Column(
            "commercial_redistribution", sa.Boolean, nullable=True,
            comment="Can records from this source appear in a paid product?",
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "source_catalog",
        sa.Column(
            "attribution_required", sa.Boolean, nullable=True,
            comment="Must we credit the source when serving records?",
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "source_catalog",
        sa.Column(
            "attribution_text", sa.Text, nullable=True,
            comment="Required attribution string for display",
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "source_catalog",
        sa.Column(
            "share_alike", sa.Boolean, nullable=True,
            comment="Does redistribution require the same license?",
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "source_catalog",
        sa.Column(
            "modifications_allowed", sa.Boolean, nullable=True,
            comment="Can we compute derived values from this data?",
        ),
        schema=SCHEMA,
    )

    # Distribution tier
    op.add_column(
        "source_catalog",
        sa.Column(
            "tier", sa.String(20), nullable=True,
            comment="free_open | free_attributed | premium | internal_only",
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "source_catalog",
        sa.Column(
            "tier_rationale", sa.Text, nullable=True,
            comment="Why this source is assigned to this tier",
        ),
        schema=SCHEMA,
    )

    # Temporal provenance
    op.add_column(
        "source_catalog",
        sa.Column(
            "data_vintage", sa.String(30), nullable=True,
            comment="When the rates were effective (e.g., 2019-2021)",
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "source_catalog",
        sa.Column(
            "collection_date", sa.String(30), nullable=True,
            comment="When the source collected/published the data",
        ),
        schema=SCHEMA,
    )

    # Provenance chain
    op.add_column(
        "source_catalog",
        sa.Column(
            "upstream_sources", sa.Text, nullable=True,
            comment="Comma-separated source_keys this was derived from",
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "source_catalog",
        sa.Column(
            "transformation", sa.String(30), nullable=True,
            comment="direct_ingest | geospatial_join | rate_computation | fuzzy_match | llm_extraction",
        ),
        schema=SCHEMA,
    )

    # Academic citation
    op.add_column(
        "source_catalog",
        sa.Column("citation_doi", sa.String(100), nullable=True, comment="DOI for citation"),
        schema=SCHEMA,
    )
    op.add_column(
        "source_catalog",
        sa.Column("source_url", sa.Text, nullable=True, comment="Primary URL for the source"),
        schema=SCHEMA,
    )


def downgrade():
    cols = [
        "license_spdx", "license_url", "license_summary",
        "commercial_redistribution", "attribution_required", "attribution_text",
        "share_alike", "modifications_allowed",
        "tier", "tier_rationale",
        "data_vintage", "collection_date",
        "upstream_sources", "transformation",
        "citation_doi", "source_url",
    ]
    for col in cols:
        op.drop_column("source_catalog", col, schema=SCHEMA)
