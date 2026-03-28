#!/usr/bin/env python3
"""
Add scraped_text and url_quality columns to scrape_registry

Purpose:
    Sprint 23 — Pipeline Flow Fix.

    scraped_text (TEXT): Persists raw scraped content so ParseAgent can
    read it from DB instead of requiring an in-memory handoff. Eliminates
    data loss when the calling script chains incorrectly or crashes
    between scrape and parse.

    url_quality (VARCHAR 20): Classifies each registry entry by parse
    outcome so sweeps can skip confirmed junk. Tiers: confirmed_rate_page,
    parse_failed, probable_junk, blacklisted, unknown.

Author: AI-Generated
Created: 2026-03-28
Modified: 2026-03-28

Revision ID: 018
Revises: 017
"""

from alembic import op
import sqlalchemy as sa

revision = "018"
down_revision = "017"
branch_labels = None
depends_on = None

SCHEMA = "utility"


def upgrade():
    # Persist raw scraped text for decoupled parse
    op.add_column(
        "scrape_registry",
        sa.Column("scraped_text", sa.Text, nullable=True),
        schema=SCHEMA,
    )

    # URL quality classification tier
    op.add_column(
        "scrape_registry",
        sa.Column(
            "url_quality",
            sa.String(20),
            server_default="unknown",
            nullable=True,
        ),
        schema=SCHEMA,
    )

    # Backfill url_quality from existing parse results
    op.execute(f"""
        UPDATE {SCHEMA}.scrape_registry
        SET url_quality = 'confirmed_rate_page'
        WHERE last_parse_result = 'success'
    """)

    op.execute(f"""
        UPDATE {SCHEMA}.scrape_registry
        SET url_quality = 'blacklisted'
        WHERE status = 'dead'
    """)

    op.execute(f"""
        UPDATE {SCHEMA}.scrape_registry
        SET url_quality = 'parse_failed'
        WHERE last_parse_result IS NOT NULL
          AND last_parse_result NOT IN ('success', 'skipped')
          AND status != 'dead'
    """)


def downgrade():
    op.drop_column("scrape_registry", "url_quality", schema=SCHEMA)
    op.drop_column("scrape_registry", "scraped_text", schema=SCHEMA)
