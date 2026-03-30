#!/usr/bin/env python3
"""
Sprint 25: Add source_url to rate_best_estimate + tier label backfill

Purpose:
    1. Adds source_url column to rate_best_estimate so that scraped rates
       carry the original utility rate page URL through to the final output.
       This enables spot-check workflows on the dashboard.

    2. Backfills tier labels in source_catalog:
       - Government/EFC/state sources → 'bulk'
       - scraped_llm → 'premium'
       - Duke stays 'free_attributed' (already set)

Author: AI-Generated
Created: 2026-03-30
Modified: 2026-03-30

Changes:
    - ALTER TABLE utility.rate_best_estimate ADD COLUMN source_url TEXT
    - UPDATE utility.source_catalog SET tier = 'bulk' WHERE source_key LIKE 'efc_%' OR ...
    - UPDATE utility.source_catalog SET tier = 'premium' WHERE source_key = 'scraped_llm'
"""

from alembic import op
import sqlalchemy as sa

# Revision identifiers
revision = "021"
down_revision = "020"
branch_labels = None
depends_on = None

SCHEMA = "utility"


def upgrade():
    """Add source_url to rate_best_estimate and backfill tier labels."""

    # --- Task 2: source_url column ---
    op.add_column(
        "rate_best_estimate",
        sa.Column(
            "source_url",
            sa.Text(),
            nullable=True,
            comment="Original rate page URL (from rate_schedules, populated for scraped sources)",
        ),
        schema=SCHEMA,
    )

    # --- Task 1: Tier label backfill ---
    conn = op.get_bind()

    # Government bulk sources → 'bulk'
    conn.execute(sa.text(f"""
        UPDATE {SCHEMA}.source_catalog
        SET tier = 'bulk',
            tier_rationale = COALESCE(tier_rationale, 'Government/institutional bulk data source')
        WHERE (
            source_key LIKE 'efc_%%'
            OR source_key LIKE 'swrcb_%%'
            OR source_key LIKE 'nm_nmed_%%'
            OR source_key LIKE 'in_iurc_%%'
            OR source_key LIKE 'ky_psc_%%'
            OR source_key LIKE 'wv_psc_%%'
            OR source_key LIKE 'tx_tml_%%'
            OR source_key = 'owrs'
        )
        AND (tier IS NULL OR tier = '')
    """))

    # scraped_llm → 'premium'
    conn.execute(sa.text(f"""
        UPDATE {SCHEMA}.source_catalog
        SET tier = 'premium',
            tier_rationale = COALESCE(tier_rationale, 'Algorithmically discovered and LLM-parsed from utility websites')
        WHERE source_key = 'scraped_llm'
        AND (tier IS NULL OR tier = '')
    """))

    # Update Duke tier_rationale to remove "internal only" framing
    conn.execute(sa.text(f"""
        UPDATE {SCHEMA}.source_catalog
        SET tier_rationale = 'Free-tier attributed source. CC BY-NC-ND 4.0. Has 10CCF bill estimates for 10 states.'
        WHERE source_key = 'duke_nieps_10state'
    """))


def downgrade():
    """Remove source_url column and revert tier labels."""

    op.drop_column("rate_best_estimate", "source_url", schema=SCHEMA)

    conn = op.get_bind()

    # Revert tier labels to NULL
    conn.execute(sa.text(f"""
        UPDATE {SCHEMA}.source_catalog
        SET tier = NULL, tier_rationale = NULL
        WHERE tier IN ('bulk', 'premium')
    """))

    # Revert Duke rationale
    conn.execute(sa.text(f"""
        UPDATE {SCHEMA}.source_catalog
        SET tier_rationale = NULL
        WHERE source_key = 'duke_nieps_10state'
    """))
