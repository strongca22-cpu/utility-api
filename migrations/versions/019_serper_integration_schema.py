#!/usr/bin/env python3
"""
Sprint 24: Serper integration schema changes

Purpose:
    1. Add search_engine column to search_log (distinguish Serper vs legacy SearXNG)
    2. Add ranked URL columns to search_log (url_rank_1/2/3 + score_rank_1/2/3)
    3. Create search_queries table (per-API-call billing audit trail)
    4. Add discovery_rank and discovery_score to scrape_registry

Author: AI-Generated
Created: 2026-03-29
Modified: 2026-03-29

Revision ID: 019
Revises: 018
"""

from alembic import op
import sqlalchemy as sa

revision = "019"
down_revision = "018"
branch_labels = None
depends_on = None

SCHEMA = "utility"


def upgrade():
    # --- 1. Extend search_log with search_engine + ranked URL columns ---

    op.add_column(
        "search_log",
        sa.Column(
            "search_engine", sa.String(20), server_default="searxng",
            comment="Search backend: searxng | serper",
        ),
        schema=SCHEMA,
    )

    # Ranked URL tracking: after a search, what were the top 3 candidates?
    # This lets us query parse success rate by rank position.
    for rank in (1, 2, 3):
        op.add_column(
            "search_log",
            sa.Column(
                f"url_rank_{rank}", sa.Text, nullable=True,
                comment=f"Rank {rank} URL written to scrape_registry",
            ),
            schema=SCHEMA,
        )
        op.add_column(
            "search_log",
            sa.Column(
                f"score_rank_{rank}", sa.Float, nullable=True,
                comment=f"Relevance score of rank {rank} URL",
            ),
            schema=SCHEMA,
        )

    # --- 2. Create search_queries table (per-API-call billing audit) ---

    op.create_table(
        "search_queries",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "pwsid", sa.String(12), nullable=True, index=True,
            comment="PWSID this query was run for (nullable for ad-hoc queries)",
        ),
        sa.Column(
            "query", sa.Text, nullable=False,
            comment="The search query string sent to the API",
        ),
        sa.Column(
            "search_engine", sa.String(20), nullable=False, server_default="serper",
            comment="Search backend: serper | searxng",
        ),
        sa.Column(
            "result_count", sa.Integer, nullable=True,
            comment="Number of organic results returned",
        ),
        sa.Column(
            "response_time_ms", sa.Integer, nullable=True,
            comment="API response time in milliseconds",
        ),
        sa.Column(
            "queried_at", sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            comment="When this query was executed",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "idx_search_queries_engine_date",
        "search_queries",
        ["search_engine", "queried_at"],
        schema=SCHEMA,
    )

    # --- 3. Add discovery_rank and discovery_score to scrape_registry ---

    op.add_column(
        "scrape_registry",
        sa.Column(
            "discovery_rank", sa.Integer, nullable=True,
            comment="Rank position from search discovery (1=best, 2, 3)",
        ),
        schema=SCHEMA,
    )

    # discovery_score may already exist as a DB-only column from Sprint 22.
    # Use raw SQL with IF NOT EXISTS to be safe.
    op.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{SCHEMA}'
                AND table_name = 'scrape_registry'
                AND column_name = 'discovery_score'
            ) THEN
                ALTER TABLE {SCHEMA}.scrape_registry
                ADD COLUMN discovery_score FLOAT;
                COMMENT ON COLUMN {SCHEMA}.scrape_registry.discovery_score
                IS 'URL relevance score from search discovery (0-100)';
            END IF;
        END $$;
    """)


def downgrade():
    op.drop_column("scrape_registry", "discovery_rank", schema=SCHEMA)
    # Don't drop discovery_score — it may have existed before this migration

    op.drop_index("idx_search_queries_engine_date", "search_queries", schema=SCHEMA)
    op.drop_table("search_queries", schema=SCHEMA)

    for rank in (3, 2, 1):
        op.drop_column("search_log", f"score_rank_{rank}", schema=SCHEMA)
        op.drop_column("search_log", f"url_rank_{rank}", schema=SCHEMA)
    op.drop_column("search_log", "search_engine", schema=SCHEMA)
