#!/usr/bin/env python3
"""
Add search_log table and search_attempted_at column

Purpose:
    Tracks SearXNG discovery attempts so failed searches are not
    infinitely re-queued. The search_log table records the full scoring
    funnel per search attempt. search_attempted_at on pwsid_coverage
    gates the priority queue (30-day re-search window).

Author: AI-Generated
Created: 2026-03-27
Modified: 2026-03-27

Revision ID: 017
Revises: 016
"""

from alembic import op
import sqlalchemy as sa

revision = "017"
down_revision = "016"
branch_labels = None
depends_on = None

SCHEMA = "utility"


def upgrade():
    # Add search_attempted_at to pwsid_coverage
    op.add_column(
        "pwsid_coverage",
        sa.Column("search_attempted_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )

    # Create search_log table
    op.create_table(
        "search_log",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("pwsid", sa.String(12), nullable=False),
        sa.Column("searched_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("queries_run", sa.Integer),
        sa.Column("raw_results_count", sa.Integer),
        sa.Column("deduped_count", sa.Integer),
        sa.Column("above_threshold_count", sa.Integer),
        sa.Column("near_miss_count", sa.Integer),
        sa.Column("below_threshold_count", sa.Integer),
        sa.Column("written_count", sa.Integer),
        sa.Column("best_score", sa.Float),
        sa.Column("best_url", sa.Text),
        sa.Column("notes", sa.Text),
        schema=SCHEMA,
    )
    op.create_index(
        "idx_search_log_pwsid",
        "search_log",
        ["pwsid"],
        schema=SCHEMA,
    )


def downgrade():
    op.drop_table("search_log", schema=SCHEMA)
    op.drop_column("pwsid_coverage", "search_attempted_at", schema=SCHEMA)
