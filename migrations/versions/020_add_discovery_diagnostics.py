#!/usr/bin/env python3
"""
Sprint 24b: Discovery diagnostics table for cascade parse tracking

Purpose:
    Tracks the full cascade pipeline per PWSID: which URLs were discovered,
    deep-crawled, scored, and parsed — and which rank/source won. This data
    drives optimization decisions for the paid-tier bulk sweep.

    Key questions this table answers:
    - How often does a deep crawl child beat the original Serper URL in scoring?
    - How often does a rank #2 or #3 starting URL produce the winning page?
    - What is the parse success rate by discovery rank?
    - How necessary is proactive deep crawl?

Author: AI-Generated
Created: 2026-03-30
Modified: 2026-03-30

Revision ID: 020
Revises: 019
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "020"
down_revision = "019"
branch_labels = None
depends_on = None

SCHEMA = "utility"


def upgrade():
    # Per-PWSID cascade run summary
    op.create_table(
        "discovery_diagnostics",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("pwsid", sa.String(12), nullable=False, index=True),
        sa.Column("run_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),

        # Discovery phase
        sa.Column("starting_urls", sa.Integer, comment="Serper URLs fed in (1-3)"),
        sa.Column("deep_crawl_children", sa.Integer,
                  comment="New URLs found via deep crawl"),
        sa.Column("total_candidates", sa.Integer,
                  comment="starting_urls + deep_crawl_children after dedup"),

        # Scoring phase
        sa.Column("candidates_above_threshold", sa.Integer,
                  comment="Candidates scoring >50 after re-score"),

        # Parse cascade
        sa.Column("parse_attempts", sa.Integer, comment="How many parses tried (max 3)"),
        sa.Column("parse_success", sa.Boolean, comment="Did any parse succeed?"),
        sa.Column("winning_rank", sa.Integer, nullable=True,
                  comment="Rank (1-based in re-scored list) of successful parse. NULL if all failed."),
        sa.Column("winning_url", sa.Text, nullable=True),
        sa.Column("winning_source", sa.String(30), nullable=True,
                  comment="url_source of winner: serper | deep_crawl"),
        sa.Column("winning_discovery_rank", sa.Integer, nullable=True,
                  comment="Original Serper discovery_rank of the winning URL "
                          "(or its parent if deep_crawl child)"),
        sa.Column("winning_score", sa.Float, nullable=True,
                  comment="Re-scored relevance score of winning URL"),

        # Full candidate list for analysis
        sa.Column("candidate_details", JSONB, nullable=True,
                  comment="Array of {url, source, discovery_rank, score, "
                          "text_len, parsed, parse_result} for every candidate"),

        # Cost tracking
        sa.Column("total_parse_cost_usd", sa.Float, default=0.0),
        sa.Column("total_fetches", sa.Integer, comment="HTTP requests across all deep crawls"),

        schema=SCHEMA,
    )


def downgrade():
    op.drop_table("discovery_diagnostics", schema=SCHEMA)
