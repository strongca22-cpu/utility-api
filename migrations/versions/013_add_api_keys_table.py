"""Add api_keys table for API authentication and rate limiting.

Sprint 15: API productization — key-based auth with tier-based rate limits.

Changes:
- CREATE api_keys table with key hash, tier, usage tracking

Revision ID: 013
Revises: 012
Create Date: 2026-03-25
"""

from alembic import op
import sqlalchemy as sa

revision = "013"
down_revision = "012"
branch_labels = None
depends_on = None

SCHEMA = "utility"


def upgrade() -> None:
    op.create_table(
        "api_keys",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "key_hash", sa.String(64), unique=True, nullable=False,
            comment="SHA-256 hash of the API key (never store plaintext)",
        ),
        sa.Column(
            "name", sa.String(100), nullable=False,
            comment="Human-readable label (e.g., 'pilot-customer-1')",
        ),
        sa.Column(
            "tier", sa.String(20), nullable=False, server_default="free",
            comment="Rate limit tier: free (100/day), basic (1000/day), premium (10000/day)",
        ),
        sa.Column(
            "is_active", sa.Boolean, nullable=False, server_default=sa.text("true"),
            comment="Whether this key is active (can be revoked)",
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            server_default=sa.func.now(), nullable=False,
        ),
        sa.Column(
            "last_used_at", sa.DateTime(timezone=True), nullable=True,
            comment="Timestamp of last API request with this key",
        ),
        sa.Column(
            "request_count", sa.BigInteger, nullable=False, server_default=sa.text("0"),
            comment="Lifetime request count",
        ),
        sa.Column(
            "daily_request_count", sa.Integer, nullable=False, server_default=sa.text("0"),
            comment="Requests today (reset by middleware at day boundary)",
        ),
        sa.Column(
            "daily_reset_date", sa.Date, nullable=True,
            comment="Date when daily_request_count was last reset",
        ),
        schema=SCHEMA,
    )

    # Index for fast key lookup
    op.create_index(
        "ix_api_keys_key_hash", "api_keys", ["key_hash"],
        schema=SCHEMA, unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_api_keys_key_hash", table_name="api_keys", schema=SCHEMA)
    op.drop_table("api_keys", schema=SCHEMA)
