"""Add batch_jobs table and source freshness checking columns.

Sprint 14: Batch API support and bulk source freshness checking.

Changes:
- ADD source_url and last_content_hash columns to source_catalog
- CREATE batch_jobs table for tracking Anthropic Batch API submissions

Revision ID: 012
Revises: 011
Create Date: 2026-03-25
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "012"
down_revision = "011"
branch_labels = None
depends_on = None

SCHEMA = "utility"


def upgrade() -> None:
    # --- Add columns to source_catalog for freshness checking ---
    op.add_column(
        "source_catalog",
        sa.Column(
            "source_url", sa.Text(),
            comment="Primary URL to check for new data availability",
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "source_catalog",
        sa.Column(
            "last_content_hash", sa.String(64),
            comment="SHA-256 hash of last-fetched page content",
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "source_catalog",
        sa.Column(
            "check_interval_days", sa.Integer(), server_default="90",
            comment="Days between freshness checks",
        ),
        schema=SCHEMA,
    )

    # --- Populate source_url for known sources ---
    op.execute(f"""
        UPDATE {SCHEMA}.source_catalog SET source_url =
            'https://www.hydroshare.org/resource/9e731c8bf7e24d1daaee5b3ab2f68f1f/'
        WHERE source_key LIKE 'swrcb_ear_%'
    """)
    op.execute(f"""
        UPDATE {SCHEMA}.source_catalog SET source_url =
            'https://efc.sog.unc.edu/water-and-sewer-rates-dashboard/'
        WHERE source_key = 'efc_nc_2025'
    """)

    # --- Create batch_jobs table ---
    op.create_table(
        "batch_jobs",
        sa.Column("batch_id", sa.String(100), primary_key=True,
                  comment="Anthropic Batch API batch ID"),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now(),
                  comment="When the batch was submitted"),
        sa.Column("task_count", sa.Integer(), nullable=False,
                  comment="Number of parse tasks in this batch"),
        sa.Column("status", sa.String(20), nullable=False,
                  server_default="'pending'",
                  comment="pending | in_progress | completed | failed | processed"),
        sa.Column("completed_at", sa.DateTime(timezone=True),
                  comment="When Anthropic reported batch complete"),
        sa.Column("processed_at", sa.DateTime(timezone=True),
                  comment="When we processed the results"),
        sa.Column("results_summary", JSONB,
                  comment="Summary: {succeeded, failed, total_cost}"),
        sa.Column("task_details", JSONB,
                  comment="Array of {pwsid, registry_id, raw_text, source_url, content_type} per task"),
        sa.Column("state_filter", sa.String(2),
                  comment="State filter used when generating this batch"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        schema=SCHEMA,
    )

    op.create_index(
        "idx_batch_jobs_status",
        "batch_jobs",
        ["status"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("batch_jobs", schema=SCHEMA)
    op.drop_column("source_catalog", "check_interval_days", schema=SCHEMA)
    op.drop_column("source_catalog", "last_content_hash", schema=SCHEMA)
    op.drop_column("source_catalog", "source_url", schema=SCHEMA)
