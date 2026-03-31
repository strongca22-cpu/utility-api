"""Widen batch_jobs.state_filter from VARCHAR(2) to VARCHAR(50).

The original VARCHAR(2) was intended for 2-letter state codes but prevents
batch submissions with descriptive labels (e.g., 'truncation_reprocess').
This caused a StringDataRightTruncation error on Scenario A submission
and the truncation reprocessing batch (Sprint 26).

Revision ID: 023
Revises: 022
Create Date: 2026-03-31
"""

from alembic import op
import sqlalchemy as sa

revision = "023"
down_revision = "022"
branch_labels = None
depends_on = None

SCHEMA = "utility"


def upgrade() -> None:
    op.alter_column(
        "batch_jobs",
        "state_filter",
        type_=sa.String(50),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.alter_column(
        "batch_jobs",
        "state_filter",
        type_=sa.String(2),
        schema=SCHEMA,
    )
