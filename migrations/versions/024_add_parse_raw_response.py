"""Add last_parse_raw_response to scrape_registry.

Stores the raw LLM response text from each parse attempt so that prompt
iteration failures can be diagnosed without re-running individual tasks.
Nullable TEXT — existing rows unaffected. Capped at 50k chars at write time.

Revision ID: 024
Revises: 023
Create Date: 2026-04-01
"""

from alembic import op
import sqlalchemy as sa

revision = "024"
down_revision = "023"
branch_labels = None
depends_on = None

SCHEMA = "utility"


def upgrade() -> None:
    op.add_column(
        "scrape_registry",
        sa.Column(
            "last_parse_raw_response",
            sa.Text(),
            nullable=True,
            comment="Raw LLM response text from last parse attempt (for debugging prompt changes)",
        ),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column("scrape_registry", "last_parse_raw_response", schema=SCHEMA)
