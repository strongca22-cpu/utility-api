"""Add city column to sdwis_systems for domain guessing.

Sprint 17: city_name from ECHO SDWA bulk download enables city-based
domain pattern generation in the DomainGuesser.

Changes:
- ADD city VARCHAR(100) to utility.sdwis_systems

Revision ID: 014
Revises: 013
Create Date: 2026-03-25
"""

from alembic import op
import sqlalchemy as sa

revision: str = "014"
down_revision: str = "013"
branch_labels = None
depends_on = None

SCHEMA = "utility"


def upgrade() -> None:
    op.add_column(
        "sdwis_systems",
        sa.Column("city", sa.String(100), nullable=True),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column("sdwis_systems", "city", schema=SCHEMA)
