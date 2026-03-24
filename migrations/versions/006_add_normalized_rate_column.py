"""Add normalized diversion rate column (gallons per day).

Revision ID: 006
Revises: 005
Create Date: 2026-03-23

Purpose:
    Add max_diversion_rate_gpd column to permits table for unit-normalized
    comparison of CA eWRIMS diversion rates. Source rates come in 7 different
    unit types; this column converts all to gallons per day (GPD).
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "006"
down_revision: Union[str, None] = "005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "utility"


def upgrade() -> None:
    op.add_column(
        "permits",
        sa.Column(
            "max_diversion_rate_gpd",
            sa.Float,
            nullable=True,
            comment="Max diversion rate normalized to gallons per day",
        ),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column("permits", "max_diversion_rate_gpd", schema=SCHEMA)
