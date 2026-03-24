"""Add county_boundaries table for Census TIGER county polygons.

Revision ID: 003
Revises: 002
Create Date: 2026-03-23

Purpose:
    Reusable county boundary layer from Census TIGER/Line 2024.
    Supports spatial join for county enrichment of CWS systems not
    covered by SDWIS geographic areas, and future spatial lookups.
"""
from typing import Sequence, Union

import geoalchemy2
import sqlalchemy as sa
from alembic import op

revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "utility"


def upgrade() -> None:
    op.create_table(
        "county_boundaries",
        sa.Column("geoid", sa.String(5), primary_key=True),
        sa.Column("state_fips", sa.String(2), nullable=False),
        sa.Column("county_fips", sa.String(3), nullable=False),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("name_lsad", sa.Text, nullable=True),
        sa.Column("class_fp", sa.String(2), nullable=True),
        sa.Column("aland", sa.BigInteger, nullable=True),
        sa.Column("awater", sa.BigInteger, nullable=True),
        sa.Column(
            "geom",
            geoalchemy2.Geometry("MULTIPOLYGON", srid=4326),
            nullable=False,
        ),
        sa.Column(
            "loaded_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "idx_county_geom", "county_boundaries", ["geom"],
        schema=SCHEMA, postgresql_using="gist",
    )
    op.create_index(
        "idx_county_state_fips", "county_boundaries", ["state_fips"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("county_boundaries", schema=SCHEMA)
