"""Add permits table for state regulatory permit layers.

Revision ID: 004
Revises: 003
Create Date: 2026-03-23

Purpose:
    Canonical permits table for VA DEQ (VWP, VPDES) and CA SWRCB (eWRIMS)
    permit data. Supports spatial radius queries via /permits endpoint.
    Stores both source-native categories and normalized category groups
    for cross-state filtering.
"""
from typing import Sequence, Union

import geoalchemy2
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "004"
down_revision: Union[str, None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "utility"


def upgrade() -> None:
    op.create_table(
        "permits",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("source", sa.String(30), nullable=False, comment="Data source: va_deq_vwp, va_deq_vpdes, ca_swrcb_ewrims"),
        sa.Column("permit_number", sa.Text, nullable=False, comment="State-assigned permit/application ID"),
        sa.Column("facility_name", sa.Text, nullable=True, comment="Facility or owner name"),
        sa.Column("source_category", sa.Text, nullable=True, comment="Category as delivered by data provider"),
        sa.Column("category_group", sa.String(30), nullable=True, comment="Normalized bucket: industrial, energy, municipal, etc."),
        sa.Column("use_codes", JSONB, nullable=True, comment="List of use codes (CA multi-use rights)"),
        sa.Column("status", sa.Text, nullable=True, comment="Permit status as delivered by source"),
        sa.Column("state_code", sa.String(2), nullable=True),
        sa.Column("county", sa.Text, nullable=True),
        sa.Column("issued_date", sa.Date, nullable=True),
        sa.Column("expiration_date", sa.Date, nullable=True),
        sa.Column("face_value_amount", sa.Float, nullable=True, comment="Permitted volume (CA eWRIMS)"),
        sa.Column("face_value_units", sa.Text, nullable=True, comment="Units for face_value_amount"),
        sa.Column("max_diversion_rate", sa.Float, nullable=True, comment="Max direct diversion rate (CA eWRIMS)"),
        sa.Column("max_diversion_units", sa.Text, nullable=True, comment="Units for max_diversion_rate"),
        sa.Column(
            "geom",
            geoalchemy2.Geometry("POINT", srid=4326),
            nullable=True,
        ),
        sa.Column("raw_attrs", JSONB, nullable=True, comment="Full original record from source"),
        sa.Column(
            "loaded_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        schema=SCHEMA,
    )
    # Note: GeoAlchemy2 auto-creates spatial index on the geom column
    op.create_index(
        "idx_permits_source", "permits", ["source"],
        schema=SCHEMA,
    )
    op.create_index(
        "idx_permits_state", "permits", ["state_code"],
        schema=SCHEMA,
    )
    op.create_index(
        "idx_permits_category_group", "permits", ["category_group"],
        schema=SCHEMA,
    )
    op.create_index(
        "idx_permits_permit_number", "permits", ["permit_number"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("permits", schema=SCHEMA)
