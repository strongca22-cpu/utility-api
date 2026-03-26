#!/usr/bin/env python3
"""
Add duke_reference_rates table

Purpose:
    Separate table for Duke/Nicholas Institute rate data.
    CC BY-NC-ND 4.0 license prohibits commercial redistribution —
    this table is INTERNAL REFERENCE ONLY and must never be exposed
    through the API or bulk download.

Author: AI-Generated
Created: 2026-03-26
Modified: 2026-03-26

Revision ID: 015
Revises: 014
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "015"
down_revision = "014"
branch_labels = None
depends_on = None

SCHEMA = "utility"


def upgrade():
    op.create_table(
        "duke_reference_rates",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("pwsid", sa.String(12), sa.ForeignKey(f"{SCHEMA}.cws_boundaries.pwsid"), nullable=False, index=True),
        sa.Column("state_code", sa.String(2)),
        sa.Column("utility_name", sa.String(255)),
        sa.Column("service_area", sa.String(255)),
        sa.Column("effective_date", sa.Date),
        sa.Column("source_url", sa.Text),
        sa.Column("bill_frequency", sa.String(20)),
        sa.Column("rate_structure_type", sa.String(30)),
        sa.Column("fixed_charge_monthly", sa.Float, comment="Service charge for 5/8 meter, normalized to monthly"),
        sa.Column("volumetric_tiers", JSONB, comment="[{tier, min_gal, max_gal, rate_per_1000_gal}]"),
        sa.Column("tier_count", sa.Integer),
        sa.Column("bill_5ccf", sa.Float, comment="Calculated monthly bill at 5 CCF (3,740 gal)"),
        sa.Column("bill_10ccf", sa.Float, comment="Calculated monthly bill at 10 CCF (7,480 gal)"),
        sa.Column("bill_20ccf", sa.Float, comment="Calculated monthly bill at 20 CCF (14,960 gal)"),
        sa.Column("notes", sa.Text),
        sa.Column("license_restriction", sa.String(50), server_default="cc_by_nc_nd_4.0",
                   comment="License: CC BY-NC-ND 4.0 — NOT for commercial redistribution"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("pwsid", "effective_date", name="uq_duke_ref_pwsid_date"),
        schema=SCHEMA,
    )


def downgrade():
    op.drop_table("duke_reference_rates", schema=SCHEMA)
