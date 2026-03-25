"""Add rate_schedules table — canonical rate schema with JSONB tiers.

Sprint 11: Replaces the fixed 4-tier columns in water_rates with flexible
JSONB arrays. water_rates is kept as legacy/audit. rate_schedules becomes
the source of truth for rate data and best-estimate computation.

Key changes from water_rates:
- volumetric_tiers: JSONB array (any number of tiers)
- fixed_charges: JSONB array (multiple fixed charges)
- surcharges: JSONB array (drought/seasonal)
- bill_20ccf: new bill snapshot
- conservation_signal: highest/lowest tier ratio
- tier_count: convenience column
- needs_review + review_reason: quality flags

Revision ID: 010
Revises: 009
Create Date: 2026-03-24
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "010"
down_revision = "009"
branch_labels = None
depends_on = None

SCHEMA = "utility"


def upgrade() -> None:
    op.create_table(
        "rate_schedules",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        # Identity
        sa.Column("pwsid", sa.String(12),
                  sa.ForeignKey(f"{SCHEMA}.cws_boundaries.pwsid"),
                  nullable=False, index=True),
        sa.Column("source_key", sa.String(50), nullable=False,
                  comment="FK → source_catalog.source_key"),
        sa.Column("vintage_date", sa.Date(),
                  comment="When these rates were effective"),
        sa.Column("customer_class", sa.String(30), server_default="residential",
                  comment="residential | commercial | industrial | irrigation"),
        # Rate structure
        sa.Column("billing_frequency", sa.String(20),
                  comment="monthly | bimonthly | quarterly"),
        sa.Column("rate_structure_type", sa.String(30),
                  comment="flat | uniform | increasing_block | budget_based | seasonal"),
        # JSONB tier storage
        sa.Column("fixed_charges", JSONB(),
                  comment="[{name, amount, frequency, meter_size}]"),
        sa.Column("volumetric_tiers", JSONB(),
                  comment="[{tier, min_gal, max_gal, rate_per_1000_gal}]"),
        sa.Column("surcharges", JSONB(),
                  comment="[{name, rate_per_1000_gal, condition}]"),
        # Bill snapshots
        sa.Column("bill_5ccf", sa.Float(),
                  comment="Monthly bill at 5 CCF (3,740 gal)"),
        sa.Column("bill_10ccf", sa.Float(),
                  comment="Monthly bill at 10 CCF (7,480 gal)"),
        sa.Column("bill_20ccf", sa.Float(),
                  comment="Monthly bill at 20 CCF (14,960 gal)"),
        # Derived metrics
        sa.Column("conservation_signal", sa.Float(),
                  comment="Ratio: highest/lowest tier rate. >1 = conservation pricing"),
        sa.Column("tier_count", sa.Integer(),
                  comment="Number of volumetric tiers"),
        # Provenance
        sa.Column("source_url", sa.Text()),
        sa.Column("scrape_timestamp", sa.DateTime(timezone=True)),
        sa.Column("confidence", sa.String(10),
                  comment="high | medium | low | failed"),
        sa.Column("raw_text_hash", sa.String(64),
                  comment="SHA-256 for change detection"),
        sa.Column("parse_model", sa.String(50)),
        sa.Column("parse_notes", sa.Text()),
        # Review flags
        sa.Column("needs_review", sa.Boolean(), server_default="false",
                  comment="Flagged for manual review"),
        sa.Column("review_reason", sa.Text()),
        # Timestamps
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        # Constraints
        sa.UniqueConstraint(
            "pwsid", "source_key", "vintage_date", "customer_class",
            name="uq_rate_schedule_pwsid_source_vintage_class",
        ),
        schema=SCHEMA,
    )

    # GIN index on JSONB columns for fast containment queries
    op.execute(f"""
        CREATE INDEX idx_rate_schedules_tiers_gin
        ON {SCHEMA}.rate_schedules USING GIN (volumetric_tiers)
    """)


def downgrade() -> None:
    op.drop_table("rate_schedules", schema=SCHEMA)
