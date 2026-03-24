"""Add water_rates table for LLM-parsed rate schedules.

Sprint 3: stores structured rate tier data extracted from utility
websites via Claude API. Replaces the never-populated avg_monthly_bill
columns on mdwd_financials.

Revision ID: 007
Revises: 006
Create Date: 2026-03-23
"""

from alembic import op
import sqlalchemy as sa

revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None

SCHEMA = "utility"


def upgrade() -> None:
    op.create_table(
        "water_rates",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("pwsid", sa.String(12), sa.ForeignKey(f"{SCHEMA}.cws_boundaries.pwsid"),
                  nullable=False, index=True),
        # Utility identity
        sa.Column("utility_name", sa.String(255)),
        sa.Column("state_code", sa.String(2)),
        sa.Column("county", sa.String(100)),
        # Rate schedule metadata
        sa.Column("rate_effective_date", sa.Date()),
        sa.Column("rate_structure_type", sa.String(30)),
        sa.Column("rate_class", sa.String(30), server_default="residential"),
        sa.Column("billing_frequency", sa.String(20)),
        # Fixed charge
        sa.Column("fixed_charge_monthly", sa.Float()),
        sa.Column("meter_size_inches", sa.Float()),
        # Tier 1
        sa.Column("tier_1_limit_ccf", sa.Float()),
        sa.Column("tier_1_rate", sa.Float()),
        # Tier 2
        sa.Column("tier_2_limit_ccf", sa.Float()),
        sa.Column("tier_2_rate", sa.Float()),
        # Tier 3
        sa.Column("tier_3_limit_ccf", sa.Float()),
        sa.Column("tier_3_rate", sa.Float()),
        # Tier 4
        sa.Column("tier_4_limit_ccf", sa.Float()),
        sa.Column("tier_4_rate", sa.Float()),
        # Computed bills
        sa.Column("bill_5ccf", sa.Float()),
        sa.Column("bill_10ccf", sa.Float()),
        # Provenance
        sa.Column("source_url", sa.Text()),
        sa.Column("raw_text_hash", sa.String(64)),
        sa.Column("parse_confidence", sa.String(10)),
        sa.Column("parse_model", sa.String(50)),
        sa.Column("parse_notes", sa.Text()),
        # Timestamps
        sa.Column("scraped_at", sa.DateTime(timezone=True)),
        sa.Column("parsed_at", sa.DateTime(timezone=True)),
        sa.Column("loaded_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        # Constraints
        sa.UniqueConstraint("pwsid", "rate_effective_date", name="uq_water_rate_pwsid_date"),
        schema=SCHEMA,
    )

    # Drop the never-populated bill columns from mdwd_financials
    op.drop_column("mdwd_financials", "avg_monthly_bill_5ccf", schema=SCHEMA)
    op.drop_column("mdwd_financials", "avg_monthly_bill_10ccf", schema=SCHEMA)


def downgrade() -> None:
    # Restore bill columns on mdwd_financials
    op.add_column("mdwd_financials", sa.Column("avg_monthly_bill_5ccf", sa.Float()),
                   schema=SCHEMA)
    op.add_column("mdwd_financials", sa.Column("avg_monthly_bill_10ccf", sa.Float()),
                   schema=SCHEMA)
    op.drop_table("water_rates", schema=SCHEMA)
