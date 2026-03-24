"""Add source column and eAR bill columns to water_rates.

Sprint 4: Supports multiple rate records per utility from different sources
(scraped_llm, swrcb_ear, owrs, etc.). Adds official eAR bill snapshot
columns at 6/9/12/24 HCF. Updates unique constraint to include source.

Revision ID: 008
Revises: 007
Create Date: 2026-03-24
"""

from alembic import op
import sqlalchemy as sa

revision = "008"
down_revision = "007"
branch_labels = None
depends_on = None

SCHEMA = "utility"


def upgrade() -> None:
    # --- Add source column ---
    op.add_column(
        "water_rates",
        sa.Column(
            "source", sa.String(50), server_default="scraped_llm",
            comment="Data source: scraped_llm | swrcb_ear_2020 | swrcb_ear_2021 | swrcb_ear_2022 | owrs",
        ),
        schema=SCHEMA,
    )

    # Backfill existing rows (all are from LLM scraping pipeline)
    op.execute(f"UPDATE {SCHEMA}.water_rates SET source = 'scraped_llm' WHERE source IS NULL")

    # --- Add eAR bill snapshot columns (official state-reported values) ---
    op.add_column(
        "water_rates",
        sa.Column("bill_6ccf", sa.Float(), comment="Monthly bill at 6 CCF (eAR: WR6HCFDWCharges)"),
        schema=SCHEMA,
    )
    op.add_column(
        "water_rates",
        sa.Column("bill_9ccf", sa.Float(), comment="Monthly bill at 9 CCF (eAR: WR9HCFDWCharges)"),
        schema=SCHEMA,
    )
    op.add_column(
        "water_rates",
        sa.Column("bill_12ccf", sa.Float(), comment="Monthly bill at 12 CCF (eAR: WR12HCFDWCharges)"),
        schema=SCHEMA,
    )
    op.add_column(
        "water_rates",
        sa.Column("bill_24ccf", sa.Float(), comment="Monthly bill at 24 CCF (eAR: WR24HCFDWCharges)"),
        schema=SCHEMA,
    )

    # --- Update unique constraint to include source ---
    # Drop old constraint (pwsid + date only)
    op.drop_constraint("uq_water_rate_pwsid_date", "water_rates", schema=SCHEMA)

    # Create new constraint (pwsid + date + source) — allows duplicates from different sources
    op.create_unique_constraint(
        "uq_water_rate_pwsid_date_source",
        "water_rates",
        ["pwsid", "rate_effective_date", "source"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_constraint("uq_water_rate_pwsid_date_source", "water_rates", schema=SCHEMA)
    op.create_unique_constraint(
        "uq_water_rate_pwsid_date", "water_rates",
        ["pwsid", "rate_effective_date"], schema=SCHEMA,
    )
    op.drop_column("water_rates", "bill_24ccf", schema=SCHEMA)
    op.drop_column("water_rates", "bill_12ccf", schema=SCHEMA)
    op.drop_column("water_rates", "bill_9ccf", schema=SCHEMA)
    op.drop_column("water_rates", "bill_6ccf", schema=SCHEMA)
    op.drop_column("water_rates", "source", schema=SCHEMA)
