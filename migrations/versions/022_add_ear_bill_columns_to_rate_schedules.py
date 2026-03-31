#!/usr/bin/env python3
"""
Sprint 26: Add eAR bill snapshot columns to rate_schedules

Purpose:
    Adds bill_6ccf, bill_9ccf, bill_12ccf, bill_24ccf columns to rate_schedules.
    These columns hold official eAR (Electronic Annual Report) bill amounts at
    consumption levels used by California SWRCB surveys. Previously stored only
    in the legacy water_rates table.

    This is Phase 1 of the water_rates deprecation: ensuring rate_schedules
    can hold all data that water_rates stored, so water_rates can be retired.

Author: AI-Generated
Created: 2026-03-31
Modified: 2026-03-31

Changes:
    - ALTER TABLE utility.rate_schedules ADD COLUMN bill_6ccf FLOAT
    - ALTER TABLE utility.rate_schedules ADD COLUMN bill_9ccf FLOAT
    - ALTER TABLE utility.rate_schedules ADD COLUMN bill_12ccf FLOAT
    - ALTER TABLE utility.rate_schedules ADD COLUMN bill_24ccf FLOAT
"""

from alembic import op
import sqlalchemy as sa

# Revision identifiers
revision = "022"
down_revision = "021"
branch_labels = None
depends_on = None

SCHEMA = "utility"


def upgrade() -> None:
    op.add_column(
        "rate_schedules",
        sa.Column("bill_6ccf", sa.Float(),
                  comment="Monthly bill at 6 CCF (4,488 gal) — eAR benchmark"),
        schema=SCHEMA,
    )
    op.add_column(
        "rate_schedules",
        sa.Column("bill_9ccf", sa.Float(),
                  comment="Monthly bill at 9 CCF (6,732 gal) — eAR benchmark"),
        schema=SCHEMA,
    )
    op.add_column(
        "rate_schedules",
        sa.Column("bill_12ccf", sa.Float(),
                  comment="Monthly bill at 12 CCF (8,976 gal) — eAR benchmark"),
        schema=SCHEMA,
    )
    op.add_column(
        "rate_schedules",
        sa.Column("bill_24ccf", sa.Float(),
                  comment="Monthly bill at 24 CCF (17,952 gal) — eAR benchmark"),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column("rate_schedules", "bill_24ccf", schema=SCHEMA)
    op.drop_column("rate_schedules", "bill_12ccf", schema=SCHEMA)
    op.drop_column("rate_schedules", "bill_9ccf", schema=SCHEMA)
    op.drop_column("rate_schedules", "bill_6ccf", schema=SCHEMA)
