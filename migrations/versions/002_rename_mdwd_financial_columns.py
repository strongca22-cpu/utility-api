"""Rename MDWD financial columns to water-utility-specific names.

Revision ID: 002
Revises: 001
Create Date: 2026-03-23

Purpose:
    MDWD source data has both general government financials (Total_Revenue,
    Total_Expenditure) and water-utility-specific financials (Water_Utility_Revenue,
    Water_Util_Total_Exp). For a water utility intelligence API, the water-specific
    columns are the correct mapping. Rename DB columns to be explicit.

    Also renames debt_outstanding -> water_utility_debt for consistency.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "utility"
TABLE = "mdwd_financials"


def upgrade() -> None:
    op.alter_column(
        TABLE, "total_revenue",
        new_column_name="water_utility_revenue",
        schema=SCHEMA,
    )
    op.alter_column(
        TABLE, "total_expenditure",
        new_column_name="water_utility_expenditure",
        schema=SCHEMA,
    )
    op.alter_column(
        TABLE, "debt_outstanding",
        new_column_name="water_utility_debt",
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.alter_column(
        TABLE, "water_utility_revenue",
        new_column_name="total_revenue",
        schema=SCHEMA,
    )
    op.alter_column(
        TABLE, "water_utility_expenditure",
        new_column_name="total_expenditure",
        schema=SCHEMA,
    )
    op.alter_column(
        TABLE, "water_utility_debt",
        new_column_name="debt_outstanding",
        schema=SCHEMA,
    )
