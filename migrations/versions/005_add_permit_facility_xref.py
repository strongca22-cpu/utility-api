"""Add permit_facility_xref table for cross-referencing permits to SS facilities.

Revision ID: 005
Revises: 004
Create Date: 2026-03-23

Purpose:
    Links state regulatory permits to canonical Strong Strategic facility
    records. Stores match distance and confidence. Also flags unmatched
    data center permits as "candidate" facilities for future validation.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "005"
down_revision: Union[str, None] = "004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "utility"


def upgrade() -> None:
    op.create_table(
        "permit_facility_xref",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("permit_id", sa.Integer, nullable=False, comment="FK to utility.permits.id"),
        sa.Column("facility_id", sa.String(30), nullable=True, comment="FK to public.facilities.facility_id (NULL for candidates)"),
        sa.Column("match_type", sa.String(30), nullable=False, comment="spatial_match, candidate, manual"),
        sa.Column("match_distance_km", sa.Float, nullable=True, comment="Distance between permit and facility centroids"),
        sa.Column("match_confidence", sa.String(20), nullable=True, comment="high (<1km), medium (1-5km), low (>5km), unmatched"),
        sa.Column("candidate_status", sa.String(30), nullable=True, comment="For unmatched: data_center_candidate, pending_validation, confirmed, rejected"),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "idx_xref_permit_id", "permit_facility_xref", ["permit_id"],
        schema=SCHEMA,
    )
    op.create_index(
        "idx_xref_facility_id", "permit_facility_xref", ["facility_id"],
        schema=SCHEMA,
    )
    op.create_index(
        "idx_xref_candidate_status", "permit_facility_xref", ["candidate_status"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("permit_facility_xref", schema=SCHEMA)
