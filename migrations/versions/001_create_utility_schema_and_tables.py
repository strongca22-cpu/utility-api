"""Create utility schema and all Sprint 1 tables.

Revision ID: 001
Revises: None
Create Date: 2026-03-23
"""
from typing import Sequence, Union

import geoalchemy2
import sqlalchemy as sa
from alembic import op

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "utility"


def upgrade() -> None:
    # Schema is created in env.py before migrations run

    # --- CWS Boundaries ---
    op.create_table(
        "cws_boundaries",
        sa.Column("pwsid", sa.String(12), primary_key=True),
        sa.Column("pws_name", sa.Text, nullable=True),
        sa.Column("state_code", sa.String(2), nullable=True),
        sa.Column("county_served", sa.Text, nullable=True),
        sa.Column("population_served", sa.Integer, nullable=True),
        sa.Column("source_type", sa.String(30), nullable=True),
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
        "idx_cws_geom", "cws_boundaries", ["geom"],
        schema=SCHEMA, postgresql_using="gist",
    )
    op.create_index(
        "idx_cws_state", "cws_boundaries", ["state_code"], schema=SCHEMA,
    )

    # --- Aqueduct Polygons ---
    op.create_table(
        "aqueduct_polygons",
        sa.Column("string_id", sa.String(50), primary_key=True),
        sa.Column("pfaf_id", sa.BigInteger, nullable=True),
        sa.Column("gid_1", sa.String(20), nullable=True),
        sa.Column("aqid", sa.Integer, nullable=True),
        sa.Column("bws_score", sa.Float, nullable=True),
        sa.Column("bws_label", sa.String(30), nullable=True),
        sa.Column("bwd_score", sa.Float, nullable=True),
        sa.Column("iav_score", sa.Float, nullable=True),
        sa.Column("sev_score", sa.Float, nullable=True),
        sa.Column("drr_score", sa.Float, nullable=True),
        sa.Column("w_awr_def_tot_score", sa.Float, nullable=True),
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
        "idx_aqueduct_geom", "aqueduct_polygons", ["geom"],
        schema=SCHEMA, postgresql_using="gist",
    )

    # --- SDWIS Systems ---
    op.create_table(
        "sdwis_systems",
        sa.Column(
            "pwsid", sa.String(12),
            sa.ForeignKey(f"{SCHEMA}.cws_boundaries.pwsid"),
            primary_key=True,
        ),
        sa.Column("pws_name", sa.Text, nullable=True),
        sa.Column("pws_type_code", sa.String(10), nullable=True),
        sa.Column("primary_source_code", sa.String(10), nullable=True),
        sa.Column("population_served_count", sa.Integer, nullable=True),
        sa.Column("service_connections_count", sa.Integer, nullable=True),
        sa.Column("owner_type_code", sa.String(10), nullable=True),
        sa.Column("is_wholesaler_ind", sa.String(1), nullable=True),
        sa.Column("activity_status_cd", sa.String(5), nullable=True),
        sa.Column("state_code", sa.String(2), nullable=True),
        sa.Column("violation_count_5yr", sa.Integer, nullable=True),
        sa.Column("health_violation_count_5yr", sa.Integer, nullable=True),
        sa.Column("last_violation_date", sa.Date, nullable=True),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        schema=SCHEMA,
    )

    # --- MDWD Financials ---
    op.create_table(
        "mdwd_financials",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "pwsid", sa.String(12),
            sa.ForeignKey(f"{SCHEMA}.cws_boundaries.pwsid"),
            index=True,
        ),
        sa.Column("fips_place_code", sa.String(7), nullable=True),
        sa.Column("year", sa.Integer, nullable=False),
        sa.Column("avg_monthly_bill_5ccf", sa.Float, nullable=True),
        sa.Column("avg_monthly_bill_10ccf", sa.Float, nullable=True),
        sa.Column("median_household_income", sa.Float, nullable=True),
        sa.Column("pct_below_poverty", sa.Float, nullable=True),
        sa.Column("total_revenue", sa.Float, nullable=True),
        sa.Column("total_expenditure", sa.Float, nullable=True),
        sa.Column("debt_outstanding", sa.Float, nullable=True),
        sa.Column("population", sa.Integer, nullable=True),
        sa.Column(
            "loaded_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("pwsid", "year", name="uq_mdwd_pwsid_year"),
        schema=SCHEMA,
    )

    # --- Pipeline Runs ---
    op.create_table(
        "pipeline_runs",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("step_name", sa.String(50), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("row_count", sa.Integer, nullable=True),
        sa.Column("status", sa.String(20), nullable=False, server_default="running"),
        sa.Column("notes", sa.Text, nullable=True),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("pipeline_runs", schema=SCHEMA)
    op.drop_table("mdwd_financials", schema=SCHEMA)
    op.drop_table("sdwis_systems", schema=SCHEMA)
    op.drop_table("aqueduct_polygons", schema=SCHEMA)
    op.drop_table("cws_boundaries", schema=SCHEMA)
