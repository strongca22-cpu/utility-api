"""Migrate pwsid_coverage from materialized view to regular table, add ingest_log.

Sprint 12: The mat view is read-only. We need to write scrape_status and
priority_tier per PWSID. Also adds ingest_log table for agent run tracking.

Changes:
- DROP MATERIALIZED VIEW pwsid_coverage
- CREATE TABLE pwsid_coverage with same columns + scrape_status + priority_tier
- Populate from the same query the mat view used
- CREATE TABLE ingest_log (richer agent audit trail)

Revision ID: 011
Revises: 010
Create Date: 2026-03-24
"""

from alembic import op
import sqlalchemy as sa

revision = "011"
down_revision = "010"
branch_labels = None
depends_on = None

SCHEMA = "utility"


def upgrade() -> None:
    # --- Drop materialized view ---
    op.execute(f"DROP MATERIALIZED VIEW IF EXISTS {SCHEMA}.pwsid_coverage")

    # --- Create pwsid_coverage as a regular table ---
    op.create_table(
        "pwsid_coverage",
        sa.Column("pwsid", sa.String(50), primary_key=True),
        sa.Column("state_code", sa.String(2), index=True),
        sa.Column("pws_name", sa.String(255)),
        # Rate coverage (derived — recomputed by refresh)
        sa.Column("has_rate_data", sa.Boolean(), server_default="false"),
        sa.Column("rate_source_count", sa.Integer(), server_default="0"),
        sa.Column("rate_sources", sa.Text()),  # comma-separated source keys
        sa.Column("last_rate_loaded_at", sa.DateTime(timezone=True)),
        # Best estimate (derived — recomputed by refresh)
        sa.Column("best_source", sa.String(50)),
        sa.Column("best_bill_10ccf", sa.Float()),
        sa.Column("best_confidence", sa.String(10)),
        # SDWIS coverage (derived — recomputed by refresh)
        sa.Column("has_sdwis", sa.Boolean(), server_default="false"),
        sa.Column("population_served", sa.Integer()),
        sa.Column("primary_source_code", sa.String(10)),
        sa.Column("owner_type_code", sa.String(10)),
        # Mutable operational columns (NOT overwritten by refresh)
        sa.Column("scrape_status", sa.String(30), server_default="not_attempted",
                  comment="not_attempted | url_discovered | attempted_failed | succeeded | stale"),
        sa.Column("priority_tier", sa.Integer(),
                  comment="1-4 priority ranking (nullable, populated later)"),
        schema=SCHEMA,
    )

    op.create_index("idx_pwsid_coverage_state", "pwsid_coverage", ["state_code"], schema=SCHEMA)
    op.create_index("idx_pwsid_coverage_has_rate", "pwsid_coverage", ["has_rate_data"], schema=SCHEMA)
    op.create_index("idx_pwsid_coverage_scrape_status", "pwsid_coverage", ["scrape_status"], schema=SCHEMA)

    # --- Populate from same query the mat view used ---
    op.execute(f"""
        INSERT INTO {SCHEMA}.pwsid_coverage (
            pwsid, state_code, pws_name,
            has_rate_data, rate_source_count, rate_sources, last_rate_loaded_at,
            best_source, best_bill_10ccf, best_confidence,
            has_sdwis, population_served, primary_source_code, owner_type_code
        )
        SELECT
            c.pwsid,
            c.state_code,
            c.pws_name,
            (EXISTS (
                SELECT 1 FROM {SCHEMA}.water_rates wr WHERE wr.pwsid = c.pwsid
            )),
            (
                SELECT COUNT(DISTINCT wr.source)
                FROM {SCHEMA}.water_rates wr
                WHERE wr.pwsid = c.pwsid
            ),
            (
                SELECT STRING_AGG(DISTINCT wr.source, ',' ORDER BY wr.source)
                FROM {SCHEMA}.water_rates wr
                WHERE wr.pwsid = c.pwsid
            ),
            (
                SELECT MAX(wr.loaded_at)
                FROM {SCHEMA}.water_rates wr
                WHERE wr.pwsid = c.pwsid
            ),
            be.selected_source,
            be.bill_estimate_10ccf,
            be.confidence,
            (s.pwsid IS NOT NULL),
            s.population_served_count,
            s.primary_source_code,
            s.owner_type_code
        FROM {SCHEMA}.cws_boundaries c
        LEFT JOIN {SCHEMA}.rate_best_estimate be ON be.pwsid = c.pwsid
        LEFT JOIN {SCHEMA}.sdwis_systems s ON s.pwsid = c.pwsid
    """)

    # --- Create ingest_log table ---
    op.create_table(
        "ingest_log",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("agent_name", sa.String(50), nullable=False,
                  comment="Agent that produced this log entry"),
        sa.Column("source_key", sa.String(50),
                  comment="source_catalog key if applicable"),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(20), nullable=False, server_default="running",
                  comment="running | success | failed | partial"),
        sa.Column("rows_affected", sa.Integer(), server_default="0"),
        sa.Column("notes", sa.Text()),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("ingest_log", schema=SCHEMA)
    op.drop_table("pwsid_coverage", schema=SCHEMA)

    # Recreate materialized view
    op.execute(f"""
        CREATE MATERIALIZED VIEW {SCHEMA}.pwsid_coverage AS
        SELECT
            c.pwsid, c.state_code, c.pws_name,
            (EXISTS (SELECT 1 FROM {SCHEMA}.water_rates wr WHERE wr.pwsid = c.pwsid)) AS has_rate_data,
            (SELECT COUNT(DISTINCT wr.source) FROM {SCHEMA}.water_rates wr WHERE wr.pwsid = c.pwsid) AS rate_source_count,
            (SELECT ARRAY_AGG(DISTINCT wr.source ORDER BY wr.source) FROM {SCHEMA}.water_rates wr WHERE wr.pwsid = c.pwsid) AS rate_sources,
            (SELECT MAX(wr.loaded_at) FROM {SCHEMA}.water_rates wr WHERE wr.pwsid = c.pwsid) AS last_rate_loaded_at,
            be.selected_source AS best_source, be.bill_estimate_10ccf AS best_bill_10ccf, be.confidence AS best_confidence,
            (s.pwsid IS NOT NULL) AS has_sdwis, s.population_served_count, s.primary_source_code, s.owner_type_code
        FROM {SCHEMA}.cws_boundaries c
        LEFT JOIN {SCHEMA}.rate_best_estimate be ON be.pwsid = c.pwsid
        LEFT JOIN {SCHEMA}.sdwis_systems s ON s.pwsid = c.pwsid
        WITH DATA
    """)
    op.execute(f"CREATE UNIQUE INDEX idx_pwsid_coverage_pwsid ON {SCHEMA}.pwsid_coverage (pwsid)")
