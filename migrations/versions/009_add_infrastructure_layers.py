"""Add infrastructure layers: source_catalog, scrape_registry, rate_best_estimate, pwsid_coverage.

Sprint 10: Data Operations Manager (Layer B) + Scrape Registry table (Layer A).

- source_catalog: registry of all known bulk data sources and their operational state
- scrape_registry: per-URL tracking for the scraping pipeline
- rate_best_estimate: ORM-managed replacement for the raw-SQL table created by build_best_estimate.py
- pwsid_coverage: materialized view joining CWS + rates + SDWIS for coverage reporting

Note: pwsid_coverage is a materialized view for Sprint 10. Sprint 12 will migrate
it to a regular table when mutable operational columns (scrape_status, priority_tier)
are needed.

Revision ID: 009
Revises: 008
Create Date: 2026-03-24
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY

revision = "009"
down_revision = "008"
branch_labels = None
depends_on = None

SCHEMA = "utility"


def upgrade() -> None:
    # --- source_catalog ---
    op.create_table(
        "source_catalog",
        sa.Column("source_key", sa.String(50), primary_key=True,
                  comment="Unique key matching water_rates.source"),
        sa.Column("display_name", sa.String(255), nullable=False,
                  comment="Human-readable source name"),
        sa.Column("source_type", sa.String(30), nullable=False,
                  comment="bulk_government | bulk_survey | scraped | curated"),
        sa.Column("states_covered", ARRAY(sa.String(2)),
                  comment="Array of 2-letter state codes"),
        sa.Column("pwsid_count", sa.Integer(),
                  comment="Number of PWSIDs this source covers"),
        sa.Column("vintage_start", sa.Date(),
                  comment="Earliest data vintage"),
        sa.Column("vintage_end", sa.Date(),
                  comment="Latest data vintage"),
        sa.Column("refresh_cadence", sa.String(30),
                  comment="annual | semi-annual | one-time | continuous"),
        sa.Column("last_ingested_at", sa.DateTime(timezone=True),
                  comment="When this source was last ingested"),
        sa.Column("next_check_date", sa.Date(),
                  comment="When to check for new data"),
        sa.Column("ingest_module", sa.String(100),
                  comment="Python module path"),
        sa.Column("ingest_command", sa.String(100),
                  comment="CLI command to run"),
        sa.Column("notes", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        schema=SCHEMA,
    )

    # --- scrape_registry ---
    op.create_table(
        "scrape_registry",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        # Identity
        sa.Column("pwsid", sa.String(12), index=True,
                  comment="PWSID (nullable for discovery-phase entries)"),
        sa.Column("url", sa.Text(), nullable=False,
                  comment="The URL being tracked"),
        sa.Column("url_source", sa.String(30),
                  comment="searxng | curated | civicplus_crawler | manual"),
        sa.Column("discovery_query", sa.Text(),
                  comment="Search query that found this URL"),
        sa.Column("content_type", sa.String(20),
                  comment="html | pdf | xlsx | unknown"),
        # Fetch tracking
        sa.Column("last_fetch_at", sa.DateTime(timezone=True),
                  comment="When last fetched"),
        sa.Column("last_http_status", sa.Integer(),
                  comment="HTTP status code from last fetch"),
        sa.Column("last_content_hash", sa.String(64),
                  comment="SHA-256 for change detection"),
        sa.Column("last_content_length", sa.Integer(),
                  comment="Content length in bytes"),
        # Parse tracking
        sa.Column("last_parse_at", sa.DateTime(timezone=True),
                  comment="When last parsed"),
        sa.Column("last_parse_result", sa.String(20),
                  comment="success | failed | partial | skipped"),
        sa.Column("last_parse_confidence", sa.String(10),
                  comment="high | medium | low"),
        sa.Column("last_parse_cost_usd", sa.Float(),
                  comment="API cost in USD for parse"),
        # Status and retry
        sa.Column("status", sa.String(20), nullable=False, server_default="pending",
                  comment="active | dead | blocked | stale | pending | pending_retry"),
        sa.Column("retry_after", sa.DateTime(timezone=True),
                  comment="When to retry"),
        sa.Column("retry_count", sa.Integer(), server_default="0",
                  comment="Number of retry attempts"),
        sa.Column("notes", sa.Text()),
        # Timestamps
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        # Constraints
        sa.UniqueConstraint("pwsid", "url", name="uq_scrape_registry_pwsid_url"),
        schema=SCHEMA,
    )

    # --- rate_best_estimate (ORM-managed replacement) ---
    # Drop the raw-SQL table if it exists (created by scripts/build_best_estimate.py)
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.rate_best_estimate")

    op.create_table(
        "rate_best_estimate",
        sa.Column("pwsid", sa.String(12),
                  sa.ForeignKey(f"{SCHEMA}.cws_boundaries.pwsid"),
                  primary_key=True),
        sa.Column("utility_name", sa.String(255)),
        sa.Column("state_code", sa.String(2)),
        sa.Column("selected_source", sa.String(50),
                  comment="Source key of the selected rate record"),
        sa.Column("bill_estimate_10ccf", sa.Float(),
                  comment="Best estimate monthly bill at ~10 CCF"),
        sa.Column("bill_5ccf", sa.Float()),
        sa.Column("bill_10ccf", sa.Float()),
        sa.Column("bill_6ccf", sa.Float()),
        sa.Column("bill_12ccf", sa.Float()),
        sa.Column("fixed_charge_monthly", sa.Float()),
        sa.Column("rate_structure_type", sa.String(30)),
        sa.Column("rate_effective_date", sa.Date()),
        sa.Column("n_sources", sa.Integer(),
                  comment="Number of distinct sources for this PWSID"),
        sa.Column("anchor_source", sa.String(50),
                  comment="Source used as anchor for validation"),
        sa.Column("anchor_bill", sa.Float(),
                  comment="Bill from anchor source"),
        sa.Column("confidence", sa.String(10),
                  comment="high | medium | low | none"),
        sa.Column("selection_notes", sa.Text()),
        sa.Column("built_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        schema=SCHEMA,
    )

    # --- pwsid_coverage materialized view ---
    # Sprint 10: simple mat view for coverage reporting.
    # Sprint 12: migrate to a regular table when mutable columns needed.
    op.execute(f"""
        CREATE MATERIALIZED VIEW {SCHEMA}.pwsid_coverage AS
        SELECT
            c.pwsid,
            c.state_code,
            c.pws_name,
            -- Rate coverage
            (EXISTS (
                SELECT 1 FROM {SCHEMA}.water_rates wr WHERE wr.pwsid = c.pwsid
            )) AS has_rate_data,
            (
                SELECT COUNT(DISTINCT wr.source)
                FROM {SCHEMA}.water_rates wr
                WHERE wr.pwsid = c.pwsid
            ) AS rate_source_count,
            (
                SELECT ARRAY_AGG(DISTINCT wr.source ORDER BY wr.source)
                FROM {SCHEMA}.water_rates wr
                WHERE wr.pwsid = c.pwsid
            ) AS rate_sources,
            (
                SELECT MAX(wr.loaded_at)
                FROM {SCHEMA}.water_rates wr
                WHERE wr.pwsid = c.pwsid
            ) AS last_rate_loaded_at,
            -- Best estimate (if exists)
            be.selected_source AS best_source,
            be.bill_estimate_10ccf AS best_bill_10ccf,
            be.confidence AS best_confidence,
            -- SDWIS coverage
            (s.pwsid IS NOT NULL) AS has_sdwis,
            s.population_served_count,
            s.primary_source_code,
            s.owner_type_code
        FROM {SCHEMA}.cws_boundaries c
        LEFT JOIN {SCHEMA}.rate_best_estimate be ON be.pwsid = c.pwsid
        LEFT JOIN {SCHEMA}.sdwis_systems s ON s.pwsid = c.pwsid
        WITH DATA
    """)

    # Index on the mat view for fast lookups
    op.execute(f"""
        CREATE UNIQUE INDEX idx_pwsid_coverage_pwsid
        ON {SCHEMA}.pwsid_coverage (pwsid)
    """)
    op.execute(f"""
        CREATE INDEX idx_pwsid_coverage_state
        ON {SCHEMA}.pwsid_coverage (state_code)
    """)
    op.execute(f"""
        CREATE INDEX idx_pwsid_coverage_has_rate
        ON {SCHEMA}.pwsid_coverage (has_rate_data)
    """)


def downgrade() -> None:
    op.execute(f"DROP MATERIALIZED VIEW IF EXISTS {SCHEMA}.pwsid_coverage")
    op.drop_table("rate_best_estimate", schema=SCHEMA)
    op.drop_table("scrape_registry", schema=SCHEMA)
    op.drop_table("source_catalog", schema=SCHEMA)
