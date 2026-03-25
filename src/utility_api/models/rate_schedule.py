#!/usr/bin/env python3
"""
Rate Schedule Model (Canonical)

Purpose:
    Canonical rate schema with JSONB tier storage. Replaces the fixed
    4-tier columns in water_rates with flexible JSONB arrays that can
    represent any number of tiers, multiple fixed charges, and surcharges.

    This is the source of truth for rate data. water_rates is kept as
    a legacy/audit table during the transition period.

    Units:
    - Volumetric tiers use gallons and $/1000 gallons (not CCF)
    - Bill snapshots use monthly amounts in USD
    - Fixed charges are monthly amounts in USD

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - sqlalchemy
    - sqlalchemy.dialects.postgresql (JSONB)

Usage:
    from utility_api.models.rate_schedule import RateSchedule

Notes:
    - JSONB fields: fixed_charges, volumetric_tiers, surcharges
    - volumetric_tiers: [{tier: 1, min_gal: 0, max_gal: 7480, rate_per_1000_gal: 5.34}, ...]
    - fixed_charges: [{name: "Service Charge", amount: 12.50, meter_size: "5/8"}, ...]
    - surcharges: [{name: "Drought surcharge", rate_per_1000_gal: 0.50, condition: "Stage 2"}, ...]
    - conservation_signal: ratio of highest to lowest tier rate (>1 = conservation pricing)
    - Unique on (pwsid, source_key, vintage_date, customer_class)

Data Sources:
    - Populated by ingest modules (dual-write with water_rates)
    - Migrated from existing water_rates records via scripts/migrate_to_rate_schedules.py
"""

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from utility_api.models.base import SCHEMA, Base


class RateSchedule(Base):
    """Canonical rate schedule with JSONB tier storage."""

    __tablename__ = "rate_schedules"
    __table_args__ = (
        UniqueConstraint(
            "pwsid", "source_key", "vintage_date", "customer_class",
            name="uq_rate_schedule_pwsid_source_vintage_class",
        ),
        {"schema": SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Identity
    pwsid: Mapped[str] = mapped_column(
        String(12),
        ForeignKey(f"{SCHEMA}.cws_boundaries.pwsid"),
        index=True,
    )
    source_key: Mapped[str] = mapped_column(
        String(50),
        comment="FK → source_catalog.source_key (e.g., swrcb_ear_2022, efc_nc_2025)",
    )
    vintage_date = mapped_column(
        Date, nullable=True,
        comment="When these rates were effective",
    )
    customer_class: Mapped[str] = mapped_column(
        String(30), default="residential",
        comment="residential | commercial | industrial | irrigation",
    )

    # Rate structure metadata
    billing_frequency: Mapped[str | None] = mapped_column(
        String(20),
        comment="monthly | bimonthly | quarterly",
    )
    rate_structure_type: Mapped[str | None] = mapped_column(
        String(30),
        comment="flat | uniform | increasing_block | decreasing_block | budget_based | seasonal",
    )

    # JSONB tier storage (the core innovation over water_rates)
    fixed_charges = mapped_column(
        JSONB, nullable=True,
        comment="[{name, amount, frequency, meter_size}] — all fixed/service charges",
    )
    volumetric_tiers = mapped_column(
        JSONB, nullable=True,
        comment="[{tier, min_gal, max_gal, rate_per_1000_gal}] — volumetric tier structure",
    )
    surcharges = mapped_column(
        JSONB, nullable=True,
        comment="[{name, rate_per_1000_gal, condition}] — drought/seasonal surcharges",
    )

    # Computed bill snapshots (monthly, in USD)
    bill_5ccf: Mapped[float | None] = mapped_column(
        Float, comment="Monthly bill at 5 CCF (3,740 gal)",
    )
    bill_10ccf: Mapped[float | None] = mapped_column(
        Float, comment="Monthly bill at 10 CCF (7,480 gal)",
    )
    bill_20ccf: Mapped[float | None] = mapped_column(
        Float, comment="Monthly bill at 20 CCF (14,960 gal)",
    )

    # Derived metrics
    conservation_signal: Mapped[float | None] = mapped_column(
        Float,
        comment="Ratio: highest tier rate / lowest tier rate. >1 = conservation pricing.",
    )
    tier_count: Mapped[int | None] = mapped_column(
        Integer,
        comment="Number of volumetric tiers",
    )

    # Provenance
    source_url: Mapped[str | None] = mapped_column(Text)
    scrape_timestamp = mapped_column(DateTime(timezone=True), nullable=True)
    confidence: Mapped[str | None] = mapped_column(
        String(10), comment="high | medium | low | failed",
    )
    raw_text_hash: Mapped[str | None] = mapped_column(
        String(64), comment="SHA-256 for change detection",
    )
    parse_model: Mapped[str | None] = mapped_column(
        String(50), comment="Claude model ID used for extraction",
    )
    parse_notes: Mapped[str | None] = mapped_column(Text)

    # Review flags
    needs_review: Mapped[bool] = mapped_column(
        Boolean, default=False,
        comment="Flagged for manual review (margin check, confidence issue, etc.)",
    )
    review_reason: Mapped[str | None] = mapped_column(Text)

    created_at = mapped_column(DateTime(timezone=True), server_default=func.now())
