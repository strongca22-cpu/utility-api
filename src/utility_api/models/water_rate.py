#!/usr/bin/env python3
"""
Water Rate Model

Purpose:
    Stores parsed water rate schedules for community water systems.
    Each record represents a single rate schedule vintage for one utility,
    including the full tier structure and computed bill amounts.

    Rate data is populated by the LLM rate parsing pipeline (Sprint 3):
    web scrape → Claude API extraction → tier calculation → DB insert.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - sqlalchemy

Notes:
    - Links to cws_boundaries via pwsid (FK)
    - Stores up to 4 volumetric tiers (covers ~95% of US utilities)
    - bill_5ccf / bill_10ccf are convenience snapshots; any consumption
      level can be calculated from the stored tier structure
    - parse_confidence reflects LLM extraction quality, not rate accuracy
    - source_url + raw_text_hash enable change detection on re-scrape

Data Sources:
    - Input: Utility websites (scraped), parsed by Claude API
    - Output: utility.water_rates table
"""

from sqlalchemy import (
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
from sqlalchemy.orm import Mapped, mapped_column

from utility_api.models.base import SCHEMA, Base


class WaterRate(Base):
    """Parsed water rate schedule for a community water system."""

    __tablename__ = "water_rates"
    __table_args__ = (
        UniqueConstraint("pwsid", "rate_effective_date", name="uq_water_rate_pwsid_date"),
        {"schema": SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pwsid: Mapped[str] = mapped_column(
        String(12),
        ForeignKey(f"{SCHEMA}.cws_boundaries.pwsid"),
        index=True,
    )

    # Utility identity (denormalized for convenience — CWS name may differ from rate page name)
    utility_name: Mapped[str | None] = mapped_column(String(255))
    state_code: Mapped[str | None] = mapped_column(String(2))
    county: Mapped[str | None] = mapped_column(String(100))

    # Rate schedule metadata
    rate_effective_date: Mapped[str | None] = mapped_column(Date)
    rate_structure_type: Mapped[str | None] = mapped_column(
        String(30),
        comment="flat | uniform | increasing_block | decreasing_block | budget_based | seasonal",
    )
    rate_class: Mapped[str] = mapped_column(
        String(30), default="residential",
        comment="residential | commercial | industrial | irrigation",
    )
    billing_frequency: Mapped[str | None] = mapped_column(
        String(20),
        comment="monthly | bimonthly | quarterly",
    )

    # Fixed charge
    fixed_charge_monthly: Mapped[float | None] = mapped_column(
        Float, comment="Base/service/availability charge, normalized to $/month"
    )
    meter_size_inches: Mapped[float | None] = mapped_column(
        Float, comment="Meter size this fixed charge applies to (default 5/8 or 3/4)"
    )

    # Volumetric tiers (up to 4 — covers ~95% of US rate structures)
    tier_1_limit_ccf: Mapped[float | None] = mapped_column(
        Float, comment="Tier 1 upper limit in CCF (NULL = unlimited / flat rate)"
    )
    tier_1_rate: Mapped[float | None] = mapped_column(Float, comment="$/CCF for tier 1")

    tier_2_limit_ccf: Mapped[float | None] = mapped_column(
        Float, comment="Tier 2 upper limit in CCF"
    )
    tier_2_rate: Mapped[float | None] = mapped_column(Float, comment="$/CCF for tier 2")

    tier_3_limit_ccf: Mapped[float | None] = mapped_column(
        Float, comment="Tier 3 upper limit in CCF"
    )
    tier_3_rate: Mapped[float | None] = mapped_column(Float, comment="$/CCF for tier 3")

    tier_4_limit_ccf: Mapped[float | None] = mapped_column(
        Float, comment="Tier 4 upper limit in CCF (highest tier, limit usually NULL)"
    )
    tier_4_rate: Mapped[float | None] = mapped_column(Float, comment="$/CCF for tier 4")

    # Computed bill snapshots (convenience — can be recalculated from tiers)
    bill_5ccf: Mapped[float | None] = mapped_column(
        Float, comment="Calculated monthly bill at 5 CCF (3,740 gal)"
    )
    bill_10ccf: Mapped[float | None] = mapped_column(
        Float, comment="Calculated monthly bill at 10 CCF (7,480 gal)"
    )

    # Provenance
    source_url: Mapped[str | None] = mapped_column(Text, comment="URL of scraped rate page")
    raw_text_hash: Mapped[str | None] = mapped_column(
        String(64), comment="SHA-256 of scraped text (change detection)"
    )
    parse_confidence: Mapped[str | None] = mapped_column(
        String(10), comment="high | medium | low | failed"
    )
    parse_model: Mapped[str | None] = mapped_column(
        String(50), comment="Claude model ID used for extraction"
    )
    parse_notes: Mapped[str | None] = mapped_column(
        Text, comment="LLM extraction notes, edge cases, warnings"
    )

    # Timestamps
    scraped_at: Mapped[str | None] = mapped_column(DateTime(timezone=True))
    parsed_at: Mapped[str | None] = mapped_column(DateTime(timezone=True))
    loaded_at = mapped_column(DateTime(timezone=True), server_default=func.now())
