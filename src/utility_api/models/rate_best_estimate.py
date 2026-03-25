#!/usr/bin/env python3
"""
Rate Best Estimate Model

Purpose:
    Source-prioritized best rate estimate per PWSID. One row per utility:
    the single "best" bill estimate selected from all available sources
    using configurable priority logic (config/source_priority.yaml).

    Replaces the previous raw-SQL-created rate_best_estimate table
    with a proper ORM model managed by Alembic.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - sqlalchemy

Usage:
    from utility_api.models.rate_best_estimate import RateBestEstimate

Notes:
    - One row per PWSID (primary key)
    - Built by ua-ops build-best-estimate (generalized, all states)
    - CA: multi-source reconciliation with eAR anchor principle
    - NC/VA: single-source, simpler selection
    - Confidence reflects both source quality and cross-source agreement

Data Sources:
    - Input: utility.water_rates (all sources)
    - Output: utility.rate_best_estimate table
"""

from sqlalchemy import (
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from utility_api.models.base import SCHEMA, Base


class RateBestEstimate(Base):
    """Best-estimate rate selection for a single PWSID."""

    __tablename__ = "rate_best_estimate"
    __table_args__ = {"schema": SCHEMA}

    pwsid: Mapped[str] = mapped_column(
        String(12),
        ForeignKey(f"{SCHEMA}.cws_boundaries.pwsid"),
        primary_key=True,
    )
    utility_name: Mapped[str | None] = mapped_column(String(255))
    state_code: Mapped[str | None] = mapped_column(String(2))

    # Selected estimate
    selected_source: Mapped[str | None] = mapped_column(
        String(50),
        comment="Source key of the selected rate record",
    )
    bill_estimate_10ccf: Mapped[float | None] = mapped_column(
        Float,
        comment="Best estimate monthly bill at ~10 CCF (interpolated if needed)",
    )
    bill_5ccf: Mapped[float | None] = mapped_column(Float)
    bill_10ccf: Mapped[float | None] = mapped_column(Float)
    bill_6ccf: Mapped[float | None] = mapped_column(Float)
    bill_12ccf: Mapped[float | None] = mapped_column(Float)
    fixed_charge_monthly: Mapped[float | None] = mapped_column(Float)
    rate_structure_type: Mapped[str | None] = mapped_column(String(30))
    rate_effective_date = mapped_column(Date, nullable=True)

    # Reconciliation metadata
    n_sources: Mapped[int | None] = mapped_column(
        Integer,
        comment="Number of distinct sources with data for this PWSID",
    )
    anchor_source: Mapped[str | None] = mapped_column(
        String(50),
        comment="Source used as anchor for cross-source validation (CA: eAR)",
    )
    anchor_bill: Mapped[float | None] = mapped_column(
        Float,
        comment="Bill amount from anchor source (for comparison)",
    )
    confidence: Mapped[str | None] = mapped_column(
        String(10),
        comment="high | medium | low | none",
    )
    selection_notes: Mapped[str | None] = mapped_column(Text)

    built_at = mapped_column(DateTime(timezone=True), server_default=func.now())
