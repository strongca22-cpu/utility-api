#!/usr/bin/env python3
"""
MDWD Financial Model

Purpose:
    Municipal Drinking Water Database financial and demographic data.
    Covers ~2,200 municipal CWS systems with rate, revenue, and
    demographic information. Sparse coverage but high value.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23 (renamed financial columns to water-utility-specific)
"""

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from utility_api.models.base import SCHEMA, Base


class MDWDFinancial(Base):
    """MDWD municipal utility financial and demographic data."""

    __tablename__ = "mdwd_financials"
    __table_args__ = (
        UniqueConstraint("pwsid", "year", name="uq_mdwd_pwsid_year"),
        {"schema": SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pwsid: Mapped[str] = mapped_column(
        String(12),
        ForeignKey(f"{SCHEMA}.cws_boundaries.pwsid"),
        index=True,
    )
    fips_place_code: Mapped[str | None] = mapped_column(String(7))
    year: Mapped[int] = mapped_column(Integer)
    avg_monthly_bill_5ccf: Mapped[float | None] = mapped_column(Float)
    avg_monthly_bill_10ccf: Mapped[float | None] = mapped_column(Float)
    median_household_income: Mapped[float | None] = mapped_column(Float)
    pct_below_poverty: Mapped[float | None] = mapped_column(Float)
    water_utility_revenue: Mapped[float | None] = mapped_column(Float)
    water_utility_expenditure: Mapped[float | None] = mapped_column(Float)
    water_utility_debt: Mapped[float | None] = mapped_column(Float)
    population: Mapped[int | None] = mapped_column(Integer)
    loaded_at = mapped_column(DateTime(timezone=True), server_default=func.now())
