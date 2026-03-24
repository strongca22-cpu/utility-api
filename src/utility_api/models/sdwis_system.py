#!/usr/bin/env python3
"""
SDWIS System Model

Purpose:
    Safe Drinking Water Information System attributes for each CWS.
    Joined to CWS boundaries on PWSID. Contains system type,
    water source, ownership, and violation history.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23
"""

from sqlalchemy import Date, DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from utility_api.models.base import SCHEMA, Base


class SDWISSystem(Base):
    """SDWIS water system attributes and violation summary."""

    __tablename__ = "sdwis_systems"
    __table_args__ = {"schema": SCHEMA}

    pwsid: Mapped[str] = mapped_column(
        String(12),
        ForeignKey(f"{SCHEMA}.cws_boundaries.pwsid"),
        primary_key=True,
    )
    pws_name: Mapped[str | None] = mapped_column(Text)
    pws_type_code: Mapped[str | None] = mapped_column(String(10))
    primary_source_code: Mapped[str | None] = mapped_column(String(10))
    population_served_count: Mapped[int | None] = mapped_column(Integer)
    service_connections_count: Mapped[int | None] = mapped_column(Integer)
    owner_type_code: Mapped[str | None] = mapped_column(String(10))
    is_wholesaler_ind: Mapped[str | None] = mapped_column(String(1))
    activity_status_cd: Mapped[str | None] = mapped_column(String(5))
    state_code: Mapped[str | None] = mapped_column(String(2))
    violation_count_5yr: Mapped[int | None] = mapped_column(Integer)
    health_violation_count_5yr: Mapped[int | None] = mapped_column(Integer)
    last_violation_date = mapped_column(Date, nullable=True)
    fetched_at = mapped_column(DateTime(timezone=True), server_default=func.now())
