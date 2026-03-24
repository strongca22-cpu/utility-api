#!/usr/bin/env python3
"""
Permit-Facility Cross-Reference Model

Purpose:
    Links state regulatory permits to canonical Strong Strategic facility
    records. Tracks match quality (distance, confidence) and flags
    unmatched data center permits as candidates for validation.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - sqlalchemy
"""

from sqlalchemy import DateTime, Float, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from utility_api.models.base import SCHEMA, Base


class PermitFacilityXref(Base):
    """Cross-reference between permits and SS facility records."""

    __tablename__ = "permit_facility_xref"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    permit_id: Mapped[int] = mapped_column(Integer, nullable=False)
    facility_id: Mapped[str | None] = mapped_column(String(30))
    match_type: Mapped[str] = mapped_column(String(30), nullable=False)
    match_distance_km: Mapped[float | None] = mapped_column(Float)
    match_confidence: Mapped[str | None] = mapped_column(String(20))
    candidate_status: Mapped[str | None] = mapped_column(String(30))
    notes: Mapped[str | None] = mapped_column(Text)
    created_at = mapped_column(DateTime(timezone=True), server_default=func.now())
