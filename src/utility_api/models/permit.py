#!/usr/bin/env python3
"""
Permit Model

Purpose:
    Canonical permits table for state regulatory permit layers.
    Stores VA DEQ (VWP, VPDES) and CA SWRCB (eWRIMS) permits with
    source-native categories and normalized category groups.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - sqlalchemy
    - geoalchemy2
"""

from geoalchemy2 import Geometry
from sqlalchemy import Date, DateTime, Float, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from utility_api.models.base import SCHEMA, Base


class Permit(Base):
    """State regulatory permit record."""

    __tablename__ = "permits"
    __table_args__ = (
        Index("idx_permits_geom", "geom", postgresql_using="gist"),
        Index("idx_permits_source", "source"),
        Index("idx_permits_state", "state_code"),
        Index("idx_permits_category_group", "category_group"),
        Index("idx_permits_permit_number", "permit_number"),
        {"schema": SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(30), nullable=False)
    permit_number: Mapped[str] = mapped_column(Text, nullable=False)
    facility_name: Mapped[str | None] = mapped_column(Text)
    source_category: Mapped[str | None] = mapped_column(Text)
    category_group: Mapped[str | None] = mapped_column(String(30))
    use_codes = mapped_column(JSONB, nullable=True)
    status: Mapped[str | None] = mapped_column(Text)
    state_code: Mapped[str | None] = mapped_column(String(2))
    county: Mapped[str | None] = mapped_column(Text)
    issued_date = mapped_column(Date, nullable=True)
    expiration_date = mapped_column(Date, nullable=True)
    face_value_amount: Mapped[float | None] = mapped_column(Float)
    face_value_units: Mapped[str | None] = mapped_column(Text)
    max_diversion_rate: Mapped[float | None] = mapped_column(Float)
    max_diversion_units: Mapped[str | None] = mapped_column(Text)
    max_diversion_rate_gpd: Mapped[float | None] = mapped_column(Float)
    geom = mapped_column(Geometry("POINT", srid=4326), nullable=True)
    raw_attrs = mapped_column(JSONB, nullable=True)
    loaded_at = mapped_column(DateTime(timezone=True), server_default=func.now())
