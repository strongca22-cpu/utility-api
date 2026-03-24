#!/usr/bin/env python3
"""
CWS Boundary Model

Purpose:
    EPA Community Water System service area polygons.
    Each row is one CWS with its service area boundary geometry.
    PWSID is the universal join key across all utility datasets.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23
"""

from geoalchemy2 import Geometry
from sqlalchemy import DateTime, Index, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from utility_api.models.base import SCHEMA, Base


class CWSBoundary(Base):
    """EPA Community Water System service area boundary."""

    __tablename__ = "cws_boundaries"
    __table_args__ = (
        Index("idx_cws_geom", "geom", postgresql_using="gist"),
        Index("idx_cws_state", "state_code"),
        {"schema": SCHEMA},
    )

    pwsid: Mapped[str] = mapped_column(String(12), primary_key=True)
    pws_name: Mapped[str | None] = mapped_column(Text)
    state_code: Mapped[str | None] = mapped_column(String(2))
    county_served: Mapped[str | None] = mapped_column(Text)
    population_served: Mapped[int | None] = mapped_column(Integer)
    source_type: Mapped[str | None] = mapped_column(Text)
    geom = mapped_column(Geometry("MULTIPOLYGON", srid=4326), nullable=False)
    loaded_at = mapped_column(DateTime(timezone=True), server_default=func.now())
