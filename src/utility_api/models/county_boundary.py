#!/usr/bin/env python3
"""
County Boundary Model

Purpose:
    Census TIGER/Line county boundary polygons. Reusable reference layer
    for spatial joins (county enrichment, aggregation, lookups).
    Updated annually from Census Bureau TIGER/Line shapefiles.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23
"""

from geoalchemy2 import Geometry
from sqlalchemy import BigInteger, DateTime, Index, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from utility_api.models.base import SCHEMA, Base


class CountyBoundary(Base):
    """Census TIGER/Line county boundary polygon."""

    __tablename__ = "county_boundaries"
    __table_args__ = (
        Index("idx_county_geom", "geom", postgresql_using="gist"),
        Index("idx_county_state_fips", "state_fips"),
        {"schema": SCHEMA},
    )

    geoid: Mapped[str] = mapped_column(String(5), primary_key=True)
    state_fips: Mapped[str] = mapped_column(String(2))
    county_fips: Mapped[str] = mapped_column(String(3))
    name: Mapped[str] = mapped_column(Text)
    name_lsad: Mapped[str | None] = mapped_column(Text)
    class_fp: Mapped[str | None] = mapped_column(String(2))
    aland: Mapped[int | None] = mapped_column(BigInteger)
    awater: Mapped[int | None] = mapped_column(BigInteger)
    geom = mapped_column(Geometry("MULTIPOLYGON", srid=4326), nullable=False)
    loaded_at = mapped_column(DateTime(timezone=True), server_default=func.now())
