#!/usr/bin/env python3
"""
Aqueduct Polygon Model

Purpose:
    WRI Aqueduct 4.0 watershed risk polygons. These are the intersection
    of HydroSHEDS Level 6 basins, GADM provinces, and WHYMAP aquifers.
    ~23K polygons globally with normalized water risk scores (0-5).

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23
"""

from geoalchemy2 import Geometry
from sqlalchemy import BigInteger, DateTime, Float, Index, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from utility_api.models.base import SCHEMA, Base


class AqueductPolygon(Base):
    """Aqueduct 4.0 watershed risk polygon."""

    __tablename__ = "aqueduct_polygons"
    __table_args__ = (
        Index("idx_aqueduct_geom", "geom", postgresql_using="gist"),
        {"schema": SCHEMA},
    )

    string_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    pfaf_id: Mapped[int | None] = mapped_column(BigInteger)
    gid_1: Mapped[str | None] = mapped_column(String(20))
    aqid: Mapped[int | None] = mapped_column(Integer)
    bws_score: Mapped[float | None] = mapped_column(Float)
    bws_label: Mapped[str | None] = mapped_column(String(30))
    bwd_score: Mapped[float | None] = mapped_column(Float)
    iav_score: Mapped[float | None] = mapped_column(Float)
    sev_score: Mapped[float | None] = mapped_column(Float)
    drr_score: Mapped[float | None] = mapped_column(Float)
    w_awr_def_tot_score: Mapped[float | None] = mapped_column(Float)
    geom = mapped_column(Geometry("MULTIPOLYGON", srid=4326), nullable=False)
    loaded_at = mapped_column(DateTime(timezone=True), server_default=func.now())
