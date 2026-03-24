#!/usr/bin/env python3
"""
Pipeline Run Model

Purpose:
    Audit trail for ingest pipeline steps. Logs start/end time,
    row counts, and status for each ingest operation.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23
"""

from sqlalchemy import DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from utility_api.models.base import SCHEMA, Base


class PipelineRun(Base):
    """Pipeline execution log entry."""

    __tablename__ = "pipeline_runs"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    step_name: Mapped[str] = mapped_column(String(50))
    started_at = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at = mapped_column(DateTime(timezone=True), nullable=True)
    row_count: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(20), default="running")
    notes: Mapped[str | None] = mapped_column(Text)
