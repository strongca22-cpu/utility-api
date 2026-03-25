#!/usr/bin/env python3
"""
API Key Model

Purpose:
    ORM model for the api_keys table. Stores hashed API keys with
    tier-based rate limiting and usage tracking.

Author: AI-Generated
Created: 2026-03-25
Modified: 2026-03-25

Dependencies:
    - sqlalchemy

Usage:
    from utility_api.models.api_key import ApiKey
"""

from sqlalchemy import BigInteger, Boolean, Date, DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from utility_api.models.base import SCHEMA, Base


class ApiKey(Base):
    """API key with tier-based rate limiting."""

    __tablename__ = "api_keys"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    tier: Mapped[str] = mapped_column(String(20), nullable=False, server_default="free")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    created_at = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_used_at = mapped_column(DateTime(timezone=True), nullable=True)
    request_count: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    daily_request_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    daily_reset_date = mapped_column(Date, nullable=True)
