#!/usr/bin/env python3
"""
API Dependencies

Purpose:
    FastAPI dependency injection for database sessions and common utilities.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23
"""

from collections.abc import Generator

from sqlalchemy.orm import Session

from utility_api.db import SessionLocal


def get_db() -> Generator[Session, None, None]:
    """Yield a database session, ensuring cleanup on exit."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
