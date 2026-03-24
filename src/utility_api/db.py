#!/usr/bin/env python3
"""
Database Engine and Session Management

Purpose:
    SQLAlchemy engine and session factory for the utility schema.
    Connects to the same PostGIS database as strong-strategic but
    operates in the 'utility' schema.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - sqlalchemy
    - psycopg

Usage:
    from utility_api.db import get_session, engine
"""

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session, sessionmaker

from utility_api.config import settings

engine = create_engine(settings.database_url, echo=False)
SessionLocal = sessionmaker(bind=engine)


@event.listens_for(engine, "connect")
def set_search_path(dbapi_connection, connection_record):
    """Set default search_path to utility schema on every new connection."""
    cursor = dbapi_connection.cursor()
    cursor.execute(f"SET search_path TO {settings.utility_schema}, public")
    cursor.close()


def get_session() -> Session:
    """Create a new database session."""
    return SessionLocal()
