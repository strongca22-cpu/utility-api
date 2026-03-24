"""Declarative base for utility schema models."""

from sqlalchemy.orm import DeclarativeBase


SCHEMA = "utility"


class Base(DeclarativeBase):
    """Base class for all utility schema models."""

    __table_args__ = {"schema": SCHEMA}
