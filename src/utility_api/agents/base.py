#!/usr/bin/env python3
"""
BaseAgent Abstract Base Class

Purpose:
    Minimal shared interface for all UAPI agents. Each agent is a plain
    Python class with a run() method. No LLM framework, no task queue,
    no async. The database is the single source of truth.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - sqlalchemy

Usage:
    class MyAgent(BaseAgent):
        agent_name = "my_agent"
        def run(self, **kwargs) -> dict:
            ...

Notes:
    - Agents are stateless — all state is in PostgreSQL
    - log_run() writes to ingest_log after every execution
    - Sprint 12 agents (BulkIngest, BestEstimate) use zero LLM calls
    - Sprint 13 agents (Discovery, Parse) will add LLM calls
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import text

from utility_api.config import settings
from utility_api.db import engine


class BaseAgent(ABC):
    """Abstract base class for all UAPI agents."""

    agent_name: str = "base"

    @abstractmethod
    def run(self, **kwargs) -> dict:
        """Execute the agent's task. Returns a summary dict."""
        pass

    def log_run(
        self,
        status: str,
        rows_affected: int = 0,
        source_key: str | None = None,
        notes: str | None = None,
    ) -> None:
        """Write an entry to ingest_log.

        Parameters
        ----------
        status : str
            running | success | failed | partial
        rows_affected : int
            Number of rows created/updated.
        source_key : str, optional
            source_catalog key if this run is source-specific.
        notes : str, optional
            Free-text details about the run.
        """
        schema = settings.utility_schema
        try:
            with engine.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO {schema}.ingest_log
                        (agent_name, source_key, completed_at, status, rows_affected, notes)
                    VALUES
                        (:agent, :source_key, :completed, :status, :rows, :notes)
                """), {
                    "agent": self.agent_name,
                    "source_key": source_key,
                    "completed": datetime.now(timezone.utc),
                    "status": status,
                    "rows": rows_affected,
                    "notes": notes,
                })
                conn.commit()
        except Exception as e:
            logger.warning(f"Failed to write ingest_log: {e}")
