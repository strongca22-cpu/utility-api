#!/usr/bin/env python3
"""
Orchestrator Agent

Purpose:
    Reads the database to generate a ranked task queue. Decides what
    acquisition work to do next based on source freshness, coverage gaps,
    retriable failures, and stale URLs.

    This agent does NOT use an LLM. It is Python + SQL + config.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-25

Dependencies:
    - sqlalchemy
    - pyyaml

Usage:
    from utility_api.agents.orchestrator import OrchestratorAgent
    result = OrchestratorAgent().run(batch_size=50)
    tasks = result["tasks"]

Notes:
    - Generates tasks, does NOT execute them
    - The CLI (ua-run-orchestrator) dispatches tasks to agents
    - Task types: check_bulk_source, discover_and_scrape, retry_scrape, change_detection
    - ~100 lines of SQL queries, one sort
"""

from datetime import datetime, timezone
from pathlib import Path

import yaml
from loguru import logger
from sqlalchemy import text

from utility_api.agents.base import BaseAgent
from utility_api.agents.task import Task
from utility_api.config import PROJECT_ROOT, settings
from utility_api.db import engine


def _load_agent_config() -> dict:
    """Load agent config from config/agent_config.yaml."""
    config_path = PROJECT_ROOT / "config" / "agent_config.yaml"
    if config_path.exists():
        with open(config_path) as f:
            return yaml.safe_load(f) or {}
    return {}


class OrchestratorAgent(BaseAgent):
    """Generates a ranked task queue from database state."""

    agent_name = "orchestrator"

    def run(
        self,
        batch_size: int = 50,
        state_filter: str | None = None,
        change_detection_days: int | None = None,
        **kwargs,
    ) -> dict:
        """Generate a ranked task queue.

        Parameters
        ----------
        batch_size : int
            Max number of discovery tasks to generate.
        state_filter : str, optional
            Limit discovery tasks to a single state.
        change_detection_days : int
            Re-check active URLs older than this many days.

        Returns
        -------
        dict
            tasks: list[Task], summary: counts by task type.
        """
        logger.info("=== OrchestratorAgent: generating task queue ===")
        schema = settings.utility_schema
        tasks: list[Task] = []

        # Load config-driven defaults
        agent_config = _load_agent_config()
        cd_config = agent_config.get("change_detection", {})
        if change_detection_days is None:
            change_detection_days = cd_config.get("freshness_threshold_days", 90)
        cd_max = cd_config.get("max_per_cycle", 20)

        with engine.connect() as conn:
            # 1. Bulk source freshness — highest priority
            rows = conn.execute(text(f"""
                SELECT source_key, display_name, next_check_date
                FROM {schema}.source_catalog
                WHERE next_check_date IS NOT NULL
                  AND next_check_date <= CURRENT_DATE
            """)).fetchall()
            for r in rows:
                tasks.append(Task(
                    task_type="check_bulk_source",
                    priority=0,
                    source_key=r.source_key,
                    notes=f"Due for re-check: {r.display_name}",
                ))
            logger.info(f"  Bulk source checks: {len(rows)}")

            # 2. Coverage gaps — discover_and_scrape for uncovered PWSIDs
            state_clause = ""
            params = {"batch": batch_size}
            if state_filter:
                state_clause = "AND pc.state_code = :state"
                params["state"] = state_filter.upper()

            rows = conn.execute(text(f"""
                SELECT pc.pwsid, pc.pws_name, pc.state_code,
                       pc.priority_tier, pc.population_served
                FROM {schema}.pwsid_coverage pc
                WHERE pc.has_rate_data = FALSE
                  AND pc.scrape_status = 'not_attempted'
                  AND pc.priority_tier IS NOT NULL
                  {state_clause}
                ORDER BY pc.priority_tier ASC,
                         pc.population_served DESC NULLS LAST
                LIMIT :batch
            """), params).fetchall()
            for r in rows:
                tasks.append(Task(
                    task_type="discover_and_scrape",
                    priority=r.priority_tier * 10,
                    pwsid=r.pwsid,
                    utility_name=r.pws_name,
                    state_code=r.state_code,
                    notes=f"Tier {r.priority_tier}, pop={r.population_served or '?'}",
                ))
            gap_count = len(rows)
            logger.info(f"  Coverage gap discoveries: {gap_count}")

            # 3. Retriable failures
            rows = conn.execute(text(f"""
                SELECT sr.id, sr.pwsid, sr.url, sr.retry_count
                FROM {schema}.scrape_registry sr
                WHERE sr.status = 'pending_retry'
                  AND sr.retry_after <= NOW()
                ORDER BY sr.retry_count ASC
                LIMIT 20
            """)).fetchall()
            for r in rows:
                tasks.append(Task(
                    task_type="retry_scrape",
                    priority=30,
                    pwsid=r.pwsid,
                    registry_id=r.id,
                    notes=f"Retry #{r.retry_count + 1}: {r.url[:60]}",
                ))
            retry_count = len(rows)
            logger.info(f"  Retriable failures: {retry_count}")

            # 4. Change detection — re-check stale active URLs
            rows = conn.execute(text(f"""
                SELECT sr.id, sr.pwsid, sr.url, sr.last_fetch_at
                FROM {schema}.scrape_registry sr
                WHERE sr.status = 'active'
                  AND sr.last_fetch_at < NOW() - MAKE_INTERVAL(days => :days)
                ORDER BY sr.last_fetch_at ASC
                LIMIT :cd_limit
            """), {"days": change_detection_days, "cd_limit": cd_max}).fetchall()
            for r in rows:
                tasks.append(Task(
                    task_type="change_detection",
                    priority=50,
                    pwsid=r.pwsid,
                    registry_id=r.id,
                    notes=f"Last fetched: {r.last_fetch_at.strftime('%Y-%m-%d') if r.last_fetch_at else '?'}",
                ))
            stale_count = len(rows)
            logger.info(f"  Change detection: {stale_count}")

        # Sort all tasks by priority
        tasks.sort(key=lambda t: t.priority)

        summary = {
            "bulk_checks": len([t for t in tasks if t.task_type == "check_bulk_source"]),
            "new_discoveries": gap_count,
            "retries": retry_count,
            "change_detections": stale_count,
            "total_tasks": len(tasks),
        }

        self.log_run(
            status="success",
            rows_affected=len(tasks),
            notes=f"Generated {len(tasks)} tasks: {summary}",
        )

        logger.info(f"  Total tasks: {len(tasks)}")
        return {"tasks": tasks, "summary": summary}
