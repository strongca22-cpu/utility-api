#!/usr/bin/env python3
"""
Bulk Ingest Agent

Purpose:
    Wraps existing ingest modules (eAR, OWRS, EFC NC, SDWIS, etc.)
    behind the BaseAgent interface. Adds logging, source_catalog
    updates, and a uniform invocation pattern.

    This agent does NOT use an LLM. It calls existing ingest functions.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - sqlalchemy
    - loguru

Usage:
    from utility_api.agents.bulk_ingest import BulkIngestAgent
    result = BulkIngestAgent().run(source_key='efc_nc_2025')

Notes:
    - Looks up source_catalog.ingest_module to find the right function
    - Updates source_catalog.last_ingested_at after successful ingest
    - Updates source_catalog.pwsid_count from actual DB data
    - Logs to ingest_log via BaseAgent.log_run()
"""

from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import text

from utility_api.agents.base import BaseAgent
from utility_api.config import settings
from utility_api.db import engine

# Mapping from source_key to the ingest function to call
# These are the existing functions that work — we just wrap them
INGEST_FUNCTIONS = {
    "swrcb_ear_2022": ("utility_api.ingest.ear_ingest", "run_ear_ingest", {"years": [2022]}),
    "swrcb_ear_2021": ("utility_api.ingest.ear_ingest", "run_ear_ingest", {"years": [2021]}),
    "swrcb_ear_2020": ("utility_api.ingest.ear_ingest", "run_ear_ingest", {"years": [2020]}),
    "owrs": ("utility_api.ingest.owrs_ingest", "run_owrs_ingest", {}),
    "efc_nc_2025": ("utility_api.ingest.efc_nc_ingest", "run_efc_nc_ingest", {}),
    "sdwis": ("utility_api.ingest.sdwis", "run_sdwis_ingest", {}),
}


class BulkIngestAgent(BaseAgent):
    """Wraps existing ingest modules behind a uniform interface."""

    agent_name = "bulk_ingest"

    def run(self, source_key: str, dry_run: bool = False, **kwargs) -> dict:
        """Run the ingest module for the given source_key.

        Parameters
        ----------
        source_key : str
            Key from source_catalog (e.g., 'efc_nc_2025', 'owrs').
        dry_run : bool
            If True, pass dry_run to the ingest function (if supported).

        Returns
        -------
        dict
            Summary with status, rows_affected, and timing.
        """
        logger.info(f"=== BulkIngestAgent: {source_key} ===")

        if source_key not in INGEST_FUNCTIONS:
            msg = f"Unknown source_key '{source_key}'. Known: {list(INGEST_FUNCTIONS.keys())}"
            logger.error(msg)
            self.log_run(status="failed", source_key=source_key, notes=msg)
            return {"status": "failed", "error": msg}

        module_path, func_name, default_kwargs = INGEST_FUNCTIONS[source_key]
        started = datetime.now(timezone.utc)

        try:
            # Dynamically import and call the ingest function
            import importlib
            module = importlib.import_module(module_path)
            func = getattr(module, func_name)

            # Merge kwargs
            call_kwargs = {**default_kwargs, **kwargs}
            if dry_run and "dry_run" in func.__code__.co_varnames:
                call_kwargs["dry_run"] = True

            logger.info(f"Calling {module_path}.{func_name}({call_kwargs})")
            result = func(**call_kwargs)

            elapsed = (datetime.now(timezone.utc) - started).total_seconds()

            # Get row count from result or query DB
            rows = 0
            if isinstance(result, dict) and "row_count" in result:
                rows = result["row_count"]
            elif isinstance(result, int):
                rows = result
            else:
                rows = self._get_source_pwsid_count(source_key)

            # Update source_catalog
            if not dry_run:
                self._update_source_catalog(source_key, rows)
                # Sync to rate_schedules
                self._sync_rate_schedules()

            self.log_run(
                status="success",
                rows_affected=rows,
                source_key=source_key,
                notes=f"Completed in {elapsed:.1f}s",
            )

            logger.info(f"=== BulkIngestAgent: {source_key} complete ({rows} rows, {elapsed:.1f}s) ===")
            return {"status": "success", "rows_affected": rows, "elapsed_seconds": elapsed}

        except Exception as e:
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            logger.error(f"BulkIngestAgent failed: {e}")
            self.log_run(
                status="failed",
                source_key=source_key,
                notes=f"Error after {elapsed:.1f}s: {str(e)[:200]}",
            )
            return {"status": "failed", "error": str(e), "elapsed_seconds": elapsed}

    def _update_source_catalog(self, source_key: str, pwsid_count: int) -> None:
        """Update source_catalog.last_ingested_at and pwsid_count."""
        schema = settings.utility_schema
        try:
            with engine.connect() as conn:
                conn.execute(text(f"""
                    UPDATE {schema}.source_catalog
                    SET last_ingested_at = :now,
                        pwsid_count = :count,
                        updated_at = :now
                    WHERE source_key = :key
                """), {
                    "now": datetime.now(timezone.utc),
                    "count": pwsid_count,
                    "key": source_key,
                })
                conn.commit()
        except Exception as e:
            logger.warning(f"Failed to update source_catalog: {e}")

    def _get_source_pwsid_count(self, source_key: str) -> int:
        """Get actual PWSID count for this source from water_rates."""
        schema = settings.utility_schema
        try:
            with engine.connect() as conn:
                count = conn.execute(text(f"""
                    SELECT COUNT(DISTINCT pwsid)
                    FROM {schema}.water_rates
                    WHERE source = :key
                """), {"key": source_key}).scalar()
                return count or 0
        except Exception:
            return 0

    def _sync_rate_schedules(self) -> None:
        """Sync any new water_rates records to rate_schedules."""
        try:
            from utility_api.ops.rate_schedule_helpers import (
                water_rate_to_schedule,
                write_rate_schedule,
            )
            schema = settings.utility_schema
            with engine.connect() as conn:
                rows = conn.execute(text(f"""
                    SELECT wr.* FROM {schema}.water_rates wr
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {schema}.rate_schedules rs
                        WHERE rs.pwsid = wr.pwsid
                          AND rs.source_key = wr.source
                          AND rs.vintage_date IS NOT DISTINCT FROM wr.rate_effective_date
                          AND rs.customer_class = COALESCE(wr.rate_class, 'residential')
                    )
                """)).mappings().all()

                if rows:
                    for r in rows:
                        schedule = water_rate_to_schedule(dict(r))
                        write_rate_schedule(conn, schedule)
                    conn.commit()
                    logger.info(f"Synced {len(rows)} new records to rate_schedules")
        except Exception as e:
            logger.warning(f"Rate schedule sync failed (non-fatal): {e}")
