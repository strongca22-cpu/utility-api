#!/usr/bin/env python3
"""
Best Estimate Agent

Purpose:
    Wraps the generalized best-estimate logic behind the BaseAgent
    interface. Config-driven business logic — no LLM.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - sqlalchemy
    - loguru

Usage:
    from utility_api.agents.best_estimate import BestEstimateAgent
    result = BestEstimateAgent().run(state='CA')
    result = BestEstimateAgent().run()  # all states

Notes:
    - Delegates to utility_api.ops.best_estimate.run_best_estimate()
    - After building estimates, refreshes pwsid_coverage derived columns
    - Logs to ingest_log via BaseAgent.log_run()
"""

from loguru import logger

from utility_api.agents.base import BaseAgent


class BestEstimateAgent(BaseAgent):
    """Config-driven best-estimate rate selection."""

    agent_name = "best_estimate"

    def run(
        self,
        state: str | None = None,
        pwsids: list[str] | None = None,
        dry_run: bool = False,
        **kwargs,
    ) -> dict:
        """Run best-estimate selection.

        Parameters
        ----------
        state : str, optional
            Limit to a single state code (e.g., "CA").
        pwsids : list[str], optional
            Limit to specific PWSIDs (not yet implemented — state filter only).
        dry_run : bool
            Preview without DB writes.

        Returns
        -------
        dict
            Summary with total_pwsids, inserted, confidence breakdown.
        """
        from utility_api.ops.best_estimate import run_best_estimate

        scope = state or "all states"
        logger.info(f"=== BestEstimateAgent: {scope} ===")

        try:
            stats = run_best_estimate(
                state_filter=state,
                dry_run=dry_run,
            )

            rows = stats.get("inserted", stats.get("total_pwsids", 0))

            if not dry_run and rows > 0:
                self._refresh_coverage_derived()

            self.log_run(
                status="success",
                rows_affected=rows,
                notes=f"Best estimates for {scope}: {stats.get('with_estimate', 0)} with estimates",
            )

            logger.info(f"=== BestEstimateAgent: {scope} complete ({rows} rows) ===")
            return {"status": "success", **stats}

        except Exception as e:
            logger.error(f"BestEstimateAgent failed: {e}")
            self.log_run(status="failed", notes=f"Error: {str(e)[:200]}")
            return {"status": "failed", "error": str(e)}

    def _refresh_coverage_derived(self) -> None:
        """Refresh derived columns in pwsid_coverage after best-estimate update."""
        try:
            from utility_api.ops.coverage import refresh_coverage_derived
            refresh_coverage_derived()
        except Exception as e:
            logger.warning(f"Coverage refresh failed (non-fatal): {e}")
