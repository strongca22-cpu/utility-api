#!/usr/bin/env python3
"""
Batch API Agent

Purpose:
    Handles submission and processing of Anthropic Message Batches API
    requests. Collects parse tasks from the orchestrator, submits them
    as a single batch (50% cost savings), and processes results when
    the batch completes.

    Used when the orchestrator runs with --batch flag. Discovery and
    scrape run synchronously; only the parse step is batched.

Author: AI-Generated
Created: 2026-03-25
Modified: 2026-03-25

Dependencies:
    - anthropic
    - sqlalchemy
    - loguru

Usage:
    from utility_api.agents.batch import BatchAgent
    # Submit a batch:
    result = BatchAgent().submit(parse_tasks=[...], state_filter='VA')
    # Process completed batches:
    result = BatchAgent().process_all_pending()

Notes:
    - Each parse task = {pwsid, raw_text, content_type, source_url, registry_id}
    - Raw texts stored in batch_jobs.task_details JSONB (survives process exit)
    - Batch results processed by the same ParseAgent logic (validation, bill calc, DB write)
    - 50% cost savings vs live API at the cost of ~24h latency
"""

import json
from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import text

from utility_api.agents.base import BaseAgent
from utility_api.agents.parse import (
    MODEL_PRICING,
    ParseAgent,
    _build_volumetric_tiers_from_parse,
    _compute_bill,
    _parse_date,
    check_bill_consistency,
    route_model,
    validate_parse_result,
)
from utility_api.config import settings
from utility_api.db import engine
from utility_api.ingest.rate_parser import SYSTEM_PROMPT, build_parse_user_message


def _repair_json(raw: str) -> dict | None:
    """Attempt to repair common LLM JSON formatting errors.

    Handles three failure modes observed in Scenario A batch:
    1. Extra data after closing brace (LLM appended notes after JSON)
    2. Missing comma delimiters between fields
    3. Unterminated strings (truncated output)

    Parameters
    ----------
    raw : str
        The malformed JSON string.

    Returns
    -------
    dict | None
        Parsed dict if repair succeeded, None if unrecoverable.
    """
    import re

    # Strategy 1: Extra data — find the first complete top-level JSON object
    # Match from first { to its balanced closing }
    brace_depth = 0
    for i, ch in enumerate(raw):
        if ch == "{":
            brace_depth += 1
        elif ch == "}":
            brace_depth -= 1
            if brace_depth == 0:
                try:
                    return json.loads(raw[: i + 1])
                except json.JSONDecodeError:
                    break  # First object isn't valid either
                break

    # Strategy 2: Missing comma — add commas before lines starting with "
    # Pattern: }\n"key" or value\n"key" without comma
    try:
        fixed = re.sub(
            r'(?<=[}\]"\d])\s*\n(\s*")',
            r',\n\1',
            raw,
        )
        return json.loads(fixed)
    except json.JSONDecodeError:
        pass

    # Strategy 3: Unterminated string — close it and complete the JSON
    # Add closing quotes, braces, brackets as needed
    try:
        patched = raw.rstrip()
        # Close any open string
        if patched.count('"') % 2 == 1:
            patched += '"'
        # Close open braces/brackets
        open_braces = patched.count("{") - patched.count("}")
        open_brackets = patched.count("[") - patched.count("]")
        patched += "]" * max(open_brackets, 0)
        patched += "}" * max(open_braces, 0)
        return json.loads(patched)
    except json.JSONDecodeError:
        pass

    return None


class BatchAgent(BaseAgent):
    """Submits and processes Anthropic Message Batches."""

    agent_name = "batch"

    def submit(
        self,
        parse_tasks: list[dict],
        state_filter: str | None = None,
    ) -> dict:
        """Submit parse tasks as an Anthropic Message Batch.

        Parameters
        ----------
        parse_tasks : list[dict]
            Each dict: {pwsid, raw_text, content_type, source_url, registry_id}
        state_filter : str, optional
            State code for tracking.

        Returns
        -------
        dict
            batch_id, task_count, status.
        """
        from anthropic import Anthropic

        if not parse_tasks:
            return {"batch_id": None, "task_count": 0, "status": "empty"}

        logger.info(f"BatchAgent: submitting {len(parse_tasks)} parse tasks")

        # Build batch requests
        batch_requests = []
        task_details = []

        for i, task in enumerate(parse_tasks):
            pwsid = task["pwsid"]
            raw_text = task["raw_text"]
            content_type = task.get("content_type", "html")
            source_url = task.get("source_url")
            registry_id = task.get("registry_id")

            model = route_model(raw_text)

            user_message = build_parse_user_message(
                raw_text[:45000],
                utility_name=task.get("utility_name", ""),
                state_code=task.get("state_code", pwsid[:2]),
                content_type=content_type,
            )

            batch_requests.append({
                "custom_id": f"{pwsid}_{registry_id or i}",
                "params": {
                    "model": model,
                    "max_tokens": 1024,
                    "system": [{
                        "type": "text",
                        "text": SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }],
                    "messages": [
                        {"role": "user", "content": user_message},
                        {"role": "assistant", "content": "{"},
                    ],
                },
            })

            # Store task details (without raw_text to keep JSONB reasonable)
            # raw_text is needed for re-parse but we store it truncated
            task_details.append({
                "pwsid": pwsid,
                "registry_id": registry_id,
                "source_url": source_url,
                "content_type": content_type,
                "model": model,
                "text_length": len(raw_text),
            })

        # Submit to Anthropic Batch API
        try:
            client = Anthropic()
            batch = client.messages.batches.create(requests=batch_requests)
            batch_id = batch.id
            logger.info(f"  Batch submitted: {batch_id} ({len(parse_tasks)} tasks)")
        except Exception as e:
            logger.error(f"  Batch submission failed: {e}")
            self.log_run(status="failed", notes=f"Batch submit error: {str(e)[:200]}")
            return {"batch_id": None, "task_count": len(parse_tasks), "status": "failed",
                    "error": str(e)}

        # Record in batch_jobs table
        with engine.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {settings.utility_schema}.batch_jobs
                    (batch_id, submitted_at, task_count, status, task_details, state_filter)
                VALUES
                    (:batch_id, :now, :count, 'pending', :details, :state)
            """), {
                "batch_id": batch_id,
                "now": datetime.now(timezone.utc),
                "count": len(parse_tasks),
                "details": json.dumps(task_details),
                "state": state_filter,
            })
            conn.commit()

        self.log_run(
            status="success",
            rows_affected=len(parse_tasks),
            notes=f"Submitted batch {batch_id}: {len(parse_tasks)} tasks",
        )

        return {
            "batch_id": batch_id,
            "task_count": len(parse_tasks),
            "status": "pending",
        }

    def check_status(self, batch_id: str | None = None) -> list[dict]:
        """Check status of one or all pending batches.

        Parameters
        ----------
        batch_id : str, optional
            Specific batch to check. If None, checks all non-processed batches.

        Returns
        -------
        list[dict]
            Status info for each batch checked.
        """
        from anthropic import Anthropic

        schema = settings.utility_schema
        client = Anthropic()

        with engine.connect() as conn:
            if batch_id:
                rows = conn.execute(text(f"""
                    SELECT batch_id, task_count, status, submitted_at
                    FROM {schema}.batch_jobs WHERE batch_id = :id
                """), {"id": batch_id}).fetchall()
            else:
                rows = conn.execute(text(f"""
                    SELECT batch_id, task_count, status, submitted_at
                    FROM {schema}.batch_jobs
                    WHERE status IN ('pending', 'in_progress')
                    ORDER BY submitted_at ASC
                """)).fetchall()

        results = []
        for row in rows:
            try:
                batch = client.messages.batches.retrieve(row.batch_id)
                api_status = batch.processing_status  # in_progress | canceling | ended

                # Map API status to our status
                if api_status == "ended":
                    new_status = "completed"
                elif api_status == "in_progress":
                    new_status = "in_progress"
                else:
                    new_status = row.status

                # Update local status if changed
                if new_status != row.status:
                    with engine.connect() as conn:
                        params = {
                            "status": new_status,
                            "now": datetime.now(timezone.utc),
                            "id": row.batch_id,
                        }
                        if new_status == "completed":
                            conn.execute(text(f"""
                                UPDATE {schema}.batch_jobs
                                SET status = :status, completed_at = :now
                                WHERE batch_id = :id
                            """), params)
                        else:
                            conn.execute(text(f"""
                                UPDATE {schema}.batch_jobs
                                SET status = :status WHERE batch_id = :id
                            """), params)
                        conn.commit()

                counts = batch.request_counts
                results.append({
                    "batch_id": row.batch_id,
                    "task_count": row.task_count,
                    "local_status": new_status,
                    "api_status": api_status,
                    "succeeded": counts.succeeded if counts else 0,
                    "errored": counts.errored if counts else 0,
                    "submitted_at": str(row.submitted_at),
                })

            except Exception as e:
                results.append({
                    "batch_id": row.batch_id,
                    "error": str(e),
                })

        return results

    def process_batch(self, batch_id: str) -> dict:
        """Process results of a completed batch.

        Downloads results from Anthropic, validates each parse result,
        writes to rate_schedules, updates scrape_registry, and triggers
        best estimate.

        Parameters
        ----------
        batch_id : str
            The batch ID to process.

        Returns
        -------
        dict
            succeeded, failed, total_cost, details.
        """
        from anthropic import Anthropic

        schema = settings.utility_schema
        client = Anthropic()

        # Load task details from DB
        with engine.connect() as conn:
            row = conn.execute(text(f"""
                SELECT batch_id, task_count, status, task_details
                FROM {schema}.batch_jobs WHERE batch_id = :id
            """), {"id": batch_id}).fetchone()

        if not row:
            return {"error": f"Batch {batch_id} not found"}
        if row.status == "processed":
            return {"error": f"Batch {batch_id} already processed"}

        task_details = row.task_details or []
        # Build lookup by custom_id
        task_lookup = {}
        for td in task_details:
            custom_id = f"{td['pwsid']}_{td.get('registry_id', '')}"
            task_lookup[custom_id] = td

        logger.info(f"BatchAgent: processing batch {batch_id} ({row.task_count} tasks)")

        # Download results
        try:
            result_iter = client.messages.batches.results(batch_id)
        except Exception as e:
            logger.error(f"  Failed to download batch results: {e}")
            return {"error": str(e)}

        succeeded = 0
        failed = 0
        total_cost = 0.0
        details = []
        parse_agent = ParseAgent()

        for result in result_iter:
            custom_id = result.custom_id
            task_info = task_lookup.get(custom_id, {})
            pwsid = task_info.get("pwsid", custom_id.split("_")[0])
            registry_id = task_info.get("registry_id")
            source_url = task_info.get("source_url")
            model = task_info.get("model", "claude-haiku-4-5-20251001")

            if result.result.type == "errored":
                logger.warning(f"  {pwsid}: batch item errored — {result.result.error}")
                failed += 1
                details.append({"pwsid": pwsid, "status": "errored"})
                continue

            if result.result.type != "succeeded":
                failed += 1
                details.append({"pwsid": pwsid, "status": result.result.type})
                continue

            # Process the message response
            message = result.result.message
            usage = message.usage
            pricing = MODEL_PRICING.get(model, MODEL_PRICING["claude-haiku-4-5-20251001"])
            # Batch API is 50% off
            cost = (usage.input_tokens * pricing["input"]
                    + usage.output_tokens * pricing["output"]) * 0.5
            total_cost += cost

            # Parse JSON from response (with repair for common LLM formatting errors)
            raw_json = None
            try:
                raw_json = "{" + message.content[0].text
                parsed = json.loads(raw_json)
            except (json.JSONDecodeError, IndexError) as e:
                # Attempt repair before giving up
                repaired = _repair_json(raw_json) if raw_json else None
                if repaired is not None:
                    parsed = repaired
                    logger.info(f"  {pwsid}: JSON repaired (original error: {e})")
                else:
                    logger.warning(f"  {pwsid}: JSON parse failed: {e}")
                    failed += 1
                    parse_agent._update_registry(registry_id, "failed", "failed", cost, model,
                                                 raw_response=raw_json)
                    details.append({"pwsid": pwsid, "status": "json_error"})
                    continue

            # Validate
            valid, issues = validate_parse_result(parsed)
            confidence = parsed.get("parse_confidence", "failed")

            # Normalize rate_structure_type to canonical enum
            from utility_api.utils.rate_structure_normalize import normalize_rate_structure_type
            parsed["rate_structure_type"] = normalize_rate_structure_type(
                parsed.get("rate_structure_type")
            )

            # Build canonical structures
            tiers = _build_volumetric_tiers_from_parse(parsed)
            raw_fc = parsed.get("fixed_charge_monthly", 0) or 0
            try:
                fixed_charge = float(str(raw_fc).replace("$", "").replace(",", "").strip())
            except (ValueError, TypeError):
                fixed_charge = 0
            fixed_charges_json = json.dumps([{
                "name": "Service Charge",
                "amount": round(float(fixed_charge), 2),
                "meter_size": str(parsed.get("meter_size_inches", "")) if parsed.get("meter_size_inches") else None,
            }]) if fixed_charge else None

            bill_5 = _compute_bill(3740, tiers, fixed_charge)
            bill_10 = _compute_bill(7480, tiers, fixed_charge)
            bill_20 = _compute_bill(14960, tiers, fixed_charge)

            # Bill consistency check: identical bills at all volumes + non-flat = suspect
            # Recovery: reclassify as flat/uniform and keep the bill amount
            rate_type = parsed.get("rate_structure_type")
            if check_bill_consistency(bill_5, bill_10, bill_20, rate_type):
                if bill_10 and bill_10 > 0:
                    # Bills are identical and non-zero — this is a flat rate misclassified
                    # as tiered. Reclassify and keep the data rather than rejecting.
                    old_type = rate_type
                    rate_type = "flat"
                    parsed["rate_structure_type"] = "flat"
                    # Flatten tiers to a single tier (the flat amount IS the bill)
                    tiers = []
                    logger.info(
                        f"  {pwsid}: bill consistency recovery — reclassified "
                        f"{old_type} → flat (bill=${bill_10:.2f})"
                    )
                else:
                    logger.warning(
                        f"  {pwsid}: bill consistency flag — 5/10/20 CCF identical "
                        f"(${bill_10 or 0:.2f}) but type={rate_type}. Downgrading to low."
                    )
                    confidence = "low"

            conservation = None
            if len(tiers) >= 2:
                rates = [t["rate_per_1000_gal"] for t in tiers if t.get("rate_per_1000_gal")]
                if len(rates) >= 2 and min(rates) > 0:
                    conservation = round(max(rates) / min(rates), 2)

            # Write to rate_schedules
            if confidence in ("high", "medium"):
                try:
                    with engine.connect() as conn:
                        conn.execute(text(f"""
                            INSERT INTO {schema}.rate_schedules (
                                pwsid, source_key, vintage_date, customer_class,
                                billing_frequency, rate_structure_type,
                                fixed_charges, volumetric_tiers,
                                bill_5ccf, bill_10ccf, bill_20ccf,
                                conservation_signal, tier_count,
                                source_url, scrape_timestamp, confidence,
                                parse_model, parse_notes
                            ) VALUES (
                                :pwsid, 'scraped_llm', :vintage, 'residential',
                                :billing_freq, :rate_type,
                                CAST(:fixed AS jsonb), CAST(:tiers AS jsonb),
                                :bill5, :bill10, :bill20,
                                :conservation, :tier_count,
                                :url, :now, :confidence,
                                :model, :notes
                            )
                            ON CONFLICT (pwsid, source_key, vintage_date, customer_class)
                            DO UPDATE SET
                                fixed_charges = EXCLUDED.fixed_charges,
                                volumetric_tiers = EXCLUDED.volumetric_tiers,
                                bill_5ccf = EXCLUDED.bill_5ccf,
                                bill_10ccf = EXCLUDED.bill_10ccf,
                                bill_20ccf = EXCLUDED.bill_20ccf,
                                conservation_signal = EXCLUDED.conservation_signal,
                                tier_count = EXCLUDED.tier_count,
                                confidence = EXCLUDED.confidence,
                                parse_model = EXCLUDED.parse_model,
                                parse_notes = EXCLUDED.parse_notes
                        """), {
                            "pwsid": pwsid,
                            "vintage": _parse_date(parsed.get("rate_effective_date")),
                            "billing_freq": (parsed.get("billing_frequency") or "")[:30] or None,
                            "rate_type": (parsed.get("rate_structure_type") or "")[:30] or None,
                            "fixed": fixed_charges_json,
                            "tiers": json.dumps(tiers) if tiers else None,
                            "bill5": bill_5,
                            "bill10": bill_10,
                            "bill20": bill_20,
                            "conservation": conservation,
                            "tier_count": len(tiers),
                            "url": source_url,
                            "now": datetime.now(timezone.utc),
                            "confidence": confidence,
                            "model": model,
                            "notes": parsed.get("notes", ""),
                        })
                        conn.commit()

                    succeeded += 1
                    logger.info(
                        f"  ✓ {pwsid}: bill@10CCF=${bill_10 or 0:.2f} "
                        f"[{confidence}] cost=${cost:.4f}"
                    )
                    details.append({
                        "pwsid": pwsid, "status": "success",
                        "bill_10ccf": bill_10, "confidence": confidence,
                    })

                    # Update registry
                    parse_agent._update_registry(registry_id, "success", confidence, cost, model,
                                                 raw_response=raw_json)

                except Exception as e:
                    logger.warning(f"  {pwsid}: DB write failed: {e}")
                    failed += 1
                    details.append({"pwsid": pwsid, "status": "db_error"})
            else:
                failed += 1
                parse_agent._update_registry(registry_id, "failed", confidence, cost, model,
                                             raw_response=raw_json)
                details.append({"pwsid": pwsid, "status": f"low_confidence:{confidence}"})

        # Update batch_jobs
        with engine.connect() as conn:
            conn.execute(text(f"""
                UPDATE {schema}.batch_jobs SET
                    status = 'processed',
                    processed_at = :now,
                    results_summary = :summary
                WHERE batch_id = :id
            """), {
                "now": datetime.now(timezone.utc),
                "summary": json.dumps({
                    "succeeded": succeeded,
                    "failed": failed,
                    "total_cost": round(total_cost, 4),
                }),
                "id": batch_id,
            })
            conn.commit()

        # Trigger best estimate refresh (scoped to affected states)
        try:
            from utility_api.agents.best_estimate import BestEstimateAgent
            affected_states = {
                d["pwsid"][:2] for d in details if d.get("status") == "success"
            }
            for state in sorted(affected_states):
                BestEstimateAgent().run(state=state)
        except Exception as e:
            logger.debug(f"  Best estimate update skipped: {e}")

        logger.info(
            f"  Batch {batch_id}: {succeeded} succeeded, {failed} failed, "
            f"cost=${total_cost:.4f}"
        )

        self.log_run(
            status="success",
            rows_affected=succeeded,
            notes=f"Processed batch {batch_id}: {succeeded}/{row.task_count} succeeded, ${total_cost:.4f}",
        )

        return {
            "batch_id": batch_id,
            "succeeded": succeeded,
            "failed": failed,
            "total_cost": total_cost,
            "details": details,
        }

    def process_all_pending(self) -> dict:
        """Check and process all completed batches.

        Returns
        -------
        dict
            batches_checked, batches_processed, total_succeeded, total_failed.
        """
        schema = settings.utility_schema

        # First, update statuses from Anthropic
        statuses = self.check_status()

        # Process any that are now completed
        completed = [s for s in statuses if s.get("local_status") == "completed"]

        total_succeeded = 0
        total_failed = 0

        for batch_info in completed:
            result = self.process_batch(batch_info["batch_id"])
            total_succeeded += result.get("succeeded", 0)
            total_failed += result.get("failed", 0)

        return {
            "batches_checked": len(statuses),
            "batches_processed": len(completed),
            "total_succeeded": total_succeeded,
            "total_failed": total_failed,
        }

    def run(self, **kwargs) -> dict:
        """BaseAgent interface — delegates to process_all_pending."""
        return self.process_all_pending()
