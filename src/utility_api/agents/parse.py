#!/usr/bin/env python3
"""
Parse Agent

Purpose:
    Takes raw text from the scrape agent and sends it to the Claude API
    for structured rate extraction. Routes to Sonnet or Haiku based on
    complexity heuristics. Validates response, computes bill benchmarks,
    writes to rate_schedules, and updates scrape_registry.

    This is the core LLM-powered component.

Author: AI-Generated
Created: 2026-03-24
Modified: 2026-03-24

Dependencies:
    - anthropic
    - sqlalchemy
    - loguru

Usage:
    from utility_api.agents.parse import ParseAgent
    result = ParseAgent().run(pwsid='VA4760100', raw_text='...')

Notes:
    - Reuses the system prompt from rate_parser.py (proven in Sprint 3)
    - Prompt caching enabled for system prompt (90% savings after first call)
    - Complexity routing: Sonnet for complex structures, Haiku for simple
    - After success: triggers BestEstimateAgent for the PWSID
    - Cost tracking: input/output tokens + computed cost logged
"""

import json
from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import text

from utility_api.agents.base import BaseAgent
from utility_api.config import settings
from utility_api.db import engine

# Reuse the proven system prompt from Sprint 3
from utility_api.ingest.rate_parser import SYSTEM_PROMPT

# Pricing (as of 2025)
MODEL_PRICING = {
    "claude-haiku-4-5-20251001": {"input": 0.80 / 1_000_000, "output": 4.0 / 1_000_000},
    "claude-sonnet-4-6-20250514": {"input": 3.0 / 1_000_000, "output": 15.0 / 1_000_000},
}

# Gallon conversions
CCF_TO_GAL = 748.0


def route_model(text_content: str) -> str:
    """Route to Sonnet or Haiku based on text complexity."""
    length = len(text_content)
    text_lower = text_content.lower()
    tier_keywords = sum(1 for kw in [
        "tier", "block", "step", "level", "ccf",
        "gallons", "1,000", "usage charge",
    ] if kw in text_lower)
    complex_signals = any(kw in text_lower for kw in [
        "budget-based", "drought", "seasonal", "surcharge", "cpuc",
        "allocation", "baseline", "customized", "water budget",
    ])

    if length > 10000 or tier_keywords > 6 or complex_signals:
        return "claude-sonnet-4-6-20250514"
    return "claude-haiku-4-5-20251001"


def validate_parse_result(result: dict) -> tuple[bool, list[str]]:
    """Validate parsed rate structure for sanity."""
    issues = []

    # Check tiers
    if not result.get("tier_1_rate"):
        issues.append("no_tier_1_rate")

    for i in range(1, 5):
        rate = result.get(f"tier_{i}_rate")
        if rate is not None:
            if rate < 0.1:
                issues.append(f"tier_{i}_rate_too_low:{rate}")
            if rate > 50:
                issues.append(f"tier_{i}_rate_too_high:{rate}")

    # Check fixed charge
    fixed = result.get("fixed_charge_monthly")
    if fixed is not None and fixed > 500:
        issues.append(f"fixed_charge_high:{fixed}")

    confidence = result.get("parse_confidence", "failed")
    if confidence == "failed":
        issues.append("confidence_failed")

    return (len(issues) == 0, issues)


def _build_volumetric_tiers_from_parse(result: dict) -> list[dict]:
    """Convert parsed CCF-based tiers to gallon-based JSONB array."""
    tiers = []
    prev_max = 0

    for i in range(1, 5):
        rate_ccf = result.get(f"tier_{i}_rate")
        if rate_ccf is None:
            break

        limit_ccf = result.get(f"tier_{i}_limit_ccf")
        max_gal = int(round(limit_ccf * CCF_TO_GAL)) if limit_ccf else None
        rate_per_1000 = round(rate_ccf * 1000.0 / CCF_TO_GAL, 4)

        tiers.append({
            "tier": i,
            "min_gal": prev_max,
            "max_gal": max_gal,
            "rate_per_1000_gal": rate_per_1000,
        })

        if max_gal:
            prev_max = max_gal
        else:
            break

    return tiers


def _compute_bill(gallons: float, tiers: list[dict], fixed: float = 0) -> float | None:
    """Compute bill at a given gallon consumption."""
    if not tiers:
        return None
    total = fixed
    remaining = gallons
    for tier in sorted(tiers, key=lambda t: t["tier"]):
        if remaining <= 0:
            break
        min_gal = tier.get("min_gal", 0) or 0
        max_gal = tier.get("max_gal")
        rate = tier.get("rate_per_1000_gal", 0) or 0
        if max_gal:
            vol = min(remaining, max_gal - min_gal)
        else:
            vol = remaining
        total += (vol / 1000.0) * rate
        remaining -= vol
    return round(total, 2)


class ParseAgent(BaseAgent):
    """Extracts structured rate data from raw text via Claude API."""

    agent_name = "parse"

    def run(
        self,
        pwsid: str,
        raw_text: str,
        content_type: str = "html",
        source_url: str | None = None,
        registry_id: int | None = None,
        **kwargs,
    ) -> dict:
        """Parse raw text to extract rate structure.

        Parameters
        ----------
        pwsid : str
            PWSID being parsed.
        raw_text : str
            Raw scraped text content.
        content_type : str
            html or pdf.
        source_url : str, optional
            URL the text was scraped from.
        registry_id : int, optional
            scrape_registry ID for status updates.

        Returns
        -------
        dict
            pwsid, success, model, cost_usd, confidence, tiers_found.
        """
        from anthropic import Anthropic

        schema = settings.utility_schema
        model = route_model(raw_text)
        logger.info(f"ParseAgent: {pwsid} ({len(raw_text):,} chars) → {model}")

        # Call Claude API with prompt caching
        client = Anthropic()
        user_message = (
            f"Extract the water rate structure from this {content_type} text.\n\n"
            f"Return a JSON object with these fields: rate_effective_date, "
            f"rate_structure_type, billing_frequency, fixed_charge_monthly, "
            f"meter_size_inches, tier_1_limit_ccf, tier_1_rate, tier_2_limit_ccf, "
            f"tier_2_rate, tier_3_limit_ccf, tier_3_rate, tier_4_limit_ccf, "
            f"tier_4_rate, parse_confidence, notes.\n\n"
            f"Text:\n{raw_text[:15000]}"  # Cap at 15K chars
        )

        try:
            response = client.messages.create(
                model=model,
                max_tokens=1024,
                system=[{
                    "type": "text",
                    "text": SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[
                    {"role": "user", "content": user_message},
                    {"role": "assistant", "content": "{"},
                ],
            )
        except Exception as e:
            logger.error(f"  API call failed: {e}")
            self._update_registry(registry_id, "failed", "low", 0, model)
            self.log_run(status="failed", notes=f"API error: {str(e)[:200]}")
            return {"pwsid": pwsid, "success": False, "error": str(e)}

        # Compute cost
        usage = response.usage
        pricing = MODEL_PRICING.get(model, MODEL_PRICING["claude-haiku-4-5-20251001"])
        cost = (usage.input_tokens * pricing["input"]
                + usage.output_tokens * pricing["output"])

        # Parse response
        try:
            raw_json = "{" + response.content[0].text
            result = json.loads(raw_json)
        except (json.JSONDecodeError, IndexError) as e:
            logger.warning(f"  JSON parse failed: {e}")
            self._update_registry(registry_id, "failed", "failed", cost, model)
            self.log_run(status="failed", notes=f"JSON parse error: {str(e)[:200]}")
            return {"pwsid": pwsid, "success": False, "cost_usd": cost, "error": "json_parse"}

        # Validate
        valid, issues = validate_parse_result(result)
        confidence = result.get("parse_confidence", "failed")
        logger.info(f"  Confidence: {confidence}, valid: {valid}, issues: {issues}")

        # Build canonical JSONB structures
        tiers = _build_volumetric_tiers_from_parse(result)
        fixed_charge = result.get("fixed_charge_monthly", 0) or 0
        fixed_charges_json = json.dumps([{
            "name": "Service Charge",
            "amount": round(float(fixed_charge), 2),
            "meter_size": str(result.get("meter_size_inches", "")) if result.get("meter_size_inches") else None,
        }]) if fixed_charge else None

        # Compute bills
        bill_5 = _compute_bill(3740, tiers, fixed_charge)
        bill_10 = _compute_bill(7480, tiers, fixed_charge)
        bill_20 = _compute_bill(14960, tiers, fixed_charge)

        # Conservation signal
        conservation = None
        if len(tiers) >= 2:
            rates = [t["rate_per_1000_gal"] for t in tiers if t.get("rate_per_1000_gal")]
            if len(rates) >= 2 and min(rates) > 0:
                conservation = round(max(rates) / min(rates), 2)

        # Write to rate_schedules
        success = False
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
                        "vintage": result.get("rate_effective_date"),
                        "billing_freq": result.get("billing_frequency"),
                        "rate_type": result.get("rate_structure_type"),
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
                        "notes": result.get("notes", ""),
                    })
                    conn.commit()
                success = True
                logger.info(
                    f"  ✓ {result.get('rate_structure_type')} | "
                    f"fixed=${fixed_charge:.2f} | "
                    f"bill@10CCF=${bill_10 or 0:.2f} | "
                    f"tiers={len(tiers)} | [{confidence}] | "
                    f"cost=${cost:.4f}"
                )

                # Trigger best estimate update
                try:
                    from utility_api.agents.best_estimate import BestEstimateAgent
                    BestEstimateAgent().run(state=pwsid[:2])
                except Exception as e:
                    logger.debug(f"  Best estimate update skipped: {e}")

            except Exception as e:
                logger.warning(f"  DB write failed: {e}")

        # Update scrape_registry
        parse_result = "success" if success else "failed"
        self._update_registry(registry_id, parse_result, confidence, cost, model)

        self.log_run(
            status="success" if success else "failed",
            rows_affected=1 if success else 0,
            notes=f"{pwsid}: {confidence}, {len(tiers)} tiers, ${cost:.4f}",
        )

        return {
            "pwsid": pwsid,
            "success": success,
            "model": model,
            "cost_usd": cost,
            "confidence": confidence,
            "tiers_found": len(tiers),
            "bill_10ccf": bill_10,
            "issues": issues if not valid else [],
        }

    def _update_registry(
        self, registry_id: int | None, parse_result: str,
        confidence: str, cost: float, model: str,
    ) -> None:
        """Update scrape_registry with parse outcome."""
        if registry_id is None:
            return
        schema = settings.utility_schema
        try:
            with engine.connect() as conn:
                conn.execute(text(f"""
                    UPDATE {schema}.scrape_registry SET
                        last_parse_at = :now,
                        last_parse_result = :result,
                        last_parse_confidence = :confidence,
                        last_parse_cost_usd = :cost,
                        status = CASE WHEN :result::text = 'success' THEN 'active' ELSE status END,
                        updated_at = :now
                    WHERE id = :id
                """), {
                    "now": datetime.now(timezone.utc),
                    "result": parse_result,
                    "confidence": confidence,
                    "cost": cost,
                    "id": registry_id,
                })
                conn.commit()
        except Exception as e:
            logger.debug(f"Registry parse update failed: {e}")
