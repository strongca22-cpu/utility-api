#!/usr/bin/env python3
"""
Rate Structure Type Normalization

Purpose:
    Maps the 100+ LLM-generated rate_structure_type variants to 6 canonical
    types: flat, uniform, increasing_block, decreasing_block, budget_based,
    seasonal. Logs unmapped values for future extension.

Author: AI-Generated
Created: 2026-03-31
Modified: 2026-03-31

Usage:
    from utility_api.utils.rate_structure_normalize import normalize_rate_structure_type
    canonical = normalize_rate_structure_type("tiered_volumetric")  # → "increasing_block"
"""

from loguru import logger

# Canonical types
VALID_TYPES = frozenset({
    "flat", "uniform", "increasing_block", "decreasing_block",
    "budget_based", "seasonal",
})

# Normalization map: lowercase variant → canonical type
# Built from 100+ observed LLM outputs across Sprint 25 batches
_NORMALIZATION_MAP = {
    # --- flat: single fixed price, no volumetric charge ---
    "flat": "flat",
    "flat rate": "flat",
    "flat_rate": "flat",
    "single_flat_rate": "flat",
    "flat rate (single tier)": "flat",
    "flat rate - single volumetric": "flat",

    # --- uniform: fixed charge + single volumetric rate ---
    "uniform": "uniform",
    "uniform_rate": "uniform",
    "uniform/flat": "uniform",
    "uniform/flat_rate": "uniform",
    "uniform_flat_rate": "uniform",
    "flat_uniform": "uniform",
    "flat_uniform_rate": "uniform",
    "flat/uniform": "uniform",
    "flat/uniform rate": "uniform",
    "flat_volumetric": "uniform",
    "flat_rate_with_usage_charge": "uniform",
    "flat_rate_with_consumption": "uniform",
    "flat_rate_with_volumetric": "uniform",
    "flat_rate_with_volumetric_over": "uniform",
    "flat_rate_with_uniform_volumet": "uniform",
    "flat_rate_plus_volumetric": "uniform",
    "flat_rate_plus_usage": "uniform",
    "flat_rate_plus_overage": "uniform",
    "flat_rate_with_variable": "uniform",
    "flat_rate_uniform": "uniform",
    "flat_rate_volumetric": "uniform",
    "flat_rate_per_volume": "uniform",
    "flat_rate_with_fixed_charge": "uniform",
    "flat_rate_with_fixed_minimum": "uniform",
    "flat_rate_with_minimum": "uniform",
    "flat_with_minimum": "uniform",
    "flat_volumetric_with_minimum_c": "uniform",
    "flat volumetric with fixed cha": "uniform",
    "flat rate with fixed charge": "uniform",
    "flat rate with usage charge": "uniform",
    "flat rate with consumption cha": "uniform",
    "flat rate (single tier volumet": "uniform",
    "flat rate - single volumetric ": "uniform",
    "fixed charge + uniform volumet": "uniform",
    "fixed charge + volumetric": "uniform",
    "single_tier_uniform": "uniform",
    "single_tiered_flat_rate": "uniform",
    "uniform (single volumetric rat": "uniform",
    "uniform with minimum charge an": "uniform",
    "uniform_volumetric": "uniform",
    "uniform_block": "uniform",
    "consumption-based": "uniform",

    # --- increasing_block: volumetric rate increases at higher tiers ---
    "tiered": "increasing_block",
    "tiered_volumetric": "increasing_block",
    "tiered_increasing": "increasing_block",
    "tiered_block": "increasing_block",
    "tiered_conservation": "increasing_block",
    "tiered_residential": "increasing_block",
    "tiered_inclining": "increasing_block",
    "tiered_inclining_block": "increasing_block",
    "tiered_flat_commodity": "increasing_block",
    "tiered_with_minimum": "increasing_block",
    "tiered_with_fixed_charge": "increasing_block",
    "tiered_with_allowance": "increasing_block",
    "tiered_volumetric_with_fixed_c": "increasing_block",
    "tiered_volumetric_with_minimum": "increasing_block",
    "tiered_volumetric_with_base_ch": "increasing_block",
    "tiered_based_on_average_winter": "increasing_block",
    "tiered_by_average_daily_consum": "increasing_block",
    "tiered volumetric": "increasing_block",
    "tiered volumetric with fixed c": "increasing_block",
    "tiered volumetric with fixed b": "increasing_block",
    "tiered volumetric with minimum": "increasing_block",
    "tiered - single volume rate": "increasing_block",
    "tiered - seasonal": "increasing_block",
    "tiered - 4 tiers": "increasing_block",
    "tiered (5 tiers)": "increasing_block",
    "tiered (8 tiers)": "increasing_block",
    "tiered (2-tier)": "increasing_block",
    "tiered with minimum charge": "increasing_block",
    "tiered/increasing block": "increasing_block",
    "tiered volumetric with fixed c": "increasing_block",
    "flat_rate_tiered": "increasing_block",
    "two_tier": "increasing_block",
    "two_tier_with_minimum": "increasing_block",
    "two-tier volumetric with fixed": "increasing_block",
    "increasing_block": "increasing_block",
    "increasing_tiered": "increasing_block",
    "increasing_tier": "increasing_block",
    "increasing block": "increasing_block",
    "increasing block rate": "increasing_block",
    "increasing block tariff": "increasing_block",
    "increasing block/tiered": "increasing_block",
    "inclining_block": "increasing_block",
    "ascending block": "increasing_block",

    # --- decreasing_block: volumetric rate decreases at higher tiers ---
    "decreasing_block": "decreasing_block",
    "declining_block": "decreasing_block",
    "decreasing block rate structur": "decreasing_block",

    # --- budget_based: individualized allocation ---
    "budget_based": "budget_based",
    "budget_based_tiered": "budget_based",

    # --- seasonal: rates vary by season ---
    "seasonal": "seasonal",
    "seasonal_tiered": "seasonal",
    "seasonal_flat": "seasonal",
    "seasonal_uniform": "seasonal",
    "seasonal uniform": "seasonal",
    "seasonal_two_tier": "seasonal",
    "seasonal_peak": "seasonal",
    "seasonal tiered": "seasonal",
    "tiered_seasonal": "seasonal",
    "tiered_with_seasonal_variation": "seasonal",
}

# Track unmapped values for logging
_unmapped_seen = set()


def normalize_rate_structure_type(raw_value: str | None) -> str | None:
    """Normalize a rate_structure_type string to one of 6 canonical types.

    Parameters
    ----------
    raw_value : str | None
        Raw LLM output or existing value.

    Returns
    -------
    str | None
        Canonical type (flat, uniform, increasing_block, decreasing_block,
        budget_based, seasonal) or None if input is None/empty.
    """
    if not raw_value:
        return None

    cleaned = raw_value.strip().lower()

    # Already canonical
    if cleaned in VALID_TYPES:
        return cleaned

    # Look up in normalization map
    canonical = _NORMALIZATION_MAP.get(cleaned)
    if canonical:
        return canonical

    # Unmapped — log once per value
    if cleaned not in _unmapped_seen:
        _unmapped_seen.add(cleaned)
        logger.warning(
            f"Unmapped rate_structure_type: '{raw_value}' — defaulting to None. "
            f"Add to rate_structure_normalize.py"
        )

    return None
