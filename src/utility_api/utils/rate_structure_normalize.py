#!/usr/bin/env python3
"""
Rate Structure Type Normalization

Purpose:
    Maps the 100+ LLM-generated rate_structure_type variants to 6 canonical
    types: flat, uniform, increasing_block, decreasing_block, budget_based,
    seasonal. Logs unmapped values for future extension.

Author: AI-Generated
Created: 2026-03-31
Modified: 2026-03-31 (Sprint 26: +80 variants from Scenario A batch)

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
    "flat_fixed_charge": "flat",
    "flat_rate_with_demand_charges": "flat",
    "flat_rate_with_capital_charges": "flat",
    "flat_rate_with_large_user_tiers": "flat",
    "flat_rate_with_allowance": "flat",
    "flat_rate_with_allowance_options": "flat",
    "flat_rate_with_base_charge": "flat",
    "flat_rate_with_service_charge": "flat",
    "flat rate with customer charge": "flat",
    "flat rate (uniform)": "flat",
    "flat volumetric": "flat",
    "flat volumetric (single tier)": "flat",
    "flat_rate_tiered_bill": "flat",
    "flat_rate_with_volume_charge": "flat",
    "single_rate_flat": "flat",
    "single_tier_flat": "flat",
    "inclining block (flat rate)": "flat",

    # --- uniform: fixed charge + single volumetric rate ---
    "uniform": "uniform",
    "uniform_rate": "uniform",
    "uniform/flat": "uniform",
    "uniform/flat_rate": "uniform",
    "uniform_flat_rate": "uniform",
    "uniform_flat": "uniform",
    "uniform rate": "uniform",
    "flat_uniform": "uniform",
    "flat_uniform_rate": "uniform",
    "flat/uniform": "uniform",
    "flat/uniform rate": "uniform",
    "flat/uniform volumetric rate": "uniform",
    "flat_volumetric": "uniform",
    "flat_rate_with_usage_charge": "uniform",
    "flat_rate_with_consumption": "uniform",
    "flat_rate_with_volumetric": "uniform",
    "flat_rate_with_volumetric_over": "uniform",
    "flat_rate_with_uniform_volumet": "uniform",
    "flat_rate_with_uniform_volumetric": "uniform",
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
    "flat rate with fixed charge plus volumetric consumption charge": "uniform",
    "fixed charge + uniform volumet": "uniform",
    "fixed charge + volumetric": "uniform",
    "fixed_charge_plus_tiered": "uniform",
    "fixed_charge_plus_tiered_volumetric": "uniform",
    "fixed_charge_plus_single_tiered_usage": "uniform",
    "single_tier_uniform": "uniform",
    "single_tiered_flat_rate": "uniform",
    "single_tier": "uniform",
    "single_rate": "uniform",
    "single_rate_flat": "uniform",
    "single_volumetric_rate": "uniform",
    "single_utility_multiple_tiers": "uniform",
    "uniform (single volumetric rat": "uniform",
    "uniform (flat rate per unit volume)": "uniform",
    "uniform with minimum charge an": "uniform",
    "uniform_volumetric": "uniform",
    "uniform_block": "uniform",
    "consumption-based": "uniform",
    "volumetric": "uniform",

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
    "ascending_block": "increasing_block",
    "ascending block (tiered)": "increasing_block",
    "ascending block (3 tiers)": "increasing_block",
    "ascending rate block": "increasing_block",
    "ascending rate block (tiered)": "increasing_block",
    "ascending rate block (3 tiers)": "increasing_block",
    "inclining block": "increasing_block",
    "inclining_block_rate": "increasing_block",
    "tiered (2 tiers)": "increasing_block",
    "tiered (3 tiers)": "increasing_block",
    "tiered (4 tiers)": "increasing_block",
    "tiered (6 tiers)": "increasing_block",
    "tiered (9 tiers)": "increasing_block",
    "tiered (4 blocks)": "increasing_block",
    "tiered (multiple tiers)": "increasing_block",
    "tiered (inclining block)": "increasing_block",
    "tiered (increasing block)": "increasing_block",
    "tiered (ascending rate block)": "increasing_block",
    "tiered (base volume + usage)": "increasing_block",
    "tiered with cap": "increasing_block",
    "tiered with base volume": "increasing_block",
    "tiered with base": "increasing_block",
    "tiered/block": "increasing_block",
    "tiered/inclining block": "increasing_block",
    "tiered/demand block": "increasing_block",
    "tiered_two_tier": "increasing_block",
    "tiered_ascending": "increasing_block",
    "tiered_incremental": "increasing_block",
    "tiered_flat_block": "increasing_block",
    "tiered_flat_minimum_plus_variable": "increasing_block",
    "tiered_volume": "increasing_block",
    "tiered_commodity": "increasing_block",
    "tiered_allowance": "increasing_block",
    "tiered_conservation_block": "increasing_block",
    "tiered_with_base": "increasing_block",
    "tiered_with_base_charge": "increasing_block",
    "tiered_with_base_volume": "increasing_block",
    "tiered_with_base_facility_charge": "increasing_block",
    "tiered_with_inclining_blocks": "increasing_block",
    "tiered_based_on_bfg": "increasing_block",
    "tiered_by_gpd_with_usage_charges": "increasing_block",
    "tiered_large_user": "increasing_block",
    "tiered_volumetric_with_fixed_charge": "increasing_block",
    "tiered_volumetric_with_fixed_charges": "increasing_block",
    "tiered_volumetric_with_seasonal_variation": "increasing_block",
    "tiered volumetric with fixed minimum charge": "increasing_block",
    "tiered volumetric with fixed base charge": "increasing_block",
    "two-tier": "increasing_block",
    "two-tiered": "increasing_block",
    "two-tiered volumetric": "increasing_block",
    "two-tier volumetric with minimum charge": "increasing_block",
    "two-tier inclining block": "increasing_block",
    "two_tier_volume": "increasing_block",
    "four_tier_inclining_block": "increasing_block",
    "four-tier inclining block": "increasing_block",
    "block": "increasing_block",
    "tier": "increasing_block",
    "commercial": "increasing_block",
    "commercial_tiered": "increasing_block",

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
    "tiered with seasonal variation": "seasonal",
    "seasonal tiered rate structure": "seasonal",
}

# Values that indicate LLM failure — should normalize to None, not a canonical type.
# These are checked BEFORE the map lookup and short-circuit to None.
_FAILURE_SIGNALS = frozenset({
    "failed", "unknown", "unable_to_determine", "unable to determine",
    "insufficient_data", "not_applicable", "not_available", "unavailable",
    "n/a", "not found", "failed to parse", "failed_to_determine",
    "uncertain",
})

# Values that indicate wrong utility type — not a water supply rate.
_WRONG_UTILITY_SIGNALS = frozenset({
    "sewer", "stormwater_fee_not_water_rate",
    "n/a - stormwater rates only",
    "this is a sewer utility, not a water utility",
    "wholesale", "wholesale_supply", "surcharge",
    "tap_fees_only",
})

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

    # Known failure/error signals → None (LLM couldn't determine type)
    if cleaned in _FAILURE_SIGNALS:
        return None

    # Wrong utility type (sewer, stormwater, wholesale) → None
    if cleaned in _WRONG_UTILITY_SIGNALS:
        return None

    # Check for verbose failure signals embedded in longer strings
    for signal in ("unable to determine", "failed to determine",
                   "multiple utilities", "not water", "no residential"):
        if signal in cleaned:
            return None

    # Look up in normalization map
    canonical = _NORMALIZATION_MAP.get(cleaned)
    if canonical:
        return canonical

    # Truncated keys — try matching first 30 chars (LLM truncation)
    if len(cleaned) > 30:
        truncated = cleaned[:30]
        canonical = _NORMALIZATION_MAP.get(truncated)
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
