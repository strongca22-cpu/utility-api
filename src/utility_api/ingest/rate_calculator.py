#!/usr/bin/env python3
"""
Water Bill Calculator

Purpose:
    Calculate monthly water bills from parsed tier structures.
    Given a rate structure (fixed charge + volumetric tiers) and a
    consumption level in CCF, compute the total monthly bill.

Author: AI-Generated
Created: 2026-03-23
Modified: 2026-03-23

Dependencies:
    - (none — pure Python)

Usage:
    from utility_api.ingest.rate_calculator import calculate_bill
    bill = calculate_bill(
        fixed_charge=15.50,
        tiers=[(5, 3.50), (10, 5.25), (None, 7.80)],
        consumption_ccf=10,
    )

Notes:
    - Tiers are (limit_ccf, rate_per_ccf) tuples in ascending order
    - A tier with limit=None means "unlimited" (highest tier)
    - For flat/uniform rates, pass a single tier: [(None, rate)]
    - Returns None if rate data is insufficient for calculation
"""

from loguru import logger


def calculate_bill(
    fixed_charge: float | None,
    tiers: list[tuple[float | None, float | None]],
    consumption_ccf: float,
) -> float | None:
    """Calculate a monthly water bill from a tier structure.

    Parameters
    ----------
    fixed_charge : float | None
        Monthly fixed/base/service charge in dollars.
    tiers : list[tuple[float | None, float | None]]
        List of (upper_limit_ccf, rate_per_ccf) tuples.
        Tiers must be in ascending order of consumption.
        The last tier should have limit=None (unlimited).
    consumption_ccf : float
        Monthly water consumption in CCF (100 cubic feet).

    Returns
    -------
    float | None
        Calculated monthly bill in dollars, or None if insufficient data.

    Examples
    --------
    >>> # Flat rate: $5.00/CCF + $10 base
    >>> calculate_bill(10.0, [(None, 5.0)], 8)
    50.0

    >>> # Two-tier increasing block: 0-5 CCF @ $3.50, 5+ @ $5.25
    >>> calculate_bill(15.50, [(5, 3.50), (None, 5.25)], 10)
    59.75
    """
    # Filter out tiers with no rate
    valid_tiers = [(limit, rate) for limit, rate in tiers if rate is not None]

    if not valid_tiers:
        return None

    total = fixed_charge or 0.0
    remaining = consumption_ccf
    prev_limit = 0.0

    for limit, rate in valid_tiers:
        if remaining <= 0:
            break

        if limit is None:
            # Unlimited tier — apply rate to all remaining consumption
            total += remaining * rate
            remaining = 0
        else:
            # Bounded tier — apply rate up to (limit - prev_limit) CCF
            tier_width = limit - prev_limit
            tier_consumption = min(remaining, tier_width)
            total += tier_consumption * rate
            remaining -= tier_consumption
            prev_limit = limit

    # If there's remaining consumption but no unlimited tier, apply last rate
    if remaining > 0 and valid_tiers:
        last_rate = valid_tiers[-1][1]
        total += remaining * last_rate

    return round(total, 2)


def calculate_bills_from_parse(parse_result) -> tuple[float | None, float | None]:
    """Calculate bills at 5 and 10 CCF from a ParseResult.

    Parameters
    ----------
    parse_result : ParseResult
        Result from rate_parser.parse_rate_text().

    Returns
    -------
    tuple[float | None, float | None]
        (bill_5ccf, bill_10ccf) — either may be None if calculation fails.
    """
    # Build tier list from parse result
    tiers = []

    if parse_result.tier_1_rate is not None:
        tiers.append((parse_result.tier_1_limit_ccf, parse_result.tier_1_rate))

    if parse_result.tier_2_rate is not None:
        tiers.append((parse_result.tier_2_limit_ccf, parse_result.tier_2_rate))

    if parse_result.tier_3_rate is not None:
        tiers.append((parse_result.tier_3_limit_ccf, parse_result.tier_3_rate))

    if parse_result.tier_4_rate is not None:
        tiers.append((parse_result.tier_4_limit_ccf, parse_result.tier_4_rate))

    if not tiers:
        return None, None

    bill_5 = calculate_bill(parse_result.fixed_charge_monthly, tiers, 5.0)
    bill_10 = calculate_bill(parse_result.fixed_charge_monthly, tiers, 10.0)

    if bill_5 is not None:
        logger.debug(f"Bill at 5 CCF: ${bill_5:.2f}")
    if bill_10 is not None:
        logger.debug(f"Bill at 10 CCF: ${bill_10:.2f}")

    return bill_5, bill_10
