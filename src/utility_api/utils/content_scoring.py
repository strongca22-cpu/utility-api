#!/usr/bin/env python3
"""
Content-Aware Scoring Boost

Purpose:
    Computes a score bonus for scraped text that contains rate-bearing
    signals. Used as a post-scrape re-ranking boost on top of the
    URL-based heuristic score from discovery.

    Funnel analysis (Sprint 26) showed that 24% of parse failures had
    rate content in rank 2/3 URLs that rank 1 lacked. The URL-based
    scorer can't distinguish rate-bearing content from rate-adjacent
    content. This content boost pulls actual rate pages up in ranking.

Author: AI-Generated
Created: 2026-03-31
Modified: 2026-03-31

Usage:
    from utility_api.utils.content_scoring import compute_content_boost
    boost = compute_content_boost(scraped_text)  # returns 0-20 (or negative)
"""


def compute_content_boost(text: str) -> int:
    """Compute a score boost based on rate-bearing signals in scraped content.

    Checks the text for patterns that indicate extractable residential
    water rate data. Returns a bonus (or penalty) to add to the URL
    relevance score.

    Parameters
    ----------
    text : str
        Scraped text content (can be full 45k or truncated).

    Returns
    -------
    int
        Bonus points (-15 to +20) to add to the URL relevance score.
    """
    if not text or len(text) < 200:
        return 0

    # Sample a manageable chunk — full text can be 45k+
    # Check first 5k and last 5k (rate tables often at top or bottom of page)
    sample = text[:5000] + text[-5000:] if len(text) > 10000 else text
    sample_lower = sample.lower()

    boost = 0

    # Strong signals: actual rate structure content
    # Dollar amounts with volumetric units = very likely a rate page
    if "per 1,000" in sample_lower or "per thousand" in sample_lower:
        boost += 8
    if "ccf" in sample_lower:
        boost += 6
    if "/1000 gal" in sample_lower or "per 1000 gal" in sample_lower:
        boost += 6

    # Residential rate schedule language
    if "residential" in sample_lower and (
        "rate" in sample_lower or "charge" in sample_lower
    ):
        boost += 4

    # Tier structure indicators
    tier_signals = sum(
        1 for t in [
            "tier 1", "tier 2", "tier 3", "block 1", "block 2",
            "first tier", "second tier", "0 - ", "0-",
        ]
        if t in sample_lower
    )
    if tier_signals >= 2:
        boost += 5

    # Negative: sewer-only, meeting docs, error pages
    if "sewer" in sample_lower and "water rate" not in sample_lower:
        boost -= 10
    if "meeting" in sample_lower and "agenda" in sample_lower:
        boost -= 10
    if "404" in sample_lower and "not found" in sample_lower:
        boost -= 15

    return max(min(boost, 20), -15)
