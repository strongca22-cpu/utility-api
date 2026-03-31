# Outlier Filter for Rate Data — v0

## Context

The best_estimate table contains bill amounts ranging from $2.80 to $9,315 at 10 CCF. The extremes are parse errors or data quality issues, not real residential rates. A plausible ceiling for residential water at 10 CCF in the US is roughly $400–500 at the extreme high end (parts of CA, some island communities). Similarly, bills <$5/mo at 10 CCF are almost certainly wrong — wholesale rates, commercial rates, or per-unit values stored as bills.

### Current Outlier Landscape

**High-end outliers (best_estimate):**

| Source | >$500 | >$1,000 | Max |
|---|---|---|---|
| efc_az_2014 | 4 | 3 | $9,315 |
| efc_nh_2016 | 1 | 1 | $3,414 |
| scraped_llm | 6 | 1 | $1,180 |

The worst offenders are EFC bulk data (Arizona 2014 survey), not LLM parsing. The scraped_llm outliers include some that may be genuine (Philadelphia at $754, Maunabo PR at $754) and some that are likely parse errors (Ferndale MI at $1,180).

**scraped_llm records >$300 (13 total):**
- $1,180 — Ferndale MI (likely commercial or annual rate)
- $866 — Bexley OH (same pattern)
- $754 — Philadelphia PA, Maunabo PR (probably genuine — very expensive utilities)
- $536, $518 — NJ utilities (NJ has high water costs, may be real)
- $414 — Kearney NE (plausible but high)
- $402 — San Joaquin CA (drought surcharges make this plausible)
- $325–$355 — Various (plausible range for expensive utilities)

**Low-end outliers:**
- 1 scraped_llm record with bill <$5 (likely per-unit rate stored as bill)
- Many $0 bills (these are already filtered by the pipeline)

## Problem Statement

Currently, the pipeline writes ALL parsed rates to rate_schedules regardless of plausibility. The bill consistency check catches one failure mode (identical bills at all volumes), but there's no range-based filter. Implausible values:
1. Flow through to best_estimate
2. Appear on the dashboard
3. Distort coverage statistics and aggregates
4. Undermine data credibility

## Requirements

### What the filter should do

1. **Flag** — don't silently delete. Mark records for review rather than dropping them entirely. Data may be correct and the threshold may need adjustment.
2. **Apply at write time** — catch outliers when writing to rate_schedules, not after the fact.
3. **Source-aware** — apply to scraped_llm and bulk ingest sources (EFC outliers are just as wrong).
4. **Configurable thresholds** — stored in config, not hardcoded.
5. **Two tiers:**
   - **Soft flag** ($300–$500 at 10 CCF): write to rate_schedules but mark `needs_review=True` with `review_reason='bill_outlier_high'`
   - **Hard reject** (>$500 at 10 CCF): do NOT write to rate_schedules. Log the rejection. (OR: write with `confidence='low'` so it can't win best_estimate)
   - **Low-end**: <$5 at 10 CCF → hard reject or flag
6. **Per-CCF validation** — if bill_5ccf > bill_10ccf (consumption goes up but bill goes down for non-decreasing_block), flag as suspect.
7. **Ratio check** — if bill_20ccf / bill_10ccf > 3.0 for increasing_block, the tier escalation is implausibly steep.

### Where to apply

- `src/utility_api/agents/batch.py` — batch processing path (after bill computation, before DB write)
- `src/utility_api/agents/parse.py` — direct pipeline path (ParseAgent.parse())
- Optionally: `src/utility_api/ops/best_estimate.py` — as a secondary filter when selecting best estimate

### Suggested config location

`config/rate_validation.yaml`:
```yaml
bill_outlier:
  # Bills above hard_ceiling are rejected (or written as low confidence)
  hard_ceiling_10ccf: 500
  # Bills between soft_ceiling and hard_ceiling are flagged for review
  soft_ceiling_10ccf: 300
  # Bills below floor are rejected
  floor_10ccf: 5
  # Bill ratio: bill_20ccf / bill_10ccf should not exceed this for increasing_block
  max_20_10_ratio: 3.0
  # Apply to these sources (empty = all)
  apply_to_sources: []  # all sources
```

## Scope

### In scope
- Outlier detection function with config-driven thresholds
- Integration into batch.py and parse.py write paths
- needs_review + review_reason flagging for soft outliers
- Rejection or confidence downgrade for hard outliers
- Retroactive cleanup: query existing rate_schedules for records above thresholds, flag or remove

### Out of scope
- Geographic-specific thresholds (e.g., CA ceiling higher than KS) — defer to v1
- Automated anomaly detection based on state/county distributions — future work
- UI for reviewing flagged records — dashboard can show flags but no approve/reject workflow yet

## Existing Infrastructure

- `rate_schedules.needs_review` (boolean) and `rate_schedules.review_reason` (text) already exist
- `check_bill_consistency()` in parse.py is the existing validation pattern — this extends it
- `config/` directory has the YAML config pattern established

## Retroactive Cleanup

After implementing the filter, run a one-time scan:

```sql
-- Flag existing outliers
UPDATE utility.rate_schedules
SET needs_review = true,
    review_reason = 'bill_outlier_high'
WHERE bill_10ccf > 300
  AND needs_review = false;

-- Check EFC outliers specifically
SELECT pwsid, source_key, bill_10ccf
FROM utility.rate_schedules
WHERE bill_10ccf > 500
ORDER BY bill_10ccf DESC;
```

Then rebuild best_estimate to exclude flagged records.

## Key Files

- `src/utility_api/agents/batch.py` — batch write path (lines ~460-480)
- `src/utility_api/agents/parse.py` — direct write path, `check_bill_consistency()` pattern
- `config/rate_validation.yaml` — new config file
- `src/utility_api/ops/best_estimate.py` — optional secondary filter
