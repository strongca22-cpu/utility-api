# Session Summary — Sprint 25 (2026-03-30)

## What Was Done

### 1. Source URL Propagation (Migration 021)
Added `source_url` column to `rate_best_estimate` table so scraped rates carry the original utility rate page URL. Enables spot-check QA on the dashboard.
- **Files:** migration 021, `rate_best_estimate.py` model, `best_estimate.py` (threaded through SQL → selection → insert)
- **Result:** 3,852 rows with URLs, 5,810 properly NULL. Fixed a pandas NaN→string bug caught during verification.

### 2. Tier Label Cleanup
- EFC/SWRCB/OWRS → `tier='bulk'`, scraped_llm → `tier='premium'`, Duke rationale updated
- `source_catalog.py` comment updated, Duke ingest "INTERNAL REFERENCE ONLY" language removed
- No enum rewrite — `tier` is `String(20)`, labels are presentation layer

### 3. Three Validation Runs (Discovery + Parse)
- **440 gap states (>=3k pop):** 261/440 success (64%), $12.10
- **ND (>=500 pop):** 56/119 success (62%), $2.72 — coverage 2%→21%
- **SD (>=500 pop):** 86/141 success (67%), $2.90 — coverage 1%→21%
- All three rebuilt best_estimate automatically on completion

### 4. Score Threshold Lowered 50→45
- Config-driven now (was hardcoded in `discovery.py`)
- `config/agent_config.yaml`: `url_score_threshold: 45`
- 1,219 near-miss URLs at 45-49 in the 440 sweep would now qualify

### 5. Coverage Analysis + Strategy Report
- Full statistical analysis of coverage by state, population bucket, source type
- `docs/coverage_strategy_sprint25.md` — three expansion scenarios with costs
- `docs/pipeline_failure_analysis.md` updated with three-run comparison

### 6. Scenario A Launched (Comprehensive Sweep)
- **Script:** `scripts/run_scenario_a.py` — batch API architecture
- **Running in:** `tmux attach -t scenario_a`
- **Scope:** 4,912 PWSIDs (all gap >=3k + 8 zero-candidate retries)
- **Design:** Discover + scrape all → submit Anthropic Batch API → wait ~24hr → process results
- **Budget:** ~19.6k Serper queries (of 49k), ~$28 Anthropic batch (of $110)
- **Expected:** ~3,000 new rates, pop coverage 73%→86%

## Key Decisions Made
- Duke stays in separate table, just relabeled as "free-tier attributed"
- `free_attributed`, `bulk`, `premium` are the active tier labels (no enum rewrite)
- Batch API for comprehensive sweep (50% cost savings, 24hr latency acceptable)
- Duke-only PWSIDs (963) handled as separate sweep, not mixed into gap sweep
- Score threshold 45 is the new standard (was 50)

## Active Processes
- `tmux attach -t scenario_a` — Scenario A discovery+scrape, started 14:10 UTC, ~27 hours
- `tmux attach -t parse_sweep` — background parse retry loop (30min cycles)
- After Scenario A discovery completes: batch auto-submitted to Anthropic
- After ~24hr: run `python scripts/run_scenario_a.py --process-batch`

## What's Next
1. **~27 hours:** Scenario A discovery+scrape completes, batch submitted
2. **~51 hours:** Batch results ready, run `--process-batch` to write rates + rebuild best_estimate
3. **Duke-only sweep:** 963 PWSIDs, separate script (TX 221, CA 167, PA 158, WA 123, NJ 108)
4. **Hard states investigation:** CO (38% parse), MT (29%), NV (25%) need keyword tuning
5. **Product decision:** Population floor (>=3k covers 86%, >=500 adds 2% for 3x PWSIDs)

## Key Numbers
| Metric | Before Sprint 25 | After Sprint 25 | After Scenario A (projected) |
|---|---|---|---|
| Systems with rates | 9,685 | 9,834 | ~12,800 |
| Population covered | 73.0% | 73.1% | ~86% |
| States >50% system coverage | 11 | 11 | ~20 |
