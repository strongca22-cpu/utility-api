# Sprint 24c: Full Gap-State Sweep — 440 PWSIDs with Optimized Cascade v2

## Context

Sprint 24b built and validated the Serper discovery + cascade processing pipeline on 150 PWSIDs. Three rounds of optimization followed based on diagnostic data:

**Pipeline architecture (current state):**
```
Serper discovery (4 queries/PWSID, top 3 URLs)
    → Fetch with Playwright escalation (thin + high-confidence → retry JS)
    → Navigation crawl (thin after Playwright → follow rate-keyword links)
    → Score all candidates
    → 3-attempt cascade parse (stop on first success)
    → Reactive deep crawl (only if all 3 Serper URLs fail)
    → BestEstimate per state (batched at end)
    → Diagnostics logged to discovery_diagnostics table
```

**150-PWSID diagnostic results:**
- 87/150 (58%) cascade parse success
- 91% discovery hit rate (Serper finds URLs for almost everything)
- Rank 2+3 contributed 49% of successes → keep top 3
- Cascade rescued 34% of successes → keep 3-attempt strategy
- Deep crawl only 5% of wins → switched to reactive (saves ~40% fetches)
- Playwright escalation + nav crawl added for thin high-confidence pages

**Source hierarchy updated:** Duke/reference datasets deprioritized to priority 8. LLM scrape (3), government (1-2), and curated (4) all beat reference when competing bills exist. Pattern-based EFC priority (2) added.

**Serper budget:** 744 queries used of 2,500 free tier. **1,756 remaining** — enough for ~440 PWSIDs at 4 queries each.

## Task

Run the full gap-state sweep using the remaining free Serper budget. This is a production run, not a diagnostic — maximize coverage.

### Step 1: Pre-flight checks

```bash
# Verify Serper budget
python scripts/serper_bulk_discovery.py --usage

# Dry run to see target list
python scripts/serper_bulk_discovery.py --max-pwsids 440 --dry-run

# Verify pipeline components import correctly
python -c "from utility_api.pipeline.process import process_pwsid; print('OK')"
```

### Step 2: Launch the sweep

```bash
# Run in tmux — this will take ~2.5-3 hours at ~150 PWSIDs/hour
tmux new-session -d -s serper_sweep \
    "cd ~/projects/utility-api && python3 scripts/serper_bulk_discovery.py \
     --max-pwsids 440 --process immediate --diagnostic 2>&1 \
     | tee logs/serper_sweep_full_$(date +%Y%m%d_%H%M).log"
```

Using `--process immediate` (not batch) because:
- Streaming results — can monitor in real-time
- Can abort early if something goes wrong
- Diagnostics accumulate as the run progresses

### Step 3: Monitor progress

```bash
# Attach to watch live
tmux attach -t serper_sweep

# Or tail the log
tail -f logs/serper_sweep_full_*.log

# Check progress checkpoints (logged every 25 PWSIDs)
grep "Progress:" logs/serper_sweep_full_*.log
```

### Step 4: Post-sweep analysis

After the sweep completes, run diagnostic queries:

```sql
-- Overall success rate (this sweep only — filter by today's date)
SELECT parse_success, count(*)
FROM utility.discovery_diagnostics
WHERE run_at > CURRENT_DATE
GROUP BY 1;

-- Winning source: serper vs deep_crawl vs nav_crawl
SELECT winning_source, count(*)
FROM utility.discovery_diagnostics
WHERE parse_success AND run_at > CURRENT_DATE
GROUP BY 1 ORDER BY 2 DESC;

-- Which discovery rank wins?
SELECT winning_discovery_rank, count(*)
FROM utility.discovery_diagnostics
WHERE parse_success AND run_at > CURRENT_DATE
GROUP BY 1 ORDER BY 1;

-- Playwright escalation effectiveness
SELECT
    count(*) FILTER (WHERE notes LIKE '%playwright_reason=thin_js_recovered%') as js_recovered,
    count(*) FILTER (WHERE notes LIKE '%playwright_reason=thin_still_thin%') as still_thin,
    count(*) FILTER (WHERE notes LIKE '%nav_crawl_success=true%') as nav_success,
    count(*) FILTER (WHERE notes LIKE '%nav_crawl_success=false%') as nav_fail
FROM utility.scrape_registry
WHERE url_source = 'serper' AND updated_at > CURRENT_DATE;

-- Coverage improvement
SELECT
    count(*) as total_pwsids,
    count(*) FILTER (WHERE has_rate_data) as with_rates,
    round(100.0 * count(*) FILTER (WHERE has_rate_data) / count(*), 1) as pct
FROM utility.pwsid_coverage;

-- State-level success (sort by volume)
SELECT left(pwsid, 2) as st,
       count(*) as total,
       count(*) FILTER (WHERE parse_success) as ok,
       round(100.0 * count(*) FILTER (WHERE parse_success) / count(*)) as pct
FROM utility.discovery_diagnostics
WHERE run_at > CURRENT_DATE
GROUP BY 1
ORDER BY 2 DESC;
```

### Step 5: Update coverage report and commit

```bash
ua-ops coverage-report
```

Git commit the results with a summary of: PWSIDs searched, success rate, new coverage total, Serper budget consumed, and any notable state-level patterns.

### Step 6: Decide on paid tier

If the sweep confirms >50% parse success rate at scale:
- Free tier is now exhausted
- Serper Starter ($50 for 50K queries) would cover ~12,500 PWSIDs
- Full gap-state universe is ~32,000 PWSIDs → ~$128 for complete sweep
- Recommendation threshold: if this sweep adds 200+ new PWSIDs with rates, the paid tier ROI is clear

## Expected Results

| Metric | Estimate | Basis |
|---|---|---|
| PWSIDs searched | ~440 | Budget cap |
| Discovery hit rate | ~91% | 150-PWSID baseline |
| URLs discovered | ~400 | 91% of 440 |
| Cascade parse success | ~55-60% | 150-PWSID baseline, slightly lower at scale |
| **New PWSIDs with rates** | **~220-260** | 55-60% of 400 |
| Serper queries used | ~1,760 | 440 × 4 |
| Estimated time | ~2.5-3 hours | 150/hr immediate mode |
| Cost | **$0** | Free tier |

## What NOT to Do

1. **Do not use `--process batch`** — immediate mode is better for a run this size (can abort, streaming diagnostics)
2. **Do not increase `--max-queries` above 2400** — stay within free tier
3. **Do not re-search PWSIDs from the 150-PWSID diagnostic run** — 30-day cooldown is enforced automatically
4. **Do not manually rebuild BestEstimate mid-run** — the script batches it per state at the end
5. **Do not buy paid Serper tier until this sweep completes** — validate at full scale first

## Key Files

- `scripts/serper_bulk_discovery.py` — main sweep script (with `--process` flag)
- `src/utility_api/pipeline/process.py` — cascade processor (reactive deep crawl)
- `src/utility_api/agents/scrape.py` — ScrapeAgent (Playwright escalation + nav crawl)
- `config/source_priority.yaml` — source hierarchy (Duke deprioritized)
- `docs/next_steps.md` — update after sweep
