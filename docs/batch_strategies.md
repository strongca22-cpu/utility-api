# Batch Parse Strategies

## Overview

When submitting parse tasks to the Anthropic Batch API, the **batch strategy** controls how many URLs per PWSID are submitted. This is the main cost/speed tradeoff in the pipeline.

**Config:** `config/agent_config.yaml` → `batch_api.default_strategy`
**CLI override:** `--strategy shotgun|cascade|rank1_only`
**Code:** `src/utility_api/ops/batch_task_builder.py` → `build_parse_tasks(strategy=...)`

## Strategies

### `shotgun` (default)

Submit ALL viable URLs per PWSID in one batch. If a PWSID has 3 URLs above the score threshold, all 3 get submitted as separate parse tasks. First success wins via `ON CONFLICT` in rate_schedules.

| Metric | Value |
|---|---|
| Duration | 1 day (single batch cycle) |
| Cost/success | ~$0.008 |
| Coverage | Maximum — tries every URL |
| Waste | ~18% (redundant parses where earlier rank succeeds) |

**When to use:** Default for most workloads. The time savings (1 day vs 4) usually outweigh the cost overhead ($3 per 3,000 PWSIDs).

### `cascade`

Submit only the single best-scored URL per PWSID. After the batch completes, re-run with `--min-rank 2` to submit rank 2 for failures, then `--min-rank 3`, etc.

```bash
# Day 1: rank 1
python scripts/submit_discovery_batch.py --strategy cascade
# Day 2: rank 2 for failures
python scripts/submit_discovery_batch.py --strategy cascade --min-rank 2
# Day 3: rank 3 for remaining failures
python scripts/submit_discovery_batch.py --strategy cascade --min-rank 3
```

| Metric | Value |
|---|---|
| Duration | N days (one batch cycle per rank) |
| Cost/success | ~$0.006 |
| Coverage | Maximum (same as shotgun, just slower) |
| Waste | Minimal — only pays for attempts that might succeed |

**When to use:** Large batches (10k+ PWSIDs) where the 18% cost savings is material, or when you want incremental results day-by-day.

### `rank1_only`

Submit only the top-scored URL per PWSID. No follow-up rounds.

| Metric | Value |
|---|---|
| Duration | 1 day |
| Cost/success | ~$0.004 |
| Coverage | ~51% of parseable content (rank 1 win rate) |
| Waste | None |

**When to use:** Quick first pass when you want results fast and will decide on cascade later. Good for initial discovery sweeps where you don't know hit rates yet.

## CLI Reference

```bash
# Use config default
python scripts/submit_discovery_batch.py

# Override strategy
python scripts/submit_discovery_batch.py --strategy shotgun
python scripts/submit_discovery_batch.py --strategy cascade
python scripts/submit_discovery_batch.py --strategy rank1_only

# Cascade round 2 (skip rank 1, try rank 2+ for failures only)
python scripts/submit_discovery_batch.py --strategy cascade --min-rank 2

# Population filter
python scripts/submit_discovery_batch.py --min-pop 3000

# Specific PWSIDs
python scripts/submit_discovery_batch.py --pwsids TX1010013 CA3010092 NV0000090

# Skip URLs that have already been parsed (avoid re-attempts)
python scripts/submit_discovery_batch.py --exclude-attempted

# Dry run (preview tasks, no API calls)
python scripts/submit_discovery_batch.py --dry-run

# Custom label for tracking
python scripts/submit_discovery_batch.py --label my_batch_v2
```

## Cost Model

All costs at Anthropic Batch API pricing ($0.002/task, 50% off direct API).

| Scenario | PWSIDs | Shotgun | Cascade | Rank 1 Only |
|---|---|---|---|---|
| 1,000 PWSIDs | 1,000 | $5.80 | $4.77 | $2.00 |
| 3,000 PWSIDs | 3,000 | $17.40 | $14.31 | $6.00 |
| 10,000 PWSIDs | 10,000 | $58.00 | $47.70 | $20.00 |

Assumes avg 2.9 URLs/PWSID, 51% rank 1 win rate, 30% rank 2, 19% rank 3.

## Architecture

```
config/agent_config.yaml          ← default_strategy setting
        │
        ▼
batch_task_builder.py             ← build_parse_tasks(strategy=...)
  Reads scrape_registry              Filters by rank, pop, score
  Re-scores with content boost       Groups by PWSID
  Applies strategy                   Returns [{pwsid, raw_text, ...}]
        │
        ▼
submit_discovery_batch.py         ← CLI wrapper, submits to BatchAgent
        │
        ▼
BatchAgent.submit()               ← Anthropic Batch API
        │
        ▼
poll_scenario_a.sh                ← Polls for completion
        │
        ▼
process_scenario_a_batch.py       ← Downloads + processes results
```

## Key Files

- `config/agent_config.yaml` — `batch_api.default_strategy`
- `src/utility_api/ops/batch_task_builder.py` — `build_parse_tasks()`
- `scripts/submit_discovery_batch.py` — unified CLI
- `scripts/poll_scenario_a.sh` — batch completion poller
- `scripts/process_scenario_a_batch.py` — result processor
