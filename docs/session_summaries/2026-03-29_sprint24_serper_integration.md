# Session Summary — Sprint 24: Serper Integration

**Date:** 2026-03-29
**Sprint:** 24
**Focus:** Replace SearXNG with Serper.dev as search discovery backend

## What Was Built

### Deliverable 1: SerperSearchClient
- `src/utility_api/search/serper_client.py` — thin API wrapper
- Every query logged to `search_queries` table (billing audit trail)
- Budget guard: warning at 2,400, hard stop at 2,500 (free tier)
- Retry logic: exponential backoff on 429, fail-fast on 401
- `.usage` property returns total/today/week/cost/remaining

### Deliverable 2: DiscoveryAgent Updated
- `src/utility_api/agents/discovery.py` — full rewrite
- SearXNG replaced with Serper (via SerperSearchClient)
- LLM fallback scoring removed (Google results don't need Haiku tiebreaker)
- Queries reduced from 7 to 4 (Google quality > SearXNG quantity)
- Top 3 URLs written with `discovery_rank` (1/2/3) tagging
- `url_source='serper'` instead of `'searxng'`
- `_log_search()` now writes `search_engine`, `url_rank_1/2/3`, `score_rank_1/2/3`
- Inter-query delay: 0.2s (was 8s for SearXNG)

### Deliverable 3: Bulk Discovery CLI
- `scripts/serper_bulk_discovery.py` — standalone script
- `ua-ops serper-discover` — CLI wrapper
- Gap-state targeting (states <20% coverage), population sorting
- Budget guard, dry-run mode, progress logging, cost confirmation
- argparse CLI: --scope, --state, --pop-min, --max-pwsids, --max-queries, --dry-run, --usage

### Deliverable 4: Schema + Monitoring
- Migration 019: `search_engine` on `search_log`, ranked URL columns,
  `search_queries` table, `discovery_rank`+`discovery_score` on `scrape_registry`
- `ua-ops serper-status` — query usage, discovery results, parse success by rank, funnel summary

### Config Changes
- `config.py`: Added `serper_api_key` and `serper_paid_mode` settings
- `agent_config.yaml`: Replaced SearXNG block with Serper discovery config
- `.env`: needs `SERPER_API_KEY=<key>` (not committed)

## Key Design Decisions

1. **Top 3 URLs with rank tagging** — not capping at 1 yet. Will evaluate
   rank 2-3 parse success after 625-PWSID validation run.
2. **Two-table logging** — `search_log` = per-PWSID funnel summary,
   `search_queries` = per-API-call billing audit.
3. **Kept `searxng_status` column name** — renaming is a separate migration
   when SearXNG is fully removed (next session).
4. **No LLM scoring** — Serper returns Google results which score higher
   on keyword heuristics alone. Thresholds unchanged (>50 = import).

## What Was NOT Done (Deferred to Next Session)

- Deliverable 5: Remove SearXNG code
- Deliverable 6: Validation run (requires live API key + paired session)
- SearXNG Docker container cleanup
- `searxng_status` column rename
- Scoring threshold tuning (needs funnel data from validation)

## State of the Codebase

- Migration 019 written but NOT applied (needs `alembic upgrade head`)
- Serper client ready but NOT tested live (needs API key in .env)
- SearXNG code still present in codebase (removal is deliberate — validate Serper first)
