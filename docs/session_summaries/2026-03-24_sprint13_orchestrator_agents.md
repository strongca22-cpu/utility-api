# Sprint 13 — Orchestrator + Research Agents

**Date**: 2026-03-24
**Context**: Sprint 12 built the agent framework and scrape registry wiring. Sprint 13 adds the autonomous pipeline — four agents that can discover, scrape, parse, and store rate data without human URL curation.

---

## What Was Built

### Agents
- **OrchestratorAgent** — Python+SQL, no LLM. 4 queries: bulk source freshness, coverage gaps (sorted by priority_tier × population), retriable failures, change detection. Returns a ranked list of Task dataclasses.
- **DiscoveryAgent** — SearXNG search with keyword relevance scoring. Haiku fallback for ambiguous URLs (score 30-60, ~20% of cases). Writes to scrape_registry with status='pending'.
- **ScrapeAgent** — Wraps rate_scraper.py. Reads from scrape_registry (Sprint 13 change). Retry logic with exponential backoff. Returns raw text in memory.
- **ParseAgent** — Claude API extraction. Complexity routing (Sonnet for complex, Haiku for simple). Prompt caching. Writes to rate_schedules JSONB. Triggers BestEstimateAgent. Cost tracking.

### Infrastructure
- **priority_tier populated**: 19,478 Tier 2 (DC states), 226 Tier 3 (pop >100K), 24,939 Tier 4
- **Task dataclass**: task_type, priority, pwsid, source_key, registry_id
- **ua-run-orchestrator CLI**: generates queue, optionally executes top N tasks

### Test Results
- Orchestrator generates sensible tasks (largest pop utilities first)
- Fairfax County Water Authority: 3 URLs discovered → 3 scraped (14-15K chars each)
- Parse requires ANTHROPIC_API_KEY in tmux environment (expected)

## Files Created
- `agents/task.py`, `orchestrator.py`, `discovery.py`, `scrape.py`, `parse.py`
- `cli/orchestrator.py`

## Key Design Decisions
1. **Sequential execution** via for-loop — correct for Sprint 13 volume (~50-100 tasks)
2. **Raw text passed in memory** — no filesystem storage needed when scrape→parse runs in one process
3. **Keyword heuristic first**, LLM fallback only for ambiguous — eliminates ~80% of API calls
4. **check_bulk_source is a stub** — Sprint 14 implements the full vintage check
