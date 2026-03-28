# Sprint 23 — Pipeline Flow Fix & Scraped Content Persistence v0

## Context

Full audit report at: `docs/sprint23_pipeline_flow_audit.md`

Two critical bugs in the scrape/parse pipeline are causing URLs to hang unparsed and scraped content to be lost. As of 2026-03-28:

- **2,259 URLs** are in `scrape_registry` with `status='active'`, `last_parse_result=NULL`, `last_content_length > 0` — meaning they were successfully scraped but **never parsed**
- **1,987** of those have >500 chars (parseable content)
- Breakdown: deep_crawl (1,950), domain_guesser (182), SearXNG (108)
- Raw scraped text is stored **only in memory** — if the calling script crashes or chains incorrectly, text is lost and the URL must be re-fetched to parse

The pipeline currently requires a Claude Code chat or manual script invocation to push URLs through each stage. This should be fully automated.

## Bug 1 — Pipeline Flow: URLs Hang Between Scrape and Parse

### How the pipeline works today

```
ScrapeAgent.run(pwsid)
  → Fetches URLs where status IN ('pending', 'pending_retry')
  → Sets status='active', records hash/length
  → Returns raw_text IN MEMORY ONLY (dict with text string)

ParseAgent.run(pwsid, raw_text=<string>, content_type, source_url, registry_id)
  → Requires raw_text as explicit parameter
  → Writes to rate_schedules on success
  → Updates scrape_registry.last_parse_result
```

The fragile handoff: ScrapeAgent returns text in a Python dict. ParseAgent needs that text passed as an argument. If the calling code doesn't do this correctly — or if the script crashes between scrape and parse — the text is gone. The URL is marked `active` (scrape succeeded) but has no parse result.

### How each pipeline chains (or doesn't):

| Pipeline | Script | Chain Correct? | Problem |
|----------|--------|:---:|---------|
| Domain guesser batch | `process_guesser_batch.py` | Yes | Inline `scrape.run()` → `parse.run(raw_text=...)` |
| SearXNG discovery | `run_mn_discovery.py` | **No** | Called ParseAgent without `raw_text` arg → error → text lost |
| Deep crawl | ScrapeAgent internal | **Partial** | Deep crawl inserts new URLs as `active` but only the top-level caller chains to parse — deep-crawled URLs may never get parsed |
| Orchestrator CLI | `ua-run-orchestrator --execute` | Yes | But rarely used for bulk processing |

### Fix needed

1. **Persist scraped text** so parse can run independently (see Bug 2)
2. **Automated sweep** — a background script that finds `active` URLs with no parse result and runs parse on them
3. **Single unified chain** — one script/function that handles scrape→parse→best_estimate as an atomic unit, used by ALL pipelines

## Bug 2 — No Persistent Text Storage

### Current state

Raw scraped HTML/PDF text is not stored in the database or filesystem. Only metadata is persisted:
- `last_content_hash` (SHA-256)
- `last_content_length` (char count)
- `last_fetch_at`, `last_http_status`

### Size analysis

| Metric | Value |
|--------|-------|
| URLs with content | 10,591 |
| Average content size | 3,469 chars (~3.5 KB) |
| Median | 1,716 chars |
| Max | 45,000 chars |
| **Total current** | **35 MB** |
| Projected at 50K URLs | ~175 MB |

This is trivially small. Desktop has multiple TB of storage.

### Solution options

**Option A: PostgreSQL TEXT column on scrape_registry**
- Add `scraped_text TEXT` column
- ScrapeAgent writes text on every successful fetch
- ParseAgent reads from DB instead of requiring in-memory handoff
- Pros: Simple, queryable, single source of truth
- Cons: DB backup size increases (marginally)

**Option B: File-based storage under data/scraped_content/**
- Write `{pwsid}/{registry_id}.txt` files
- ScrapeAgent writes file, ParseAgent reads file
- Pros: Keeps DB lean, easy to browse/grep content
- Cons: Two storage systems to manage, file I/O vs DB query

**Recommendation: Option A** — the total size is so small that DB storage is simpler and more robust. Option B is fine too but adds unnecessary filesystem management.

## Bug 3 — Logging Gaps

### What works well

| Log | Table | What It Tracks |
|-----|-------|---------------|
| Agent runs | `ingest_log` | Every agent execution with status and duration |
| SearXNG funnel | `search_log` | Per-PWSID query counts, score distributions |
| URL lifecycle | `scrape_registry` | HTTP status, content hash, parse result, cost |
| Batch jobs | `batch_jobs` | Batch submission/completion lifecycle |

### Key gaps

| Gap | Impact |
|-----|--------|
| `scrape_status` conflates domain guesser and SearXNG | Can't independently query pipeline outcomes. Sprint 22 added `searxng_status` but the state machine is still confused. |
| No URL quality tier | URLs that return city homepages or press releases get re-scraped forever. Need blacklist/greylist/whitelist. |
| Individual URL scores from discovery lost | Can't tune scoring thresholds without re-running discovery |
| Deep crawl link-following decisions not logged | Can't diagnose why crawl chose wrong page |
| No content change history | Only current hash stored, can't see when/how content changed |

## Desired End State

### Automated pipeline flow
```
Input Stage (manual):
  - Feed PWSIDs to SearXNG discovery or domain guesser
  - Curate URLs manually for high-value targets

Everything below is FULLY AUTOMATED:

scrape_registry(status='pending')
  ↓ [Automated sweep script, runs every N minutes]
ScrapeAgent.run()
  → Fetches URL
  → Writes scraped_text to persistent storage
  → Sets status='active'
  ↓ [Same script, continues automatically]
ParseAgent.run()
  → Reads scraped_text from storage (NOT in-memory handoff)
  → Writes to rate_schedules on success
  → Updates scrape_registry.last_parse_result
  → Sets URL quality tier (whitelist/greylist/blacklist)
  ↓ [Same script, continues automatically]
BestEstimateAgent.run(state)
  → Refreshes best estimates
  → Updates pwsid_coverage
```

### URL quality tiers

| Tier | Meaning | Automated Action |
|------|---------|-----------------|
| **whitelisted** | Confirmed rate page, parsed successfully | Monitor for changes on schedule |
| **greylisted** | Scraped OK but parse failed/ambiguous | Queue for retry when parser improves |
| **blacklisted** | Confirmed junk (homepage, news, wrong utility, non-rate PDF) | Never re-scrape or re-parse |

### Memory persistence

- All scraped text stored persistently (DB column or filesystem)
- Parse can be re-run on any previously scraped content without re-fetching
- Historical content preserved for re-parsing with improved models/prompts

## Key Files

| File | Role |
|------|------|
| `src/utility_api/agents/scrape.py` | ScrapeAgent — fetches URLs, deep crawl, returns text in memory |
| `src/utility_api/agents/parse.py` | ParseAgent — LLM rate extraction, requires `raw_text` parameter |
| `src/utility_api/agents/discovery.py` | DiscoveryAgent — SearXNG search, URL scoring |
| `src/utility_api/agents/orchestrator.py` | Task generation (does not execute) |
| `src/utility_api/agents/task.py` | Task dataclass |
| `src/utility_api/agents/base.py` | BaseAgent.log_run() — writes to ingest_log |
| `src/utility_api/agents/batch.py` | BatchAgent — Anthropic batch API |
| `scripts/process_guesser_batch.py` | Domain guesser → scrape → parse chain (working) |
| `scripts/run_mn_discovery.py` | MN SearXNG discovery (Sprint 22) |
| `config/agent_config.yaml` | Throttle settings, deep crawl depth, SearXNG config |

## Immediate Wins (before full refactor)

1. **Parse the 1,987 hanging URLs** — write a one-time script that re-fetches `active` URLs with no parse result and chains to parse. This alone could add hundreds of PWSIDs to coverage.
2. **Add `scraped_text TEXT` column** — ScrapeAgent writes on every fetch, ParseAgent reads from DB. One migration, two agent edits.
3. **Automated parse sweep** — tmux script that polls for unparsed `active` URLs every 30 minutes and runs parse on them.

## Scope Estimate

- Text persistence + decoupled parse: 1 day
- Automated sweep script: 0.5 day
- URL quality tiers (blacklist/greylist/whitelist): 0.5 day
- Logging gap fixes: 1 day
- Validation & cleanup of 2,259 hanging URLs: 1 day
