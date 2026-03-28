# Sprint 23 — Pipeline Flow & Memory Audit Report

**Created:** 2026-03-28
**Purpose:** Full audit of the scrape/parse pipeline to identify where URLs hang unparsed, where scraped content is lost, and where logging gaps exist. This report feeds Sprint 23 implementation.

---

## Bug 1: Pipeline Flow — URLs Hang Unparsed

### Root Cause

The pipeline has a **memory-only handoff** between ScrapeAgent and ParseAgent. Raw scraped text is returned in a Python dict and must be consumed immediately. If the calling script doesn't chain scrape→parse correctly, the text is lost and the URL sits in `scrape_registry` with `status='active'` and `last_parse_result=NULL` forever.

### How It Happens

**ScrapeAgent.run():**
```
Input:  pwsid or registry_id
Action: Fetches all URLs where status IN ('pending', 'pending_retry')
Output: Returns dict with raw_texts: [{registry_id, pwsid, url, text, content_type, ...}]
Side effect: Sets status='active', records last_content_hash, last_content_length
```

**ParseAgent.run():**
```
Input:  pwsid, raw_text (STRING — must be passed explicitly), content_type, source_url, registry_id
Action: Pre-filters junk, routes to Haiku/Sonnet, extracts rate structure
Output: Returns dict with success/skipped/failed, confidence, cost
Side effect: Writes to rate_schedules on success, updates scrape_registry.last_parse_result
```

**The gap:** ScrapeAgent moves URLs from `pending` → `active` and returns text in memory. If ParseAgent is never called (script crash, wrong invocation, missing `raw_text` argument), the URL is permanently stuck as `active` with no parse result and no way to recover the text without re-fetching.

### Evidence

As of 2026-03-28:
- **scrape_registry** has URLs where `status='active'` AND `last_parse_result IS NULL` AND `last_content_length > 0`
- The MN discovery run found 10 SearXNG URLs with scores 90-100, scraped them successfully, but the calling script (`run_mn_discovery.py`) passed the wrong arguments to ParseAgent → parse never ran → text lost
- Re-processing required resetting URLs to `pending` and re-fetching

### Where Each Pipeline Breaks

| Pipeline | Entry Point | Scrape→Parse Chain | Failure Mode |
|----------|------------|-------------------|-------------|
| **process_guesser_batch.py** | Domain guesser results | Correct: `scrape.run()` → `parse.run(raw_text=...)` inline | Works if script doesn't crash mid-batch |
| **run_mn_discovery.py** | SearXNG discovery | Broken: `run_scrape_cycle()` called `ScrapeAgent(registry_id=...)` then `ParseAgent(registry_id=...)` without `raw_text` | ParseAgent raised `missing raw_text` error |
| **Orchestrator CLI** | `ua-run-orchestrator --execute N` | Correct chain for discover_and_scrape tasks | Works if no timeout |
| **Deep crawl parse** | `parse_deep_crawl_backlog.py` | Re-fetches URLs (acknowledges text isn't persisted) | Works but wasteful |

---

## Bug 2: No Persistent Text Storage

### Current State

| What | Persisted? | Location |
|------|-----------|----------|
| URL metadata | Yes | `scrape_registry` (hash, length, status, timestamps) |
| Raw scraped HTML/PDF text | **No** | In-memory dict only, lost after script exits |
| Parsed rate structure | Yes | `rate_schedules` (JSONB tiers, bills, confidence) |
| Parse cost/model | Yes | `scrape_registry.last_parse_cost_usd`, `rate_schedules.parse_model` |

### Size Impact of Storing Text

From current data:
- **10,591 URLs** with content (across all sources)
- **Average: 3,469 chars** per page
- **Median: 1,716 chars**
- **Max: 45,000 chars**
- **Total: 35 MB** across all scraped content

At full scale (50,000 URLs): **~175 MB** — trivially small for PostgreSQL TEXT column or file-based storage.

### Benefits of Persistence

1. **Re-parse without re-fetch:** Parser improvements (better prompts, new models) can be applied to existing content
2. **Audit trail:** Can inspect what the LLM actually saw when it produced a given parse result
3. **Pipeline resilience:** Scrape and parse can be fully decoupled — scrape writes content, parse reads it asynchronously
4. **Cost savings:** No duplicate HTTP requests to re-scrape pages that already returned good content

---

## Bug 3: Logging & Tracking Gaps

### What IS Tracked (working well)

| System | Table | What It Logs |
|--------|-------|-------------|
| Agent runs | `ingest_log` | Every agent execution: name, status, rows_affected, timestamps |
| SearXNG funnel | `search_log` | Per-PWSID: queries_run, raw→deduped→scored→written counts, best_score |
| URL lifecycle | `scrape_registry` | Per-URL: fetch status, HTTP code, content hash, parse result, confidence, cost |
| Batch jobs | `batch_jobs` | Batch submission/completion lifecycle, task count, cost summary |
| Coverage state | `pwsid_coverage` | Per-PWSID: has_rate_data, scrape_status, search_attempted_at |

### What Is NOT Tracked (gaps)

| Gap | Impact | Priority |
|-----|--------|----------|
| **Individual URL scores from discovery** | Can't tune scoring thresholds without re-running | High |
| **Deep crawl link-following decisions** | Can't diagnose why crawl chose wrong page | Medium |
| **Model routing decisions** (Haiku vs Sonnet) | Can't validate cost optimization | Low |
| **scrape_status conflates domain_guesser and SearXNG** | Can't independently query pipeline outcomes | High (Sprint 22 partial fix: `searxng_status` column added) |
| **Content change history** | Only stores current hash, not when/how content changed | Medium |
| **Per-URL retry timeline** | Only `retry_count` and current `retry_after`, no history | Low |

---

## Current Schema Reference

### scrape_registry (URL-level tracking)

| Column | Type | Purpose |
|--------|------|---------|
| id | INT PK | Auto-increment |
| pwsid | VARCHAR(12) | Utility |
| url | TEXT | The URL |
| url_source | VARCHAR(30) | searxng, domain_guesser, deep_crawl, curated, etc. |
| discovery_query | TEXT | Search query that found it (SearXNG only) |
| discovery_score | INT | URL relevance score (Sprint 22) |
| content_type | VARCHAR(20) | html, pdf |
| status | VARCHAR(20) | pending → active → dead / pending_retry |
| last_fetch_at | TIMESTAMP | When last fetched |
| last_http_status | INT | HTTP response code |
| last_content_hash | VARCHAR(64) | SHA-256 for change detection |
| last_content_length | INT | Char count |
| last_parse_at | TIMESTAMP | When last parsed |
| last_parse_result | VARCHAR(20) | success, failed, skipped |
| last_parse_confidence | VARCHAR(10) | high, medium, low |
| last_parse_cost_usd | FLOAT | API cost |
| retry_after | TIMESTAMP | Next retry time |
| retry_count | INT | Retry attempts |
| notes | TEXT | Free text |
| created_at / updated_at | TIMESTAMP | Lifecycle timestamps |

### pwsid_coverage (PWSID-level state)

| Column | Type | Purpose |
|--------|------|---------|
| pwsid | VARCHAR PK | Utility |
| state_code | VARCHAR | State |
| pws_name | VARCHAR | Name |
| has_rate_data | BOOL | Derived: has parsed rates |
| rate_source_count | INT | How many sources have rates |
| scrape_status | VARCHAR | not_attempted, url_discovered, attempted_failed, succeeded |
| searxng_status | VARCHAR | not_attempted, searched_no_hits, url_found, parsed_success (Sprint 22) |
| search_attempted_at | TIMESTAMP | When SearXNG last searched |
| priority_tier | INT | 1-4 priority ranking |
| population_served | INT | From SDWIS |

### search_log (SearXNG discovery funnel)

| Column | Type | Purpose |
|--------|------|---------|
| id | INT PK | Auto-increment |
| pwsid | VARCHAR | Utility searched |
| searched_at | TIMESTAMP | When |
| queries_run | INT | Number of SearXNG queries |
| raw_results_count | INT | Total raw results |
| deduped_count | INT | After dedup |
| above_threshold_count | INT | Score > 50 |
| near_miss_count | INT | Score 15-50 |
| below_threshold_count | INT | Score < 15 |
| written_count | INT | URLs written to registry |
| best_score | INT | Top URL score |
| best_url | TEXT | Top URL |

### ingest_log (agent execution log)

| Column | Type | Purpose |
|--------|------|---------|
| id | INT PK | |
| agent_name | VARCHAR | discovery, scrape, parse, batch, etc. |
| source_key | VARCHAR | State or source being processed |
| started_at / completed_at | TIMESTAMP | Duration tracking |
| status | VARCHAR | running, success, failed, partial |
| rows_affected | INT | Items processed |
| notes | TEXT | Summary |

---

## Pipeline Flow Diagram (Current State)

```
                    ┌─────────────────┐
                    │  Manual Input   │
                    │  (state, PWSIDs)│
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
     ┌────────────┐  ┌────────────┐  ┌──────────┐
     │ Domain     │  │ SearXNG    │  │ Curated  │
     │ Guesser    │  │ Discovery  │  │ URLs     │
     │ (VPS auto) │  │ (targeted) │  │ (manual) │
     └─────┬──────┘  └─────┬──────┘  └────┬─────┘
           │               │              │
           ▼               ▼              ▼
     ┌─────────────────────────────────────────┐
     │         scrape_registry                  │
     │         status = 'pending'               │
     │         (URLs waiting to be fetched)     │
     └────────────────────┬────────────────────┘
                          │
                          ▼
     ┌─────────────────────────────────────────┐
     │         ScrapeAgent.run()                │
     │  • Fetches URL (HTTP/PDF)               │
     │  • Deep crawl if thin content           │
     │  • Updates registry: status='active'    │
     │  • Returns raw_text IN MEMORY ONLY  ◄───┼─── BUG: Text not persisted
     └────────────────────┬────────────────────┘
                          │
                  ┌───────┴──────────┐
                  │ raw_text in RAM  │◄──── FRAGILE HANDOFF
                  └───────┬──────────┘
                          │
                          ▼
     ┌─────────────────────────────────────────┐
     │         ParseAgent.run()                 │
     │  • Pre-filter (junk, too short)         │
     │  • Route to Haiku or Sonnet             │
     │  • Extract rate tiers via LLM           │
     │  • Write to rate_schedules on success   │
     │  • Update registry: last_parse_result   │
     └────────────────────┬────────────────────┘
                          │
                          ▼
     ┌─────────────────────────────────────────┐
     │     BestEstimateAgent.run(state)         │
     │  • Compute best rate estimate per PWSID │
     │  • Refresh pwsid_coverage               │
     └─────────────────────────────────────────┘
```

**Where URLs Hang:**
- After ScrapeAgent: `status='active'`, `last_parse_result=NULL` — scraped but never parsed
- After failed parse: `last_parse_result='failed'` — no automatic retry
- After deep crawl: new URLs inserted as `url_source='deep_crawl'`, `status='active'` — may never get parsed if the calling script doesn't loop back

---

## Hanging URL Census (2026-03-28)

```sql
-- URLs that were scraped but never parsed
SELECT url_source, count(*) as hanging
FROM utility.scrape_registry
WHERE status = 'active'
  AND last_parse_result IS NULL
  AND last_content_length > 0
GROUP BY url_source ORDER BY hanging DESC;
```

This query will reveal the scale of the problem across all pipeline sources.

---

## Desired End State

1. **Scraped text persisted** — `scraped_text TEXT` column on `scrape_registry`, or file-based storage under `data/scraped_content/`
2. **Scrape and parse fully decoupled** — Parse reads from persistent storage, not in-memory handoff
3. **Automated sweep script** — Cron/tmux script that periodically checks for `status='active' AND last_parse_result IS NULL AND last_content_length > 500` and runs parse on them
4. **URL quality tiers** — blacklisted (confirmed junk), greylisted (parse failed, worth retry with better parser), whitelisted (confirmed rate page, monitor for changes)
5. **Consistent logging** — Every pipeline stage writes to a common log with PWSID, URL, stage, outcome, timestamp
