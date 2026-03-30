# Sprint 24: Serper Integration — Replace SearXNG, Bulk Discovery Pipeline

## Context

SearXNG has produced 85 URLs from 35 PWSIDs across the entire project. It requires a self-hosted Docker container, burns IPs, has port mismatch bugs, infinite re-queue loops, and caps at 10-20 queries/day behind 8-second delays. It's being replaced entirely.

Serper.dev provides Google search results via API at $1/1,000 queries (or free for the first 2,500). No IP risk, 300 queries/sec throughput, clean JSON responses. A Serper API key is available.

**Sprint 23 prerequisite is complete:** `scraped_text` persistence exists on `scrape_registry`, `ParseAgent` reads from DB, the unified `scrape_and_parse()` chain function exists, and the `parse_sweep.py` daemon catches orphaned entries. Serper-discovered URLs feed directly into this pipeline.

**Goal:** Replace SearXNG with Serper as the sole search discovery backend. Build a bulk discovery pipeline that can sweep thousands of PWSIDs in minutes. Validate on the free 2,500 queries (~625 PWSIDs targeting gap states). If results are good, scale to full gap-state sweep (~32,000 queries, ~$32).

---

## Architecture

```
                    ┌─────────────────────┐
                    │  Serper API          │
                    │  google.serper.dev   │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │  SerperSearchClient  │
                    │  - query()           │
                    │  - bulk_discover()   │
                    │  - usage tracking    │
                    │  - result logging    │
                    └──────────┬──────────┘
                               │
              ┌────────────────▼────────────────┐
              │  DiscoveryAgent (updated)        │
              │  - Serper replaces SearXNG       │
              │  - Scoring v2 (Sprint 21)        │
              │  - Gap-state targeting           │
              │  - Failed search logging         │
              └────────────────┬────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  scrape_registry    │
                    │  url_source='serper' │
                    └──────────┬──────────┘
                               │
              ┌────────────────▼────────────────┐
              │  Existing Pipeline              │
              │  (Sprint 23 unified chain)      │
              │  scrape → persist → parse →     │
              │  quality classify → best_estimate│
              └─────────────────────────────────┘
```

---

## Deliverable 1: SerperSearchClient (1 hour)

A thin client that wraps the Serper API with usage tracking, cost logging, and rate limiting.

`src/utility_api/search/serper_client.py`

```python
"""
Serper.dev API client with usage tracking and cost awareness.

Usage:
    client = SerperSearchClient()
    results = client.search("Portland water rates OR")
    # Returns: [{"url": "...", "title": "...", "snippet": "..."}]
    
    print(client.usage)
    # {"queries_today": 47, "queries_total": 2103, "estimated_cost": "$2.10"}
"""

import requests
from datetime import date
from utility_api.config import settings
from utility_api.db import engine

class SerperSearchClient:
    BASE_URL = "https://google.serper.dev/search"
    
    def __init__(self):
        self.api_key = settings.serper_api_key
        if not self.api_key:
            raise ValueError("SERPER_API_KEY not configured")
        self._daily_count = 0
        self._daily_date = date.today()
    
    def search(self, query: str, num_results: int = 10) -> list[dict]:
        """Single search query. Returns normalized results."""
        response = requests.post(
            self.BASE_URL,
            headers={"X-API-KEY": self.api_key},
            json={"q": query, "num": num_results},
            timeout=10,
        )
        response.raise_for_status()
        
        self._track_usage(query)
        
        # Normalize to standard format
        raw = response.json()
        return [
            {
                "url": r.get("link", ""),
                "title": r.get("title", ""),
                "snippet": r.get("snippet", ""),
            }
            for r in raw.get("organic", [])
        ]
    
    def _track_usage(self, query: str):
        """Log every query for cost tracking and debugging."""
        # Reset daily counter at midnight
        if date.today() != self._daily_date:
            self._daily_count = 0
            self._daily_date = date.today()
        self._daily_count += 1
        
        # Persist to DB
        with engine.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {schema}.search_log 
                    (query, search_engine, searched_at)
                VALUES (:query, 'serper', NOW())
            """), {'query': query})
            conn.commit()
    
    @property
    def usage(self) -> dict:
        """Current usage stats from search_log."""
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT 
                    count(*) as total,
                    count(*) FILTER (WHERE searched_at > CURRENT_DATE) as today,
                    count(*) FILTER (WHERE searched_at > CURRENT_DATE - INTERVAL '7 days') as week
                FROM {schema}.search_log
                WHERE search_engine = 'serper'
            """)).fetchone()
        return {
            'queries_total': result.total,
            'queries_today': result.today,
            'queries_this_week': result.week,
            'estimated_cost': f"${result.total / 1000:.2f}",
        }
```

### Key requirements:

- **Every query logged to `search_log`** with timestamp, query string, and `search_engine='serper'`. This is non-negotiable — we need to know exactly how many queries have been consumed at all times.
- **Cost awareness:** The `usage` property gives a running total. The free tier is 2,500 queries. The client should log a warning when approaching 2,000 and refuse to query past 2,500 unless a `--force` or `--paid` flag is set.
- **Error handling:** Serper returns HTTP 429 if rate-limited and 401 if the key is invalid. Handle both gracefully with retries (429) or clear error messages (401).

### Update `search_log` table

Sprint 21 created `search_log` for SearXNG. Extend it for Serper:

```sql
-- Add search_engine column if not present
ALTER TABLE utility.search_log 
ADD COLUMN IF NOT EXISTS search_engine VARCHAR(20) DEFAULT 'searxng';

-- Add query column if not present (Sprint 21 may have used different column name)
ALTER TABLE utility.search_log
ADD COLUMN IF NOT EXISTS query TEXT;
```

---

## Deliverable 2: Update DiscoveryAgent to Use Serper (1 hour)

Replace all SearXNG references in the DiscoveryAgent with the SerperSearchClient.

### What changes:

```python
# BEFORE (SearXNG):
response = requests.get(
    f"http://localhost:{self.searxng_port}/search",
    params={"q": query, "format": "json", "engines": "bing,yahoo"},
    timeout=15
)
results = response.json().get("results", [])
# Field names: url, title, content

# AFTER (Serper):
from utility_api.search.serper_client import SerperSearchClient
client = SerperSearchClient()
results = client.search(query, num_results=10)
# Field names: url, title, snippet (already normalized by client)
```

### What stays the same:

- **Scoring v2** (Sprint 21) — domain authority, utility-name-in-domain matching, aggregator penalties. These operate on `url`, `title`, `snippet` which the Serper client normalizes.
- **Gap-state targeting** (Sprint 21) — the priority queue query that selects PWSIDs in states with <20% coverage.
- **`search_attempted_at` logging** (Sprint 21) — still marks PWSIDs as searched to prevent re-queuing.
- **URL cap of 1 per PWSID** (Sprint 21) — keep the top-scoring URL only.
- **Failed search logging** — PWSIDs with zero results still get `search_attempted_at` set.

### What to remove:

- All SearXNG config references in `agent_config.yaml` (port, engines, delay settings)
- The `_searxng_search()` method
- Any SearXNG health check / fallback logic
- The LLM fallback scoring (Haiku call for ambiguous 15-60 scores) — Serper returns Google results which are higher quality than SearXNG's aggregated results. The scoring v2 thresholds may need adjusting since Google results tend to score higher than Bing/Yahoo results. Start with the same thresholds and tune based on the diagnostic funnel data.

### Timing adjustments:

SearXNG needed 8-second delays between queries to avoid IP bans. Serper has no such constraint — rate limit is 300/sec on paid, probably 3-5/sec on free tier. For the initial validation run, use a 0.5-second delay between queries (polite but fast). For bulk sweeps, no delay needed.

```python
# Remove or dramatically reduce inter-query delays
# BEFORE:
time.sleep(8)  # SearXNG anti-ban delay

# AFTER:
time.sleep(0.5)  # Serper needs minimal delay (free tier courtesy)
```

---

## Deliverable 3: Bulk Discovery CLI (1.5 hours)

A new script for running large-scale Serper discovery sweeps with full logging and cost guards.

`scripts/serper_bulk_discovery.py`

```python
"""
Bulk Serper discovery sweep for gap-state PWSIDs.

Usage:
    # Validate on free tier (625 PWSIDs)
    python scripts/serper_bulk_discovery.py --max-pwsids 625 --dry-run
    python scripts/serper_bulk_discovery.py --max-pwsids 625
    
    # Full gap-state sweep (after buying Starter pack)
    python scripts/serper_bulk_discovery.py --scope gap_states --pop-min 3000
    
    # Specific state
    python scripts/serper_bulk_discovery.py --state NY --pop-min 5000
    
    # Check usage before and after
    python scripts/serper_bulk_discovery.py --usage
"""
```

### CLI arguments:

```
--scope          gap_states | all_uncovered | specific_state (default: gap_states)
--state          Two-letter state code (for specific_state scope)
--pop-min        Minimum population filter (default: 3000)
--max-pwsids     Hard cap on PWSIDs to search (default: no cap)
--max-queries    Hard cap on total Serper queries (default: 2400 for free tier safety)
--queries-per    Queries per PWSID (default: 4)
--dry-run        Show what would be searched, no API calls
--usage          Show current Serper usage stats and exit
--process        Also run scrape+parse after discovery (default: discovery only)
--delay          Seconds between queries (default: 0.2)
```

### PWSID selection query:

```sql
SELECT pc.pwsid, s.pws_name, s.state_code, s.city, 
       s.county_served, s.population_served_count, s.owner_type_code
FROM utility.pwsid_coverage pc
JOIN utility.sdwis_systems s ON s.pwsid = pc.pwsid
WHERE pc.has_rate_data = FALSE
AND (pc.search_attempted_at IS NULL 
     OR pc.search_attempted_at < NOW() - INTERVAL '30 days')
AND s.population_served_count >= :pop_min
AND s.pws_type_code = 'CWS'
AND pc.state_code IN (
    -- Gap states: <20% bulk coverage (computed dynamically)
    SELECT state_code FROM (
        SELECT state_code,
               round(100.0 * count(*) FILTER (WHERE has_rate_data) / count(*), 1) as pct
        FROM utility.pwsid_coverage
        GROUP BY state_code
    ) s WHERE pct < 20
)
ORDER BY s.population_served_count DESC NULLS LAST
LIMIT :max_pwsids
```

### Query construction per PWSID:

Optimized to 4 queries (reduced from SearXNG's 7 — Serper returns Google results which are higher quality per query):

```python
def build_queries(pwsid_meta: dict) -> list[str]:
    name = pwsid_meta['pws_name']
    city = pwsid_meta['city']
    state = pwsid_meta['state_code']
    county = pwsid_meta.get('county_served', '')
    
    queries = [
        f'"{name}" water rates {state}',
        f'{city} {state} water rate schedule',
        f'{city} {state} water utility rates fees',
    ]
    
    # 4th query varies by owner type
    if pwsid_meta.get('owner_type_code') == 'P':  # private/IOU
        queries.append(f'"{name}" tariff rate schedule filetype:pdf')
    elif county:
        queries.append(f'{county} county {state} water rates')
    else:
        queries.append(f'{city} water department rates {state}')
    
    return queries
```

### Execution flow:

```python
def run_bulk_discovery(scope, pop_min, max_pwsids, max_queries, 
                        queries_per, delay, dry_run, process):
    client = SerperSearchClient()
    
    # 1. Check budget
    usage = client.usage
    logger.info(f"Current Serper usage: {usage['queries_total']} total, "
                f"estimated cost: {usage['estimated_cost']}")
    
    remaining_free = max(0, 2500 - usage['queries_total'])
    if max_queries is None:
        max_queries = remaining_free
        logger.info(f"Budget: {remaining_free} queries remaining in free tier")
    
    # 2. Select PWSIDs
    pwsids = get_target_pwsids(scope, pop_min, max_pwsids)
    total_queries_needed = len(pwsids) * queries_per
    
    if total_queries_needed > max_queries:
        pwsids = pwsids[:max_queries // queries_per]
        logger.warning(f"Capped to {len(pwsids)} PWSIDs to stay within "
                       f"{max_queries} query budget")
    
    logger.info(f"Targeting {len(pwsids)} PWSIDs × {queries_per} queries = "
                f"{len(pwsids) * queries_per} total queries")
    
    if dry_run:
        for p in pwsids[:20]:
            logger.info(f"  {p['pwsid']} | {p['pws_name'][:35]:35s} | "
                        f"pop {p['population']:>8,} | {p['state']}")
        logger.info(f"  ... and {len(pwsids) - 20} more")
        logger.info(f"Estimated cost: ${len(pwsids) * queries_per / 1000:.2f}")
        return
    
    # 3. Cost confirmation for large sweeps
    if len(pwsids) * queries_per > 2500:
        cost = len(pwsids) * queries_per / 1000
        confirm = input(f"This will use {len(pwsids) * queries_per} Serper queries "
                        f"(est. ${cost:.2f}). Continue? [y/N] ")
        if confirm.lower() != 'y':
            return
    
    # 4. Discovery loop
    stats = {
        'searched': 0, 'urls_found': 0, 'urls_imported': 0,
        'no_results': 0, 'errors': 0
    }
    
    for pwsid_meta in pwsids:
        pwsid = pwsid_meta['pwsid']
        queries = build_queries(pwsid_meta)
        
        all_results = []
        for query in queries:
            try:
                results = client.search(query, num_results=10)
                all_results.extend(results)
                time.sleep(delay)
            except Exception as e:
                logger.warning(f"  {pwsid} query error: {e}")
                stats['errors'] += 1
        
        # Dedup by URL
        seen = set()
        unique_results = []
        for r in all_results:
            if r['url'] not in seen:
                seen.add(r['url'])
                unique_results.append(r)
        
        # Score and select best
        scored = []
        for r in unique_results:
            score = score_url_relevance(
                url=r['url'], title=r['title'], snippet=r['snippet'],
                utility_name=pwsid_meta['pws_name'],
                city=pwsid_meta['city'],
                state=pwsid_meta['state'],
            )
            r['score'] = score
            if score > 50:
                scored.append(r)
        
        # Mark as searched regardless of outcome
        mark_searched(pwsid)
        stats['searched'] += 1
        
        if not scored:
            stats['no_results'] += 1
            log_search_attempt(pwsid, queries_run=len(queries),
                              raw_count=len(all_results), 
                              above_threshold=0, best_score=0)
            continue
        
        # Take top 1 URL (Sprint 21: cap at 1)
        best = max(scored, key=lambda r: r['score'])
        stats['urls_found'] += 1
        
        # Log the full funnel
        log_search_attempt(
            pwsid=pwsid,
            queries_run=len(queries),
            raw_count=len(all_results),
            deduped_count=len(unique_results),
            above_threshold=len(scored),
            best_score=best['score'],
            best_url=best['url'],
        )
        
        # Import to registry
        imported = import_to_registry(
            pwsid=pwsid,
            url=best['url'],
            url_source='serper',
            notes=f"Serper bulk | score={best['score']} | {best['title'][:50]}",
        )
        if imported:
            stats['urls_imported'] += 1
        
        # Progress logging every 50 PWSIDs
        if stats['searched'] % 50 == 0:
            logger.info(f"  Progress: {stats['searched']}/{len(pwsids)} searched, "
                        f"{stats['urls_found']} URLs found, "
                        f"{client.usage['queries_total']} total queries used")
    
    # 5. Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"Serper Bulk Discovery Complete")
    logger.info(f"{'='*60}")
    logger.info(f"  PWSIDs searched:  {stats['searched']}")
    logger.info(f"  URLs found:       {stats['urls_found']} ({stats['urls_found']/max(1,stats['searched'])*100:.0f}%)")
    logger.info(f"  URLs imported:    {stats['urls_imported']}")
    logger.info(f"  No results:       {stats['no_results']}")
    logger.info(f"  Errors:           {stats['errors']}")
    logger.info(f"  Serper usage:     {client.usage}")
    
    # 6. Optionally process through pipeline
    if process and stats['urls_imported'] > 0:
        logger.info(f"\nProcessing {stats['urls_imported']} URLs through pipeline...")
        # Use Sprint 23 unified chain or trigger parse_sweep
```

### Add as CLI command:

```bash
ua-ops serper-discover --max-pwsids 625 --dry-run
ua-ops serper-discover --max-pwsids 625
ua-ops serper-discover --scope gap_states --pop-min 3000
ua-ops serper-discover --usage
```

---

## Deliverable 4: Search Logging & Monitoring (30 min)

### Extend search_log for Serper funnel tracking

The Sprint 21 `search_log` table needs to track per-PWSID search funnels:

```sql
-- Ensure search_log has all needed columns
ALTER TABLE utility.search_log ADD COLUMN IF NOT EXISTS search_engine VARCHAR(20) DEFAULT 'searxng';
ALTER TABLE utility.search_log ADD COLUMN IF NOT EXISTS query TEXT;
ALTER TABLE utility.search_log ADD COLUMN IF NOT EXISTS raw_results_count INTEGER;
ALTER TABLE utility.search_log ADD COLUMN IF NOT EXISTS deduped_count INTEGER;
ALTER TABLE utility.search_log ADD COLUMN IF NOT EXISTS above_threshold_count INTEGER;
ALTER TABLE utility.search_log ADD COLUMN IF NOT EXISTS written_count INTEGER;
ALTER TABLE utility.search_log ADD COLUMN IF NOT EXISTS best_score FLOAT;
ALTER TABLE utility.search_log ADD COLUMN IF NOT EXISTS best_url TEXT;
```

### Add Serper usage tracking to pipeline health

```python
# In ua-ops pipeline-health or ua-ops coverage-report:
def serper_report():
    usage = SerperSearchClient().usage
    
    # Results breakdown
    results = db_query(f"""
        SELECT 
            count(*) as total_urls,
            count(*) FILTER (WHERE last_parse_result = 'success') as parsed,
            count(*) FILTER (WHERE last_parse_result IS NULL) as pending,
            count(*) FILTER (WHERE url_quality = 'blacklisted') as blacklisted
        FROM {schema}.scrape_registry
        WHERE url_source = 'serper'
    """)
    
    logger.info(f"Serper Discovery:")
    logger.info(f"  Queries used:    {usage['queries_total']} (${usage['estimated_cost']})")
    logger.info(f"  URLs found:      {results.total_urls}")
    logger.info(f"  Parsed success:  {results.parsed}")
    logger.info(f"  Pending parse:   {results.pending}")
```

### Free tier budget guard

```python
# In SerperSearchClient.search():
def search(self, query, num_results=10):
    # Budget guard
    total_used = self._get_total_usage()
    if total_used >= 2400 and not self._paid_mode:
        raise BudgetExceededError(
            f"Approaching free tier limit ({total_used}/2,500 used). "
            f"Set SERPER_PAID_MODE=true or use --paid flag to continue."
        )
    if total_used >= 2500 and not self._paid_mode:
        raise BudgetExceededError(
            f"Free tier exhausted ({total_used} used). "
            f"Purchase credits at serper.dev or set SERPER_PAID_MODE=true."
        )
    # ... proceed with search
```

---

## Deliverable 5: Remove SearXNG (30 min)

### Code removal:

- Remove `_searxng_search()` from DiscoveryAgent
- Remove SearXNG config block from `agent_config.yaml`
- Remove any SearXNG health check / connection test code
- Remove SearXNG port configuration
- Update `DiscoveryAgent.__init__()` to initialize `SerperSearchClient` instead of SearXNG connection

### Docker cleanup (manual, document in sprint):

```bash
# Stop and remove SearXNG container
docker stop searxng && docker rm searxng

# Remove SearXNG config/data
rm -rf ~/searxng-docker/  # or wherever it's configured

# Verify it's gone
docker ps | grep searx  # should return nothing
```

### Update documentation:

- Update any references to SearXNG in README, agent docs, or config comments
- Update `pipeline-health` to remove SearXNG status checks
- Note in agent_config.yaml that discovery uses Serper

### Keep search_log history:

Do NOT delete existing `search_log` entries where `search_engine = 'searxng'`. They're historical data. Just stop writing new ones.

---

## Deliverable 6: Validation Run (30 min)

### Phase 1: Dry run (0 queries)

```bash
# See what would be searched
python scripts/serper_bulk_discovery.py --max-pwsids 625 --dry-run

# Expected output:
#   Targeting 625 PWSIDs × 4 queries = 2,500 total queries
#   NY0102001 | CITY OF ALBANY                      | pop  97,856 | NY
#   CO0102600 | DENVER WATER                        | pop 1,400,000 | CO
#   ... (all gap-state utilities, sorted by population desc)
#   Estimated cost: $2.50
```

### Phase 2: Small validation (100 queries, 25 PWSIDs)

```bash
# Search 25 PWSIDs to validate scoring and result quality
python scripts/serper_bulk_discovery.py --max-pwsids 25

# Check results
psql -c "
SELECT sr.pwsid, s.pws_name, s.state_code, sr.url, 
       sr.notes, sr.status
FROM utility.scrape_registry sr
JOIN utility.sdwis_systems s ON s.pwsid = sr.pwsid
WHERE sr.url_source = 'serper'
ORDER BY sr.created_at DESC
LIMIT 25;"

# Check search funnel
psql -c "
SELECT pwsid, raw_results_count, deduped_count, 
       above_threshold_count, best_score, 
       substring(best_url, 1, 60) as url
FROM utility.search_log
WHERE search_engine = 'serper'
ORDER BY searched_at DESC
LIMIT 25;"

# Check usage
python scripts/serper_bulk_discovery.py --usage
```

### Phase 3: Process found URLs

```bash
# Scrape and parse the discovered URLs
ua-ops process-backlog --url-source serper --max 50

# Or let parse_sweep.py pick them up automatically
# (if running in tmux from Sprint 23)

# Check results
psql -c "
SELECT sr.url_source, sr.last_parse_result, count(*)
FROM utility.scrape_registry sr
WHERE sr.url_source = 'serper'
GROUP BY sr.url_source, sr.last_parse_result;"
```

### Phase 4: Full free-tier sweep (remaining ~2,400 queries)

```bash
# Use remaining free queries on gap states
python scripts/serper_bulk_discovery.py --max-pwsids 600 --process

# Final check
ua-ops coverage-report
```

### Expected validation results:

| Metric | Expected Range |
|---|---|
| PWSIDs searched (Phase 4) | ~625 |
| Serper queries used | ~2,500 (free tier) |
| URLs found (>50 score) | ~250-310 (40-50% hit rate) |
| URLs imported (after dedup) | ~230-290 |
| Parse successes | ~85-115 (35-40% of imports) |
| **New PWSIDs with rate data** | **~85-115** |
| Cost | **$0** |

If the validation shows >35% parse success rate on Serper-found URLs, the paid tier ($50 for 50K queries) is justified for the full gap-state sweep.

---

## Build Order

1. **Deliverable 1: SerperSearchClient** (1 hour) — API wrapper, usage tracking, budget guard
2. **Deliverable 4: Search logging** (30 min) — extend search_log, monitoring additions
3. **Deliverable 2: Update DiscoveryAgent** (1 hour) — swap SearXNG → Serper, keep scoring v2
4. **Deliverable 3: Bulk discovery CLI** (1.5 hours) — `serper_bulk_discovery.py` with gap-state targeting
5. **Deliverable 6: Validation run** (30 min) — dry run → 25 PWSIDs → full free tier sweep
6. **Deliverable 5: Remove SearXNG** (30 min) — code removal, Docker cleanup, docs update

**Total: ~5 hours**

Build in this order so you can validate the Serper integration (deliverables 1-4) before removing SearXNG (deliverable 5). If Serper somehow doesn't work, SearXNG is still available as fallback until you confirm.

---

## Config Changes

### Add to `.env` or environment:

```bash
SERPER_API_KEY=<your-key>
SERPER_PAID_MODE=false  # set to true after purchasing credits
```

### Add to `settings.py` or config loader:

```python
serper_api_key: str = os.getenv("SERPER_API_KEY", "")
serper_paid_mode: bool = os.getenv("SERPER_PAID_MODE", "false").lower() == "true"
```

### Update `agent_config.yaml`:

```yaml
# REMOVED:
# searxng:
#   url: http://localhost:8888
#   engines: [bing, yahoo, mojeek]
#   max_results: 10
#   delay_between_queries: 8

# ADDED:
discovery:
  search_backend: serper          # serper | ddgs (future fallback)
  queries_per_pwsid: 4
  max_pwsids_per_run: 625         # conservative default
  max_queries_per_run: 2500       # matches free tier
  inter_query_delay: 0.2          # seconds
  url_score_threshold: 50         # minimum score to import
  urls_per_pwsid: 1               # top 1 only
  gap_state_threshold_pct: 20     # states below this % are targeted
  min_population: 3000            # skip tiny systems
  search_cooldown_days: 30        # don't re-search within this window
```

---

## What NOT to Do

1. **Do not exceed 2,500 queries without explicitly confirming paid mode.** The budget guard exists for a reason. Validate on the free tier first.
2. **Do not remove SearXNG before validating Serper works end-to-end.** Build and validate Serper first (deliverables 1-4), then remove SearXNG (deliverable 5).
3. **Do not reduce the inter-query delay below 0.1 seconds on free tier.** Serper's free tier rate limit isn't documented precisely — be conservative until you're on the paid tier.
4. **Do not search PWSIDs in states with >20% bulk coverage.** The gap-state targeting query handles this. Don't override it for the validation run.
5. **Do not change the scoring v2 thresholds without reviewing the funnel data.** Serper returns Google results which may score differently than SearXNG's Bing/Yahoo mix. Run the validation, check the `search_log` funnel, then adjust if needed.
6. **Do not keep SearXNG Docker running after removal.** It consumes resources for zero benefit once Serper is validated.
7. **Do not store the Serper API key in code or config files checked into git.** Environment variable only.
8. **Do not skip the `--dry-run` step.** Always preview before committing API budget.
