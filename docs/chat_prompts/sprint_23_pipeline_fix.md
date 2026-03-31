# Sprint 23 — Pipeline Flow Fix & Scraped Content Persistence

## Context

Full audit report at: `docs/sprint23_pipeline_flow_audit.md`

**Prerequisite sprints completed:**
- Sprint 20: Registry cleanup (2,568 junk entries → dead), `.docx` skip, Playwright `finally`, subdomain crawl fix, deep crawl quality gate, `ua-ops process-backlog` CLI
- Sprint 21: SearXNG optimization (port fix, failed search logging via `search_attempted_at` + `search_log` table, scoring v2 with domain authority + utility matching, URL cap 3→1, gap-state targeting, funnel diagnostics)

**The root cause bug:** ScrapeAgent returns scraped text in a Python dict (memory only). ParseAgent requires that text passed as an explicit parameter. If the calling script doesn't chain correctly, crashes between scrape and parse, or is a pipeline path that never calls parse at all — the text is gone. The URL sits `active` with content but no parse result. Forever.

**Current state (2026-03-28):**
- 2,259 URLs with `status='active'`, `last_parse_result=NULL`, `last_content_length > 0`
- 1,987 of those have >500 chars (parseable content)
- Breakdown: deep_crawl (1,950), domain_guesser (182), SearXNG (108)
- Raw scraped text is stored **only in memory** — lost if the calling script chains incorrectly

---

## Fix 1: Persist Scraped Text in Database (45 min)

The total scraped content is ~35 MB. Projected at 50K URLs: ~175 MB. This is trivially small. A `TEXT` column on `scrape_registry` is simpler and more robust than filesystem storage.

### Migration

```sql
ALTER TABLE utility.scrape_registry ADD COLUMN IF NOT EXISTS scraped_text TEXT;
```

### Update ScrapeAgent

In `src/utility_api/agents/scrape.py`, after every successful fetch, persist the text:

```python
# After successful HTTP/Playwright fetch, before returning:
with engine.connect() as conn:
    conn.execute(text(f"""
        UPDATE {schema}.scrape_registry
        SET scraped_text = :text,
            last_content_length = :length,
            last_content_hash = :hash,
            last_fetch_at = NOW(),
            last_http_status = :status
        WHERE id = :registry_id
    """), {
        'text': raw_text,
        'length': len(raw_text),
        'hash': hashlib.sha256(raw_text.encode()).hexdigest(),
        'status': http_status,
        'registry_id': registry_id,
    })
    conn.commit()
```

This replaces the current metadata-only update. The ScrapeAgent already writes `last_content_hash` and `last_content_length` — now it also writes the actual text.

### Update ParseAgent

In `src/utility_api/agents/parse.py`, add a DB read fallback when `raw_text` is not provided:

```python
def run(self, pwsid: str, raw_text: str = None, content_type: str = None,
        source_url: str = None, registry_id: int = None, **kwargs):
    
    # If raw_text not provided, read from DB
    if raw_text is None and registry_id is not None:
        raw_text = self._load_scraped_text(registry_id)
    
    if raw_text is None and pwsid is not None:
        # Fallback: find the best available scraped text for this PWSID
        raw_text, registry_id, source_url = self._load_best_text_for_pwsid(pwsid)
    
    if not raw_text:
        logger.warning(f"  {pwsid}: no raw_text available and none in DB")
        return {'success': False, 'error': 'no_text_available'}
    
    # ... existing parse logic continues unchanged ...


def _load_scraped_text(self, registry_id: int) -> str | None:
    """Load persisted scraped text from scrape_registry."""
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT scraped_text FROM {schema}.scrape_registry
            WHERE id = :id AND scraped_text IS NOT NULL
        """), {'id': registry_id}).fetchone()
    return result.scraped_text if result else None


def _load_best_text_for_pwsid(self, pwsid: str) -> tuple:
    """Find the best available scraped text for a PWSID."""
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT id, scraped_text, url
            FROM {schema}.scrape_registry
            WHERE pwsid = :pwsid
            AND scraped_text IS NOT NULL
            AND last_content_length > 500
            AND status = 'active'
            AND last_parse_result IS NULL
            ORDER BY last_content_length DESC
            LIMIT 1
        """), {'pwsid': pwsid}).fetchone()
    if result:
        return result.scraped_text, result.id, result.url
    return None, None, None
```

This is the key decoupling: ParseAgent no longer depends on an in-memory handoff. It can read text from the database at any time, from any script, on any schedule.

---

## Fix 2: Triage and Process the 1,987 Hanging URLs (1 hour setup + overnight run)

Before processing, triage the backlog. Most of the 1,950 deep crawl entries are likely pre-Sprint 20 junk (before the quality gate was added).

### Step 1: Classify the backlog

```sql
-- How many deep crawl entries are rate-relevant?
SELECT 
    url_source,
    count(*) as total,
    count(*) FILTER (WHERE url ~* '(rate|fee|tariff|billing|water|utility|schedule|charge)') as rate_relevant,
    count(*) FILTER (WHERE url ~* '(norton|facebook|amazon|paris\.|mountain\.org|wikipedia)') as obvious_junk
FROM utility.scrape_registry
WHERE status = 'active'
AND last_parse_result IS NULL
AND last_content_length > 500
GROUP BY url_source;
```

### Step 2: Blacklist obvious junk

```sql
-- Mark obviously irrelevant deep crawl entries as dead
UPDATE utility.scrape_registry
SET status = 'dead',
    url_quality = 'blacklisted',
    notes = COALESCE(notes, '') || ' | CLEANUP: pre-Sprint20 junk deep crawl'
WHERE status = 'active'
AND last_parse_result IS NULL
AND url_source = 'deep_crawl'
AND NOT (url ~* '(rate|fee|tariff|billing|water|utility|schedule|charge|service|customer)')
AND (
    -- External domains that aren't utilities
    url ~* '(norton|facebook|amazon|google|youtube|twitter|linkedin|wikipedia|reddit|yelp|patch\.com|nextdoor)'
    OR
    -- Generic paths that aren't rate pages  
    url ~* '(/about|/contact|/career|/job|/news|/press|/blog|/event|/meeting|/agenda|/bid|/rfp)'
);
```

### Step 3: Re-fetch and persist text for surviving entries

The 1,987 entries have content (`last_content_length > 0`) but no `scraped_text` column yet (column didn't exist when they were scraped). Need to re-fetch to populate:

```python
"""
One-time backfill: Re-fetch active URLs with no scraped_text and persist.
Run AFTER Fix 1 (scraped_text column exists).
"""

def backfill_scraped_text(max_count=200, url_source=None):
    entries = get_entries_needing_text(max_count, url_source)
    
    scrape = ScrapeAgent()
    
    for entry in entries:
        try:
            # Re-fetch the URL
            result = scrape.fetch_single_url(entry.url)
            if result and result.get('text') and len(result['text']) > 100:
                # Persist text to DB
                persist_scraped_text(entry.id, result['text'])
                logger.info(f"  Backfilled {entry.pwsid} ({len(result['text'])} chars)")
            else:
                logger.debug(f"  {entry.pwsid}: re-fetch returned no content")
        except Exception as e:
            logger.warning(f"  {entry.pwsid}: re-fetch failed: {e}")
```

### Step 4: Parse the backfilled entries

Once text is persisted, use the updated `ua-ops process-backlog`:

```bash
# Parse SearXNG entries first (highest yield)
ua-ops process-backlog --url-source searxng --max 120

# Parse rate-relevant deep crawl entries
ua-ops process-backlog --url-source deep_crawl --max 100

# Parse domain guesser entries
ua-ops process-backlog --url-source domain_guesser --max 200
```

**Expected yield:**

| Source | Entries | After Triage | Success Rate | Expected PWSIDs |
|---|---|---|---|---|
| SearXNG | 108 | ~108 (all valid) | ~36% | ~39 |
| Deep crawl (rate-relevant) | ~60 | ~60 | ~21% | ~13 |
| Deep crawl (junk) | ~1,890 | 0 (blacklisted) | — | 0 |
| Domain guesser | 182 | ~182 | ~2.5% | ~5 |
| **Total** | **1,987** | **~350** | — | **~57** |

API cost: ~350 parse calls × $0.01-0.04 = **~$5-10**

---

## Fix 3: URL Quality Classification (30 min)

Add a quality tier to every registry entry, set automatically based on parse outcome.

### Migration

```sql
ALTER TABLE utility.scrape_registry 
ADD COLUMN IF NOT EXISTS url_quality VARCHAR(20) DEFAULT 'unknown';

-- Backfill from existing parse results
UPDATE utility.scrape_registry
SET url_quality = 'confirmed_rate_page'
WHERE last_parse_result = 'success';

UPDATE utility.scrape_registry
SET url_quality = 'blacklisted'
WHERE status = 'dead';

UPDATE utility.scrape_registry
SET url_quality = 'parse_failed'
WHERE last_parse_result IS NOT NULL 
AND last_parse_result != 'success'
AND status != 'dead';
```

### Auto-classification in ParseAgent

After parse completes, set the quality tier:

```python
# In ParseAgent, after parse attempt:
if parse_result.get('success'):
    url_quality = 'confirmed_rate_page'
elif parse_result.get('skipped'):
    # Pre-parse filter caught it — probably junk
    url_quality = 'probable_junk'
elif parse_result.get('confidence') in ('low', 'none', 'failed'):
    url_quality = 'parse_failed'
else:
    url_quality = 'unknown'

update_registry(registry_id, url_quality=url_quality)
```

### Quality tiers

| Tier | Meaning | Automated Action |
|------|---------|-----------------|
| `confirmed_rate_page` | Parsed successfully | Monitor for changes (future sprint) |
| `parse_failed` | Content fetched, parse didn't extract rates | Retry if parser improves |
| `probable_junk` | Pre-parse filter caught it (no financial content) | Skip on future sweeps |
| `blacklisted` | Confirmed irrelevant (external site, news, etc.) | Never re-process |
| `unknown` | Not yet classified | Process on next sweep |

The sweep script (Fix 4) skips `blacklisted` and `probable_junk` entries. `parse_failed` entries are eligible for retry after 30 days (when the parser may have improved or the page may have been updated).

---

## Fix 4: Automated Parse Sweep (30 min)

A continuously running process that finds unparsed URLs and processes them. Replaces the need for manual `ua-ops process-backlog` invocations.

### The sweep script

```python
"""
scripts/parse_sweep.py

Continuously processes unparsed URLs from scrape_registry.
Runs in a tmux session. Polls every 30 minutes.

Usage:
    python scripts/parse_sweep.py [--interval 1800] [--max-per-sweep 25]
"""

import time
import argparse
from utility_api.db import engine
from utility_api.config import settings
from utility_api.agents.scrape import ScrapeAgent
from utility_api.agents.parse import ParseAgent
from utility_api.agents.best_estimate import BestEstimateAgent

schema = settings.utility_schema

def get_unparsed_entries(max_count=25):
    """Find registry entries that need parsing."""
    query = f"""
        SELECT sr.id, sr.pwsid, sr.url, sr.url_source, 
               sr.scraped_text, sr.last_content_length,
               s.pws_name, s.state_code
        FROM {schema}.scrape_registry sr
        LEFT JOIN {schema}.sdwis_systems s ON s.pwsid = sr.pwsid
        WHERE sr.status = 'active'
        AND sr.last_parse_result IS NULL
        AND sr.last_content_length > 500
        AND sr.url LIKE 'http%'
        AND COALESCE(sr.url_quality, 'unknown') NOT IN ('blacklisted', 'probable_junk')
        ORDER BY 
            -- Prioritize: SearXNG > curated > duke > deep_crawl > guesser
            CASE sr.url_source
                WHEN 'searxng' THEN 1
                WHEN 'curated' THEN 2
                WHEN 'curated_portland' THEN 2
                WHEN 'metro_research' THEN 2
                WHEN 'duke_reference' THEN 3
                WHEN 'state_directory' THEN 4
                WHEN 'deep_crawl' THEN 5
                WHEN 'domain_guesser' THEN 6
                ELSE 7
            END,
            s.population_served_count DESC NULLS LAST
        LIMIT :max
    """
    with engine.connect() as conn:
        return conn.execute(text(query), {'max': max_count}).fetchall()


def run_sweep(max_per_sweep=25):
    """One sweep: find unparsed entries, parse them."""
    entries = get_unparsed_entries(max_per_sweep)
    
    if not entries:
        logger.info("Sweep: no unparsed entries found")
        return {'processed': 0}
    
    logger.info(f"Sweep: processing {len(entries)} entries")
    
    parse = ParseAgent()
    stats = {'processed': 0, 'parsed': 0, 'failed': 0, 'skipped': 0}
    successful_states = set()
    
    for entry in entries:
        stats['processed'] += 1
        
        # Read text from DB (Fix 1) or re-fetch if not persisted
        raw_text = entry.scraped_text
        if not raw_text:
            # Text not persisted yet — need to re-fetch
            scrape = ScrapeAgent()
            result = scrape.fetch_single_url(entry.url)
            if result and result.get('text'):
                raw_text = result['text']
                # Persist for next time
                persist_scraped_text(entry.id, raw_text)
            else:
                stats['failed'] += 1
                continue
        
        # Parse
        parse_result = parse.run(
            pwsid=entry.pwsid,
            raw_text=raw_text,
            source_url=entry.url,
            registry_id=entry.id,
            skip_best_estimate=True,  # batch at end
        )
        
        if parse_result.get('skipped'):
            stats['skipped'] += 1
        elif parse_result.get('success'):
            stats['parsed'] += 1
            successful_states.add(entry.state_code)
        else:
            stats['failed'] += 1
    
    # Batch BestEstimate per state (Sprint 20 fix)
    if successful_states:
        best_estimate = BestEstimateAgent()
        for state in successful_states:
            best_estimate.run(state=state)
    
    logger.info(f"Sweep complete: {stats}")
    return stats


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--interval', type=int, default=1800, help='Seconds between sweeps')
    parser.add_argument('--max-per-sweep', type=int, default=25, help='Max entries per sweep')
    parser.add_argument('--once', action='store_true', help='Run once and exit')
    args = parser.parse_args()
    
    logger.info(f"Parse sweep starting (interval={args.interval}s, max={args.max_per_sweep})")
    
    while True:
        try:
            stats = run_sweep(args.max_per_sweep)
        except Exception as e:
            logger.error(f"Sweep error: {e}")
        
        if args.once:
            break
        
        time.sleep(args.interval)


if __name__ == '__main__':
    main()
```

### Run in tmux

```bash
# Start the sweep daemon
tmux new-session -d -s parse_sweep "cd ~/projects/utility-api && python scripts/parse_sweep.py --interval 1800 --max-per-sweep 25 2>&1 | tee -a logs/parse_sweep.log"
```

### Cost guard

At 25 entries per sweep, 48 sweeps/day max, the theoretical ceiling is 1,200 parse calls/day × $0.01-0.04 = $12-48/day. In practice the backlog drains quickly and most sweeps find 0-5 entries:

- Day 1: processes ~350 backlog entries (~$5-10)
- Day 2+: processes ~5-15 new entries/day from ongoing discovery (~$0.15-0.60)

---

## Fix 5: Unified Scrape→Parse Chain Function (30 min)

Multiple scripts implement their own scrape→parse→best_estimate chain. Some do it correctly (`process_guesser_batch.py`), some don't (`run_mn_discovery.py`). Create one function that all pipelines call.

```python
# src/utility_api/pipeline/chain.py

def scrape_and_parse(pwsid: str, url: str = None, registry_id: int = None,
                      skip_best_estimate: bool = False) -> dict:
    """
    Atomic scrape → parse → best_estimate chain.
    
    All pipeline entry points should call this instead of
    manually chaining ScrapeAgent → ParseAgent.
    
    Args:
        pwsid: EPA PWSID
        url: specific URL to scrape (optional — if not provided, scrapes all pending URLs for PWSID)
        registry_id: specific registry entry (optional)
        skip_best_estimate: if True, caller handles BestEstimate batching
    
    Returns:
        dict with 'success', 'parse_result', 'pwsid', 'url'
    """
    scrape = ScrapeAgent()
    parse = ParseAgent()
    
    # Step 1: Scrape (persists text to DB via Fix 1)
    scrape_result = scrape.run(pwsid=pwsid, url=url, registry_id=registry_id)
    
    if not scrape_result or not scrape_result.get('raw_texts'):
        return {'success': False, 'error': 'scrape_failed', 'pwsid': pwsid}
    
    # Step 2: Parse each scraped text
    results = []
    for text_entry in scrape_result['raw_texts']:
        parse_result = parse.run(
            pwsid=pwsid,
            raw_text=text_entry['text'],
            content_type=text_entry.get('content_type'),
            source_url=text_entry.get('url', url),
            registry_id=text_entry.get('registry_id', registry_id),
            skip_best_estimate=skip_best_estimate,
        )
        results.append(parse_result)
    
    any_success = any(r.get('success') for r in results)
    
    # Step 3: BestEstimate (unless caller is batching)
    if any_success and not skip_best_estimate:
        state = pwsid[:2]
        BestEstimateAgent().run(state=state)
    
    return {
        'success': any_success,
        'parse_results': results,
        'pwsid': pwsid,
        'url': url,
    }
```

### Update existing callers

All pipeline scripts should use this chain instead of manual ScrapeAgent→ParseAgent calls:

```python
# In process_guesser_batch.py, run_mn_discovery.py, and any other pipeline scripts:

# Before (manual chain — error-prone):
scrape_result = scrape.run(pwsid=pwsid)
for text in scrape_result['raw_texts']:
    parse.run(pwsid=pwsid, raw_text=text['text'], ...)

# After (unified chain):
from utility_api.pipeline.chain import scrape_and_parse
result = scrape_and_parse(pwsid=pwsid, skip_best_estimate=True)
```

This eliminates the entire class of bugs where different scripts chain the agents differently.

---

## Fix 6: Logging Cleanup (30 min)

### What to fix now

**Add URL quality to pipeline reporting:**

```bash
# ua-ops pipeline-health should show:
ua-ops pipeline-health

# Output includes:
# URL Quality Distribution:
#   confirmed_rate_page:  967
#   parse_failed:         2,912
#   probable_junk:        3,851
#   blacklisted:          3,085
#   unknown:              2,369
```

**Add url_quality to `ua-ops process-backlog --dry-run` output** so you can see what the sweep will process.

### What to defer

- `scrape_status` state machine redesign → future architecture sprint
- Content change history → future monitoring sprint
- Deep crawl link-following logging → low ROI until deep crawl volume increases
- Individual URL scores from discovery → Sprint 21's `search_log` table already captures this for SearXNG

---

## Build Order

1. **Fix 1: `scraped_text` column** — migration + ScrapeAgent write + ParseAgent read (45 min)
2. **Fix 5: Unified chain function** — `pipeline/chain.py` + update callers (30 min)
3. **Fix 2: Triage hanging URLs** — classify, blacklist junk, re-fetch + persist + parse (1 hour setup + overnight)
4. **Fix 3: URL quality tiers** — migration + auto-classification in ParseAgent (30 min)
5. **Fix 4: Automated sweep** — `parse_sweep.py` + tmux daemon (30 min)
6. **Fix 6: Logging** — pipeline health additions (30 min)

**Total: ~3.5 hours of code + overnight batch run**

---

## Validation

### After Fix 1 (text persistence):

```bash
# Verify column exists
psql -c "\d utility.scrape_registry" | grep scraped_text

# Run a single scrape and verify text is persisted
ua-run-orchestrator --execute 1 --state NY

# Check it was stored
psql -c "
SELECT pwsid, url_source, last_content_length, length(scraped_text) as text_length
FROM utility.scrape_registry
WHERE scraped_text IS NOT NULL
ORDER BY last_fetch_at DESC
LIMIT 5;"
```

### After Fix 2 (backlog processing):

```bash
# Check how many entries were processed
psql -c "
SELECT url_source, last_parse_result, count(*)
FROM utility.scrape_registry
WHERE status = 'active'
AND url_source IN ('searxng', 'deep_crawl', 'domain_guesser')
GROUP BY url_source, last_parse_result
ORDER BY url_source, last_parse_result;"

# Check new coverage
ua-ops coverage-report
```

### After Fix 4 (automated sweep):

```bash
# Verify sweep is running
tmux ls | grep parse_sweep

# Check sweep log
tail -20 logs/parse_sweep.log

# Verify entries are being processed
psql -c "
SELECT date_trunc('hour', last_fetch_at) as hour, count(*), 
       count(*) FILTER (WHERE last_parse_result = 'success') as parsed
FROM utility.scrape_registry
WHERE last_fetch_at > NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour DESC;"
```

### Full pipeline test (after all fixes):

```bash
# Register a test URL manually
psql -c "
INSERT INTO utility.scrape_registry (pwsid, url, url_source, status)
VALUES ('OR4100657', 'https://www.portland.gov/water/rates-charges', 'test_sprint23', 'pending')
ON CONFLICT DO NOTHING;"

# Wait for sweep to pick it up (or run once manually)
python scripts/parse_sweep.py --once --max-per-sweep 5

# Verify the full chain worked:
# 1. scraped_text populated
# 2. last_parse_result set
# 3. url_quality classified
# 4. rate_schedules entry created (if parse succeeded)
psql -c "
SELECT sr.pwsid, sr.status, sr.url_quality, sr.last_parse_result,
       length(sr.scraped_text) as text_len,
       (SELECT count(*) FROM utility.rate_schedules rs WHERE rs.pwsid = sr.pwsid) as rate_records
FROM utility.scrape_registry sr
WHERE sr.url_source = 'test_sprint23';"
```

---

## Expected Impact

| Fix | Immediate PWSIDs | Ongoing Value |
|-----|-------------------|---------------|
| Text persistence | 0 | Eliminates data loss between scrape and parse |
| Unified chain | 0 | Prevents all future chain bugs across all pipeline paths |
| Backlog triage + parse | ~57 | Recovers data from 1,987 hanging entries |
| URL quality tiers | 0 | Prevents re-processing confirmed junk |
| Automated sweep | ~5-15/day ongoing | Orphaned entries become impossible |
| Logging | 0 | Visibility into pipeline health |

Post-sprint: the pipeline becomes fully automated from registry insertion to rate extraction. Any URL that enters `scrape_registry` from any source (metro scan, domain guesser, SearXNG, manual curation) will be scraped, persisted, parsed, and classified without manual intervention.

---

## What NOT to Do

1. **Do not store `scraped_text` as a file.** The total volume is ~35MB — DB storage is simpler, more queryable, and doesn't require filesystem management.
2. **Do not process the 1,950 deep crawl entries without triaging first.** Most are pre-Sprint 20 junk. Blacklist the irrelevant ones, then parse the survivors.
3. **Do not implement content change history.** That's a monitoring feature for tracking rate updates over time. Useful later, not now.
4. **Do not redesign the `scrape_status` state machine.** Sprint 22's `searxng_status` workaround is functional. A clean redesign is a future architecture sprint.
5. **Do not set the sweep interval below 15 minutes.** At $0.01-0.04 per parse call, a hyperactive sweep wastes API cost on entries that just arrived and may not be worth parsing.
6. **Do not run the backfill re-fetch at full speed.** Some of these URLs are 2+ years old. Rate-limit re-fetches at 2-3 per second to be polite to utility websites.
7. **Do not delete `scraped_text` after successful parse.** Keep it for potential re-parsing with improved prompts/models in future sprints.
