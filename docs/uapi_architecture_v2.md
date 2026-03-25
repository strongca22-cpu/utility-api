# UAPI — Architecture & Buildout Plan v2

**Date**: 2026-03-24
**Status**: Architecture phase. Execution follows.
**Context**: 9 sprints of proof-of-concept work complete. 846 PWSIDs with rate data across 3 states, 6 sources, 3 acquisition methods. The concept is validated. The system now needs architecture before it needs more data.

---

## Part 1: Where We Are

### What Has Been Proven

The first 9 sprints answered three critical questions:

**Can LLM agents reliably parse water rate schedules?** Yes. 22/31 VA utilities parsed successfully from web scraping. Sonnet handles multi-district tariffs, unit conversions, and complex tier structures. Total API cost: $0.40. The parser works.

**Are there bulk government data sources that bypass per-utility scraping?** Yes, and they're dramatically more efficient. One CA eAR ingest covered 194 utilities in a single pipeline run. NC EFC covered 403 utilities from a single CSV. Bulk government data is 100x more efficient than per-utility scraping.

**Can multiple sources be reconciled into a single best estimate?** Yes, with caveats. CA's multi-source reconciliation (eAR + OWRS + scraped) showed 38% agreement, 32% moderate variance, and 15% divergent. A source-priority hierarchy resolves this to a single best estimate per PWSID, but the hierarchy logic is currently CA-hardcoded.

### What Hasn't Been Built

The system has no memory, no coordination, and no strategic awareness. Specifically:

- No record of what has been attempted vs. what succeeded vs. what failed
- No way to answer "which PWSIDs should I work on next?" without ad-hoc SQL
- No automated refresh or change detection
- No generalized best-estimate logic (CA-only)
- SDWIS metadata for only 2 of 50 states (the `/resolve` endpoint is incomplete for 93% of systems)
- No pipeline orchestration — every sprint starts with a human deciding what to do
- No path from "run Claude Code to scrape" to autonomous operation

### Current Coverage

| State | CWS Systems | With Rates | Coverage | Sources |
|-------|------------|------------|----------|---------|
| NC | 1,842 | 403 | 21.9% | EFC 2025 |
| CA | 2,801 | 415 | 14.8% | eAR (3 vintages) + OWRS + scraped |
| VA | 910 | 28 | 3.1% | scraped only |
| Other 47 states | 39,090 | 0 | 0% | — |
| **Total** | **44,643** | **846** | **1.9%** | 6 sources |

---

## Part 2: The Target State

### What "Done" Looks Like

A self-aware, largely autonomous pipeline that:

1. **Knows its own coverage**: For every PWSID in the US, knows whether rate data exists, from which source, how fresh it is, and what confidence level it carries
2. **Knows what to do next**: A priority queue ranks unserved PWSIDs by value (DC adjacency, population, state coverage gaps) and routes them to the right acquisition method
3. **Acquires data without manual intervention**: Bulk government sources are ingested on schedule. Per-utility scraping is handled by research agents dispatched by an orchestrator. New sources are discovered and cataloged.
4. **Maintains data without manual intervention**: Change detection identifies stale records. Re-scraping and re-parsing happen automatically. Best estimates update when new data arrives.
5. **Serves data through a production API**: `/resolve`, `/utility/{pwsid}`, `/site-report` endpoints with auth, rate limiting, usage metering, and documentation
6. **Exports data as a product**: Bulk dataset downloads (GeoPackage, CSV) per state and nationally, versioned, with provenance

### What "Done" Does NOT Look Like

- A human running `ua-ingest` commands for each new state
- A human curating YAML files of utility URLs
- A hardcoded best-estimate script per state
- Ad-hoc SQL to determine what's been covered
- Claude Code sessions as the primary acquisition method

---

## Part 3: System Architecture

### Three Layers + Two Agents

```
┌─────────────────────────────────────────────────────┐
│                    API / Export                       │
│  /resolve  /utility  /site-report  /bulk-download    │
└──────────────────────┬──────────────────────────────┘
                       │ reads
┌──────────────────────▼──────────────────────────────┐
│              Canonical Data Store                     │
│  cws_boundaries ← sdwis ← rate_schedules            │
│  permits ← aqueduct ← county_boundaries             │
│  rate_best_estimate (materialized, auto-refreshed)   │
└──────────────────────┬──────────────────────────────┘
                       │ writes
┌──────────────────────▼──────────────────────────────┐
│           Data Operations Layer                      │
│  source_catalog ← pwsid_coverage ← scrape_registry  │
│  pipeline_runs ← ingest_log                          │
└───────────┬─────────────────────────────┬───────────┘
            │ dispatches                  │ updates
   ┌────────▼────────┐          ┌────────▼────────┐
   │   Orchestrator   │          │ Research Agents  │
   │   (scheduler)    │─────────▶│ (scrape + parse) │
   │                  │ assigns  │                  │
   │ - reads coverage │ tasks    │ - fetch URLs     │
   │ - picks targets  │          │ - extract text   │
   │ - routes method  │          │ - call LLM       │
   │ - schedules cron │          │ - write results  │
   │ - monitors health│          │ - update registry│
   └──────────────────┘          └──────────────────┘
```

### Layer Definitions

**Canonical Data Store** — The product. What the API serves and the dataset exports contain. Tables are optimized for query performance. Schema is stable and versioned.

**Data Operations Layer** — The machine's self-awareness. Tracks what data exists, where it came from, what's been attempted, what failed, what's stale, and what should be done next. This layer is internal — not exposed through the API.

**Orchestrator** — The decision-maker. Reads the Data Operations Layer to determine what acquisition work to do. Dispatches tasks to research agents. Runs on cron (daily/weekly). Does not scrape or parse — it coordinates.

**Research Agents** — The workers. Each agent handles one task type: bulk ingest, URL discovery, web scraping, LLM parsing, or change detection. Agents write results to the Canonical Data Store and update the Data Operations Layer (scrape registry, coverage). Agents are stateless — all state lives in the database.

### Agent Roles (Detailed)

**Orchestrator Agent**
- Runs on schedule (daily or triggered)
- Reads `source_catalog` to check for new vintages of bulk data (e.g., "is eAR 2023 available on HydroShare yet?")
- Reads `pwsid_coverage` to identify gaps: which high-priority PWSIDs lack rate data?
- Reads `scrape_registry` to identify stale or retriable URLs
- Produces a task queue: ordered list of (pwsid, method, priority) tuples
- Dispatches tasks to appropriate research agents
- Does NOT make API calls, fetch URLs, or parse data

**Bulk Ingest Agent**
- Handles structured data sources: eAR, OWRS, EFC CSVs, state water board datasets
- Triggered by orchestrator when a new vintage is detected or a new bulk source is cataloged
- Reads source-specific config from `source_catalog`
- Writes to `rate_schedules` (canonical) and `water_rates` (legacy)
- Updates `pwsid_coverage` and `source_catalog.last_ingested_at`

**Discovery Agent**
- Finds rate page URLs for PWSIDs that have no known URL
- Uses SearXNG, utility name + state, and known URL patterns
- Writes discovered URLs to `scrape_registry` with `status = pending`
- Does NOT fetch or parse — only discovers

**Scrape Agent**
- Fetches content from URLs in `scrape_registry` where `status = pending` or `status = pending_retry`
- HTTP GET, Playwright for JS-rendered pages, pymupdf for PDFs
- Writes raw content hash, HTTP status, content length to `scrape_registry`
- Stores raw text in a content store (filesystem or blob column)
- Compares hash to previous fetch for change detection
- Does NOT parse — only fetches and records

**Parse Agent**
- Takes raw text from scrape agent output and sends to Claude API (Batch for bulk, live for on-demand)
- Writes structured rate data to `rate_schedules`
- Updates `scrape_registry` with parse result, confidence, cost
- Routes to Sonnet for complex structures, Haiku for simple flat-rate utilities (complexity determined by content length and tier-keyword count)

**Best Estimate Agent**
- Triggered after any rate data write (bulk ingest, parse, or manual correction)
- Reads `source_catalog` priority rankings for the affected state
- For each affected PWSID, selects the highest-priority source record
- Merge logic: prefer newest vintage from highest-priority source; only replace an existing estimate if the new data is from a higher-priority source OR from the same source with a newer vintage AND within a configurable margin of the previous value (default: 20%)
- Writes/updates `rate_best_estimate`
- Refreshes derived columns on `pwsid_coverage`

---

## Part 4: Database Schema (Target State)

### Canonical Data Store

These tables are the product. Schema changes require migration and versioning.

```sql
-- Existing, stable
utility.cws_boundaries          -- 44,643 rows, EPA polygons, PWSID PK
utility.sdwis_systems           -- Target: all 50 states via ECHO API
utility.aqueduct_polygons       -- 68,506 rows, WRI water stress
utility.county_boundaries       -- 3,235 rows, Census TIGER
utility.mdwd_financials         -- 225 rows (VA+CA), Census of Govts
utility.permits                 -- 61,530 rows (VA DEQ + CA eWRIMS)

-- New canonical rate schema (replaces water_rates as the source of truth)
utility.rate_schedules (
    id                  SERIAL PRIMARY KEY,
    pwsid               TEXT NOT NULL REFERENCES cws_boundaries(pwsid),
    source_key          TEXT NOT NULL,  -- FK → source_catalog
    vintage_date        DATE,           -- when the rates were effective
    customer_class      TEXT DEFAULT 'residential',
    billing_frequency   TEXT,           -- monthly | quarterly | bimonthly
    rate_structure_type TEXT,           -- flat | uniform | increasing_block | budget_based | seasonal
    fixed_charges       JSONB,          -- [{name, amount, frequency, meter_size}]
    volumetric_tiers    JSONB,          -- [{tier, min_gal, max_gal, rate_per_1000}]
    surcharges          JSONB,          -- [{name, rate_per_1000, condition}]
    bill_5ccf           NUMERIC(8,2),   -- monthly cost at 3,740 gal
    bill_10ccf          NUMERIC(8,2),   -- monthly cost at 7,480 gal
    bill_20ccf          NUMERIC(8,2),   -- monthly cost at 14,960 gal
    conservation_signal NUMERIC(4,2),   -- ratio: highest tier / lowest tier rate
    source_url          TEXT,
    scrape_timestamp    TIMESTAMPTZ,
    confidence          TEXT,           -- high | medium | low
    raw_text_hash       TEXT,           -- SHA-256 for change detection
    needs_review        BOOLEAN DEFAULT FALSE,
    review_reason       TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(pwsid, source_key, vintage_date, customer_class)
);

-- Materialized best estimate (refreshed by best estimate agent)
utility.rate_best_estimate (
    pwsid               TEXT PRIMARY KEY REFERENCES cws_boundaries(pwsid),
    source_key          TEXT NOT NULL,
    vintage_date        DATE,
    bill_5ccf           NUMERIC(8,2),
    bill_10ccf          NUMERIC(8,2),
    bill_20ccf          NUMERIC(8,2),
    rate_structure_type TEXT,
    confidence          TEXT,
    tier_count          INTEGER,
    conservation_signal NUMERIC(4,2),
    fixed_charge        NUMERIC(8,2),   -- primary residential fixed charge
    lowest_tier_rate    NUMERIC(8,4),   -- $/1000 gal, first tier
    highest_tier_rate   NUMERIC(8,4),   -- $/1000 gal, last tier
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);
```

### Data Operations Layer

These tables are internal. Not exposed through the API. Schema can evolve freely.

```sql
-- Source catalog: every known data source, bulk or scraped
utility.source_catalog (
    source_key          TEXT PRIMARY KEY,  -- e.g., swrcb_ear_2022, efc_nc_2025, scraped_llm
    source_type         TEXT NOT NULL,     -- bulk_government | bulk_survey | scraped_llm | curated
    display_name        TEXT,
    states_covered      TEXT[],            -- array of 2-letter state codes
    pwsid_count         INTEGER,           -- how many PWSIDs this source covers (updated after ingest)
    vintage_start       DATE,
    vintage_end         DATE,
    refresh_cadence     TEXT,              -- annual | semi_annual | one_time | continuous
    priority_rank       INTEGER,           -- lower = higher priority, per state context
    trust_level         TEXT,              -- authoritative | verified | provisional | unverified
    ingest_module       TEXT,              -- Python module path for automated ingest
    source_url          TEXT,              -- where to check for new data
    last_ingested_at    TIMESTAMPTZ,
    next_check_date     DATE,
    notes               TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- PWSID-level coverage: the strategic view of every system
utility.pwsid_coverage (
    pwsid               TEXT PRIMARY KEY REFERENCES cws_boundaries(pwsid),
    state               TEXT,
    population_served   INTEGER,
    has_rate_data       BOOLEAN DEFAULT FALSE,
    rate_source_count   INTEGER DEFAULT 0,
    rate_sources        TEXT[],            -- array of source_keys that have data for this PWSID
    best_estimate_source TEXT,
    best_estimate_confidence TEXT,
    best_estimate_bill_10ccf NUMERIC(8,2),
    scrape_status       TEXT DEFAULT 'not_attempted',
        -- not_attempted | url_discovered | attempted_failed | succeeded | stale
    priority_tier       INTEGER,           -- 1=DC adjacent, 2=DC state, 3=large pop, 4=other
    last_updated        TIMESTAMPTZ DEFAULT NOW()
);

-- Scrape registry: every URL interaction
utility.scrape_registry (
    id                  SERIAL PRIMARY KEY,
    pwsid               TEXT REFERENCES cws_boundaries(pwsid),
    url                 TEXT NOT NULL,
    url_source          TEXT,              -- searxng | curated | civicplus | manual | bulk_index
    discovery_query     TEXT,
    content_type        TEXT,              -- html | pdf | xlsx | unknown

    -- Fetch tracking
    last_fetch_at       TIMESTAMPTZ,
    last_http_status    INTEGER,
    last_content_hash   TEXT,
    last_content_length INTEGER,
    content_changed     BOOLEAN,           -- did hash change since previous fetch?

    -- Parse tracking
    last_parse_at       TIMESTAMPTZ,
    last_parse_result   TEXT,              -- success | failed | partial | skipped
    last_parse_confidence TEXT,
    last_parse_cost_usd NUMERIC(6,4),
    last_parse_model    TEXT,              -- sonnet | haiku

    -- Lifecycle
    status              TEXT DEFAULT 'pending',
        -- pending | active | dead | blocked | stale | pending_retry
    retry_after         TIMESTAMPTZ,
    retry_count         INTEGER DEFAULT 0,
    failure_reason      TEXT,              -- 403 | timeout | empty_content | parse_error | etc.
    notes               TEXT,

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(pwsid, url)
);

-- Ingest log: every pipeline run with provenance
utility.ingest_log (
    id                  SERIAL PRIMARY KEY,
    source_key          TEXT REFERENCES source_catalog(source_key),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    status              TEXT,              -- success | partial | failed
    rows_inserted       INTEGER,
    rows_updated        INTEGER,
    rows_skipped        INTEGER,
    error_message       TEXT,
    triggered_by        TEXT,              -- cron | manual | orchestrator
    notes               TEXT
);
```

### Source Priority Configuration

The merge hierarchy for best estimates is driven by `source_catalog.priority_rank`, contextualized by state. Lower rank = higher priority.

```yaml
# config/source_priority.yaml
# Per-state override. Fallback to default if state not listed.

default:
  - source_type: bulk_government    # e.g., eAR, state water board reports
    trust_level: authoritative
    prefer_newest_vintage: true
  - source_type: bulk_survey        # e.g., EFC dashboards
    trust_level: verified
    prefer_newest_vintage: true
  - source_type: scraped_llm        # LLM-parsed from utility websites
    trust_level: provisional
    prefer_newest_vintage: true
    margin_check: true              # only replace existing if within 20% margin
  - source_type: curated            # e.g., OWRS, manually entered
    trust_level: verified
    prefer_newest_vintage: true

CA:
  # CA has the richest multi-source landscape
  priority_order:
    - swrcb_ear_2022    # State-reported, highest authority
    - swrcb_ear_2021
    - scraped_llm       # If agrees with eAR anchor (within margin)
    - owrs              # Curated but often older vintage
    - swrcb_ear_2020
  margin_threshold: 0.20  # 20% — scraped replaces eAR only if within this

NC:
  priority_order:
    - efc_nc_2025       # Single authoritative source, high coverage
    - scraped_llm       # Fill gaps only

VA:
  priority_order:
    - scraped_llm       # Only source currently
    # Future: VA state data if it becomes available
```

The best estimate agent reads this config to determine, for each PWSID, which `rate_schedules` record to promote to `rate_best_estimate`. The merge logic:

1. For each PWSID, gather all `rate_schedules` records
2. Rank by state-specific priority order (from config)
3. Within same source, prefer newest vintage
4. If replacing an existing best estimate with a lower-priority source, apply margin check: new value must be within `margin_threshold` of existing value, otherwise flag `needs_review`
5. Write to `rate_best_estimate`, log the selection rationale

---

## Part 5: Migration from Current State

### What Stays

- `cws_boundaries` — untouched, the spatial backbone
- `aqueduct_polygons` — untouched
- `county_boundaries` — untouched
- `permits` — untouched
- `permit_facility_xref` — untouched
- `water_rates` — kept as legacy/audit table, no longer the source of truth for API
- All existing ingest modules — still work, just need to also write to `rate_schedules`

### What Gets Built New

- `rate_schedules` — canonical rate schema with JSONB tiers
- `rate_best_estimate` — generalized, replaces CA-only version
- `source_catalog` — seeded from `config/sources.yaml` + sprint history
- `pwsid_coverage` — populated from cross-join of `cws_boundaries` × existing rate data
- `scrape_registry` — seeded from `config/rate_urls_*.yaml` + `water_rates.source_url`
- `ingest_log` — replaces `pipeline_runs` with richer schema

### What Gets Migrated

- Existing `water_rates` rows (1,472 records) → `rate_schedules` (transform fixed columns to JSONB)
- Existing `rate_best_estimate` (443 CA rows) → new generalized `rate_best_estimate`
- Existing `config/rate_urls_*.yaml` → `scrape_registry` rows
- Existing `config/sources.yaml` → `source_catalog` rows

---

## Part 6: Sprint Plan

### Principles

- **Architecture before execution.** Build the machine, then feed it data.
- **Every sprint produces a shippable artifact.** If a sprint slips, the prior sprint's output still works.
- **15 hrs/week alongside primary DC project.** Each 2-week sprint = ~30 hours.
- **No sprint requires Claude Code for data acquisition.** After the architecture sprints, acquisition is agent-driven.

---

### Sprint 10 (Weeks 1–2): Data Operations Foundation

> **Goal**: The system knows itself. Layer B is live. SDWIS covers all 50 states.

| Task | Hours | Deliverable |
|------|-------|-------------|
| Create `source_catalog` table + seed from sprint history (6 known sources + their metadata) | 3 | Queryable source inventory |
| Create `pwsid_coverage` table + populate from cross-join of `cws_boundaries` × `water_rates` | 4 | 44,643 rows, 846 marked `has_rate_data = true` |
| Run SDWIS expansion: all 50 states via ECHO API (same module, loop over states) | 3 | `sdwis_systems` goes from 3,711 → ~150,000. `/resolve` complete nationwide |
| Populate `pwsid_coverage.population_served` and `pwsid_coverage.state` from SDWIS + CWS | 2 | Coverage table enriched with population data |
| Assign `priority_tier` on `pwsid_coverage`: tier 1 = DC-adjacent PWSIDs (from `permit_facility_xref`), tier 2 = DC states, tier 3 = pop > 100K, tier 4 = rest | 2 | Priority queue populated |
| Build `ua-coverage-report` CLI command: per-state summary (CWS count, rate count, coverage %, sources, priority breakdown) | 4 | `ua-coverage-report` prints strategic dashboard |
| Alembic migrations for all new tables | 2 | Clean migration path |
| Write `source_priority.yaml` config for CA, NC, VA (from sprint history knowledge) | 2 | Priority config checked into repo |
| Update `docs/` to reflect new architecture | 2 | Architecture docs current |

**Sprint 10 output**: `ua-coverage-report` prints: "44,643 systems, 846 with rates (1.9%), SDWIS complete for all 50 states, priority tier 1: 41 PWSIDs (30 with rates), priority tier 2: 12,000 PWSIDs (815 with rates)..." The system knows its own state.

---

### Sprint 11 (Weeks 3–4): Canonical Schema + Best Estimate Generalization

> **Goal**: Single source of truth for rates. Best estimate works for any state.

| Task | Hours | Deliverable |
|------|-------|-------------|
| Create `rate_schedules` table with JSONB schema | 2 | Canonical table ready |
| Write migration transform: `water_rates` → `rate_schedules` for all 1,472 existing records | 5 | All existing data in canonical format |
| Update each ingest module to write to `rate_schedules` (in addition to legacy `water_rates`) | 4 | Dual-write: new ingests populate both tables |
| Generalize `build_best_estimate.py`: read `source_priority.yaml`, apply margin-check logic, run for all states | 5 | `rate_best_estimate` covers CA (443) + NC (403) + VA (28) = 874 rows |
| Create `ingest_log` table, replace `pipeline_runs` | 2 | Richer audit trail |
| Wire best-estimate refresh to trigger after any `rate_schedules` insert | 3 | Best estimate auto-updates on new data |
| Update `/resolve` to pull from generalized `rate_best_estimate` (not CA-only) | 2 | `/resolve` returns best-estimate rate for any state with data |
| Update `/rates/best-estimate` to accept any state, not just CA | 2 | API generalized |
| Compute and store `conservation_signal`, `bill_20ccf` on all `rate_schedules` records | 2 | New derived metrics populated |

**Sprint 11 output**: One table (`rate_schedules`) is the source of truth. Best estimate covers all 3 states. API serves rates nationwide where data exists. Adding a new state's data automatically flows through to best estimate.

---

### Sprint 12 (Weeks 5–6): Scrape Registry + Agent Skeleton

> **Goal**: Layer A is live. Agent interfaces defined. Scraping has memory.

| Task | Hours | Deliverable |
|------|-------|-------------|
| Create `scrape_registry` table | 2 | Registry ready |
| Seed from existing `config/rate_urls_*.yaml` + `water_rates.source_url` for scraped records | 3 | ~130 URLs imported with known status |
| Backfill `scrape_registry` with known failures from sprint history (CivicPlus 403s, dead URLs, etc.) | 2 | Historical failures recorded |
| Define agent interfaces (Python ABCs): `BaseAgent`, `OrchestratorAgent`, `BulkIngestAgent`, `DiscoveryAgent`, `ScrapeAgent`, `ParseAgent`, `BestEstimateAgent` | 4 | Agent contracts defined, not yet implemented |
| Implement `BulkIngestAgent` wrapping existing ingest modules (eAR, OWRS, EFC NC) | 4 | First working agent — runs existing pipelines through agent interface |
| Implement `BestEstimateAgent` wrapping generalized best-estimate logic | 3 | Second working agent — triggered after bulk ingest |
| Build `ua-scrape-status` CLI: query scrape_registry, show per-state summary of URL statuses | 2 | Tactical visibility into scrape pipeline |
| Write agent dispatch protocol: how orchestrator creates tasks, how agents report results | 3 | Protocol documented + stub implementation |

**Sprint 12 output**: Scrape registry tracks all known URLs. Two agents operational (bulk ingest, best estimate). Agent interfaces defined for all roles. The dispatch protocol is the contract that future agents implement.

---

### Sprint 13 (Weeks 7–8): Orchestrator + Research Agents

> **Goal**: The orchestrator can decide what to do and dispatch work. Research agents can discover and scrape autonomously.

| Task | Hours | Deliverable |
|------|-------|-------------|
| Implement `OrchestratorAgent`: reads `source_catalog` for stale bulk sources, reads `pwsid_coverage` for gaps, reads `scrape_registry` for retriable URLs, produces ranked task queue | 6 | Orchestrator generates "next 50 tasks" ranked by priority |
| Implement `DiscoveryAgent`: takes PWSID + utility name, runs SearXNG search, writes URLs to `scrape_registry` | 4 | URL discovery without human intervention |
| Implement `ScrapeAgent`: takes URL from `scrape_registry`, fetches content, updates registry with status/hash/length | 4 | Fetch without human intervention |
| Implement `ParseAgent`: takes raw text, routes to Sonnet or Haiku based on complexity heuristic, writes to `rate_schedules`, updates `scrape_registry` | 5 | Parse without human intervention |
| Wire pipeline: orchestrator → discovery → scrape → parse → best estimate, with registry updates at each step | 4 | End-to-end autonomous pipeline for targeted scraping |
| Build `ua-run-orchestrator` CLI: runs one orchestrator cycle, prints task queue, optionally executes top N tasks | 3 | Manual trigger for orchestrator (precursor to cron) |
| Test: run orchestrator against VA priority tier 1 PWSIDs. Verify it discovers URLs, scrapes, parses, and updates best estimate for 5 previously-uncovered utilities | 4 | Proof that the pipeline works end-to-end without human URL curation |

**Sprint 13 output**: `ua-run-orchestrator --execute 10` autonomously finds, scrapes, parses, and stores rate data for 10 high-priority utilities. The scrape registry records every step. The coverage table updates. No YAML files edited. No Claude Code session required.

---

### Sprint 14 (Weeks 9–10): Automation + Scheduling

> **Goal**: The pipeline runs on its own. Cron-driven, monitored, alerting.

| Task | Hours | Deliverable |
|------|-------|-------------|
| Set up cron schedule: orchestrator runs daily (task queue generation), bulk source check runs weekly, change detection runs weekly | 4 | `crontab` entries or systemd timers |
| Implement change detection: scrape agent re-fetches URLs in `scrape_registry` with `status = active`, compares content hash, flags `content_changed = true` | 4 | Automatic detection of rate page updates |
| Wire change detection → re-parse: when content changes, parse agent re-processes and best estimate agent re-evaluates | 3 | Rates auto-update when utilities publish changes |
| Build health monitoring: after each cron cycle, log to `ingest_log`, check for errors, optionally send alert (email/webhook) | 3 | Failed runs don't go unnoticed |
| Implement rate limiting and backoff in scrape agent: respect `retry_after`, exponential backoff for 403s, dead-letter after N failures | 3 | Polite scraping, no wasted cycles on dead URLs |
| Implement Batch API routing for parse agent: when orchestrator generates >20 parse tasks, submit as Claude Batch instead of individual calls | 3 | Cost-efficient bulk parsing |
| Run unattended for 1 week. Monitor logs. Fix issues. | 6 | Pipeline survives 7 days without human intervention |
| Build `ua-pipeline-health` CLI: last run times, error counts, coverage delta since last run | 2 | Operator dashboard |

**Sprint 14 output**: The pipeline runs daily without human intervention. Change detection catches utility rate page updates. Batch API keeps costs minimal. Health monitoring alerts on failures.

---

### Sprint 15 (Weeks 11–12): API Productization

> **Goal**: API is production-ready for internal consumption and pilot external users.

| Task | Hours | Deliverable |
|------|-------|-------------|
| Auth: API key management, rate limiting (per-key), usage metering (per-endpoint, per-key) | 5 | Auth infrastructure |
| Update all endpoints to read from `rate_schedules` / `rate_best_estimate` (canonical schema) | 4 | API serves from single source of truth |
| Add `/bulk-download?state=XX&format=csv` endpoint: exports `rate_best_estimate` joined with CWS metadata | 4 | Dataset product delivery mechanism |
| OpenAPI spec + auto-generated docs | 3 | Published API documentation |
| MCP server: wrap `/resolve` and `/utility` as MCP tools | 4 | Agent-native distribution channel |
| Provenance endpoint: `/utility/{pwsid}/provenance` returns source chain, scrape history, confidence rationale | 3 | Consultant-grade citations |
| Redis caching for `/resolve` with geohash keys | 3 | <100ms cached responses |
| Pricing middleware: track billable calls per API key, enforce tier limits | 2 | Billing infrastructure ready |

**Sprint 15 output**: Production API with auth, docs, bulk download, MCP server, caching, and billing. Ready for pilot customers.

---

### Sprint 16 (Weeks 13–14): Expansion + Coverage Push

> **Goal**: Leverage the machine to rapidly expand coverage. First sprint where the architecture pays off.

| Task | Hours | Deliverable |
|------|-------|-------------|
| Catalog new bulk sources: research EFC dashboards for remaining states (WV, FL, GA, TX, etc.), add to `source_catalog` with `next_check_date` | 4 | Source catalog expanded |
| Ingest any EFC states with CSV downloads (reuse NC ingest pattern, parameterized by state) | 6 | Multiple new states with bulk rate data |
| Run orchestrator with increased budget: `ua-run-orchestrator --execute 100` against priority tier 2 (DC states) | 2 | 100 new PWSIDs scraped autonomously |
| Run `ua-coverage-report` and compare to Sprint 10 baseline | 1 | Measurable coverage growth |
| Quality audit: sample 50 newly-parsed rates, verify against source URLs | 4 | Accuracy metrics for the autonomous pipeline |
| Onboard 2–3 pilot users: issue API keys, share docs, collect feedback | 3 | Real users on real data |
| Write `docs/runbook.md`: how to add a new state, how to add a new bulk source, how to investigate a failed scrape, how to trigger a re-parse | 3 | Operational documentation |

**Sprint 16 output**: Coverage jumps significantly. Multiple new states. 100+ new PWSIDs from autonomous scraping. Pilot customers live. The machine is working.

---

## Part 7: What the Codebase Looks Like After

```
uapi/
├── agents/
│   ├── base.py                    # BaseAgent ABC
│   ├── orchestrator.py            # OrchestratorAgent
│   ├── bulk_ingest.py             # BulkIngestAgent
│   ├── discovery.py               # DiscoveryAgent
│   ├── scrape.py                  # ScrapeAgent
│   ├── parse.py                   # ParseAgent
│   └── best_estimate.py           # BestEstimateAgent
├── ingest/
│   ├── cws.py                     # EPA CWS boundaries
│   ├── sdwis.py                   # ECHO SDWIS (all 50 states)
│   ├── aqueduct.py                # WRI Aqueduct 4.0
│   ├── tiger_county.py            # Census TIGER
│   ├── mdwd.py                    # Harvard Dataverse
│   ├── va_deq.py                  # VA DEQ permits
│   ├── ca_ewrims.py               # CA water rights
│   ├── ear_ingest.py              # CA eAR rates
│   ├── owrs_ingest.py             # CA OWRS rates
│   ├── efc_ingest.py              # EFC rates (parameterized by state)
│   └── scraped_rates.py           # LLM-scraped rates (wraps scrape+parse agents)
├── api/
│   ├── app.py                     # FastAPI app
│   ├── routes/
│   │   ├── resolve.py             # /resolve
│   │   ├── utility.py             # /utility/{pwsid}
│   │   ├── permits.py             # /permits
│   │   ├── rates.py               # /rates, /rates/best-estimate
│   │   ├── bulk_download.py       # /bulk-download
│   │   ├── provenance.py          # /utility/{pwsid}/provenance
│   │   └── health.py              # /health, /pipeline-health
│   ├── auth.py                    # API key management + rate limiting
│   └── mcp_server.py              # MCP tool definitions
├── config/
│   ├── source_priority.yaml       # Per-state merge hierarchy
│   ├── source_seed.yaml           # Initial source_catalog entries
│   └── agent_config.yaml          # Orchestrator thresholds, cron schedule, batch limits
├── migrations/
│   └── versions/                  # Alembic migrations
├── scripts/
│   ├── seed_source_catalog.py     # source_seed.yaml → source_catalog table
│   ├── populate_pwsid_coverage.py # Build coverage from existing data
│   └── migrate_water_rates.py     # water_rates → rate_schedules transform
├── cli.py                         # ua-ingest, ua-coverage-report, ua-scrape-status,
│                                  # ua-run-orchestrator, ua-pipeline-health
└── docs/
    ├── architecture.md            # This document
    ├── runbook.md                 # Operational procedures
    ├── api_spec.yaml              # OpenAPI spec
    └── rate_data_strategy.md      # Updated acquisition strategy
```

---

## Part 8: Key Decision Log

| Decision | Rationale | Reversible? |
|----------|-----------|-------------|
| JSONB for rate tiers in `rate_schedules` | Real-world tier counts range from 1 to 7+. Fixed columns lose data. JSONB queries are fast enough with GIN indexes. | Yes — can add fixed columns as computed/materialized if query patterns demand it |
| Dual-write to `water_rates` (legacy) + `rate_schedules` (canonical) during transition | No disruption to existing queries. Can drop `water_rates` once all consumers migrated. | Yes — remove dual-write when ready |
| Source priority via YAML config, not database column | Priority logic changes with new source discovery. YAML is version-controlled and diff-able. `source_catalog.priority_rank` provides a queryable fallback. | Yes — can move entirely to DB if config becomes unwieldy |
| Agents are stateless, all state in DB | Agents can crash, restart, run in parallel without coordination. The database is the single source of truth. | Hard to reverse, but this is the right default |
| Scrape registry is one table, not normalized (attempts/fetches/parses) | Simpler. One row per (PWSID, URL). The `last_*` columns track most-recent state. If full history needed, add an `attempts` child table later. | Yes — add child table if audit trail becomes important |
| Best estimate uses margin check for source replacement | Prevents a parsing error from silently overwriting good data. A scraped rate that's 50% different from state-reported data gets flagged, not auto-promoted. Configurable threshold per state. | Yes — threshold is config-driven |
| SDWIS expansion before new rate acquisition | `/resolve` returning incomplete records for 93% of systems is a worse user experience than having 1.9% rate coverage. SDWIS makes the spatial backbone useful even without rates. | N/A — already decided |

---

## Part 9: What This Enables

After Sprint 16, the system:

- **Resolves any US address** to its water utility, SDWIS metadata, water stress risk, and best-estimate rates (where available)
- **Grows coverage autonomously**: the orchestrator identifies gaps, dispatches discovery and scraping agents, and integrates results without human intervention
- **Integrates bulk sources efficiently**: when a new EFC dashboard or state water board dataset appears, adding it is a config change + ingest module, not a redesign
- **Maintains freshness automatically**: change detection catches utility website updates, re-parses trigger best-estimate refresh
- **Serves two products from one asset**: the API (Product B) and the downloadable dataset (Product A) are both views on the same canonical data store
- **Supports the primary platform**: your DC project calls `/resolve` and gets utility context; the enrichment layer is an independent service consumed via HTTP
- **Has a path to revenue**: API keys, usage metering, and bulk download endpoints are ready for pilot customers

The architecture phase (Sprints 10–14) builds the machine. The execution phase (Sprint 15+) feeds it data and ships the product. The critical discipline is: **do not skip the architecture to chase more data.** The data acquisition methods are proven. The machine to run them at scale is what's missing.
