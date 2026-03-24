# UAPI — Progress Review & Infrastructure Architecture

**Date**: 2026-03-24
**Context**: Sprint 9 mid-session strategic pause. NC EFC ingest complete. IA tabled (no bulk download). OR LOC deferred. Decision to prioritize infrastructure over additional data acquisition.

---

## Part 1: Sprint History & What Has Been Built

### Sprint 1 — Foundation (Session 1–2)

**Purpose**: Scaffold the project and load the core geographic + financial layers.

**Delivered**:
- PostGIS `utility` schema (shared DB with strong-strategic)
- **EPA CWS boundaries**: 44,643 community water system service area polygons — this is the spatial backbone. Every other layer joins to PWSIDs resolved through these polygons.
- **WRI Aqueduct 4.0**: 68,506 watershed-level water stress polygons
- **SDWIS system data**: 3,711 systems (VA + CA only) — source water type, owner type, population, violations
- **MDWD financial data**: 225 utilities (VA + CA) — Harvard Dataverse Census of Governments water utility revenue/expenditure/debt
- **Census TIGER county boundaries**: 3,235 polygons — used for spatial join to fill county on all CWS records (100% coverage)
- **`/resolve` endpoint**: Given lat/lng → returns PWSID, CWS metadata, SDWIS info, Aqueduct water stress, MDWD financials
- **`/health` endpoint**: Pipeline run timestamps and row counts per table
- **Alembic migrations 001–003**
- 8 ingest modules: `cws`, `sdwis`, `mdwd`, `aqueduct`, `tiger_county` + CLI (`ua-ingest all`)

**Key limitation exposed**: SDWIS only loaded for VA + CA. The 44,643 CWS boundaries cover all 50 states, but SDWIS metadata only resolves for 2 states. This means `/resolve` returns incomplete records for 42,000+ systems.

---

### Sprint 2 — Permits (Session 3)

**Purpose**: Load state-level permit data and cross-reference with data center facilities.

**Delivered**:
- **VA DEQ permits**: 16,519 records across 3 layers (VWP Individual, VWP General, VPDES Outfalls) from EDMA MapServer API
- **CA eWRIMS water rights**: 45,011 records from SWRCB CKAN API with volume normalization (7 unit types → GPD)
- **`utility.permits` table** with two-tier categorization (`source_category` raw + `category_group` normalized)
- **`utility.permit_facility_xref`**: 30 matched VA data center permits + 11 unproven candidates
- **`/permits` endpoint**: Spatial radius query with category/source filters
- **`/facility/{id}/permits` endpoint**: Linked + nearby permits for SS facilities
- Category mapping config: `config/category_mapping.yaml`
- Alembic migrations 004–006

**Key findings**:
- 41 VA DEQ permits tagged "Data Center" — 30 matched existing SS facilities, 11 are new candidates
- VA DEQ has NO volume/quantity data in GIS layers (admin/spatial only)
- CA eWRIMS face values can be enormous (State Water Project = 9.1M AFY) — these are aggregate permitted volumes, not facility withdrawals

---

### Sprint 3 — LLM Rate Scraping Pipeline (Sessions 4–5)

**Purpose**: Build the web scraping + Claude API pipeline for extracting structured water rate data from utility websites.

**Delivered across 3 sub-versions (v0, v1, v2)**:
- **Rate pipeline modules**: `rate_discovery.py` (SearXNG/DDG web search), `rate_scraper.py` (HTTP + Playwright + PDF via pymupdf), `rate_parser.py` (Claude Sonnet structured extraction), `rate_calculator.py` (tier → bill calculation), `rates.py` (orchestrator)
- **SearXNG self-hosted meta-search**: Docker container aggregating Google/DDG/Bing/Brave at `localhost:8888`
- **Curated URL files**: `config/rate_urls_va.yaml` (26 URLs), `config/rate_urls_ca.yaml`
- **CivicPlus DocumentCenter crawler**: `civicplus_crawler.py` — search-based approach with Playwright rendering + relevance scoring
- **`utility.water_rates` table** (migration 007): fixed charge + 4 volumetric tiers + bill snapshots + provenance
- **`/rates/{pwsid}` and `/rates?state=VA` endpoints**
- API cost tracking (`--max-cost` flag)

**Results**:
- **VA**: 22/31 utilities parsed (90% population coverage). 9 tabled (CivicPlus 403s, PDF-only, no web presence)
- **CA**: 5 utilities parsed from web scraping (EBMUD, San Diego, LADWP partial, Huntington Beach, Modesto)
- **Total API cost**: $0.36 across all scraping rounds
- **Total scraped_llm records in DB**: 101 (including failed/partial)

**Key findings**:
- PDF extraction is the most reliable path (near-100% success when you have the right PDF)
- CivicPlus CMS serves ~14% of utilities — now handled after bug fix
- SearXNG rate-limits after ~60 queries per session
- Sonnet extraction quality is high — correctly handles multi-district tariffs, unit conversions, complex tier structures
- **Search keyword optimization is critical**: generic "city state water rates" misses; need authority-specific names

**Known data quality issues**:
- Colonial Heights VA: tier limits wrong (unit conversion error, unfixed)
- Manassas Park VA: combined water+sewer charges, not water-only (unfixed)
- Suffolk VA: $130 at 10 CCF — high but parsing appears correct
- LADWP: seasonal/budget-based, only partial parse

---

### Sprint 4 — Bulk Data: eAR + CivicPlus (Session 6)

**Purpose**: Shift from per-utility scraping to bulk government data sources.

**Delivered**:
- **SWRCB eAR ingest** (`ear_ingest.py`): 3 years of CA state-reported rate data from HydroShare-processed Electronic Annual Reports
  - 2020: 194 records (tier data, no bill columns)
  - 2021: 193 records (tier data + bills)
  - 2022: 194 records (tier data + bills)
  - Dynamic column mapping (eAR schema changes between years: 1314 → 2315 → 2978 columns)
  - Billing frequency normalization (bimonthly/quarterly → monthly)
- **Schema evolution** (migration 008): `source` column, `bill_6ccf/9ccf/12ccf/24ccf` columns, unique constraint updated
- **CivicPlus DocumentCenter crawler**: tested on 3 sites, found rate documents via search queries + relevance scoring

**Key insight**: One bulk ingest (eAR) covered all 194 CA MDWD utilities in a single pipeline run. This is 100x more efficient than per-utility web scraping. **Bulk government data is the highest-ROI acquisition path.**

---

### Sprint 5 — OWRS Ingest + Reconciliation (Session 7)

**Purpose**: Add a third CA rate source (OWRS) and build cross-source reconciliation.

**Delivered**:
- **OWRS ingest** (`owrs_ingest.py`): 387 records from CA Data Collaborative's Open Water Rate Specification
  - Pre-curated YAML rate structures from utility OWRS filings
  - 229 net-new PWSIDs not covered by eAR or scraping
  - Handles tiered (291), uniform (109), budget-based (16) structures
  - Unit conversion for kgal-reporting utilities
- **Reconciliation diagnostic** (`scripts/reconcile_rates.py`): cross-source variance analysis across 190 multi-source utilities
  - 38% agree (<10% CV), 32% moderate, 15% divergent, 7% major
  - 157 records flagged for quality issues

---

### Sprint 6 — Data Quality Fixes + Best Estimate (Session 7 cont.)

**Purpose**: Fix systematic data quality issues and build the source priority system.

**Delivered**:
- **eAR tier limit inflation fix**: 97 records across 61 PWSIDs had tier limits in gallons instead of CCF (748x–5110x inflation). Fixed by NULLing inflated tiers, preserving state-reported bills where valid.
- **Combined water+sewer investigation**: 7 scraped records re-parsed with water-only prompt. Result: NOT combined — high CA prices (2024–2026 vintage) explain divergence from eAR/OWRS (2017–2022).
- **`utility.rate_best_estimate` table**: 443 PWSIDs with source-priority selection
  - Priority: eAR 2022 > eAR 2021 > scraped (if agrees with anchor) > OWRS > scraped (diverges) > eAR 2020
  - Confidence: high=184 (42%), medium=252 (57%), none=7 (2%)
  - Script: `scripts/build_best_estimate.py`

**Key limitation**: best_estimate is CA-only. The reconciliation and priority logic is hardcoded for CA's multi-source situation. NC and VA records don't flow into it.

---

### Sprint 7 — API + Cross-Year Analysis (Session 7 cont.)

**Purpose**: Expose best-estimate data through API and analyze rate trends.

**Delivered**:
- **`GET /rates/best-estimate?state=CA`** endpoint
- **`/resolve` updated** with best_estimate_bill_10ccf, rate_source, rate_confidence
- **Cross-year eAR rate change analysis**: median +1.0%/yr, mean +3.5%/yr (2021→2022, 173 clean utilities)

---

### Sprint 8 — State Expansion Research (Session 7 cont.)

**Purpose**: Identify next data sources for state expansion.

**Delivered**: Research only (no code). Identified:
- UNC EFC NC dashboard (high priority — free CSV, 498 utilities)
- UNC EFC IA dashboard (high priority — free, but no bulk download)
- OR League of Cities 2023 CSV (medium — 71 cities, free-text bills)
- AWWA Rate Survey (low — paywall)
- TX TWDB, AZ ADWR (low — no rate data found)

---

### Sprint 9 — NC EFC Ingest (Current Session)

**Purpose**: First state expansion beyond VA/CA.

**Delivered**:
- **NC EFC ingest** (`efc_nc_ingest.py`): 403 NC utility records from UNC EFC 2025 CSV
  - Novel tier extraction algorithm: reverse-engineers tier breakpoints from pre-computed bill curves at 500-gallon increments
  - Handles allowances, billing period normalization, >4 tier collapse
  - Flags duplicate PWSIDs (2 cases: different utilities sharing a PWSID)
  - CLI: `ua-ingest efc-nc [--dry-run]`
- **IA tabled**: No bulk download exists. Scrape approach documented in memory (690 utilities accessible via per-utility HTML endpoint, no PWSIDs).

**Results**: 403 records, median bill $55/mo at 10 CCF, 400/403 high confidence.

---

## Part 2: Current Database State

### Tables

| Table | Rows | Source | Notes |
|-------|------|--------|-------|
| `cws_boundaries` | 44,643 | EPA CWS | All 50 states, the spatial backbone |
| `aqueduct_polygons` | 68,506 | WRI Aqueduct 4.0 | Watershed-level water stress |
| `sdwis_systems` | 3,711 | EPA ECHO | **VA + CA only** — missing 48 states |
| `mdwd_financials` | 225 | Harvard Dataverse | VA + CA only — Census of Governments fiscal data |
| `county_boundaries` | 3,235 | Census TIGER | Used for spatial joins, 100% CWS county coverage |
| `permits` | 61,530 | VA DEQ (16,519) + CA eWRIMS (45,011) | 2 states only |
| `permit_facility_xref` | 41 | Cross-reference | 30 matched + 11 DC candidates |
| `water_rates` | 1,472 | 6 sources (see below) | 3 states: CA, NC, VA |
| `rate_best_estimate` | 443 | Derived | **CA only** — not generalized |
| `pipeline_runs` | 35 | Audit trail | Pipeline execution log |

### Water Rates by Source

| Source | Records | Unique PWSIDs | State | Vintage | Tier data? |
|--------|---------|---------------|-------|---------|------------|
| `efc_nc_2025` | 403 | 403 | NC | Jul 2024 | Yes (back-calculated) |
| `owrs` | 387 | 381 | CA | 2002–2021 (median ~2017) | Yes (curated YAML) |
| `swrcb_ear_2022` | 194 | 194 | CA | 2022 | Partial (97 NULLed after inflation fix) |
| `swrcb_ear_2021` | 193 | 193 | CA | 2021 | Partial |
| `swrcb_ear_2020` | 194 | 194 | CA | 2020 | Partial |
| `scraped_llm` | 101 | 97 | VA+CA | 2014–2026 | Yes (LLM-parsed) |

### Rate Coverage Against CWS Systems

| State | CWS Systems | With Rates | Coverage |
|-------|------------|------------|----------|
| NC | 1,842 | 403 | 21.9% |
| CA | 2,801 | 415 | 14.8% |
| VA | 910 | 28 | 3.1% |
| TX | 4,584 | 0 | 0% |
| WA | 2,125 | 0 | 0% |
| PA | 1,854 | 0 | 0% |
| NY | 1,791 | 0 | 0% |
| IL | 1,632 | 0 | 0% |
| FL | 1,386 | 0 | 0% |
| GA | 1,396 | 0 | 0% |
| IA | 980 | 0 | 0% |
| OR | 747 | 0 | 0% |
| **All 50** | **44,643** | **846** | **1.9%** |

---

## Part 3: Current API Surface

| Endpoint | Purpose | State coverage |
|----------|---------|----------------|
| `GET /resolve?lat=X&lng=Y` | Spatial lookup → PWSID + CWS + SDWIS + MDWD + Aqueduct + best-estimate rate | CWS: all 50 states. SDWIS: VA+CA. Rates: CA only (best-estimate). |
| `GET /permits?lat=X&lng=Y&radius_km=10` | Permits within radius | VA + CA only |
| `GET /facility/{id}/permits` | Linked + nearby permits for SS facility | VA only (xref) |
| `GET /rates/{pwsid}` | Full rate detail with tiers and provenance | CA + NC + VA |
| `GET /rates?state=XX` | All rates for a state | CA, NC, VA |
| `GET /rates/best-estimate?state=CA` | Source-prioritized best estimate | **CA only** |
| `GET /health` | Pipeline run timestamps and row counts | All tables |

---

## Part 4: Current Infrastructure Components

### Ingest Modules (17 files)

| Module | Type | Purpose |
|--------|------|---------|
| `cws.py` | Bulk API | EPA CWS service area boundaries |
| `sdwis.py` | Bulk API | EPA ECHO SDWIS system metadata |
| `mdwd.py` | Bulk file | Harvard Dataverse fiscal data |
| `aqueduct.py` | Bulk file | WRI Aqueduct 4.0 GDB |
| `tiger_county.py` | Bulk API | Census TIGER county boundaries |
| `va_deq.py` | Bulk API | VA DEQ MapServer permit layers |
| `ca_ewrims.py` | Bulk API | CA SWRCB CKAN water rights |
| `ear_ingest.py` | Bulk file | CA eAR HydroShare Excel → water_rates |
| `owrs_ingest.py` | Bulk file | CA OWRS CSV → water_rates |
| `efc_nc_ingest.py` | Bulk file | NC EFC CSV → water_rates (with tier extraction) |
| `rate_discovery.py` | Web search | SearXNG/DDG URL discovery |
| `rate_scraper.py` | Web scrape | HTTP + Playwright + PDF text extraction |
| `rate_parser.py` | LLM parse | Claude API structured rate extraction |
| `rate_calculator.py` | Calculation | Tier structure → bill amounts |
| `rates.py` | Orchestrator | End-to-end discover → scrape → parse → store |
| `civicplus_crawler.py` | Web crawl | CivicPlus DocumentCenter search + scoring |

### Scripts (10 files)

| Script | Purpose |
|--------|---------|
| `build_best_estimate.py` | Source-priority selection → rate_best_estimate table |
| `reconcile_rates.py` | Cross-source variance analysis |
| `fix_ear_tier_inflation.py` | One-time fix for eAR tier limit 1000x inflation |
| `reparse_combined_rates.py` | Re-parse suspected combined water+sewer scrapes |
| `analyze_ear_rate_changes.py` | Cross-year eAR rate trend analysis |
| `populate_permit_xref.py` | Build permit ↔ SS facility cross-reference |
| `batch_discover_va_urls.py` | Batch SearXNG discovery for VA utilities |
| `batch_discover_ca_urls.py` | Batch SearXNG discovery for CA utilities |
| `standalone_discover.py` | Standalone URL discovery (no API calls) |
| `validate_addresses.py` | Address validation utility |

### Config Files

| File | Purpose |
|------|---------|
| `config/rate_urls_va.yaml` | 26 curated VA utility rate page URLs |
| `config/rate_urls_ca.yaml` | Curated CA utility rate page URLs |
| `config/rate_urls_ca_discovered.yaml` | SearXNG-discovered CA URLs (unverified) |
| `config/rate_urls_va_candidates.yaml` | CivicPlus-discovered VA URLs (unverified) |
| `config/category_mapping.yaml` | Permit source_category → category_group mapping |
| `config/sources.yaml` | Data source metadata (URLs, DOIs, formats) |

### External Dependencies

| Component | Purpose | Location |
|-----------|---------|----------|
| SearXNG | Self-hosted meta-search | Docker at `~/searxng/`, JSON API at `localhost:8888` |
| Playwright | Headless browser for JS-rendered pages | pip dependency, auto-installs Chromium |
| Claude API (Sonnet) | Structured rate extraction from page text | `ANTHROPIC_API_KEY` in `.env` |
| PostgreSQL/PostGIS | Spatial database | Shared with strong-strategic |

---

## Part 5: What's Missing — The Three Infrastructure Layers

The proof of concept is built. The data acquisition works at multiple tiers (bulk government, curated survey, LLM scraping). But the system has no memory, no coordination, and no strategic awareness. It's a collection of one-shot ingest scripts, not a machine.

### Layer A — Scrape Registry (Tactical: per-URL, per-attempt)

**What it does**: Tracks every interaction with an external URL — searches, fetches, parse attempts, outcomes. This is the "chip tracker" equivalent for the scraping pipeline.

**What exists today**:
- `pipeline_runs` table: logs when an ingest script ran and how many rows it produced. No per-URL granularity.
- `water_rates.source_url`: stores the URL a record was scraped from. But only for successful parses. Failed attempts, 403s, timeouts, empty pages — all lost.
- `water_rates.raw_text_hash`: SHA-256 of scraped text, intended for change detection. Populated for scraped_llm records only. Never actually used for change detection.
- `config/rate_urls_*.yaml`: curated URL files. These are flat YAML, not queryable, don't track failures, and are manually maintained.

**What's missing**:
- **Search attempt log**: Which utility × query combinations have been tried? What URLs were returned? Which were dead ends? Currently, if SearXNG returns 5 URLs for a utility and 4 are irrelevant, we retry the same 5 next session.
- **Scrape attempt log**: URL → HTTP status, content type, text length, timestamp, retry eligibility. A 403 today might succeed tomorrow (rate limiting). A timeout might be a transient server issue. A 200 with empty content is a different failure than a 404.
- **Parse attempt log**: Text → Claude API call → structured output. Was the parse successful? What confidence? What model? How much did it cost? Did we retry with a different prompt?
- **Retry scheduling**: Which failed URLs should be retried? When? What's the backoff strategy? Currently: no backoff, no retry, no memory.
- **Change detection**: When was a URL last checked? Has the content changed (hash comparison)? Should we re-parse? Currently: `raw_text_hash` exists but is never compared against.

**Proposed structure** (conceptual):

```
scrape_registry
├── id
├── pwsid (FK → cws_boundaries, nullable for discovery-phase entries)
├── url
├── url_source (searxng | curated | civicplus_crawler | manual)
├── discovery_query (the search query that found this URL)
├── content_type (html | pdf | xlsx | unknown)
│
├── last_fetch_at (timestamp)
├── last_http_status (200 | 403 | 404 | 500 | timeout)
├── last_content_hash (SHA-256 for change detection)
├── last_content_length (bytes)
│
├── last_parse_at (timestamp)
├── last_parse_result (success | failed | partial | skipped)
├── last_parse_confidence (high | medium | low)
├── last_parse_cost_usd (API cost for this parse)
│
├── status (active | dead | blocked | stale | pending_retry)
├── retry_after (timestamp — when to try again)
├── retry_count (how many times we've retried)
├── notes (free text — why it failed, what we tried)
│
├── created_at
├── updated_at
```

This table would be the authoritative record of "what have we tried, what worked, what didn't." Every scraping session starts by querying this table to decide what to attempt.

---

### Layer B — Data Operations Manager (Operational: per-PWSID, per-source)

**What it does**: The command-and-control layer. Tracks which data sources exist for each state, which PWSIDs have been covered by which sources, what's fresh vs. stale, and what should be prioritized next.

**What exists today**:
- `water_rates` table: stores the actual rate records. You can query it to see which PWSIDs have data, but it doesn't tell you which PWSIDs were *attempted and failed*, or which PWSIDs haven't been attempted at all.
- `rate_best_estimate` table: source-priority selection, but CA-only.
- `config/sources.yaml`: static metadata about data sources (URLs, DOIs). Not queryable, not stateful.
- `docs/rate_data_strategy.md`: frozen at Session 5, describes Layer 1–4 acquisition strategy but doesn't track execution.
- `docs/next_steps.md`: running log of what was done and what's pending. Not machine-readable.

**What's missing**:
- **Source catalog**: A structured registry of all known bulk data sources, their coverage (which states, which PWSIDs), their vintage, their refresh cadence, and their ingest status. Example: "eAR covers CA, 7,228 systems, 2020–2022 vintages ingested, 2023 not yet available, check HydroShare quarterly."
- **PWSID-level status**: For each of the 44,643 CWS systems, what's the state of our knowledge?
  - Has rate data from N sources (list them)
  - Has been attempted via scraping but failed (link to scrape_registry)
  - Has never been attempted
  - Has stale data (>2 years old, needs refresh)
  - Is a priority target (near a data center)
- **Pipeline health dashboard**: Which ingest endpoints are working? Which bulk data sources have new vintages available? Which scraping targets are blocked?
- **Priority queue**: Not all PWSIDs are equal. The product vision says "coverage grows over time" — but which PWSIDs should we cover first?
  - Tier 1: Utilities serving data center facilities (from SS facility database)
  - Tier 2: Utilities in states with significant data center presence (VA, TX, OR, GA, etc.)
  - Tier 3: Large population utilities (>100K served)
  - Tier 4: Everything else
- **Generalized best-estimate logic**: Currently hardcoded for CA. Needs to handle any state with any combination of sources. The source priority ranking should be configurable, not buried in a script.

**Proposed structure** (conceptual):

```
source_catalog
├── source_key (e.g., swrcb_ear_2022, efc_nc_2025, scraped_llm)
├── source_type (bulk_government | bulk_survey | scraped | curated)
├── states_covered (array)
├── pwsid_count (how many PWSIDs this source covers)
├── vintage_start, vintage_end
├── refresh_cadence (annual | semi-annual | one-time | continuous)
├── last_ingested_at
├── next_check_date
├── ingest_module (Python module path)
├── notes

pwsid_coverage
├── pwsid (FK → cws_boundaries)
├── has_rate_data (boolean — does water_rates have at least one record?)
├── rate_sources (array — which sources contribute data)
├── best_estimate_source (which source was selected)
├── best_estimate_confidence
├── best_estimate_bill_10ccf
├── scrape_status (not_attempted | attempted_failed | succeeded | stale)
├── priority_tier (1–4, based on DC proximity)
├── last_updated
```

This layer answers questions like:
- "Which VA utilities near data centers still have no rate data?"
- "What percentage of CA systems have been refreshed in the last 12 months?"
- "Which bulk sources should be re-checked for new vintages?"
- "What's the next highest-priority batch of utilities to scrape?"

---

### Layer C — Coverage Dashboard (Strategic: per-state, aggregate)

**What it does**: Strategic visibility into coverage progress, gaps, and prioritization. This is the view a product owner uses to decide where to invest acquisition effort.

**What exists today**:
- Ad-hoc SQL queries (like the ones run earlier in this session)
- `docs/next_steps.md` has some manual coverage notes
- No visualization, no automated reporting

**What's missing**:
- **State-level coverage summary**: For each of the 50 states, how many CWS systems exist, how many have rate data, from which sources, and what's the population coverage percentage?
- **Data center overlay**: Which states have the most data centers (from SS facility database)? How does rate coverage in those states compare?
- **Source availability map**: Which states have known bulk data sources (EFC dashboards, state water boards, etc.) vs. which require per-utility scraping?
- **Freshness report**: What percentage of rate records are <1 year old, 1–2 years, 2–5 years, >5 years? By state?
- **Progress tracking**: How has coverage grown over time? Sprint-over-sprint metrics.

This layer could be:
- A materialized view or set of summary tables that update after each pipeline run
- A simple CLI command (`ua-coverage-report`) that prints the dashboard to console
- Eventually: an actual web dashboard (but that's premature)

**The strategic view drives acquisition priorities**:
- State X has 500 data center-adjacent utilities and 0% rate coverage → highest priority
- State Y has an EFC dashboard but hasn't been ingested yet → medium priority, bulk ROI
- State Z has 95% coverage from bulk sources → low priority, only gaps need filling

---

## Part 6: Other Infrastructure Gaps

Beyond the three layers above, several foundational pieces need attention:

### SDWIS Expansion (48 missing states)
The EPA ECHO API supports all 50 states. We loaded VA + CA in Sprint 1 and never expanded. The `/resolve` endpoint returns incomplete records for 93% of CWS systems. This is a straightforward bulk API ingest — same module, just more states.

### Best-Estimate Generalization
`scripts/build_best_estimate.py` is hardcoded for CA's multi-source reconciliation. NC now has single-source data (EFC only) — simpler case but still needs to flow into a generalized best-estimate table. VA has 28 scraped records that should also be included.

### Schema Alignment with Vision
The product vision document describes a `rate_schedules` table with:
- JSONB for flexible tier storage (vs. our fixed 4-tier columns)
- `conservation_signal` (ratio of highest to lowest tier rate)
- `cost_at_20ccf` (we only have bill_5ccf and bill_10ccf)
- `next_scheduled_change` date
- `surcharges` JSONB array
- `needs_review` boolean flag

The actual schema was built sprint-by-sprint and diverges from the vision. Whether to migrate toward the vision schema or update the vision to reflect reality is a design decision.

### Rate Data Strategy Document
`docs/rate_data_strategy.md` is frozen at Session 5 (Sprint 3 era). It describes a 4-layer acquisition hierarchy that's still valid but needs updating to reflect:
- eAR, OWRS, and EFC bulk sources (completed)
- IA dashboard access limitations (discovered)
- The 3-layer infrastructure architecture (proposed)
- The priority queue concept (data center adjacency first)

### Change Detection Pipeline
The `raw_text_hash` column on `water_rates` was designed for change detection (re-scrape, compare hash, re-parse if changed). This has never been implemented. For the scraped_llm source, this would be the mechanism for keeping rates current without re-processing the entire pipeline.

---

## Part 7: Recommended Priorities

The principle: **build the machine, not force-push results into random tables.**

1. **Layer B first** — Data Operations Manager. This is the command-and-control layer. Without it, every acquisition session starts blind ("what do we have? what do we need? what should we try next?"). The source catalog and PWSID-level status views are the foundation for everything else.

2. **Layer A second** — Scrape Registry. Once we have the strategic view (Layer B), we need the tactical tracking (Layer A) before scaling scraping to new states. Every URL attempt should be recorded, every failure should be retrievable, every retry should be schedulable.

3. **SDWIS expansion** — Quick win, high impact. Load all 50 states of SDWIS data. This makes `/resolve` return complete records nationwide and populates Layer B's coverage views.

4. **Best-estimate generalization** — Extend to NC and VA. Make it source-aware (not CA-hardcoded).

5. **Layer C last** — Coverage Dashboard. This is a view layer on top of Layers A and B. Build it once the underlying data exists.

6. **Resume acquisition** — Only after Layers A+B are in place. Then: EFC states with CSV downloads, IA dashboard scrape, targeted scraping for DC-adjacent utilities.

---

## Appendix: Cumulative Costs

| Resource | Amount | Notes |
|----------|--------|-------|
| Claude API (Sonnet) | ~$0.40 total | All LLM rate parsing across all sessions |
| SearXNG | $0 | Self-hosted Docker container |
| Data sources | $0 | All public domain or free download |
| Hosting | $0 | Local PostgreSQL, no cloud infra |
