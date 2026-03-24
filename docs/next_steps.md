# Next Steps — Utility API

## Completed (Sprint 1 — Session 1)

- [x] Repo scaffolded at ~/projects/utility-api/
- [x] PostGIS `utility` schema created (shared DB with strong-strategic)
- [x] Aqueduct 4.0 polygons loaded (68,506 rows)
- [x] EPA CWS boundaries loaded (44,643 rows)
- [x] SDWIS system data loaded for VA + CA (3,711 rows)
- [x] MDWD financial data loaded for VA + CA (225 rows)
- [x] `/resolve` endpoint live and tested
- [x] Validation: 20/20 CWS match, 20/20 Aqueduct match

## Completed (Sprint 1 Cleanup — Session 2)

- [x] MDWD column mapping fix — `pct_below_poverty` (mapped to `POV_PCT`) and `water_utility_debt` (mapped to `Total_Debt_Outstanding`) now resolve correctly
- [x] MDWD financial columns renamed for clarity: `total_revenue` → `water_utility_revenue`, `total_expenditure` → `water_utility_expenditure`, `debt_outstanding` → `water_utility_debt` (Alembic migration 002)
- [x] MDWD ingest now maps water-utility-specific columns (`Water_Utility_Revenue`, `Water_Util_Total_Exp`) instead of general government financials
- [x] MDWD year-preference fix: prefers 2017 Census of Governments vintage (has both financials + demographics) over 2018 ACS-only vintage
- [x] County enrichment: `county_served` populated for 44,100/44,643 CWS boundaries (98.8%) from SDWA_GEOGRAPHIC_AREAS
- [x] `/health` endpoint now returns data vintage (last pipeline run timestamps, row counts per layer)
- [x] `/resolve` response expanded with `water_utility_revenue`, `water_utility_expenditure`, `water_utility_debt`, `mdwd_population`

## Data Quality Notes

- **Bill columns** (`avg_monthly_bill_5ccf`, `avg_monthly_bill_10ccf`) remain NULL — MDWD is a Census of Governments fiscal dataset, not a rate survey. Rate data is a Sprint 3 deliverable (LLM parsing from utility websites).
- **MDWD dual cadence**: Census of Governments financials publish every 5 years (2017 latest), ACS demographics are annual (2018 latest). Ingest prefers the vintage with financial data.
- **County gaps** (543 systems without county): Mostly tribal systems and independent cities (e.g., Richmond, VA) where SDWIS geographic areas don't list a county. Could be filled via spatial join to Census TIGER in a future pass.

## Completed (Sprint 1 Cleanup — Session 2, cont.)

- [x] Census TIGER county boundaries loaded (3,235 polygons) into `utility.county_boundaries`
- [x] Spatial join filled remaining 543 CWS boundaries missing county → 100% county coverage (44,643/44,643)
- [x] `ua-ingest tiger-county` CLI command added; included in `ua-ingest all` pipeline

## Completed (Sprint 2 — Session 3)

- [x] VA DEQ EDMA MapServer ingest — 3 layers:
  - VWP Individual Permits (Layer 192): 1,467 rows (excl. Residential) — includes 41 "Data Center" tagged permits
  - VWP General Permits (Layer 193): 4,387 rows (incl. Industrial, Commercial, Municipal, Mining, Agricultural, Other; excl. Linear Transportation)
  - VPDES Outfalls (Layer 119): 10,665 discharge permit outfalls
- [x] CA SWRCB eWRIMS ingest — data.ca.gov CKAN API:
  - Demand Analysis Flat File + Uses/Seasons table join: 45,011 water rights (targeted load, excl. Domestic)
  - Volume data preserved: face_value_amount (Acre-feet/Year), max_diversion_rate (various units)
  - Multi-use rights stored as JSON list in use_codes column
- [x] `utility.permits` table (Alembic migration 004) with:
  - `source_category`: as delivered by data provider
  - `category_group`: normalized bucket (industrial, energy, municipal, mining, commercial, institutional, environmental, agricultural, water_withdrawal, infrastructure, other)
  - Category mapping config: `config/category_mapping.yaml`
- [x] `/permits` endpoint: `GET /permits?lat=X&lng=Y&radius_km=10` with optional `category_group` and `source` filters
- [x] CLI commands: `ua-ingest va-deq`, `ua-ingest ca-ewrims`; added to `ua-ingest all` pipeline (steps 6–7)
- [x] Total permits loaded: 61,530 (VA: 16,519 + CA: 45,011)

### Sprint 2 Design Decisions

- **Two-tier category system**: `source_category` preserves the exact label from each data provider; `category_group` maps to our normalized buckets for cross-state filtering.
- **`water_withdrawal` is its own bucket** — VA DEQ VWP_ACTIVITY_TYPE "Water Withdrawal" is not assumed to be industrial until proven otherwise.
- **Targeted CA load**: Excluded "Domestic" USE_CODE. All other use types included.
- **Multi-use CA rights**: Stored as list in `use_codes` JSONB column. `category_group` assigned based on highest-priority use code (industrial > energy > municipal > mining > environmental > ...).
- **No volume data from VA DEQ**: GIS layers expose permit ID, facility name, county, activity type, status, and point geometry only. Volume data would require cross-referencing individual permit PDFs from DEQ CEDS.
- **CA volume data**: `face_value_amount` (always Acre-feet/Year or NULL) and `max_diversion_rate` (7 different unit types) normalized to `max_diversion_rate_gpd` (gallons per day).

### Sprint 2 Data Quality Notes

- **VA DEQ has no volume/quantity fields** in any GIS layer — just administrative/spatial data.
- **CA eWRIMS face values can be extremely large** (e.g., 9.1M AFY for State Water Project) — these are aggregate permitted volumes for large infrastructure, not individual facility withdrawals.
- **1,572 CA records lack geometry** (2.5%) — APPLICATION_NUMBERs with no lat/lng in the flat file.

## Completed (Sprint 2 Enrichment — Session 3, cont.)

- [x] CA county enrichment: 43,438 permits filled via spatial join to TIGER counties (1 with geom but outside county boundaries, 1,572 without geom)
- [x] VA VPDES county enrichment: 10,736 permits filled via spatial join
- [x] `permit_facility_xref` table (migration 005): cross-references DEQ data center permits with SS facilities
  - 30 matched (23 high confidence <1km, 5 medium 1-3km, 2 low 3-5km)
  - 11 flagged as `data_center_candidate` (unproven new locations >5km from any SS facility)
- [x] `max_diversion_rate_gpd` column (migration 006): normalized 24,156 CA records to gallons/day
  - CFS × 646,317 | GPM × 1,440 | GPD × 1 | AFY × 893 | AF × 325,851
  - NULL units assumed CFS (most common)
- [x] `/facility/{id}/permits` endpoint: returns linked permits (from xref) + nearby permits (spatial radius)
  - Tested: Microsoft Boydton → 4 linked DC permits + 64 nearby within 15km
- [x] `scripts/populate_permit_xref.py`: rerunnable xref population script (replaces temp file)

### Data Center Candidates (11 unproven locations)

| Permit | Name | County | Nearest SS Facility | Distance |
|--------|------|--------|---------------------|----------|
| 22-2715 | LYH03 Bailey Data Center | Mecklenburg | Microsoft Boydton | 13.4 km |
| 22-1758 | AVC17 Lakeside | Mecklenburg | Microsoft Boydton | 13.9 km |
| 25-1440 | Hanover Technology Park - Phase I | Hanover | Flexential Richmond | 15.3 km |
| 19-0029 | Chirisa Data Center | Chesterfield | Meta Henrico | 15.6 km |
| 19-1094 | American Tobacco - Data Center | Chesterfield | Meta Henrico | 16.3 km |
| 22-1432 | Hillcrest Site Data Center | Mecklenburg | Microsoft Boydton | 25.2 km |
| 22-0149 | Melrod | Stafford | CloudHQ MCC3 | 36.2 km |
| 23-2060 | Lake Anna Tech Campus | Louisa | Equinix CU2 | 47.5 km |
| 24-1857 | Cosner Tech Park | Spotsylvania | Equinix CU2 | 51.9 km |
| 24-2491 | Mattameade Data Center | Caroline | Flexential Richmond | 52.7 km |
| 24-2396 | Northeast Creek Technology Campus | Louisa | Equinix CU2 | 52.7 km |

## Completed (Sprint 3 v0 — Session 4)

- [x] `utility.water_rates` table (migration 007) — structured rate tier storage with provenance
  - Replaces never-populated `avg_monthly_bill` columns on `mdwd_financials` (dropped)
  - Schema: fixed charge + 4 volumetric tiers + computed bill snapshots + LLM parse metadata
- [x] Rate pipeline modules built:
  - `rate_discovery.py` — DuckDuckGo web search per utility name → rate page URL
  - `rate_scraper.py` — HTTP fetch + BeautifulSoup HTML text extraction
  - `rate_parser.py` — Claude API structured extraction (JSON output, Sonnet default)
  - `rate_calculator.py` — tier structure → bill calculation at any CCF level
  - `rates.py` — end-to-end orchestrator (discover → scrape → parse → calculate → store)
- [x] CLI command: `ua-ingest rates --state VA --limit 10` (with `--dry-run` mode)
- [x] Dependencies added: `anthropic>=0.40`, `beautifulsoup4>=4.12`
- [x] `/resolve` endpoint updated: `avg_monthly_bill` fields replaced with `has_rate_data` boolean

### Sprint 3 v0 Findings

- **Most VA/CA municipal sites are JS-rendered** (CivicPlus, Granicus CMS) — static HTTP scraping gets empty pages
- **403 blocks** common from municipal sites detecting bot User-Agent
- **PDF rate schedules** are the most common format (linked from CivicPlus pages)
- **Fairfax Water** (`fairfaxwater.org/rates`) is a working static HTML site — proved the scraping pipeline works (5,575 chars extracted with real rate data)
- **Search discovery works** — DuckDuckGo HTML search finds relevant rate pages/PDFs for most utilities
- **Key blocker**: ANTHROPIC_API_KEY needed to test Claude parsing end-to-end

### Sprint 3 v0 Design Decisions

- **`water_rates` as separate table** (not columns on `mdwd_financials`): rate data has its own provenance, tier structure, and vintage. MDWD is fiscal data; rates are a distinct dataset.
- **4 tiers max**: covers ~95% of US rate structures. Budget-based and seasonal flagged for review.
- **Residential only** for v0. Commercial/industrial rate classes are future expansion.
- **DuckDuckGo HTML** for URL discovery (no API key required). Query: `{utility_name} {county} {state} water rates`.
- **Sonnet** for extraction (fast, cheap, good at structured output). Model is configurable per call.

## Completed (Sprint 3 v1 — Session 4, cont.)

- [x] Playwright auto-fallback for JS-rendered pages (CivicPlus) and 403-blocked sites
- [x] PDF rate schedule extraction via pymupdf — handles multi-page tariff documents
- [x] SearXNG self-hosted meta-search (Docker: `~/searxng/`) replaces DuckDuckGo for URL discovery
  - Aggregates Google, DuckDuckGo, Bing, Brave — no single-engine rate limiting
  - JSON API at `http://localhost:8888/search?q=...&format=json`
  - Falls back to DuckDuckGo direct if SearXNG container is down
- [x] Curated URL file support: `--url-file config/rate_urls_va.yaml`
- [x] Claude API parsing verified end-to-end — penny-level accuracy on bill calculations
- [x] Fixed: unit conversion ($/1000gal → $/CCF), meter size coercion, PDF pipeline flow

### Sprint 3 v1 Verified Results (in DB)

| Utility | Structure | Fixed | Bill@5CCF | Bill@10CCF | Confidence |
|---------|-----------|-------|-----------|------------|------------|
| Blacksburg (VA) | tiered | $28.00 | $31.34 | $48.19 | high |
| Alexandria / VA-American Water | uniform | $15.00 | $28.03 | $41.06 | high |
| Arlington County | tiered | $6.03 | $23.59 | $44.64 | medium |

### Sprint 3 v1 Findings

- **SearXNG solved discovery**: 5/5 URLs found (vs 0/5 with rate-limited DuckDuckGo)
- **PDF extraction is the reliable path**: most utility rate data lives in PDFs, not HTML pages
- **CivicPlus remains problematic**: headless Playwright gets served wrong page content by CivicPlus CMS routing. Not solvable via scraping alone — need curated URLs or PDF links for CivicPlus utilities.
- **Claude Sonnet extraction quality is high**: correctly identifies rate structures, converts units, handles multi-district tariffs (VA-American Water 53-page PDF → correct Alexandria district rates)

## Completed (Sprint 3 v2 — Session 5)

- [x] **CivicPlus scraper bug fix**: `_clean_html_text()` matched `id="skipToContentLinks"` (20 chars) instead of actual page content. Fixed to select the largest matching element by text length. Unblocked 11 CivicPlus sites.
- [x] Batch URL discovery: SearXNG search + HTTP HEAD verification for all 28 uncurated VA utilities
- [x] Curated `config/rate_urls_va.yaml` with 26 verified URLs (PDF + HTML mix)
- [x] API cost tracking: `--max-cost` CLI flag, Sonnet pricing ($3/M in + $15/M out), pipeline stops at cap
- [x] Batch discovery script: `scripts/batch_discover_va_urls.py`
- [x] Hardened `.gitignore` for standalone repo isolation from strong-strategic
- [x] GitHub remote added: `git@github.com:strongca22-cpu/utility-api.git`

### Sprint 3 v2 Results (16/26 VA utilities parsed — 81% population coverage)

| Utility | Pop | Structure | Fixed/mo | Bill@5CCF | Bill@10CCF |
|---------|-----|-----------|----------|-----------|------------|
| Virginia Beach | 452,745 | uniform | $6.00 | $29.30 | $52.60 |
| Norfolk | 246,393 | flat | N/A | $32.55 | $65.10 |
| Chesapeake | 235,429 | tiered | $11.36 | $33.86 | $56.36 |
| Richmond | 220,289 | tiered | $17.66 | $36.81 | $69.96 |
| Newport News | 182,385 | tiered | N/A | $17.49 | $35.94 |
| Alexandria | 153,511 | uniform | $15.00 | $28.03 | $41.06 |
| Suffolk | 88,161 | uniform | $16.50 | $73.50 | $130.50 |
| Harrisonburg | 52,538 | tiered | $13.32 | $29.92 | $46.52 |
| Charlottesville | 46,597 | uniform | $10.00 | $38.26 | $66.52 |
| Blacksburg | 44,215 | tiered | $28.00 | $31.34 | $48.19 |
| Fredericksburg | 28,118 | uniform | $21.11 | $37.21 | $53.31 |
| Christiansburg | 21,943 | tiered | $11.00 | $51.40 | $88.80 |
| Colonial Heights | 17,820 | tiered | $6.57 | $69.47 | $132.37 |
| Manassas Park | 15,726 | tiered | $52.77 | $129.62 | $206.47 |
| Williamsburg/JCSA | 15,052 | tiered | $9.02 | $25.67 | $54.92 |
| Arlington | N/A | tiered | $6.03 | $23.59 | $44.64 |

**Total API cost**: $0.26 across 2 rounds (well under $4 cap)

### Sprint 3 v2 Findings

- **CivicPlus scraper bug was the primary blocker**: not a CivicPlus rendering issue but a BeautifulSoup content selector bug. All CivicPlus sites render fine with Playwright once the selector is fixed.
- **PDF remains the most reliable source**: direct PDF links parsed at near-100% success. HTML pages succeed when they contain actual rate tables, but many utility pages are landing/navigation pages with rates in linked PDFs.
- **SearXNG rate-limits after ~60 queries**: hit empty results after sustained search sessions. Plan searches in batches.
- **Search keyword optimization matters**: generic "city VA water rates" returns statewide reports. Need authority-specific names (e.g., "Loudoun Water", "Newport News Waterworks") and domain-specific queries.

### Spot-Check Flags

- **Suffolk** ($130 at 10CCF), **Colonial Heights** ($132), **Manassas Park** ($206) — unusually high. May include combined water+sewer charges. Needs manual verification against source URLs.
- **Norfolk** shows "flat" structure with N/A fixed charge — verify against source.
- **Charlottesville** parsed from FY2020 report (older vintage) — check for newer rates.

### Still Failed (10 utilities — need manual PDF curation)

| Utility | Pop | Issue |
|---------|-----|-------|
| Portsmouth | 96,201 | Billing info page, no actual dollar amounts — need rate schedule PDF |
| Lynchburg | 79,812 | Playwright timeout — retry or find PDF |
| Leesburg/Loudoun Water | 51,209 | Billing policy page — need Loudoun Water rate schedule PDF |
| Danville | 42,082 | Page has meter charges but consumption rates "per 1,000 gallons" not parsed |
| Manassas | 41,764 | Directory page linking to documents — need actual rate sheet PDF |
| Petersburg | 32,477 | General utility billing page, no rate amounts |
| Salem | 25,432 | Has rate structure but multi-year columns confused parser — prompt tune |
| Vienna/Fairfax Water | 16,522 | Rates page links to PDF schedule but page itself lacks $/CCF — need PDF |
| Front Royal | 15,070 | Bill explanation page, not actual rates |
| Martinsville | 13,645 | References "Schedule of Water and Sewer Rates (PDF)" but link not followed |

### Tabled (5 utilities — CivicPlus 403/404, deferred)

- Winchester (27K) — winchesterva.gov returns 403
- Radford (17K) — all DocumentCenter PDF links 404
- Staunton (24K) — ci.staunton.va.us returns 403
- Waynesboro (21K) — no rate page found
- Western VA Water Authority (100K) — WVWA PDFs on chooseroanokecounty.com all 404

## Completed (Sprint 3 v2 round 3 — Session 5, cont.)

- [x] Playwright PDF link crawling: extracted DocumentCenter/direct PDF URLs from 9 failed utility pages
- [x] Round 3 pipeline: 6/7 more successes → **22/26 VA utilities parsed (90% pop coverage)**
- [x] `/rates/{pwsid}` endpoint: single-utility detail + `/rates?state=VA` list endpoint
- [x] Spot-check outliers:
  - **Colonial Heights**: tier limits likely wrong (6,683 CCF = 5M gal per tier). Parser may have misapplied $/1,000 cubic feet conversion to limits.
  - **Manassas Park**: parse extracted combined water+sewer rates (not water-only). Inflated by ~50%.
  - **Suffolk**: $11.40/CCF is high but parsing logic looks correct.
  - **Norfolk**: Correct — simple flat rate $6.51/CCF.
- [x] Total API cost: $0.36 across 3 rounds ($4 cap never approached)

### Spot-Check Action Items

- [ ] Colonial Heights: re-parse or manually correct tier limits (likely should be ~7, 33, 167 CCF not 6683/33415/167075)
- [ ] Manassas Park: re-parse targeting water-only rates (exclude sewer component)
- [ ] Add parser prompt guidance: "Extract WATER rates only, not combined water+sewer"

### Tabled VA Utilities (9 — need manual browser PDF curation)

| Utility | Pop | Issue |
|---------|-----|-------|
| Western VA Water Auth | 99,897 | WVWA PDFs on chooseroanokecounty.com all 404 |
| Portsmouth | 96,201 | No city water rate PDF in DocumentCenter |
| Lynchburg | 79,812 | Municode ordinance page wrong section (sewer regs) |
| Winchester | 27,284 | winchesterva.gov 403 (CivicPlus) |
| Salem | 25,432 | CivicPlus table volumetric values don't render |
| Staunton | 24,416 | ci.staunton.va.us 403 |
| Waynesboro | 21,491 | No rate page found |
| Front Royal | 15,070 | No PDF links on billing page |
| Radford | 17,403 | All DocumentCenter links 404 |

## Completed (Sprint 3 v2 CA — Session 5, cont.)

- [x] Curated URLs for top 21 CA utilities by population (~10.7M pop)
- [x] CA pipeline round 1: 3/21 parsed — EBMUD (high), San Diego (high), LADWP (partial)
- [x] SearXNG 0-result DuckDuckGo fallback added to rate_discovery.py
- [x] CA batch discovery script with rate-limit detection
- [x] **Key finding**: CA has same PDF requirement as VA. HTML rate pages are mostly navigation/CivicPlus/Granicus — need PDF links.
- [x] **LADWP is complex**: seasonal + budget-based rates with temperature zone allotments. Parsed partially but needs specialized handling.

## Sprint 3 — Remaining Work (tabled for manual curation session)

### VA Manual PDF Curation (9 tabled + 2 re-parse)
- [ ] Portsmouth, Lynchburg, Winchester, Salem, Staunton, Waynesboro, Front Royal, Radford, Western VA Water Authority
- [ ] Fix Colonial Heights tier limits (unit conversion) + Manassas Park water-only re-parse

### CA PDF Curation (18 remaining from top 21 + 173 untouched)
- [ ] Playwright PDF-link crawling on 18 failed CA utilities (same technique as VA round 3)
- [ ] Batch discovery for remaining 173 CA utilities (needs SearXNG cooldown or chunked runs)
- [ ] LADWP specialized handling (seasonal/budget-based rate structure)

### Infrastructure
- [ ] Parser prompt refinement: water-only extraction, multi-year columns, seasonal structures
- [ ] Claude Batch API integration (replace single calls once prompt is stable)
- [ ] Hughes et al. 2025 outreach — request raw rate data for validation corpus

## Future Enhancements (Parking Lot)

- [ ] VA DEQ volume enrichment from CEDS permit documents
- [ ] Validate 11 data_center_candidate permits (imagery review → confirm/reject)
- [ ] Additional states: TX TCEQ, AZ ADWR, OR WRD (water, wastewater, energy permits as facility discovery)
- [ ] Stormwater pond identification from VPDES SWI_GP permits
- [ ] Cross-reference matched DC permits → enrich SS facility records with permit IDs
- [ ] Face value unit normalization (AFY → GPD) for cross-comparison with diversion rates
- [ ] UNC EFC state dashboards (NC, IA, WV, FL) as verification data source
- [ ] Strong et al. (WRI) governance indicators: supply reliability, CCR availability, drought plans — natural scraping expansion targets

## Current API Surface

| Endpoint | Purpose |
|----------|---------|
| `GET /resolve?lat=X&lng=Y` | Water utility + SDWIS + MDWD + Aqueduct + rate flag for a point |
| `GET /permits?lat=X&lng=Y&radius_km=10` | All permits within radius (filters: `category_group`, `source`) |
| `GET /facility/{id}/permits` | Linked + nearby permits for an SS facility |
| `GET /rates/{pwsid}` | Full rate detail: tiers, bills, provenance for one utility |
| `GET /rates?state=VA` | List all parsed rates for a state (summary view) |
| `GET /health` | Data vintage for all pipeline steps |

## Database State (as of Session 5)

| Table | Rows | Source |
|-------|------|--------|
| `utility.cws_boundaries` | 44,643 | EPA CWS |
| `utility.aqueduct_polygons` | 68,506 | WRI Aqueduct 4.0 |
| `utility.sdwis_systems` | 3,711 | EPA ECHO (VA + CA) |
| `utility.mdwd_financials` | 225 | Harvard Dataverse (VA + CA) — bill columns removed |
| `utility.county_boundaries` | 3,235 | Census TIGER |
| `utility.permits` | 61,530 | VA DEQ (16,519) + CA eWRIMS (45,011) |
| `utility.permit_facility_xref` | 41 | 30 matched + 11 candidates |
| `utility.water_rates` | 25 | VA: 22 high/med + 4 failed; CA: 2 high + 1 partial (LLM-parsed) |
| `utility.pipeline_runs` | 14 | Audit trail |

## Completed (Sprint 4 — eAR Bulk Ingest — Session 6)

- [x] Schema evolution (migration 008):
  - `source` column on `water_rates`: `scraped_llm | swrcb_ear_YYYY | owrs`
  - Unique constraint updated to `(pwsid, rate_effective_date, source)` — allows duplicate records from different sources
  - eAR bill snapshot columns: `bill_6ccf`, `bill_9ccf`, `bill_12ccf`, `bill_24ccf`
  - Existing scraped records backfilled with `source='scraped_llm'`
- [x] HydroShare eAR 2022 downloaded (17 MB Excel, 7,228 CA systems)
- [x] `ear_ingest.py` — bulk ingest from formatted eAR Excel:
  - Maps SF residential tier structure: base charge, up to 4 tiers (limits + rates), billing frequency
  - Bill columns are monthly-equivalent (bimonthly charges ÷ 2)
  - Fixed charge normalized to monthly
  - Tier limits normalized to monthly (per-billing-period ÷ divisor)
  - Rate structure mapping: Variable Base → increasing_block, Uniform Usage → uniform, Fixed Base → flat
  - Idempotent: clears source-tagged records before reinserting
- [x] CLI: `ua-ingest ear --year 2022` (with `--dry-run`)
- [x] **194/194 CA MDWD utilities ingested** from eAR 2022:
  - 188 have pre-computed bill amounts (6/9/12/24 HCF)
  - 187 have explicit tier structure
  - All have rate structure type and billing frequency
- [x] **Dynamic column mapping** — eAR column indices vary by year (2020: 1314 cols, 2021: 2315, 2022: 2978). Replaced hardcoded indices with name-based header lookup.
- [x] **eAR 2020 + 2021 ingested**: 194 + 193 = 387 additional records. 2020 has tier data but no bill columns.
- [x] **CivicPlus DocumentCenter crawler** — search-based approach:
  - Playwright renders CivicPlus site search (JS-rendered)
  - Runs 4 search queries per site, extracts all links, deduplicates
  - Relevance scoring classifier for link titles (strong/moderate/negative signals)
  - Tested on 3 sites: Fredericksburg (+23 rate ordinance), Martinsville (+8.5 rate schedule), Colonial Heights (+1.0 utility rates page)
  - CLI: `ua-ingest civicplus-crawl --domain fredericksburgva.gov`

### Sprint 4 — eAR Reconciliation (14 overlapping utilities)

14 CA utilities now have both scraped (LLM) and eAR records. Notable discrepancies:

| PWSID | Name | Scraped @5CCF | eAR @6CCF | Scraped @10CCF | eAR @12CCF | Notes |
|-------|------|---------------|-----------|----------------|------------|-------|
| CA0110011 | Livermore | $61.38 | $43.49 | $88.28 | $66.83 | Scraped much higher — vintage or combined charges? |
| CA4110022 | Redwood City | $92.09 | $47.91 | $137.99 | $68.74 | Scraped ~2x eAR — likely combined water+sewer |
| CA4810007 | Vallejo | $115.08 | $41.64 | $183.03 | $55.23 | Scraped ~3x eAR — almost certainly combined charges |
| CA3910005 | Manteca | $33.59 | $1.03 | $45.94 | $1.04 | eAR suspiciously low ($1/mo) — data quality issue |
| CA3710005 | Carlsbad | $43.97 | $50.85 | $67.72 | $75.75 | Close — both plausible, different vintages |
| CA3010001 | Anaheim | $15.75 | $16.88 | $16.50 | $28.80 | Scraped bill@10 < bill@6 — parser error likely |
| CA3410020 | Sacramento | $43.01 | $44.47 | $50.31 | $53.22 | Close agreement — good cross-validation |

**Key insight**: several scraped rates likely include combined water+sewer charges (Vallejo, Redwood City, Livermore). The eAR data is water-only by design (state filing). This validates having both sources for comparison. Conflict resolution is deferred.

## Database State (as of Session 6)

| Table | Rows | Source |
|-------|------|--------|
| `utility.cws_boundaries` | 44,643 | EPA CWS |
| `utility.aqueduct_polygons` | 68,506 | WRI Aqueduct 4.0 |
| `utility.sdwis_systems` | 3,711 | EPA ECHO (VA + CA) |
| `utility.mdwd_financials` | 225 | Harvard Dataverse (VA + CA) |
| `utility.county_boundaries` | 3,235 | Census TIGER |
| `utility.permits` | 61,530 | VA DEQ (16,519) + CA eWRIMS (45,011) |
| `utility.permit_facility_xref` | 41 | 30 matched + 11 candidates |
| `utility.water_rates` | 677 | scraped_llm: 96 + swrcb_ear_2020: 194 + swrcb_ear_2021: 193 + swrcb_ear_2022: 194 |
| `utility.pipeline_runs` | 19 | Audit trail |

## Sprint 4 — Remaining Work

### eAR Additional Years
- [x] Download and ingest 2020 + 2021 eAR files — 387 records inserted
- [ ] Cross-year rate change analysis for utilities with all 3 years

### CivicPlus DocumentCenter Crawler
- [x] Search-based crawler with relevance scoring classifier
- [x] Tested on 3 sites — finds correct rate documents
- [ ] Run crawler on all known CivicPlus utilities lacking rate URLs
- [ ] Feed discovered URLs into the existing rate parsing pipeline
- [ ] Extend to non-CivicPlus sites (general site search + classify approach)

### OWRS Ingest (Layer 1, medium ROI)
- [ ] Download CA Data Collaborative Open Water Rate Specification from OpenEI
- [ ] Machine-readable YAML → water_rates mapping
- [ ] Source tag: `owrs`

### Reconciliation Framework (deferred)
- [ ] Design conflict resolution for duplicate source records
- [ ] Prioritize: scraped combined charges → flag for water-only re-parse
- [ ] eAR Manteca ($1/mo) → flag as data quality issue

### Infrastructure
- [ ] Parser prompt refinement: water-only extraction, multi-year columns, seasonal structures
- [ ] Claude Batch API integration (replace single calls once prompt is stable)

## Recommended Next Chat Prompt

```
UAPI Sprint 4 cont. v1 — Run CivicPlus crawler on VA/CA utilities lacking rate URLs. Identify CivicPlus sites from existing URL configs + web discovery. Feed best candidate URLs into rate parsing pipeline. Then OWRS ingest (OpenEI CA rate specs). Start from docs/next_steps.md.
```
