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
| `utility.water_rates` | ~684 | scraped_llm: ~103 (41 high/med) + swrcb_ear_2020: 194 + swrcb_ear_2021: 193 + swrcb_ear_2022: 194 |
| `utility.pipeline_runs` | 21 | Audit trail |

## Sprint 4 — CivicPlus + CA Discovery Results (Session 6 cont.)

### CivicPlus Crawler — VA Results (9 utilities)
- [x] Crawled all 9 VA utilities missing scraped rates
- **Radford**: Found FY2026 Utility Rates PDF but DocumentCenter serves Excel (not parseable)
- **Salem**: FAQ page with rate values found (+13 score) but static scraper misses JS content
- **Waynesboro**: Document was building permits, not water rates (false positive)
- **Front Royal**: Fee schedule page found but CivicPlus rendering blocks scraping
- **Lynchburg**: Rate study PDFs found but only FY10-FY17 vintage
- **Portsmouth, Winchester, Staunton, WVWA**: No actionable results

### CA SearXNG Discovery — Top 10 by population
- [x] **Huntington Beach** (201K): flat rate $22.46 fixed, $34.48@5CCF [high] — .php page parsed
- [x] **Modesto** (219K): uniform $26.48 fixed, $37.03@5CCF [high] — rate study PDF parsed
- Santa Rosa: fee schedule PDF was development fees, not water rates
- Fresno, Long Beach, Santa Ana, Riverside, Glendale, Ontario, Oxnard: landing pages without rate numbers

### Key Findings
1. **CivicPlus crawler works on CivicPlus sites** but most CA cities aren't CivicPlus
2. **SearXNG finds overview pages**, not rate tables — the persistent Layer 3 problem
3. **PDF-first discovery** (searching for `filetype:pdf site:{domain} water rate schedule`) is still the highest-ROI approach for individual utilities
4. **eAR bulk data already covers all 194 CA utilities** — scraped rates add cross-validation and current pricing but are diminishing returns
5. **VA tabled utilities** need manual browser curation (Layer 4) — CivicPlus rendering and Excel format issues aren't solvable via automated scraping

## Completed (Sprint 5 — OWRS Ingest + Reconciliation Diagnostic — Session 7)

### OWRS Ingest
- [x] Cloned CA Data Collaborative OWRS repo (433 CA utilities, 492 YAML files)
- [x] Discovered pre-computed summary table in OWRS-Analysis repo with PWSID crosswalk
- [x] Built `owrs_ingest.py`: CSV → water_rates mapping with tier parsing, unit conversion, bill recalculation
- [x] **387 records inserted** (419 rows → 397 parsed → 392 matched CWS → 387 after dedup)
- [x] Handles tiered (291), uniform (109), budget-based (16) rate structures
- [x] Unit conversion for 61 kgal-reporting utilities → CCF
- [x] Billing frequency normalization (bimonthly/quarterly → monthly)
- [x] Bill_5ccf and bill_10ccf recalculated from tier structure at standard usage
- [x] CLI: `ua-ingest owrs [--dry-run]`
- [x] Cross-validation: Anaheim OWRS vs scraped = 2% diff (validates calculation logic)

### OWRS Data Characteristics
- **Vintage**: 2002-2021 (median ~2017). Older than eAR (2020-2022) and scraped (2024-2026).
- **Coverage**: 381 unique PWSIDs, 229 net new (not in any prior source)
- **Quality**: Pre-curated by CA Data Collaborative from utility OWRS filings. Water-only.
- **Limitation**: 3 records skipped (anomalous bill_type: formula strings, "0")
- **Limitation**: 16 budget-based rates have non-numeric tier limits (% of allocation). Prices stored, limits NULL.

### Reconciliation Diagnostic
- [x] Built `scripts/reconcile_rates.py` — cross-source variance analysis
- [x] 190 multi-source utilities with comparable bill amounts analyzed
- [x] **Variance distribution**: 38% agree (<10% CV), 32% moderate (10-25%), 15% divergent (25-50%), 7% major (50-100%)
- [x] **157/1069 records flagged** for quality issues:
  - 88 eAR tier limit inflation (1000x factor, 54 utilities) — tier limits in gallons not CCF
  - 76 stale vintage (pre-2015, mostly OWRS)
  - 8 suspected combined water+sewer scrapes (Vallejo 3.7x, Redwood City 2.2x, San Diego 2.1x, EBMUD 2.0x)
  - 2 suspiciously low bills (Manteca eAR = $1/mo confirmed)
  - 2 suspiciously high fixed charges
  - 7 suspiciously high bills (eAR inflation artifacts)
- [x] CSV report: `data/interim/rate_reconciliation.csv`

### Key Insights for Reconciliation Methodology
1. **eAR tier limit inflation is systematic**: 54 utilities across all 3 eAR years have limits 1000x too high. Root cause likely in the HydroShare processing — tier limits appear to be in gallons, not HCF/CCF. The eAR 2022 data is partially corrected (Escondido: 2020/2021 have 7000 CCF, 2022 has 7 CCF).
2. **Scraped combined charges are identifiable**: when scraped bill is >1.5x the median of eAR+OWRS, and >$40, it's likely combined water+sewer. 8 utilities flagged.
3. **Vintage gaps explain most moderate divergence**: OWRS (2017) vs scraped (2024) is a 7-year gap. Water rates typically increase 3-5% annually → ~25-40% expected divergence.
4. **"Unexplained" divergence** (13 utilities, CV>25% with no flagged issues) needs individual review.

### Cross-Year eAR Analysis
- **Tabled** — current-state accuracy supercedes historical trend analysis. The eAR tier inflation issue needs to be resolved before cross-year comparisons are meaningful.

## Database State (as of Session 7)

| Table | Rows | Source |
|-------|------|--------|
| `utility.cws_boundaries` | 44,643 | EPA CWS |
| `utility.aqueduct_polygons` | 68,506 | WRI Aqueduct 4.0 |
| `utility.sdwis_systems` | 3,711 | EPA ECHO (VA + CA) |
| `utility.mdwd_financials` | 225 | Harvard Dataverse (VA + CA) |
| `utility.county_boundaries` | 3,235 | Census TIGER |
| `utility.permits` | 61,530 | VA DEQ (16,519) + CA eWRIMS (45,011) |
| `utility.permit_facility_xref` | 41 | 30 matched + 11 candidates |
| `utility.water_rates` | 1,069 | owrs: 387 + scraped_llm: 101 + ear_2020: 194 + ear_2021: 193 + ear_2022: 194 |
| `utility.pipeline_runs` | 23 | Audit trail |

## Completed (Sprint 6 — Reconciliation Fixes + Best Estimate — Session 7 cont.)

### eAR Tier Limit Fix
- [x] 97 records across 61 PWSIDs had tier limits in gallons instead of CCF
- [x] Inflation factors varied: 748x (gallons), 1000x (kgal), up to 5110x
- [x] Fix strategy: NULL inflated tier structures (>100 CCF residential threshold)
- [x] 90 records: pre-computed state bills preserved (reasonable despite bad tiers)
- [x] 7 records: bills also NULLed (state's own calculation used inflated tiers)
- [x] 3 additional inflated bill records caught in second pass
- [x] Result: eAR tier inflation flags → 0, conflict category → 0, mean CV 18.1% → 16.4%
- [x] Script: `python scripts/fix_ear_tier_inflation.py [--dry-run]`

### Combined Water+Sewer Investigation (Sprint 7)
- [x] 7 scraped records re-parsed with explicit water-only prompt
- [x] **Result: NOT combined water+sewer.** 6/7 identical bills, 1 minor change (EBMUD -3.5%)
- [x] High CA water prices (2024-2026 vintage) explain divergence from eAR/OWRS (2017-2022)
- [x] All 7 restored to high confidence. EBMUD bill corrected to $105.75.
- [x] Re-parse cost: $0.04 (7 Sonnet calls)

### Best-Estimate Source Priority
- [x] Built `utility.rate_best_estimate` table — one row per PWSID
- [x] Priority: eAR 2022 (government anchor) > eAR 2021 > scraped (if agrees <25% with anchor) > OWRS > scraped (diverges) > eAR 2020
- [x] 443 PWSIDs: eAR 2022=179 (40%), OWRS=227 (51%), scraped=30 (7%), none=7
- [x] Confidence: high=184 (42%), medium=252 (57%), none=7 (2%)
- [x] 5 scraped rates upgraded (agreed with eAR anchor within 25%)
- [x] Bill @10CCF: mean=$54, median=$48, range=$4-$462
- [x] Script: `python scripts/build_best_estimate.py [--dry-run] [--csv]`

## Database State (as of Session 7 final)

| Table | Rows | Source |
|-------|------|--------|
| `utility.cws_boundaries` | 44,643 | EPA CWS |
| `utility.aqueduct_polygons` | 68,506 | WRI Aqueduct 4.0 |
| `utility.sdwis_systems` | 3,711 | EPA ECHO (VA + CA) |
| `utility.mdwd_financials` | 225 | Harvard Dataverse (VA + CA) |
| `utility.county_boundaries` | 3,235 | Census TIGER |
| `utility.permits` | 61,530 | VA DEQ (16,519) + CA eWRIMS (45,011) |
| `utility.permit_facility_xref` | 41 | 30 matched + 11 candidates |
| `utility.water_rates` | 1,069 | owrs: 387 + scraped_llm: 101 + ear: 581 (97 tiers NULLed, 10 bills NULLed) |
| `utility.rate_best_estimate` | 443 | Best-estimate selection per PWSID |
| `utility.pipeline_runs` | 26 | Audit trail |

## Completed (Sprint 7 — API + Re-parse + Verification — Session 7 cont.)

### API Endpoints
- [x] `GET /rates/best-estimate?state=CA&min_confidence=high` — serves from rate_best_estimate table
- [x] `GET /resolve` updated — returns best_estimate_bill_10ccf, rate_source, rate_confidence, rate_n_sources, rate_effective_date
- [x] Route ordering fix: `/rates/best-estimate` registered before `/rates/{pwsid}`
- [x] Tested: Sacramento resolves $50.31/mo, San Diego $56.19/mo (eAR 2022 anchor)

## Completed (Sprint 8 — Cross-Year Analysis + State Expansion Research — Session 7 cont.)

### Cross-Year eAR Rate Change Analysis
- [x] 180 utilities with bill_12ccf in both 2021 and 2022
- [x] 173 clean, 7 outliers (>50% change, likely reporting corrections)
- [x] **Median bill change: +1.0%/yr**, mean +3.5%/yr
- [x] **Fixed charge: +1.9%/yr** median (2020→2022, 170 utilities)
- [x] 45% flat (±2%), 47% increasing, 8% decreasing
- [x] Script: `python scripts/analyze_ear_rate_changes.py [--csv]`

### State Expansion Research
- [x] Researched TX, AZ, OR, UNC EFC, AWWA sources

**Findings by source:**

| Source | Coverage | Format | Access | Priority |
|--------|----------|--------|--------|----------|
| **OR League of Cities 2023** | 71 cities, 40 with bills | Socrata CSV (free-text bills, needs parsing) | Free, Socrata API | Medium — small coverage, messy format |
| **UNC EFC NC Dashboard** | NC statewide, 2024 data | Interactive dashboard, download button | Free | High — most recent, good coverage |
| **UNC EFC IA Dashboard** | IA statewide, 2023 survey | Interactive dashboard | Free | High — IA is a target state |
| **UNC EFC WV/FL** | WV (2022 PSC data), FL (2020 Raftelis) | Interactive dashboards | Free | Medium — older data |
| **AWWA Rate Survey** | 450 utilities, all 50 states | Subscription platform (Raftelis) | Paid subscription | Low — paywall |
| **TX TWDB** | Water use data, not rate data | Various | Free | Low — no rate data found |
| **AZ ADWR** | Water supply/demand, not rates | ArcGIS, CSV | Free | Low — no rate data found |

**Recommendation for Sprint 9:**
1. UNC EFC NC dashboard (most mature, 2024 data, free download)
2. UNC EFC IA dashboard (IA is DC-relevant, 2023 data)
3. OR League of Cities CSV (small but free, needs bill amount parsing)

## Completed (Sprint 10 — Infrastructure Layers — Session 10)

### Layer B — Data Operations Manager
- [x] **source_catalog table** (migration 009): registry of all 6 known data sources with operational state (type, states, vintage, refresh cadence, last_ingested_at, next_check_date)
- [x] **ORM models**: SourceCatalog, ScrapeRegistry, RateBestEstimate — proper Alembic-managed models
- [x] **pwsid_coverage materialized view**: joins CWS + water_rates + SDWIS + best_estimate for coverage reporting. Indexes on pwsid, state_code, has_rate_data. Sprint 12 will migrate to a regular table when mutable columns needed.
- [x] **Seed script** (`scripts/seed_source_catalog.py`): populates catalog with all 6 sources, updates pwsid_count from water_rates

### Layer A — Scrape Registry (table only, Sprint 12 wires agents)
- [x] **scrape_registry table** (migration 009): per-URL tracking with fetch/parse/retry state, composite unique on (pwsid, url)
- [x] **YAML migration** (`scripts/migrate_urls_to_registry.py`): loaded 128 entries — 27 VA curated, 56 CA curated, 22 VA candidates, 101 backfilled from water_rates scraped_llm records

### Best-Estimate Generalization
- [x] **Generalized build_best_estimate** (`src/utility_api/ops/best_estimate.py`): all states, config-driven priority from `config/source_priority.yaml`
- [x] **846 PWSIDs** with best estimates (up from 443 CA-only): CA=415, NC=403, VA=28
- [x] **Source priority config** (`config/source_priority.yaml`): per-state anchor sources, tolerance thresholds, fallback priority
- [x] Confidence: high=587 (69%), medium=252 (30%), none=7 (1%)

### SDWIS 50-State Expansion
- [x] **44,633 SDWIS records** loaded (up from 3,711 VA+CA)
- [x] Config: `sdwis_states: ALL` in sources.yaml, sdwis.py handles None target_states
- [x] `/resolve` endpoint now returns complete SDWIS records for all 50 states

### ua-ops CLI (`ua-ops` entry point)
- [x] `ua-ops status` — state-of-the-world: table sizes, source catalog, rate coverage, scrape registry, recent pipeline runs
- [x] `ua-ops coverage-report` — detailed analysis: coverage by state, by source, freshness breakdown, top gaps
- [x] `ua-ops refresh-coverage` — refreshes pwsid_coverage materialized view
- [x] `ua-ops build-best-estimate [--state XX] [--dry-run] [--csv]` — generalized best-estimate builder

### Database State (as of Sprint 10)

| Table | Rows | Source |
|-------|------|--------|
| `utility.cws_boundaries` | 44,643 | EPA CWS |
| `utility.aqueduct_polygons` | 68,506 | WRI Aqueduct 4.0 |
| `utility.sdwis_systems` | **44,633** | EPA ECHO (**all 50 states**) |
| `utility.mdwd_financials` | 225 | Harvard Dataverse (VA + CA) |
| `utility.county_boundaries` | 3,235 | Census TIGER |
| `utility.permits` | 61,530 | VA DEQ + CA eWRIMS |
| `utility.permit_facility_xref` | 41 | 30 matched + 11 candidates |
| `utility.water_rates` | 1,472 | 6 sources across 3 states |
| `utility.rate_best_estimate` | **846** | **All states** (CA + NC + VA) |
| `utility.source_catalog` | **6** | Source registry |
| `utility.scrape_registry` | **128** | URL tracking |
| `utility.pipeline_runs` | 37 | Audit trail |

---

## Remaining Work

### Completed (Sprint 11 — Canonical Schema — Session 10 cont.)

- [x] **rate_schedules table** (migration 010): canonical rate schema with JSONB tiers
  - `volumetric_tiers` JSONB: `[{tier, min_gal, max_gal, rate_per_1000_gal}]` — any number of tiers
  - `fixed_charges` JSONB: `[{name, amount, meter_size}]` — multiple fixed charges
  - `surcharges` JSONB: `[{name, rate_per_1000_gal, condition}]` — drought/seasonal
  - `conservation_signal`: highest/lowest tier ratio (>1 = conservation pricing)
  - `bill_20ccf`: new bill snapshot at 20 CCF
  - `needs_review` + `review_reason`: quality flags
  - GIN index on volumetric_tiers for containment queries
- [x] **Migration transform**: all 1,472 water_rates → rate_schedules
  - 1,291 with volumetric tiers, 1,384 with fixed charges
  - 716 with conservation signal, 1,397 with bill_20ccf
  - Units: gallons + $/1000gal (canonical, not CCF)
- [x] **Best-estimate reads from rate_schedules** when populated, falls back to water_rates
- [x] **`/rates/{pwsid}` serves JSONB tiers** from rate_schedules with legacy fallback
- [x] **`ua-ops sync-rate-schedules`** CLI: syncs any new water_rates records to rate_schedules
- [x] Rate schedule helpers: `water_rate_to_schedule()`, `compute_bill_at_gallons()`, `compute_conservation_signal()`
- [x] `/rates/best-estimate` and `/resolve` already serve all states (from Sprint 10 best-estimate generalization)

### Database State (as of Sprint 11)

| Table | Rows | Source |
|-------|------|--------|
| `utility.rate_schedules` | **1,472** | Canonical JSONB — migrated from water_rates |
| `utility.water_rates` | 1,472 | Legacy fixed-tier — kept as audit table |
| All other tables | (unchanged from Sprint 10) | |

### Completed (Sprint 12 — Scrape Registry Wiring + Agent Skeleton — Session 10 cont.)

- [x] **Migration 011**: pwsid_coverage mat view → regular table with `scrape_status` + `priority_tier` columns. Also added `ingest_log` table for agent audit trail.
- [x] **Coverage refresh**: `ua-ops refresh-coverage` recomputes derived columns via UPDATE, preserves mutable columns (scrape_status, priority_tier). Also syncs scrape_status from scrape_registry.
- [x] **BaseAgent ABC** (`src/utility_api/agents/base.py`): minimal abstract base class with `run()` and `log_run()`. No LLM, no framework, no async.
- [x] **BulkIngestAgent** (`src/utility_api/agents/bulk_ingest.py`): wraps existing ingest modules. Updates source_catalog.last_ingested_at, syncs rate_schedules, logs to ingest_log.
- [x] **BestEstimateAgent** (`src/utility_api/agents/best_estimate.py`): wraps ops/best_estimate.py. Refreshes pwsid_coverage after building estimates.
- [x] **Scraping pipeline wired** (write-only): `rates.py` now writes to scrape_registry at each stage — discovery (URL found), fetch (HTTP status, content hash), parse (confidence, cost, model). Wrapped in try/except — never breaks the pipeline.
- [x] **Registry writer** (`src/utility_api/ops/registry_writer.py`): `log_discovery()`, `log_fetch()`, `log_parse()` helper functions.
- [x] **`ua-ops scrape-status [--state XX]`**: URL status breakdown, parse outcomes, HTTP codes, recent failures.
- [x] **pwsid_coverage.scrape_status**: populated from scrape_registry — 97 succeeded, 3 url_discovered, 44,543 not_attempted.

### Database State (as of Sprint 12)

| Table | Rows | Source |
|-------|------|--------|
| `utility.pwsid_coverage` | 44,643 | **Regular table** (was mat view) — scrape_status + priority_tier |
| `utility.ingest_log` | 1 | Agent audit trail |
| All other tables | (unchanged from Sprint 11) | |

### Completed (Sprint 13 — Orchestrator + Research Agents — Session 10 cont.)

- [x] **priority_tier populated**: Tier 2 = 19,478 (DC states), Tier 3 = 226 (pop >100K), Tier 4 = 24,939. Tier 1 (DC-adjacent) deferred — requires cross-schema spatial join.
- [x] **OrchestratorAgent** (`agents/orchestrator.py`): generates ranked task queue from 4 SQL queries — bulk source freshness, coverage gaps, retriable failures, change detection. Pure Python + SQL, no LLM.
- [x] **DiscoveryAgent** (`agents/discovery.py`): SearXNG search with targeted queries + keyword relevance scoring. Optional Haiku fallback for ambiguous URLs (score 30-60). Writes to scrape_registry with status='pending'.
- [x] **ScrapeAgent** (`agents/scrape.py`): reads from scrape_registry, fetches URLs via rate_scraper.py, updates registry with HTTP status/hash/length. Retry logic: 403 → exponential backoff, 404 → dead, 5xx → 6h retry. Returns raw text in memory for ParseAgent.
- [x] **ParseAgent** (`agents/parse.py`): Claude API extraction with complexity routing (Sonnet for complex, Haiku for simple). Prompt caching enabled. Writes to rate_schedules (JSONB), triggers BestEstimateAgent. Cost tracking per parse.
- [x] **ua-run-orchestrator** CLI: generates queue, optionally executes top N tasks. Pipeline: orchestrator → discovery → scrape → parse → best estimate. Sequential for-loop execution.
- [x] **End-to-end test**: Fairfax County Water Authority (VA6059501) — discovered 3 URLs via SearXNG, scraped all 3 successfully (14-15K chars each). Parse requires ANTHROPIC_API_KEY in environment.

### Sprint 14 — Cron + Change Detection + Batch API ✅
- [x] **Migration 012**: `batch_jobs` table + `source_url`/`last_content_hash`/`check_interval_days` on `source_catalog`
- [x] **SourceChecker agent** (`agents/source_checker.py`): fetches source URLs, hashes content, detects new vintages. Source-specific checks for eAR (HydroShare year detection) and EFC (survey year). Updates hash + next_check_date.
- [x] **check_bulk_source fully implemented**: was a stub since Sprint 13, now wired into orchestrator and `ua-ops check-sources` CLI command.
- [x] **BatchAgent** (`agents/batch.py`): submits parse tasks to Anthropic Message Batches API (50% cost savings). Stores task details in JSONB for 24h gap survival. Downloads, validates, writes results.
- [x] **`--batch` flag** on `ua-run-orchestrator`: discovery+scrape run live, parse tasks collected and submitted as batch.
- [x] **ua-ops pipeline-health**: operational health report (agent runs, batch jobs, registry status, 7-day activity, errors, source check schedule).
- [x] **ua-ops batch-status / process-batches**: check and process completed Batch API jobs.
- [x] **ua-ops check-sources**: check all overdue bulk sources for new data.
- [x] **Cron scheduling**: `scripts/setup_cron.sh` installs 4 cron jobs (orchestrator 2AM, coverage 5AM, batch 10AM, sources Sunday 6AM).
- [x] **Change detection fix**: `INTERVAL ':days days'` → `MAKE_INTERVAL(days => :days)` — was matching all active URLs instead of only stale ones.
- [x] **Config-driven thresholds**: `config/agent_config.yaml` for change detection, batch sizing, source checking.
- [x] **Parse agent hardening**: `_parse_date()` handles varied LLM date formats, truncation for VARCHAR(30) columns.
- [ ] **VA coverage push**: 1 new PWSID added (VA1191883). Low yield due to SearXNG rate limiting (0 results after ~13 queries) and keyword scoring threshold too strict for abbreviated VA utility names. Discovery agent tuning needed — see Sprint 14.5 notes.

**Known issues from Sprint 14 push:**
1. SearXNG rate-limits after ~13 rapid queries (2s delay insufficient). Need longer delay or query batching.
2. Keyword scoring threshold (>50) filters out valid rate pages when utility names are abbreviated (PWCSA, ACSA, BVU).
3. issuu.com links return 0 chars even with Playwright (embedded viewer, not extractable text).

### Sprint 14.5 — Discovery Agent Tuning (before next coverage push)
- [ ] Increase SearXNG search delay to 5-10s or implement adaptive backoff
- [ ] Lower keyword scoring threshold to 30 (accept more candidates, let parse agent filter)
- [ ] Add alternate search query patterns for abbreviated utility names
- [ ] Handle issuu.com/similar embedded content hosts (detect and skip)

### State Expansion (after Sprint 13)
- [ ] UNC EFC IA dashboard (690 utilities, per-utility HTML scrape)
- [ ] OR League of Cities 2023 CSV
- [ ] EFC states with CSV downloads
- [ ] Targeted scraping for DC-adjacent utilities

### VA Remaining
- [ ] 9 VA utilities need manual PDF curation
- [ ] CivicPlus crawler found some URLs but content not parseable

## Current API Surface

| Endpoint | Purpose | Coverage |
|----------|---------|----------|
| `GET /resolve?lat=X&lng=Y` | Spatial lookup → PWSID + CWS + SDWIS + MDWD + Aqueduct + best-estimate rate | CWS: all 50. SDWIS: **all 50**. Rates: **all states** (best-estimate). |
| `GET /permits?lat=X&lng=Y&radius_km=10` | All permits within radius | VA + CA |
| `GET /facility/{id}/permits` | Linked + nearby permits for an SS facility | VA only |
| `GET /rates/{pwsid}` | Full rate detail: **JSONB tiers**, bills, conservation signal, provenance | CA + NC + VA |
| `GET /rates?state=XX` | All parsed rates for a state | CA, NC, VA |
| `GET /rates/best-estimate?state=XX` | Best-estimate rates with confidence | **All states** |
| `GET /health` | Data vintage for all pipeline steps | All tables |

## CLI Commands

| Command | Purpose |
|---------|---------|
| `ua-ingest <step>` | Data ingest pipeline steps |
| `ua-api` | Launch FastAPI server |
| **`ua-ops status`** | State-of-the-world dashboard |
| **`ua-ops coverage-report`** | Detailed coverage analysis |
| **`ua-ops refresh-coverage`** | Refresh pwsid_coverage mat view |
| **`ua-ops build-best-estimate`** | Build best-estimate rates (all states) |
| **`ua-ops sync-rate-schedules`** | Sync water_rates → rate_schedules |
| **`ua-ops scrape-status [--state]`** | Scrape registry status breakdown |
| **`ua-ops check-sources`** | Bulk source freshness checking |
| **`ua-ops pipeline-health`** | Pipeline health report |
| **`ua-ops batch-status [batch_id]`** | Check Batch API job status |
| **`ua-ops process-batches`** | Process completed batch jobs |
| **`ua-run-orchestrator [--execute N]`** | Autonomous pipeline: discover → scrape → parse |
| **`ua-run-orchestrator --batch`** | Same but uses Batch API for parse (async, 50% cheaper) |

## Recommended Next Chat Prompt

```
UAPI Sprint 14.5 — Bulk URL Discovery from State Directories

## Context
Sprint 14 infrastructure is complete (cron, Batch API, source checker, health monitoring). The VA coverage push (25 utilities) yielded only 1 new PWSID. Root cause: SearXNG rate-limits after ~15-20 queries regardless of config (Google disabled, 5s delays, 60+ engines). This is an IP-level upstream engine constraint, not fixable via SearXNG tuning.

## The strategic shift
Stop searching for each utility one at a time. Instead: scrape state utility directories to bulk-discover website URLs, then feed them directly into the existing pipeline. SearXNG becomes a gap-filler for the 20-30% not in any directory.

The infrastructure already supports this. The pipeline ingests URLs from YAML config files via a migration script. The output of this session is YAML files.

## Failure analysis (from Sprint 14 session)
- 12/25 (48%): SearXNG returned 0 URLs — upstream rate-limited
- 9/25 (36%): URLs found but keyword scorer filtered them out
- 2/25: Parse or scrape failed (bugs now fixed)
- 1/25: Success (VA1191883, $98.73/mo)

## What's already fixed
1. Search delay 2s → 5s, batch cap 50 → 15
2. Haiku LLM fallback band 30-60 → 15-60 (validated: Stafford County rate page scored kw=15 → Haiku=95)
3. SearXNG: Google disabled, all others default
4. Parse agent: _parse_date(), VARCHAR(30) truncation

## What this session should do

### Step 1: Research VA utility directories
Identify state-level directories that list water utilities with website URLs:
- VA DEQ drinking water program
- VA Department of Health waterworks directory
- SDWIS state pages
- Any other state directory that maps PWSID → utility website

### Step 2: Output YAML files
For each directory found, produce a YAML file in this exact format:

```yaml
# VA utility rate page URLs from {directory name}
# Source: state_directory
# Generated: {date}

# {Utility Name} (pop {N})
VA1234567: "https://example.com/rates"

# {Utility Name} (pop {N})
VA2345678: "https://example.com/water/fees"
```

Rules:
- One file per source directory: config/rate_urls_va_directory_{source}.yaml
- Simple flat mapping: PWSID → URL string
- Comments with utility name and population are helpful but optional
- URL should point to the utility's rate/billing page if identifiable, or the utility homepage if not
- Only include URLs that start with http/https
- If the directory gives a homepage but not a rate page, use the homepage — the parse agent can handle it

### Step 3: Load into pipeline
After YAML files are created:
```bash
# Add new file to the migration script's YAML_FILES list
# Then run:
python scripts/migrate_urls_to_registry.py
```

This writes all URLs to scrape_registry with status='pending'. The orchestrator will pick them up on the next run.

### Step 4: Test pipeline
Run a small batch through the full pipeline:
```bash
ua-run-orchestrator --execute 5 --state VA
```
Verify: the orchestrator should find PWSIDs where scrape_status='url_discovered' (URLs already in registry) and skip discovery, going straight to scrape → parse.

## YAML file contract
The migration script (scripts/migrate_urls_to_registry.py) reads YAML files as:
```python
data = yaml.safe_load(f)  # returns dict
for pwsid, url in data.items():
    if isinstance(url, str) and url.startswith("http"):
        # insert into scrape_registry with status='pending'
```
That's the entire contract. Comments are ignored. Non-string values are skipped.

## Existing curated URLs (don't duplicate these)
- config/rate_urls_va.yaml — 31 VA utilities already curated
- config/rate_urls_ca.yaml — 56 CA utilities
- 104 active entries in scrape_registry

## Current VA coverage gap
- 44,633 total SDWIS PWSIDs (all states)
- VA has ~900 community water systems
- 30 VA PWSIDs currently have rate data
- ~870 VA PWSIDs need URLs

## Key files
- config/rate_urls_va.yaml — existing curated VA URLs (31 entries, reference format)
- scripts/migrate_urls_to_registry.py — YAML → scrape_registry loader
- agents/scrape.py — reads pending URLs from registry, fetches content
- agents/parse.py — Claude API extraction
- agents/discovery.py — SearXNG search (fallback path)
- ops/registry_writer.py — direct registry writer (alternative to YAML path)
```

## Completed (Sprint 15 Half 1 — 2026-03-25)

- [x] IOU mapper (`ua-ops iou-map`): matched 231 PWSIDs across 17 states to 7 parent companies
  - American Water Works: 110, Aqua/Essential: 70, CalWater: 24, SJW: 16, Aquarion: 7, Artesian: 3, Middlesex: 1
  - All 231 written to scrape_registry (status=pending) + per-state YAML config files
  - Lower than spec estimate of 1,000-1,500 — name-based regex is conservative; many subsidiaries don't contain parent name
- [x] CCR link ingester (`ua-ops ingest-ccr-links`): manual CSV pipeline for EPA CCR URL → candidate rate URLs
- [x] Discovery query templates: added CCR search + .gov site operator to DiscoveryAgent (query budget 5→7)
- [x] `log_discovery()` now accepts `notes` parameter for annotations

## Completed (Sprint 15 Half 2 — 2026-03-25)

- [x] Alembic migration 013 for api_keys table
- [x] FastAPI auth middleware + `ua-ops create-api-key` CLI
- [x] API docs improvements (better docstrings for OpenAPI spec)
- [x] `/bulk-download` endpoint (CSV + GeoJSON export)
- [x] MCP server wrapping `/resolve` and `/utility/{pwsid}`

## Completed (Sprint 16 — 2026-03-25)

### Prerequisite: IOU URL Validation
- [x] Fixed orchestrator to include `url_discovered` PWSIDs and check for pending URLs before SearXNG
- [x] Validation run: ALL toolkit IOU URLs were 404 (American Water migrated to amwater.com/{state}aw/, Aqua uses single URL)
- [x] Corrected all IOU URLs, marked old entries dead, re-ran mapper with 228 corrected entries

### Deliverable 1: Deep Crawl
- [x] `ScrapeAgent._is_thin_content()` — heuristic detecting landing pages
- [x] `ScrapeAgent._follow_best_links()` — same-domain link scoring + following (max 3 links)
- [x] `ScrapeAgent._register_deep_url()` — inserts new registry row for deeper URL (preserves original)
- [x] No LLM, no search engine — pure HTTP + keyword heuristic

### Deliverable 2: IOU Subsidiary Name Database
- [x] `config/iou_subsidiaries.yaml` — maps local subsidiary names to parent company URLs
- [x] `_match_subsidiary()` in iou_mapper.py — normalized name comparison
- [x] 3 new matches found (Avon Water CT, Pinelands NJ, Beckley Water WV)
- [x] YAML has TODO markers for entries needing verification from 10-K filings

### Deliverable 3: Domain Guesser
- [x] `ops/domain_guesser.py` — generates county/name-based domain candidates, DNS-checks them
- [x] Integrated into DiscoveryAgent as first step before SearXNG
- [x] `ua-ops domain-guess` CLI command
- [x] `--domain-guess-only` orchestrator flag
- [x] Tested: found live domains for 3/3 VA test utilities
- [x] County-only patterns (no city column in SDWIS)

### Deliverable 5: Parse Retry
- [x] Retry with rate-search addendum when first attempt fails with `no_tier_1_rate` on substantive content
- [x] Same system prompt (cache hit) with modified user message

## Completed (Sprint 17 — 2026-03-25)

### Deliverable 1: IOU Subsidiary Database (SEC-sourced)
- [x] Replaced sparse `config/iou_subsidiaries.yaml` with SEC-researched version (82 named subsidiaries)
- [x] 12 parent companies, SDWIS name variants, confidence levels, corrected URLs
- [x] Updated `_load_subsidiary_database()` to read `named_subsidiaries` key + `sdwis_name_variants`
- [x] IOU mapper now produces 431 matches (228 pattern + 203 subsidiary), up from 231
- [x] New parent companies matched: Liberty Utilities, Golden State Water, CSWR, Nexus Water Group
- [x] Registry notes now distinguish "IOU pattern match" vs "IOU subsidiary match"

### Deliverable 2: City Data in SDWIS
- [x] Alembic migration 014: added `city VARCHAR(100)` to `utility.sdwis_systems`
- [x] SDWIS ingest updated to capture `CITY_NAME` from ECHO bulk CSV
- [x] Re-ran ingest: 44,633 systems loaded, 44,552 (99.8%) have city data
- [x] ORM model updated (`SDWISSystem.city`)

### Deliverable 3: Domain Guesser Pattern Update
- [x] Added 11 city-based patterns (highest priority: `{city}{state}.gov`, `cityof{city}.gov`)
- [x] Added hyphenated, .us, ci. prefix, cityof.net patterns from research
- [x] Added subdomain checks: `utilities.`, `water.`, `publicworks.` on confirmed base domains
- [x] `guess_urls()` now accepts `city` parameter
- [x] `run_domain_guessing()` query updated to include `s.city` from sdwis_systems
- [x] `config/domain_patterns.yaml` placed as reference documentation

### Deliverable 4: Fresh Export
- [x] Exported `data/sdwis_for_guessing.csv`: 21,197 rows with city_name column

## Completed (Sprint 17 Bugfix — 2026-03-25)

### Deep Crawl for Corporate Rate Landing Pages
- [x] `_is_thin_content()` now checks for precise dollar amounts ($X.XX with 2+ decimals), not just keyword presence
  - Corporate landing pages (keywords but no rate prices) → classified as thin → deep crawl activates
  - Actual rate schedules (keywords + rate prices) → classified as substantive → parsed directly
- [x] `ScrapeResult.raw_html` field added — raw HTML preserved for deep crawl link extraction
  - Previously, HTML was stripped to plain text before deep crawl, losing `href` attributes
  - `_follow_best_links()` now receives actual HTML, can find tariff PDF links
- [x] Smart PDF extraction for large tariff documents (20+ pages)
  - Extracts pages containing dollar amounts + rate keywords, not just first 15K chars
  - Cover page + TOC → skipped. Rate schedule pages → extracted. Up to 45K chars / 30 rate pages.
  - NJ AmWater tariff (130 pages): extracted 72 rate pages with actual $/unit data
- [x] Validated: Middlesex Water Company (NJ1225001) successfully parsed from tariff PDF via deep crawl

### Known Issue: American Water Link Scoring
- [ ] AmWater NJ rate page links to both the tariff PDF AND a 1,020-page rate case petition
  - Rate case petition scores higher (65 vs 60) because its link text ("here") contains "rate" in surrounding context
  - Deep crawl picks the rate case first; it passes thin-content check (has dollar amounts); tariff is never tried
  - Fix: boost score for links with "tariff" or "rate schedule" in text; penalize vague link text like "here" or "click here"
  - Affects all AmWater state subsidiaries (same page template). ~110 PWSIDs.

## Completed (Sprint 17 — Multi-Company IOU Test)

- [x] Tested 9 IOU companies (all except AmWater): Aqua/Essential, Aquarion, Artesian, CalWater, CSWR, Golden State, Liberty, Middlesex, Nexus, SJW
- [x] Only SJW/Maine Water parsed successfully — deep crawl found division-specific tariff PDFs
- [x] Root cause: IOU URLs are corporate homepages; rates are 2-3 levels deep or behind JS/district selection
- [x] Fix: lowered thin-content threshold from 3+ to 1+ dollar amounts; deep crawl now returns best candidate page even if still "thin" (rate-adjacent page > homepage for parsing)
- [x] Batch-processed 11 SJW/Maine Water divisions — 11/11 high confidence, $0.22 API cost
- [x] Coverage: 851 → 862 PWSIDs with rate data
- [x] Deferred 370 non-working IOU URLs to pending_retry (retry_after=2026-06-01)

### IOU Company Test Results

| Company | URLs | Result | Failure Mode |
|---------|------|--------|-------------|
| American Water | 107 | DEFERRED | Legal tariff format (known) |
| Aqua/Essential | 206 | DEFERRED | URL 404, needs new discovery |
| Aquarion | ~15 | DEFERRED | Needs 2-level deep crawl |
| Artesian | 1 | DEFERRED | Tariff page is link directory |
| CalWater | 6 | DEFERRED | District selection required |
| CSWR | 3 | DEFERRED | Subsidiary landing page |
| Golden State | 22 | DEFERRED | JS SPA, Playwright fails |
| Liberty | 14 | DEFERRED | Minimal homepage |
| Middlesex | 2 | DEFERRED | Needs 2-level deep crawl to tariff PDF |
| Nexus | 12 | DEFERRED | Corporate parent site |
| SJW/Maine Water | 11 | **SUCCESS** | Tariff PDFs found via deep crawl |
| SJW/CT Water | 4 | DEFERRED | Homepage, no rate links |
| SJW/TX | 3 | DEFERRED | URL 404 |

## Completed (Sprint 17b — Multi-Level Deep Crawl + Domain Guesser Import)

- [x] Multi-level deep crawl implemented (configurable depth, default 3)
  - Level 1: broad navigation scoring (water/utility/departments from homepage)
  - Level 2+: rate-focused scoring (rate/fee/tariff schedule pages)
  - Max 15 HTTP fetches per utility to prevent runaway crawling
  - Configurable via `config/agent_config.yaml` or `--max-depth` CLI flag
- [x] Validated: Juneau AK navigated homepage → utilities-division → rates-flat at depth 2
- [x] Regression: 3/3 previously-working utilities still pass (no unnecessary deep crawling)
- [x] Domain guesser results imported: VA (345 PWSIDs) + AK (89 PWSIDs) = 434 new URLs
  - Best URL per PWSID selected (preferring .gov domains, filtering bad redirects)
- [x] Domain guesser success rate: ~6% (1/16 tested). Low because:
  - Most URLs are city/county gov homepages, not water utility sites
  - Many cities outsource water to separate authorities (different domain)
  - JS-heavy CivicPlus/Granicus platforms block link extraction
- [x] Coverage: 862 → 866 PWSIDs (+4: 1 Juneau + 3 from VA batch)
- [x] Total session API cost: ~$0.85

## Completed (Sprint 18 — EFC FL API + WV PSC Ingest)

- [x] FL EFC API ingest: 281 records from 227 utilities via Topsail JSON API
  - Prototype for generic EFC API client (all 24 state dashboards use same platform)
  - API endpoint: `/dashboards/15/chart_data.json` — one call per utility, 1 req/sec
  - Bill curves at 500-gal increments → NC tier extraction logic reused directly
  - 10 utilities had no PWSID in API data, 1 PWSID not in CWS boundaries
  - Source key: `efc_fl_2020`, vintage: Raftelis 2020 survey
  - Avg bill @10CCF: $36.95 (range $13.91–$86.02)
- [x] WV PSC HTML scrape ingest: 241 records from 325 PSC-listed utilities
  - Scraped cost rankings at 3,400 and 4,000 gallon levels
  - 2-point bill curve → derived volumetric rate + base charge
  - Fuzzy name matching against SDWIS: 241/325 matched (74%), 42 unmatched, 42 duplicate PWSIDs
  - Source key: `wv_psc_2026`, vintage: March 2026 (current PSC rates)
  - Avg bill @10CCF: $100.84 (range $3.00–$251.07)
- [x] Key architectural finding: all 24 EFC dashboards use same Rails/Topsail platform with same JSON API
  - "Download Data" button returns 500 error on all dashboards (broken server-side)
  - JSON API is unauthenticated, returns full bill curves + PWSID + metadata per utility
  - Generic EFC module (Track B) can cover all 24 states with one API client + per-state config
- [x] CLI commands: `ua-ingest efc-fl [--dry-run] [--refresh]`, `ua-ingest wv-psc [--dry-run] [--refresh]`
- [x] WV PSC requires browser User-Agent header (blocks default httpx UA with 404)

## Completed (Sprint 18b — Generic EFC Module, 20-State Systematic Ingest)

- [x] Generic EFC module: `efc_generic.py` — one module, 20 states, 7,096 dashboard utilities
  - Auto-discovers utility IDs from dashboard HTML `<option>` elements
  - Per-state config in `config/efc_dashboards.yaml` (dashboard_id, vintage, source_key)
  - CLI: `ua-ingest efc --state WI`, `ua-ingest efc --all --skip-ingested`, `ua-ingest efc --list`
  - Handles variable bill curve increments (500-gal default, 1000-gal for AR, custom for SC)
  - Caches API responses per state at `data/raw/efc_{state}/api_cache.json`
- [x] All 20 EFC states ingested — 5,677 records from EFC + WV PSC:
  | State | Records | Avg @10CCF |
  |-------|---------|-----------|
  | AR | 599 | $48.76 |
  | IA | 570 | $60.40 |
  | WI | 569 | $54.72 |
  | GA | 488 | $41.88 |
  | NC | 403 | $60.52 |
  | OH | 367 | $64.57 |
  | MS | 359 | $40.61 |
  | AZ | 329 | $140.91 |
  | AL | 323 | $55.43 |
  | FL | 281 | $36.95 |
  | MA | 272 | $70.13 |
  | IL | 242 | $59.29 |
  | WV (PSC) | 241 | $100.84 |
  | NH | 167 | $97.86 |
  | CT | 151 | $63.44 |
  | ME | 144 | $45.83 |
  | MO | 73 | $44.82 |
  | HI | 69 | $55.45 |
  | DE | 24 | $40.36 |
  | SC | 6 | $31.31 |
- [x] Total database: **6,746 water_rates records, 6,120 unique PWSIDs** (up from ~1,391)
- [x] Zero API cost (all EFC JSON API, no LLM calls)
- [x] AZ has outlier max ($9,315) — likely EFC source data error
- [x] SC low yield (6 records) — 223/257 utilities lack PWSIDs in API

## Completed (Sprint 18c — Duke Reference Ingest + URL Extraction)

- [x] Duke/Nicholas Institute Water Affordability Dataset integrated (CC BY-NC-ND 4.0)
  - Git clone from GitHub: 10 states, 3,297 PWSIDs, 43,595 rate rows
  - **Track A (reference):** 3,178 records in `duke_reference_rates` (internal only)
    - Full tier structure with JSONB, bill calculations at 5/10/20 CCF
    - Handles NC non-standard PWSID format (dashed → EPA)
  - **Track B (URLs):** 6,384 utility URLs extracted, 3,718 gap-fill imported to scrape_registry
    - These point directly to rate pages — much higher quality than domain guesser
  - CLI: `ua-ingest duke-reference --state TX [--dry-run]`
- [x] Gap analysis: 2,372 PWSIDs have Duke data but no commercial source
  - TX: 723, KS: 411, PA: 324, WA: 244, NJ: 213, NM: 50, OR: 9 (100% gap-fill)
  - CA: 296 new, NC: 90 new, CT: 12 new (overlap states)
- [x] Combined coverage: 8,492 PWSIDs with any rate data (19.0% of 44,643 CWS)
  - Commercial: 6,120 PWSIDs (13.7%)
  - Duke reference: +2,372 PWSIDs (internal only)

## Completed (Sprint 18d — Source Provenance + Duke NIEPS Production Ingest)

- [x] **Source provenance schema** (migration 016): Added 16 columns to `source_catalog`:
  - Licensing: `license_spdx`, `license_url`, `license_summary`
  - Redistribution: `commercial_redistribution`, `attribution_required`, `attribution_text`, `share_alike`, `modifications_allowed`
  - Distribution tier: `tier` (free_open | free_attributed | premium | internal_only), `tier_rationale`
  - Temporal: `data_vintage`, `collection_date`
  - Provenance chain: `upstream_sources`, `transformation`
  - Citation: `citation_doi`, `source_url`
- [x] **Duke NIEPS production ingest** (`duke_nieps_ingest.py`) — writes to canonical `rate_schedules`:
  - 3,177 records across 10 states (TX:722, CA:667, NC:479, KS:411, PA:324, WA:244, NJ:213, CT:58, NM:50, OR:9)
  - Full rate structures: fixed charges (JSONB) + volumetric tiers (JSONB) + bill snapshots at 5/10/20 CCF
  - Unit normalization: handles both gallons and cubic feet (PA 18%, NJ 17%, CT 42% cubic feet)
  - `source_key = "duke_nieps_10state"`, `tier = "free_attributed"`, `confidence = "high"`
  - 114 PWSIDs skipped (not in CWS boundaries), 1 skipped (no extractable structure)
  - Conservation signal computed for multi-tier utilities
  - CLI: `ua-ingest duke-nieps --all [--seed-catalog] [--dry-run]`
- [x] **Legacy artifact:** `duke_reference_rates` table + `duke_reference_ingest.py` retained but superseded
- [x] **Source catalog seeding:** `--seed-catalog` flag populates `source_catalog` with full Duke provenance (SPDX, DOI, attribution text, tier rationale)
- [x] **SourceCatalog ORM model** updated with all 16 provenance fields

## Completed (Sprint 19b — NM NMED + VT EFC + Gap State Research)

- [x] **NM NMED rate survey** (`nm_nmed_rate_survey_2025`) → `water_rates`:
  - PDF: 186 utilities parsed, 176 with bill data, 175 matched (99.4% match rate)
  - Bill at 6,000 gal/month. Avg @10CCF: $60.55 (range $8-263)
  - CLI: `ua-ingest nm-nmed [--dry-run]`
- [x] **VT EFC dashboard** (`efc_vt_2021`) → `water_rates`:
  - Topsail JSON API: 187 utilities, 170 matched and inserted
  - Rates as of July 2021. First VT coverage in the database.
  - Used existing `efc_generic.py` — just added VT to efc_dashboards.yaml config
- [x] **Gap state research:** 16 states investigated, 12 skipped (no bulk rate data path),
  2 deferred (NJ WaterCheck, VA EFC 2019), 2 ingested (NM, VT)
- [x] Skip list updated with 13 new entries from gap state research

## Completed (Sprint 18 — Duke Batch + TX TML)

- [x] TX TML 2023 ingest: 476 cities matched to PWSIDs (98.8% match rate, $0 cost)
  - TX coverage: 0 → 485 PWSIDs (33.2% pop). Median bill $42 @5,000 gal, $68 @10,000 gal.
  - Outlier corrected (Gregory $4,141 → $41.41), 6 duplicates handled, 6 unmatched (apostrophes/tiny towns)
- [x] Duke URL batch processing started: 3,718 URLs across 7 gap-fill states (TX, KS, PA, WA, NJ, NM, OR)
  - ~15% success rate on non-dead URLs. 113 successes from 771 processed so far.
  - ~35% of URLs are dead (404) — expected from 4-5 year old URLs
  - Batch running in tmux `duke_batch`, ~2,947 remaining
- [x] Fixed LLM string-vs-float type errors in parse agent (crashed Duke batch twice)
- [x] Domain guesser automation running unattended: 21 states processed, 99 successes
- [x] Coverage: 6,170 → 6,780 PWSIDs, 38.7% → 44.6% population. 9 new states with data.

## Completed (Sprint 19a — IN IURC + KY PSC State Ingest)

- [x] **Indiana IURC** (`in_iurc_water_billing_2024`) → `water_rates`:
  - PDF table: 80 utilities parsed, 58 matched to SDWIS PWSIDs, 14 unmatched (small NFPs, IOU sub-areas)
  - Bill at 4,000 gal consumption. Avg bill @10CCF: $67.00 (range $11–$128)
  - 8 duplicate PWSIDs resolved (inside/outside city variants)
  - CLI: `ua-ingest in-iurc [--dry-run]`
- [x] **Kentucky PSC** (`ky_psc_water_tariffs_2025`) → `rate_schedules`:
  - IIS directory crawl: 136 directories, 134 PDFs downloaded, 98 parsed via Claude Haiku
  - 84 matched to SDWIS PWSIDs, 12 unmatched. 36 parse failures (wholesale-only, unusual formats)
  - Full JSONB tier structures. Median bill @10CCF: $76.66 (range $5–$333)
  - API cost: ~$0.10 (Haiku). 15.5 min total runtime.
  - CLI: `ua-ingest ky-psc [--dry-run] [--limit N]`
- [x] **Database state after both ingests:**
  - `rate_schedules`: 5,679 rows, 3,687 unique PWSIDs
  - `water_rates`: 6,804 rows, 6,178 unique PWSIDs
  - Two new states with coverage: IN (58 PWSIDs), KY (84 PWSIDs)

## Next (Sprint 19+)

### Duke 404 URLs → Domain Guesser Seeding (after Duke batch completes)
- [ ] Duke batch will produce ~500-700 dead (404) URLs with known utility PWSIDs and stale domains
- [ ] These are NOT throwaway — the domain is stale but the PWSID and utility identity are known
- [ ] Feed failed Duke domains into the domain guesser as additive seeds: use the utility name + state
  to guess the current domain (utility may have moved from `cityofirving.org/357/Rates` to `irvingtx.gov/water/rates`)
- [ ] Only run after Duke batch is fully complete (don't want to double-process pending URLs)
- [ ] Domain guesser is still running on VPS — no need to stop it. The seeding adds to the queue, not replaces it
- [ ] Expected yield: Duke 404 URLs are high-quality leads (researcher-verified utilities), so domain guesser
  hit rate on these should be higher than on blind SDWIS PWSIDs

### Data Quality
- [ ] AZ outlier review: $9,315 max bill @10CCF — investigate and flag
- [ ] NH outlier: $3,414 max — investigate
- [ ] SC low yield: 223 utilities lack SDWIS PWSIDs — name matching possible?

### WV PSC Name Matching Improvements
- [ ] 42 unmatched WV PSC utilities — manual PWSID mapping file
- [ ] Consider scraping tariff detail pages (325 URLs) for richer rate data

### TX TML Multi-Year Union (Optional)
- [ ] Ingest 2019-2021 XLSX files for cities not in 2023 — estimated +100-150 unique cities
- [ ] Each city gets most recent year's rates. Union across 2019-2024 = ~600-650 unique.

### IOU Parser Tuning (deferred companies)
- [ ] AmWater tariff parser: 130-page legal format needs tariff-specific parse prompt or structured PDF table extraction
- [ ] Aqua/Essential: needs fresh URL discovery (current URLs 404)
- [ ] Golden State: needs Playwright SPA rendering fix
- [ ] CalWater: needs district-aware crawling
- [ ] 370 IOU URLs total deferred to 2026-06-01

### Domain Guesser Improvements
- [ ] Automated pipeline running (tmux `guesser_sync`). ~2.2% parse success rate across 21 states.
- [ ] Main blocker: city gov homepages ≠ water utility sites. Many water utils are separate authorities
- [ ] URL selection fix applied: water-specific domains now prioritized over generic .gov
- [ ] ~29 states still running on VPS. Will auto-import as each completes.

### UAPI Rate Explorer Dashboard
- [x] Session 1 complete (2026-03-26): export pipeline + interactive map
  - `scripts/export_dashboard_data.py` — PostGIS → simplified GeoJSON (52 MB, 44,643 features)
  - React + Vite + MapLibre GL + Tailwind dashboard in `dashboard/`
  - Coverage choropleth (blue/amber/gray) + bill-at-10CCF color ramp
  - Click-to-inspect detail panel with tier breakdown + source info
  - Bottom coverage bar: 3,190/44,643 PWSIDs (7.1%), 31.9% pop coverage
  - Run: `python scripts/export_dashboard_data.py && cd dashboard && npm run dev`
  - Node 22 via nvm required (system Node v12 too old for Vite)
- [x] Session 2 complete (2026-03-26): settings, tier filtering, bill legend, deploy
  - Settings gear: data tier filter (free/premium/reference), opacity slider, visibility toggles
  - Data tier system: free (gov), premium (LLM-scraped), reference (Duke NIEPS internal)
  - Duke PWSIDs reclassified as reference-only (CC BY-NC-ND compliance)
  - Dynamic coverage bar: split progress bar (green/blue/amber), live stat updates
  - Three neutral bill color ramps (Teal, Violet→Indigo, Earth) — no good/bad valence
  - Dev Tools sidebar (Ctrl+Shift+D) for ramp comparison
  - Bill legend overlay with gradient bar + dollar labels
  - Detail panel: slide-in animation, sticky header, ESC close, zoom-to-feature
  - Deployed at http://100.103.211.71:9090/utility-rate-explorer/ (Tailscale)
  - Rebuild: `cd dashboard && npm run build` then restart uvicorn on 9090
  - Refresh data: `python scripts/export_dashboard_data.py` (re-run build after)
- [ ] Session 3: PMTiles optimization if GeoJSON performance is poor at scale
- [ ] Session 3: state summary view at national zoom
- [ ] Session 3: pick final bill color ramp (currently selectable via Dev Tools)
- [ ] Session 3: filter panel (state, population range, source, rate structure type)

### Sprint 20: Pipeline Hardening (2026-03-26)
- [x] **Fix 1:** Marked 1,394 duke_reference non-URL entries dead ("not found", "OS 1997", etc.)
- [x] **Fix 2:** Normalized 134 non-standard confidence values (partial→medium, success→high, etc.)
- [x] **Fix 3:** Extended deep crawl file extension skip list (+.docx, .xls, .jpeg, .svg, .pptx, etc.)
- [x] **Fix 4:** Playwright browser.close() wrapped in try/finally (prevents Chromium leaks)
- [x] **Fix 5:** Deep crawl now follows subdomain links (water.city.gov ↔ www.city.gov)
- [x] **Fix 6:** Deep crawl registration gated on same-domain + rate-relevant keywords
- [x] **Fix 7:** `ua-ops process-backlog` CLI — sweeps orphaned registry entries for parsing
- [x] **Fix 8:** BestEstimateAgent scoped to affected states only (skip_best_estimate flag for batch callers)
- [x] **Cleanup:** Marked 1,040 irrelevant deep crawl entries dead (norton.com, paris.fr, etc.)
- [x] **Deep crawl backlog run:** Parsed 1,183 rate-relevant deep crawl URLs → 254 new rates ($11.32 API cost)
- [x] Duke backlog run: 400 entries via `ua-ops process-backlog --source duke_reference` (running)
- [x] SearXNG orphan processing: 16 new rates from 30 entries ($0.57, 53% hit rate)

### Sprint 21: SearXNG Fix + Retarget (2026-03-27)
- [x] Port mismatch: verified config correct (8889 = VPS tunnel, 8888 = local Docker)
- [x] **Scoring v2:** domain authority (.gov +15), utility-name-in-domain (+15-25), aggregator penalty (-25)
- [x] **Failed search logging:** search_attempted_at column + search_log table (migration 017)
- [x] **Pipeline runs logging:** DiscoveryAgent now writes to pipeline_runs
- [x] **URL cap 3→1:** top result only (saves 2/3 of fetch+parse resources per PWSID)
- [x] **Scoring funnel diagnostics:** full funnel logged (raw→dedup→scored→written), --diagnostic mode
- [x] **Gap-state targeting:** orchestrator focuses on states with <20% coverage, skips recently-searched
- [x] **Query improvements:** added county water authority + consumer billing query templates
- [ ] Run scoring diagnostic for 1 week, then tune threshold based on near-miss data
- [x] Consider content caching (persist fetched text to disk/DB to avoid re-fetching on re-parse) → **Done in Sprint 23**
- [ ] Investigate 15K char truncation — targeted extraction for large tariff PDFs

### Sprint 23: Pipeline Flow Fix & Scraped Content Persistence (2026-03-28)
- [x] **Migration 018:** `scraped_text TEXT` + `url_quality VARCHAR(20)` on scrape_registry (one migration, both columns)
- [x] **Fix 1 — Text persistence:** ScrapeAgent persists raw text to DB on every fetch (initial + deep crawl). ParseAgent reads from DB when raw_text not passed in memory. Eliminates data loss between scrape and parse.
- [x] **Fix 3 — URL quality:** Auto-classified after parse: confirmed_rate_page, parse_failed, probable_junk, blacklisted, unknown. Backfilled from existing parse results. Sweeps skip blacklisted/probable_junk.
- [x] **Fix 5 — Unified chain:** `src/utility_api/pipeline/chain.py` with `scrape_and_parse()`. All callers updated: process_guesser_batch.py, parse_deep_crawl_backlog.py, process-backlog CLI, run_mn_discovery.py.
- [x] **Fix 2 — Triage CLI:** `ua-ops triage-backlog` — classifies backlog, shows rate-relevant vs junk breakdown, --execute to blacklist. Reusable after any bulk import.
- [x] **Fix 4 — Parse sweep daemon:** `scripts/parse_sweep.py` — polls every 30 min, parses unparsed entries with text in DB, batches BestEstimate per state. Run in tmux.
- [x] **Fix 6 — Logging:** URL quality distribution added to `ua-ops pipeline-health`. `process-backlog --dry-run` shows text availability and url_quality.

**Immediate next steps (post-Sprint 23):**
- [ ] Run migration: `alembic -c migrations/alembic.ini upgrade head`
- [ ] Run triage: `ua-ops triage-backlog` (preview), then `ua-ops triage-backlog --execute`
- [ ] Process backlog: `ua-ops process-backlog --max 120 --source searxng` (highest yield first)
- [ ] Start sweep daemon: `tmux new-session -d -s parse_sweep "cd ~/projects/utility-api && python scripts/parse_sweep.py --interval 1800 --max-per-sweep 25 2>&1 | tee -a logs/parse_sweep.log"`
- [ ] Verify pipeline health: `ua-ops pipeline-health` (check url_quality distribution)

### Sprint 24: Serper Integration — Replace SearXNG (2026-03-29)
- [x] **SerperSearchClient** (`src/utility_api/search/serper_client.py`): Thin API wrapper with usage tracking (per-query to `search_queries` table), budget guard (2,400 warning, 2,500 hard stop on free tier), retry logic (429/401), cost reporting.
- [x] **Migration 019:** `search_engine` column on `search_log`, ranked URL columns (`url_rank_1/2/3`, `score_rank_1/2/3`), new `search_queries` table (billing audit trail), `discovery_rank` + `discovery_score` on `scrape_registry`.
- [x] **DiscoveryAgent updated:** SearXNG replaced with Serper. LLM fallback scoring removed (Google results are high enough quality for keyword-only scoring). Query count reduced from 7 to 4. Top 3 URLs written with `discovery_rank` tagging. `url_source='serper'`. Inter-query delay reduced from 8s to 0.2s.
- [x] **Bulk discovery CLI:** `scripts/serper_bulk_discovery.py` + `ua-ops serper-discover`. Gap-state targeting, population sorting, budget guards, dry-run mode, progress logging.
- [x] **Monitoring:** `ua-ops serper-status` command — query usage, cost tracking, parse success by discovery rank, search funnel summary.
- [x] **Config:** `SERPER_API_KEY` + `SERPER_PAID_MODE` env vars in Settings. `agent_config.yaml` updated with Serper discovery block (4 queries/PWSID, 0.2s delay, top 3 URLs).

**Sprint 24 validation (2026-03-29):**
- [x] Dry run: 600 PWSIDs across 30 gap states
- [x] Small validation: 25 PWSIDs, 92% hit rate (far above 40-50% estimate)
- [x] 63 URLs scraped, 55 submitted to batch API (batch cancelled)
- [x] Direct API parse: 24 success, 35 failed, 1 skipped, 3 no text = 38% URL-level parse rate
- [x] PWSID-level: 15/23 (65%) got at least one successful parse

### Sprint 24b: Cascade Process Pipeline (2026-03-30)
- [x] **Migration 020:** `discovery_diagnostics` table for cascade tracking (per-PWSID: starting URLs, deep crawl children, total candidates, parse attempts, winning rank/source/score, full candidate JSONB)
- [x] **`pipeline/process.py`:** `process_pwsid()` orchestrator — deep crawl all 3 Serper URLs proactively (15 fetches each, 45 total), re-score all candidates with scoring v2, cascade parse top 3 until success
- [x] **`scripts/reprocess_failed_serper.py`:** Re-run failed PWSIDs through cascade
- [x] **Validated on 8 failed PWSIDs:** 1/8 succeeded (NYC was false PWSID match — Richmondville Village). Deep crawl found children for 4/8 PWSIDs. Aurora CO: deep_crawl child scored #1 (85) beating all Serper originals.

**Immediate next steps (Sprint 24b):**
- [x] Integrate `process_pwsid()` into `serper_bulk_discovery.py` with `--process` flag
  - Added `--process [immediate|batch]` — immediate cascades each PWSID after discovery, batch discovers all then cascades
  - 5-PWSID validation: 100% discovery, 80% cascade parse success
- [x] 150-PWSID diagnostic run launched: `tmux attach -t serper_diag` to monitor
  - Command: `python3 scripts/serper_bulk_discovery.py --max-pwsids 150 --process immediate --diagnostic`
  - Log: `logs/serper_diag_20260330_0000.log`
- [x] 150-PWSID diagnostic complete: 87/150 (58%) cascade parse success, 720 queries ($0.72)
  - **URL cap:** Keep top 3. Rank 2+3 contributed 43/87 successes (49%)
  - **Deep crawl:** Switched to reactive. Proactive added 33% fetches for 5% of wins (4/87)
  - **Cascade:** Keep 3-attempt cascade — rescued 30/87 (34%) that would have failed with single-parse
  - **CO analysis:** 12/16 failures from overlapping special districts, JS-heavy sites, .colorado.gov blocking. Hard state.
- [x] Reactive deep crawl implemented in `process_pwsid()` — crawl only if all 3 Serper URLs fail
- [x] Playwright escalation for thin high-confidence pages in ScrapeAgent
  - Thin (<2000 chars) + high-confidence URL → Playwright retry → nav link extraction → follow best link
  - Validated: fclwd.com 495 chars → nav crawl → recovered 8,637 char PDF
  - Monitoring: `notes LIKE '%playwright_reason%'` and `notes LIKE '%nav_crawl%'` on scrape_registry
- [ ] Address false PWSID-URL match issue (NYC → Richmondville Village, CO special districts)
- [ ] Remove SearXNG (deliverable 5): code removal + Docker cleanup
- [ ] Full gap-state sweep: ~440 PWSIDs with reactive cascade + Playwright (~1,756 free queries remaining)

### Sprint 25 — Completed (2026-03-30)
- [x] **source_url propagation:** Added `source_url` column to `rate_best_estimate` (migration 021). Threaded through `best_estimate.py` — scraped_llm rows now carry the original utility rate page URL for spot-checking. 3,852 rows with URLs, 5,810 properly NULL.
- [x] **Tier label backfill:** EFC/SWRCB/OWRS → `bulk`, scraped_llm → `premium`, Duke rationale updated. `source_catalog.tier` comment updated.
- [x] **Duke language cleanup:** Removed "INTERNAL REFERENCE ONLY" framing from `duke_reference_ingest.py`. Duke is a free-tier attributed source with usable 10CCF rates.
- [x] **Score threshold lowered 50→45:** Config-driven (was hardcoded). 1,219 near-miss URLs in 440 sweep would now qualify.
- [x] **Sprint 25 validation runs:** 440 gap-state sweep (64% success), ND 119 (62%), SD 141 (67%). ND went 2%→21%, SD 1%→21%.
- [x] **Coverage strategy report:** `docs/coverage_strategy_sprint25.md` — scenarios, state priorities, cost projections.
- [x] **Pipeline failure analysis updated:** `docs/pipeline_failure_analysis.md` — three-run comparison added.

### Sprint 25b — In Progress (Scenario A)
- [x] **Scenario A script:** `scripts/run_scenario_a.py` — batch API sweep for all gap >=3k
- [x] **Scenario A discovery complete:** 4,912 PWSIDs, 4,540 parse tasks submitted
  - Batch `msgbatch_01FhetQeo9TfoTkBroYFHT1T` — in_progress at Anthropic (submitted 2026-03-31 06:40 UTC)
  - **NOTE:** task_details missing from batch_jobs (VARCHAR(2) state_filter bug). Reconstruct before processing.
  - Process when complete: `python scripts/run_scenario_a.py --process-batch`
  - **After processing: rebuild best_estimate + re-export dashboard** (don't re-export before batch completes)
- [x] **Duke-only sweep:** 963 PWSIDs. 434 succeeded (47%), 508 remain Duke-only.
  - Batch `msgbatch_01AT77529EDstWZn3ygzR2ZH` processed. $4.59 cost.
  - Duke bill unit mismatch documented in `docs/duke_upgrade_batch_report.md`
  - Failure decomposition in `docs/duke_failure_decomposition.md`

### Sprint 25c — Parse Quality Fixes (2026-03-31)
- [x] **Canonical enum in parse prompt** — rate_structure_type constrained to 6 types
- [x] **Normalization map** — 100+ LLM variants → 6 canonical types (`src/utility_api/utils/rate_structure_normalize.py`)
- [x] **Bill consistency validator** — identical 5/10/20 CCF bills + non-flat → low confidence
- [x] **Duke-only → low confidence** in dashboard export, reference estimate caveat
- [x] **58 failed-but-wrote records cleaned** from rate_schedules
- [x] **Bill computation fix** — `_compute_bill` returns fixed charge for empty tiers, string rates cleaned. 116 records recovered.
- [ ] **Scenario A batch processing** — waiting on Anthropic. After processing: rebuild best_estimate, re-export dashboard, rebuild coverage_stats.

### Later
- [ ] Automate EPA CCR APEX form scraping
- [ ] Stripe/payment integration for API tiers
- [ ] Self-hosted LLM for discovery scoring (Llama 3.1 8B)
