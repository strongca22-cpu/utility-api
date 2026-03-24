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
- **CA volume data**: `face_value_amount` (always Acre-feet/Year or NULL) and `max_diversion_rate` (7 different unit types) are stored as-is. Unit normalization deferred.

### Sprint 2 Data Quality Notes

- **VA DEQ has no volume/quantity fields** in any GIS layer — just administrative/spatial data.
- **CA eWRIMS face values can be extremely large** (e.g., 9.1M AFY for State Water Project) — these are aggregate permitted volumes for large infrastructure, not individual facility withdrawals.
- **1,572 CA records lack geometry** (2.5%) — APPLICATION_NUMBERs with no lat/lng in the flat file.
- **CA county data is in the POD Detail table** (not the flat file used for ingest) — county column is NULL for CA records. Could be enriched via spatial join to TIGER counties.

## Sprint 3 — LLM Rate Parsing

- [ ] Build utility website scraper (agent-driven URL discovery)
- [ ] Rate page content extraction (requests + Playwright for JS-heavy sites)
- [ ] Claude Batch API rate parsing with structured prompt
- [ ] MDWD 2022 baseline validation
- [ ] Populate `avg_monthly_bill_5ccf` and `avg_monthly_bill_10ccf` from parsed rates
- [ ] Build `/provider/{id}` and `/site-report` endpoints

## Future Enhancements (Parking Lot)

- [ ] CA county enrichment via spatial join to TIGER counties
- [ ] VA DEQ volume enrichment from CEDS permit documents
- [ ] Unit normalization for CA max_diversion_rate (7 unit types → standard)
- [ ] Cross-reference VA DEQ data center permits with Strong Strategic entity registry
- [ ] Additional states: TX TCEQ, AZ ADWR, OR WRD
- [ ] Stormwater pond identification from VPDES SWI_GP permits

## Recommended Next Chat Prompt

```
UAPI Sprint 2 v1 — CA county enrichment + VA DEQ entity cross-reference. Start from docs/next_steps.md.
```
