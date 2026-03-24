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

## Sprint 3 — LLM Rate Parsing

- [ ] Build utility website scraper (agent-driven URL discovery)
- [ ] Rate page content extraction (requests + Playwright for JS-heavy sites)
- [ ] Claude Batch API rate parsing with structured prompt
- [ ] MDWD 2022 baseline validation
- [ ] Populate `avg_monthly_bill_5ccf` and `avg_monthly_bill_10ccf` from parsed rates
- [ ] Build `/provider/{id}` and `/site-report` endpoints

## Future Enhancements (Parking Lot)

- [ ] VA DEQ volume enrichment from CEDS permit documents
- [ ] Validate 11 data_center_candidate permits (imagery review → confirm/reject)
- [ ] Additional states: TX TCEQ, AZ ADWR, OR WRD (water, wastewater, energy permits as facility discovery)
- [ ] Stormwater pond identification from VPDES SWI_GP permits
- [ ] Cross-reference matched DC permits → enrich SS facility records with permit IDs
- [ ] Face value unit normalization (AFY → GPD) for cross-comparison with diversion rates

## Current API Surface

| Endpoint | Purpose |
|----------|---------|
| `GET /resolve?lat=X&lng=Y` | Water utility + SDWIS + MDWD + Aqueduct for a point |
| `GET /permits?lat=X&lng=Y&radius_km=10` | All permits within radius (filters: `category_group`, `source`) |
| `GET /facility/{id}/permits` | Linked + nearby permits for an SS facility |
| `GET /health` | Data vintage for all 7 pipeline steps |

## Database State (as of Session 3)

| Table | Rows | Source |
|-------|------|--------|
| `utility.cws_boundaries` | 44,643 | EPA CWS |
| `utility.aqueduct_polygons` | 68,506 | WRI Aqueduct 4.0 |
| `utility.sdwis_systems` | 3,711 | EPA ECHO (VA + CA) |
| `utility.mdwd_financials` | 225 | Harvard Dataverse (VA + CA) |
| `utility.county_boundaries` | 3,235 | Census TIGER |
| `utility.permits` | 61,530 | VA DEQ (16,519) + CA eWRIMS (45,011) |
| `utility.permit_facility_xref` | 41 | 30 matched + 11 candidates |
| `utility.pipeline_runs` | 8 | Audit trail |

## Recommended Next Chat Prompt

```
UAPI Sprint 3 v0 — LLM rate parsing: utility website scraping + Claude Batch API for water rate extraction. Populate avg_monthly_bill columns. Start from docs/next_steps.md.
```
