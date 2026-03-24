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

## Sprint 2 — State Regulatory Layers

- [ ] VA DEQ GeoHub: ArcGIS REST client for VWP permits, VPDES discharge permits
- [ ] CA SWRCB eWRIMS: water rights permits, diversions
- [ ] Normalize permits to canonical schema
- [ ] Build `/permits` endpoint: lat/lng + radius → all permits

## Sprint 3 — LLM Rate Parsing

- [ ] Build utility website scraper (agent-driven URL discovery)
- [ ] Rate page content extraction (requests + Playwright for JS-heavy sites)
- [ ] Claude Batch API rate parsing with structured prompt
- [ ] MDWD 2022 baseline validation
- [ ] Populate `avg_monthly_bill_5ccf` and `avg_monthly_bill_10ccf` from parsed rates
- [ ] Build `/provider/{id}` and `/site-report` endpoints

## Recommended Next Chat Prompt

```
UAPI Sprint 2 v0 — VA DEQ + CA SWRCB permit layers and /permits endpoint. Start from docs/next_steps.md.
```
