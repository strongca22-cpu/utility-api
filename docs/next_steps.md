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

## Remaining for Sprint 1

- [ ] MDWD column mapping incomplete — `avg_monthly_bill_5ccf` and `avg_monthly_bill_10ccf` returned NULL for all records. The MDWD tab file has different column names than expected. Need to inspect the MDWD codebook to find the correct bill columns.
- [ ] county_served field is NULL in CWS data — the EPA feature service may have a county field under a different name, or it needs to be derived from spatial join to county boundaries
- [ ] Add `/health` endpoint with data vintage info (last pipeline run timestamps)

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
- [ ] Build `/provider/{id}` and `/site-report` endpoints

## Recommended Next Chat Prompt

```
Utility API Sprint 1 cleanup v1 — MDWD column mapping fix, county enrichment, data vintage endpoint. Start from docs/next_steps.md.
```
