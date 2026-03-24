# Session Summary — Sprint 1 Foundation Ingest

**Date:** 2026-03-23
**Duration:** ~1 session
**Objective:** Build the utility-api repo and load all Layer 1 federal data into PostGIS with a working `/resolve` endpoint.

## What Was Built

### Repository: ~/projects/utility-api/
- Separate repo, sibling to strong-strategic
- Shares PostGIS database (`strong_strategic` DB, `utility` schema)
- Python package: `src/utility_api/`
- CLI: `ua-ingest` (data loading), `ua-api` (API server)
- API: FastAPI on port 8000

### Database Schema (utility.*)
| Table | Rows | Source |
|-------|------|--------|
| cws_boundaries | 44,643 | EPA ArcGIS Feature Service (paginated, ~44K polygons nationally) |
| aqueduct_polygons | 68,506 | WRI Aqueduct 4.0 GDB (reused from strong-strategic raw data) |
| sdwis_systems | 3,711 | EPA ECHO bulk download (VA + CA only) |
| mdwd_financials | 225 | Harvard Dataverse MDWD (VA + CA municipal systems) |
| pipeline_runs | 4 | Audit log |

### Ingest Pipeline
- `ua-ingest aqueduct` — loads from existing GDB (~5s)
- `ua-ingest cws` — downloads from EPA ArcGIS REST API, paginated 2K features/page (~20 min)
- `ua-ingest sdwis` — downloads ECHO bulk ZIP (~400MB), filters to target states
- `ua-ingest mdwd` — downloads from Harvard Dataverse API

### API Endpoint
- `GET /resolve?lat=X&lng=Y` — single PostGIS spatial query returns:
  - CWS identity (pwsid, name, state, population)
  - SDWIS enrichment (system type, water source, owner, violations)
  - MDWD financials (median income, bill data — sparse)
  - Aqueduct water risk (stress score + label, drought, depletion, etc.)

## Validation Results

20 addresses tested (10 VA, 10 CA):
- **100% CWS match** — every location resolved to a water utility
- **100% Aqueduct match** — every location got water stress scores
- **75% PWSID match** — 5 "misses" are actually correct utilities with different PWSID numbering than expected

Key results:
- Loudoun Water (Ashburn VA): Low stress (0.16), surface water, 334K population
- LADWP (LA): Extremely High stress (5.0), surface water, 3.86M population
- EBMUD (Oakland): Low-Medium stress, 1.44M population
- San Diego: Extremely High stress, 1.39M population

## Technical Decisions

1. **Separate repo** — utility-api is a different product with different runtime needs
2. **Shared PostGIS** — same database, `utility` schema for clean separation
3. **CWS download approach** — paginated ArcGIS REST API with GeoJSON caching. Pages 36K-44K cause 504 timeouts on 2K batch; fell back to 500-record pages for tail end.
4. **Aqueduct reuse** — loaded same GDB from strong-strategic raw data, no re-download needed

## Known Issues

- MDWD bill columns (avg_monthly_bill_5ccf, avg_monthly_bill_10ccf) return NULL — the MDWD tab file uses different column names. Needs codebook inspection.
- county_served field is always NULL — EPA feature service may have it under a different field name
- PWSID widened to VARCHAR(50) because some EPA records have compound PWSIDs (semicolon-separated)
- source_type column widened to TEXT because some values exceed 30 chars

## Files Created/Modified

All files in ~/projects/utility-api/ — this is a new repo. Key files:
- `src/utility_api/ingest/{aqueduct,cws,sdwis,mdwd}.py` — data ingestion
- `src/utility_api/api/routers/resolve.py` — /resolve endpoint
- `src/utility_api/models/` — SQLAlchemy ORM models
- `migrations/versions/001_create_utility_schema_and_tables.py`
- `scripts/validate_addresses.py` — Sprint 1 validation
