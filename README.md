# Utility Intelligence API

Water utility enrichment API for geographic lookup. Given a lat/lng, returns:
- Water utility identity (EPA CWS boundaries + SDWIS)
- Financial health (MDWD from Harvard Dataverse)
- Water stress risk (WRI Aqueduct 4.0)
- Regulatory permits (Sprint 2+)
- Rate schedules via LLM parsing (Sprint 3+)

## Quick Start

```bash
pip install -e "."
# Set DATABASE_URL in .env (shared PostGIS with strong-strategic)
alembic -c migrations/alembic.ini upgrade head
ua-ingest all     # Download and load all data sources
ua-api            # Launch API on port 8000
```

## Architecture

- **Database:** Shared PostGIS (`strong_strategic` database, `utility` schema)
- **API:** FastAPI on port 8000
- **Data:** EPA CWS boundaries, SDWIS, MDWD, WRI Aqueduct 4.0

## Sprint Status

- **Sprint 1 (current):** Foundation ingest — Layer 1 federal data in PostGIS, `/resolve` endpoint
- Sprint 2: State regulatory layers (VA DEQ, CA SWRCB)
- Sprint 3: LLM rate parsing (VA top utilities)
- Sprint 4+: See `docs/utility_enrichment_sprint_plan.md` in strong-strategic
