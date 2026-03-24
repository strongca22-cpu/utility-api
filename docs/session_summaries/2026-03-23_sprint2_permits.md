# Session Summary — Sprint 2: State Regulatory Permit Layers

**Date**: 2026-03-23
**Session**: 3 (Sprint 2 v0 → v1)

## What Was Done

Built and deployed the permits layer — ingesting state regulatory permit data from VA DEQ and CA SWRCB into a canonical `utility.permits` table, with a spatial `/permits` endpoint for radius queries.

## Data Sources

### VA DEQ (EDMA ArcGIS MapServer)
- **VWP Individual Permits** (Layer 192): 1,727 features → 1,467 rows (excl. Residential). Key finding: 41 permits tagged `Data Center`, 215 tagged `Water Withdrawal`.
- **VWP General Permits** (Layer 193): 8,129 features → 4,387 rows (incl. Industrial, Commercial, Municipal, Mining, Agricultural, Other).
- **VPDES Outfalls** (Layer 119): 10,665 discharge permit outfalls.
- **No volume data** exposed in any VA DEQ GIS layer.

### CA SWRCB eWRIMS (data.ca.gov CKAN API)
- **Demand Analysis Flat File**: 63,806 records with lat/lng + volume data.
- **Uses and Seasons**: 83,502 records with USE_CODE (joined on APPLICATION_NUMBER).
- **Targeted load**: 45,011 rows after excluding Domestic use rights.
- **Volume data**: `face_value_amount` (Acre-feet/Year) + `max_diversion_rate` (7 unit types).
- **ArcGIS REST service is bot-blocked** — all data accessed via CKAN datastore API.

## Architecture

### Database
- `utility.permits` table (Alembic migration 004)
- Two-tier category system: `source_category` (as-delivered) + `category_group` (normalized bucket)
- Category mapping config: `config/category_mapping.yaml`
- `use_codes` stored as JSONB list for CA multi-use rights
- `raw_attrs` JSONB preserves full original record from each source

### Category Groups
industrial, energy, municipal, mining, commercial, institutional, environmental, agricultural, water_withdrawal, infrastructure, other

### API
- `GET /permits?lat=X&lng=Y&radius_km=10` — spatial radius query with optional `category_group` and `source` filters
- Returns distance_km from query point, ordered by proximity

### CLI
- `ua-ingest va-deq` — VA DEQ (VWP IP + GP + VPDES)
- `ua-ingest ca-ewrims` — CA eWRIMS (flat file + uses/seasons join)
- Both added to `ua-ingest all` (steps 6–7)

## Key Design Decisions

1. **`water_withdrawal` is its own bucket** — not assumed industrial until cross-referenced with other data.
2. **Targeted CA load** — excluded Domestic. All other use types retained (including agricultural, environmental) for contextual value.
3. **Multi-use as list** — CA rights with multiple USE_CODEs stored as JSON list, not one row per use. Exception: if volumes are use-specific, would need separate rows.
4. **Category priority** for multi-use rights: industrial > energy > municipal > mining > environmental > water_withdrawal > infrastructure > agricultural > commercial > other.
5. **This is OUR endpoint** — we ingest state data, normalize it, and serve it spatially enriched. Combined with `/resolve`, users get comprehensive water regulatory context for any point.

## Files Created/Modified

### New Files
- `migrations/versions/004_add_permits_table.py`
- `src/utility_api/models/permit.py`
- `src/utility_api/ingest/va_deq.py`
- `src/utility_api/ingest/ca_ewrims.py`
- `src/utility_api/api/routers/permits.py`
- `config/category_mapping.yaml`

### Modified Files
- `src/utility_api/api/app.py` — added permits router
- `src/utility_api/api/schemas.py` — added PermitRecord, PermitsResponse
- `src/utility_api/cli/ingest.py` — added va-deq, ca-ewrims commands; updated `all` to 7 steps

## Sprint 2 Enrichment (v1, same session)

### County Enrichment
- CA: 43,438 permits filled via spatial join to TIGER counties
- VA VPDES: 10,736 permits filled (entire layer had no county data)

### Permit-Facility Cross-Reference
- `permit_facility_xref` table (migration 005)
- 30 DC permits matched to SS facilities (23 high, 5 medium, 2 low confidence)
- 11 flagged as `data_center_candidate` — new facility locations not in SS DB
- Script: `scripts/populate_permit_xref.py` (idempotent, rerunnable)

### Unit Normalization
- `max_diversion_rate_gpd` column (migration 006)
- 24,156 CA records normalized from 7 unit types to gallons/day
- Conversion factors: CFS×646,317 | GPM×1,440 | GPD×1 | AFY×893 | AF×325,851

### Facility Permits Endpoint
- `GET /facility/{facility_id}/permits` — returns linked permits + nearby permits
- Two response sets: linked (from xref with match metadata) and nearby (spatial radius)
- Example: Microsoft Boydton → 4 linked DC permits + 64 nearby within 15km

### Additional Files Created
- `migrations/versions/005_add_permit_facility_xref.py`
- `migrations/versions/006_add_normalized_rate_column.py`
- `src/utility_api/models/permit_facility_xref.py`
- `scripts/populate_permit_xref.py`

### Strategic Insight
Water, wastewater, and energy permits are public record across all states. Scraping these three sources should fill out the DC database beyond OSM/imagery detection — permits are filed before construction, making them a leading indicator for facility discovery. The 11 unmatched candidates prove this concept.

## Verified Working

- VA DEQ ingest: 16,519 permits loaded
- CA eWRIMS ingest: 45,011 permits loaded
- `/permits` endpoint tested with Ashburn VA and Sacramento CA
- `/facility/{id}/permits` tested with Microsoft Boydton and Loudoun facilities
- `/health` endpoint shows data vintage for all 8 pipeline steps
- 41 data center VWP permits visible, 30 matched to SS facilities, 11 candidates flagged
- County coverage: 98%+ across all permits
- GPD normalization: 24,156 CA records converted

## Commits

```
918532c  Sprint 2: VA DEQ + CA SWRCB permit layers and /permits endpoint
64e277d  Sprint 2 enrichment: county join, DC cross-reference, rate normalization
e50c05a  Update next_steps with Sprint 2 enrichment results and DC candidates
c1af209  Add /facility/{id}/permits endpoint + xref population script
```
