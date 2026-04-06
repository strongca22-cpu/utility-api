# State PUC/PSC Ingest Modules — Build Per-State Tariff Ingest v0

## Context

The discovery + scrape pipeline handles individual utility websites well, but state public utility commissions (PUCs/PSCs) regulate hundreds of investor-owned and cooperative water utilities centrally. PUCs are an underused source category — they publish authoritative tariff data, but each state has its own filing system and we currently scrape them ad-hoc on a per-PWSID basis (which misses the bulk discovery opportunity).

This task builds **per-state PUC ingest modules** following the pattern of existing bulk source ingests (UNC EFC, Duke, Tennessee `myutility.us`). Each module knows the state's filing system, fetches the full list of regulated utilities, downloads tariff documents, and registers them in `scrape_registry` with PWSID linkage.

A prior research-oriented prompt (`research_gap_states_puc.md`) investigated whether such sources exist for top gap states. **This prompt is the implementation follow-up** — assume sources exist and build the ingestion code.

## Why This Matters

Discovered during Sprint 29 NV investigation: GBWC (Great Basin Water Co.) is a NV PUC-regulated utility serving multiple PWSIDs across 4 NV counties. The complete tariff is filed at `myutility.us` as 8 PDFs covering 258 sheets (rules + rate schedules). Standard discovery returns one of these PDFs but the parser misses rate data because:

1. Most rate schedule pages are scanned images (require OCR)
2. The same tariff covers multiple PWSIDs in the registry — we'd parse it 5+ times
3. Per-PWSID rate matching requires knowing which schedule applies to which subdivision

A per-state PUC ingest would solve all three: download tariff once, OCR once, parse once, link to all affected PWSIDs via a name/area lookup.

## Background — Existing Bulk Source Pattern

Look at how other bulk sources are integrated:
- `src/utility_api/ops/ccr_ingester.py` — EPA CCR APEX form scraper
- `src/utility_api/ingest/efc_*.py` — UNC EFC dashboard ingests (multiple states)
- `scripts/ingest_*_curation.py` — manual curation patterns
- Tennessee `myutility.us` — Utilities Inc subsidiary handling (see `scrape_registry` entries with `myutility.us` URLs)

Each bulk source:
1. Fetches a list of utilities/filings from a state authority
2. Resolves each to a PWSID (name match against `sdwis_systems`)
3. Downloads source documents (PDF, HTML, or API JSON)
4. Registers in `scrape_registry` with `url_source='<source_key>'`
5. Submits to parse pipeline

## Targets — State PUCs by Tier

Tier by ease of access. Build Tier 1 first (structured APIs), then Tier 2 (HTML-scrapeable lists), defer Tier 3 (paper-era PDF systems).

### Tier 1: Structured APIs / Open Data

| State | PUC | Notes |
|-------|-----|-------|
| **CA** | CA PUC | EnergySafe and water IRRA portal. Class A/B/C water utilities. ~100+ regulated. |
| **IL** | IL ICC | eDocket system has API. Water utilities filed under "Water and Sewer." |
| **OH** | PUCO | DIS (Docket Information System) has filterable web interface, possibly API. |
| **NY** | NYS DPS | Has DMM (Document and Matter Management) portal. NY PSC regulates 50+ water IOUs. |

### Tier 2: HTML-Scrapeable Filing Lists

| State | PUC | Notes |
|-------|-----|-------|
| **NC** | NC Utilities Commission | Has docket search; tariffs published as PDFs. Carolina Water Service is major. |
| **CO** | CO PUC | Class A water utilities filed via E-Filings system. |
| **NV** | NV PUC | `pucweb1.state.nv.us/puc2/Dktinfo.aspx?Util=Water`. ~15-20 active water dockets. Most rate schedules are scanned (require OCR). |
| **PA** | PA PUC | Aqua PA, PA American Water, etc. PUC has filing system. |
| **NJ** | NJ BPU | Regulates NJ American Water, SUEZ NJ, Aqua NJ. |
| **VA** | VA SCC | Aqua VA, VA American Water. |

### Tier 3: Defer (Paper-Era Systems)

| State | PUC | Notes |
|-------|-----|-------|
| MT, WY, ID, NM, AK | Various | Limited or no online access. Need manual cross-reference. |

## Architecture Plan

### New module per state PUC

```
src/utility_api/ingest/puc/
├── __init__.py
├── base.py          # PUCSource base class with common scrape/register methods
├── nv_puc.py        # Nevada PUC docket ingest
├── nc_puc.py        # NC Utilities Commission
├── co_puc.py        # CO PUC E-Filings
├── ca_puc.py        # CA PUC water IRRA
└── (etc.)
```

### Base class interface

```python
class PUCSource:
    state_code: str
    source_key: str  # e.g., 'nv_puc_docket'

    def list_regulated_utilities(self) -> list[dict]:
        """Return [{name, docket_id, utility_type, tariff_urls}]"""
        ...

    def resolve_pwsids(self, utility_name: str) -> list[str]:
        """Match against sdwis_systems, return PWSID(s)"""
        ...

    def fetch_tariff(self, url: str) -> bytes:
        """Download tariff document"""
        ...

    def register(self, pwsid: str, url: str, content: bytes):
        """Insert into scrape_registry with url_source=self.source_key"""
        ...

    def run(self):
        """End-to-end: list → resolve → fetch → register"""
        ...
```

### CLI integration

```bash
ua-ingest puc-source nv     # Run NV PUC ingest
ua-ingest puc-source --all  # Run all PUC sources
```

### Database

- New `source_catalog` entry per state PUC (state, source_key, type='puc_docket', frequency='quarterly')
- `scrape_registry.url_source` values: `nv_puc_docket`, `nc_puc_docket`, etc.
- `pipeline_runs` tracks each ingest run

## Open Questions for Planning Phase

Before implementation, resolve:

1. **OCR integration:** Many state PUCs file tariffs as scanned PDFs. Tesseract is now installed (Sprint 29). Should the PUC ingest module call OCR inline, or hand off to a separate OCR step? PyMuPDF supports `page.get_textpage_ocr()` directly.

2. **Tariff-to-PWSID mapping:** Some tariffs cover multiple PWSIDs (e.g., GBWC covers 5+ NV PWSIDs across 4 counties). Need a strategy:
   - Option A: Register the same scraped_text against multiple PWSIDs (storage overhead, parse repetition)
   - Option B: Build a `tariff_pwsid_xref` table linking parsed rate schedules to PWSIDs
   - Option C: Parse once into `rate_schedules`, link to multiple PWSIDs via `customer_class` or `service_area` columns

3. **Update cadence:** PUC tariffs change when rate cases settle (1-3 yr cycles). Should ingest run quarterly? Annually? On-demand when discovery returns no rates?

4. **Coverage estimate:** Before building, estimate how many gap PWSIDs each PUC source would cover. NV PUC covers ~6.4k pop in current gaps (3 PWSIDs). The big wins are likely NY (50+ IOU water utilities), CA (~100), PA, NJ (American Water subsidiaries).

## First Sprint Scope

1. **Build `PUCSource` base class** in `src/utility_api/ingest/puc/base.py`
2. **Implement `nv_puc.py`** as the proof of concept (small dataset, already familiar from Sprint 29 work)
3. **OCR integration:** Add helper for scanned PDF extraction using `page.get_textpage_ocr()`
4. **Test against the 3 NV PWSIDs** (NV0000920, NV0005032, NV0000300) — validate end-to-end works
5. **Document the pattern** in a `puc_ingest_module_template.md` so future state modules can be built from it

Defer to subsequent sprints:
- Tier 1 states (CA, IL, OH, NY) — bigger lift, need API exploration
- Tier 2 expansion (NC, CO, PA, NJ, VA)
- Tariff-to-PWSID mapping schema redesign

## Key Files / References

- `src/utility_api/ingest/efc_*.py` — existing bulk source pattern
- `src/utility_api/agents/scrape.py` — `_extract_rate_links()` and `ScrapeAgent.run()` for scrape registry conventions
- `src/utility_api/ops/coverage.py` — for measuring gap impact before/after
- `docs/chat_prompts/research_gap_states_puc.md` — prior research on which states have viable PUC sources
- `docs/session_summaries/2026-04-03_sprint29_nv_targeted_investigation.md` — Sprint 29 NV findings (the GBWC case study)
- Sprint 29 fix: `scrape.py:_extract_rate_links()` scoring improvements

## Notes

- This is a multi-sprint initiative. Don't try to ship all 10 states in one chat.
- **Plan first, build second.** Discuss the base class interface and the tariff-to-PWSID mapping strategy *before* writing code. Methodology decisions matter more than implementation speed.
- Sprint 29 confirmed Tesseract is installed and pymupdf has built-in OCR support. Use that.
- The NV PUC docket database (`pucweb1.state.nv.us/puc2/Dktinfo.aspx?Util=Water`) only has post-October-2023 documents. Older filings show metadata only.
- Tennessee `myutility.us` is already partially integrated — that's a private operator portal, not a PUC. Different pattern but worth referencing.

## Success Criteria

- One working state PUC ingest (NV) end-to-end
- Documented base class pattern that other state modules can copy
- At least 1 Sprint 29 NV gap PWSID covered via the new ingest (proof it works)
- Coverage uplift metric: `+N PWSIDs, +M pop` that wouldn't be covered by standard discovery
