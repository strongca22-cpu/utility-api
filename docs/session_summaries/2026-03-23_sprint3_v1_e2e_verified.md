# Session 4b: Sprint 3 v1 — End-to-End Verified + SearXNG

**Date:** 2026-03-23
**Focus:** Playwright + PDF extraction, Claude API end-to-end verification, SearXNG deployment

## What Was Built

### Playwright Auto-Fallback
- Scraper detects JS-heavy pages (<200 chars extracted) → auto-retries with headless Chromium
- Also triggers on HTTP 403 (bot blocks)
- CivicPlus remains unsolvable via scraping — their CMS routes headless browsers to wrong content

### PDF Extraction (pymupdf)
- Handles multi-page PDF rate schedules (tested on 53-page VA-American Water tariff)
- Text extraction → Claude parsing → bill calculation — full chain works
- PDF is the most reliable format for rate data (most utilities publish PDFs)

### SearXNG Self-Hosted Meta-Search
- Docker Compose at `~/searxng/` — SearXNG + Valkey (Redis fork)
- Aggregates Google, DuckDuckGo, Bing, Brave, Qwant, Startpage
- JSON API: `http://localhost:8888/search?q=...&format=json`
- Solved the DuckDuckGo rate-limiting problem completely
- Discovery module falls back to DuckDuckGo direct if container is down

### Curated URL File
- `config/rate_urls_va.yaml` — manually curated pwsid → url mappings
- CLI: `ua-ingest rates --url-file config/rate_urls_va.yaml`
- Bypasses search discovery for known URLs (reliable for CivicPlus utilities)

## Verified Results

Three utilities successfully parsed and stored to `utility.water_rates`:

| Utility | Source | Structure | Fixed | Bill@5CCF | Bill@10CCF | Confidence |
|---------|--------|-----------|-------|-----------|------------|------------|
| Blacksburg | GBPW PDF (1 page) | tiered | $28.00 | $31.34 | $48.19 | high |
| Alexandria | VA-American Water PDF (53 pages) | uniform | $15.00 | $28.03 | $41.06 | high |
| Arlington | VA statewide rate report PDF | tiered | $6.03 | $23.59 | $44.64 | medium |

Manual verification on Blacksburg: $31.34 calculated vs $31.33 expected — penny-level accuracy.

## Key Fixes
- Unit conversion: $/1000gal × 0.748 = $/CCF (was incorrectly × 7.48)
- Anthropic SDK: uses assistant prefill for JSON output (not `response_format`)
- Meter size coercion: "5/8" string → 0.625 float
- Removed stale PDF-skip logic in pipeline

## Infrastructure
- Docker installed on desktop (WSL)
- SearXNG running at `localhost:8888` — survives reboots via `restart: unless-stopped`
- ANTHROPIC_API_KEY in `~/projects/utility-api/.env`

## Scraping Success Rate Analysis

| Site Type | Discovery | Scrape | Parse | Path Forward |
|-----------|-----------|--------|-------|-------------|
| PDF rate schedules | SearXNG finds them | pymupdf extracts text | Claude parses well | **Best path** |
| Static HTML (Fairfax Water style) | SearXNG finds them | httpx + BS4 works | Claude parses well | Works already |
| CivicPlus .gov sites | SearXNG finds URLs | Playwright renders but CMS misroutes | Fails | Curated URLs + PDF links |
| 403-blocked sites | SearXNG finds URLs | Playwright sometimes bypasses | Mixed | Curated URLs |
