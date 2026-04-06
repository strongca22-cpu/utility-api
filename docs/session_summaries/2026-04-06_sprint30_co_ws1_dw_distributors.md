# Sprint 30 — CO WS1 Auto-Parse + Denver Water Distributor Methodology

**Date:** 2026-04-06
**Focus:** Close WS1 (CO gap auto-parse queue) + establish Denver Water distributor rate methodology
**Coverage delta:** CO 254 → 257 PWSIDs with estimates (+3 net, +5 newly loaded, +0 net Yuma, -2 wrong-entity removed)

---

## Context

Sprint 29 closed CO from 78.1% → 93.7% population coverage. 20 PWSIDs / 426k pop remained, organized into 4 workstreams (WS1 auto-parse, WS2 manual WebFetch, WS3 Denver Water AWC, WS4 wrong-entity locality scrape). This sprint targeted **WS1** and surfaced **WS3** as a methodology decision.

---

## WS1 Auto-Parse Queue (5 PWSIDs, ~56k pop)

### Diagnosis

The 5 PWSIDs had URLs in scrape_registry but zero successful parses. Root cause was **two compounding issues**:

1. **Parse_sweep stalled** — already-tried URLs failed (wrong entity contamination), and high-confidence locality_discovery URLs were stuck in `pending` status (parse_sweep only picks up `active`)
2. **Best URLs were 12-18k positions deep** in the parse queue (sources `serper`, `domain_guess`, `locality_discovery` all fall into priority bucket 7 alongside generic guesses)

### Resolution Pattern

Created `scripts/scrape_batch_ws1_co.py` — generalizable workflow:
1. Promote pending locality_discovery URLs (filter wrong-entity by domain blacklist)
2. Scrape via ScrapeAgent with `max_depth=0` (targeted, no deep crawl)
3. Select best per PWSID by content length + parse status
4. Submit as one Anthropic batch
5. Process results

### Outcomes

| PWSID | Pop | Final State | bill@10CCF | Notes |
|---|---|---|---|---|
| **CO0163020 Yuma** | 4,049 | DONE | $53.05 | parse_sweep had already parsed UTILITY RATES PDF; just needed best_estimate trigger |
| **CO0144005 Fort Morgan** | 12,000 | DONE | $125.07 | Citywide fee schedule (38k chars) was too broad — but Fort Morgan Times rate plan article + Archive PDF parsed at high confidence on retry batch |
| **CO0103614 Platte Canyon** | 19,485 | DONE (partial) | $43.61 | plattecanyon.org/2026 rates page parsed but **missed PC's $18 surcharge** and only captured Tier 1; flagged for surcharge spot-check |
| **CO0139600 Palisade** | 3,060 | DONE | $55.00 | Manually entered from Water Rate Notice 2025 PDF + KJCT8 news article corroboration. Flat $55/mo for first 5,000 gal, effective June 2025. Tagged `[MANUAL_WEBFETCH sprint30]` |
| **CO0107725 Superior MD No 1** | 17,900 | DEFERRED | — | Not on Denver Water distributor list. superiorcolorado.gov blocks all automated requests (Akamai-class CDN). Requires manual browser save or different approach. |

**WS1 net: 4 of 5 closed (38,594 pop). Superior deferred.**

---

## Wrong-Entity Contamination Cleanup

Discovered systematic issue: parser assigned multi-district rate documents to multiple PWSIDs incorrectly.

**Deleted 4 wrong-entity rate_schedules:**

| ID | PWSID | Source (wrong) | Was bill |
|---|---|---|---|
| 6694 | CO0130138 Bear Creek WSD | amwater.com/inaw (American Water Indiana) | $44.41 |
| 31640 | CO0130138 Bear Creek WSD | evergreenmetro.colorado.gov | $58.53 |
| 31647 | CO0130138 Bear Creek WSD | evergreenmetro.colorado.gov | $55.25 |
| 30477 | CO0116552 North Lincoln WSD | evergreenmetro.colorado.gov | $55.25 |

Also deleted 2 wrong-entity records for Palisade (mesacortinawater.colorado.gov, $76 and $79).

**Pattern:** Parser saw "multiple districts with identical structure" notes and propagated rate to all, despite only being valid for the source district.

---

## Denver Water Distributor Methodology (WS3 Foundation)

### Scope clarification

Original prompt assumed ~84 Denver Water distributors required special handling. Investigation showed reality is much smaller:

- **Total Service distributors (32):** Don't have own PWSIDs in SDWIS — customers served directly under Denver Water Board (CO0116001, 1.287M pop). **No work required.**
- **Read-and-Bill distributors (11):** 7 in SDWIS (4 had existing rates, 3 had nothing, 2 had wrong-entity contamination). **Bulk loader applies here.**
- **Master Meter distributors (21):** Set their own rates independently. **Normal scrape pipeline.**

### Methodology

Denver Water 2026 Read-and-Bill rate structure (Outside Denver):
- Fixed: $20.91/mo (5/8" & 3/4" meter)
- Tier 1 (0–AWC): $3.03/1,000 gal
- Tier 2 (AWC to AWC+15,000): $5.45/1,000 gal
- Tier 3 (>AWC+15,000): $7.26/1,000 gal
- **AWC assumption: 5,000 gal** (DW published minimum, conservative — produces higher bill estimates)

Computed bills (no surcharge):
- bill_5ccf  (3,740 gal): $32.24
- bill_10ccf (7,480 gal): $49.58
- bill_20ccf (14,960 gal): $90.34

### Bulk Loader

`scripts/load_denver_water_distributors.py` — inserts DW base rates as fallback for distributors lacking real scraped data. Each insert tagged `[denver_water_read_and_bill_base]` for downstream surcharge enrichment.

**Inserted 5 PWSIDs (134,748 pop):**

| PWSID | Pop | Status |
|---|---|---|
| CO0103721 Southgate WSD | 55,000 | DW base ($49.58), known surcharge +$14.97 pending |
| CO0103723 SW Metropolitan WSD | 48,648 | DW base ($49.58), surcharge unknown pending |
| CO0130138 Bear Creek WSD | 30,000 | DW base ($49.58), wrong-entity cleaned, surcharge pending |
| CO0116552 North Lincoln WSD | 1,000 | DW base ($49.58), wrong-entity cleaned, surcharge pending |
| CO0103186 Country Homes | 100 | DW base ($49.58), surcharge pending |

**Flagged for surcharge spot-check:**
- CO0103614 Platte Canyon ($43.61) — partial parse, missing $18 surcharge
- CO0103100 Willows ($53.16) — willowswater.org high-confidence parse, may already include surcharge

---

## Coverage Impact

| Metric | Before | After |
|---|---|---|
| CO PWSIDs with rates | 254 | 257 |
| CO PWSIDs total tracked | 255 | 258 |
| Net new pop covered (this sprint) | — | ~138k (Palisade 3k + Yuma 4k + Fort Morgan 12k + Platte Canyon 19k + 5 DW distributor inserts 134k = correctly 173k since Platte Canyon was previously $0/no_rate) |

---

## Key Findings

### 1. parse_sweep daemon priorities
The `serper` and `locality_discovery` URL sources fall into priority bucket 7 (default fallback) in `parse_sweep.py`. They're targeted, high-confidence sources but get the same priority as generic `domain_guess`. **Future fix:** add these to the priority dict at bucket 3-4.

### 2. Multi-district rate document contamination
When parser sees "multiple districts with identical structure", it propagates the rate to all matched PWSIDs even when only one is valid. This created the Bear Creek / North Lincoln / Palisade contamination. **Future fix:** parser should refuse to write to multiple PWSIDs from one document, or require explicit per-PWSID confirmation.

### 3. Denver Water distributor scope is smaller than feared
Total Service distributors don't have separate PWSIDs in SDWIS — they're rolled into Denver Water Board's PWSID. Only Read-and-Bill (7 in SDWIS) and Master Meter (21, mostly already covered) need separate handling.

### 4. AWC = 5,000 is the right anchor
Using DW's published minimum (rather than estimated typical) gives a defensible, conservative number. Tier 1 ends at 5k, putting more consumption in Tier 2 = higher (more conservative) bill estimates for water risk assessment.

### 5. Akamai/CDN-class blocking remains a systemic issue
superiorcolorado.gov blocks all automated requests including WebFetch — same pattern as Las Vegas Akamai blocks identified in Sprint 29. Affects CO0107725 (Superior MD No 1) and likely others. Manual browser save is the only known workaround.

---

## Files Created/Modified

### Created
- `scripts/scrape_batch_ws1_co.py` — WS1 scrape + batch parse workflow (reusable pattern)
- `scripts/load_denver_water_distributors.py` — DW Read-and-Bill bulk loader
- `docs/session_summaries/2026-04-06_sprint30_co_ws1_dw_distributors.md` — this file

### Modified
- `dashboard/public/data/coverage_stats.json` — auto-refreshed by best_estimate runs
- `src/utility_api/agents/scrape.py` — minor (pre-existing change)

### Database
- Inserted 6 new `rate_schedules` rows (Palisade + 5 DW distributors)
- Deleted 6 wrong-entity `rate_schedules` rows (Mesa-Cortina x2, Evergreen Metro x3, American Water Indiana x1)
- 2 batch_jobs submitted/processed: `msgbatch_01Fr17D8jBHQGB6dd1Axm5jp` (5 tasks), `msgbatch_01PYtnX2qkapRRtvozsEwg7R` (4 tasks), plus `msgbatch_01P96CC5c1QVTtw4ompQkKxK` (1 Palisade backup)

---

## Next Steps (deferred to fresh chats)

### Immediate
1. **DW Read-and-Bill surcharge lookup** — scrape each of the 5 newly-loaded distributors for their local surcharge. Known anchors:
   - Southgate: +$14.97 (confirmed)
   - Platte Canyon: +$18.00 (confirmed, needs to be added to existing parse)
   - SW Metro: + infrastructure fee (amount unknown)
   - Bear Creek, North Lincoln, Country Homes: unknown
2. **Superior MD No 1 (CO0107725, 17.9k pop)** — investigate non-Akamai paths: manual browser save, archive.org snapshots, or alternative public records (resolution PDFs filed with Boulder County)

### WS2 — Manual WebFetch (deferred)
- Loveland (95k pop) — lovelandwaterandpower.org/resident/rates-charges-and-fees
- Aspen (31k pop) — aspen.gov/203/Water or aspen.gov/185/Utility-Billing

### WS4 — Locality Scrape for 10 wrong-entity PWSIDs (deferred)
- Berthoud, East Larimer County WD, Brush, Parkville, etc. — see Sprint 29 audit doc

### Methodology improvements (deferred)
- Add `serper` and `locality_discovery` to parse_sweep priority dict (current: bucket 7, should be 3-4)
- Add safeguard to ParseAgent against multi-PWSID propagation from single document
- Consider Master Meter distributor coverage audit (21 distributors, most already have rates)
