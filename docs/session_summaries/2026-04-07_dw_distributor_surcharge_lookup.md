# DW Read-and-Bill Distributor Surcharge Lookup (Sprint 30 follow-up)

**Date:** 2026-04-07
**Focus:** Add documented local surcharges to the 5 Denver Water Read-and-Bill distributor base rates loaded in Sprint 30, plus the partial Platte Canyon parse and a Willows methodology check.
**Coverage delta:** No new PWSIDs covered; **rate accuracy improvement on 5 PWSIDs / 154,133 pop** that were previously sitting at the unsurcharged DW base ($49.58 @10CCF) or a sub-base partial parse.

---

## Context

Sprint 30 (2026-04-06) loaded Denver Water's 2026 Read-and-Bill base rates ($20.91 fixed + 3-tier $3.03/$5.45/$7.26, AWC=5,000 gal → bill_10ccf=$49.58) for 5 distributors as a contractually-valid floor. Each distributor adds a local surcharge on top — known anchors at the time were Southgate (+$14.97) and Platte Canyon (+$18.00). This chat resolves the surcharges for all 5 newly-loaded distributors plus the Platte Canyon partial parse and an evaluation of Willows.

The original prompt: `docs/chat_prompts/dw_read_and_bill_surcharge_lookup_v0.md` (drafted at end of Sprint 30).

---

## Outcomes

| PWSID | District | Pop | Surcharge | bill_10ccf was → now | Source |
|---|---|---|---|---|---|
| **CO0103721** | Southgate WSD | 55,000 | **+$14.97/mo** | $49.58 → **$64.55** | [southgatedistricts.org/157](https://southgatedistricts.org/157/Monthly-Water-Service-Charges) |
| **CO0103723** | SW Metro WSD | 48,648 | **+$12.00/mo** | $49.58 → **$61.58** | [swmetrowater.org/2026-budget-highlights](https://swmetrowater.org/2026-budget-highlights/) |
| **CO0103614** | Platte Canyon WSD | 19,485 | **+$18.00/mo** | $43.61 → **$67.58** | [plattecanyon.org/2024-water-and-sewer-rates](https://plattecanyon.org/2024-water-and-sewer-rates/) |
| **CO0130138** | Bear Creek WSD | 30,000 | **+$8.00/mo** | $49.58 → **$57.58** | [bearcreekwater.org/services](https://www.bearcreekwater.org/services) |
| **CO0116552** | North Lincoln WSD | 1,000 | **+$18.00/mo** CIS | $49.58 → **$67.58** | [northlincolnwsd.colorado.gov](https://northlincolnwsd.colorado.gov) |
| CO0103186 | Country Homes Land Co | 100 | (no change) | $49.58 (unchanged) | No web presence found |
| CO0103100 | Willows WD | ~19,000 | (no change) | $53.16 (unchanged) | Standalone integrated rates, NOT a DW pass-through |

**5 of 7 updated. 154,133 population now reflects what customers actually pay rather than the unsurcharged DW pass-through.**

---

## Per-distributor notes

### CO0103721 Southgate WSD — +$14.97/mo (confirmed)
3/4-inch tap monthly service charge from the district's Monthly Water Service Charges page. Tap-size scaling table follows AWWA equivalent ratios (1" = $29.94, 1.5" = $59.88, etc.). Page explicitly states "Service charges will be billed by Denver Water on a monthly basis on behalf of Southgate Water District." Effective 1/1/2026. Clean confirmation.

### CO0103723 Southwest Metropolitan WSD — +$12.00/mo (newly discovered)
Discovered via the district's 2026 Budget Highlights page (not the news-arts/2026-rates page, which covers only DW pass-through). Quote: *"The monthly service fee is proposed to stay at $12 per 3/4″ meter for 2026. This represents no change from the prior year."* Historical: $7 (pre-2020) → $8 (2020) → $12 (2022) → $12 (2026). The district's billing-information page does NOT list this number; the budget highlights page is the only public source.

### CO0103614 Platte Canyon WSD — +$18.00/mo (confirmed, partial parse overwritten)
"Infrastructure Fee" for 5/8-3/4 inch meters per the district's 2024 water and sewer rates page (effective 1/1/2024, increased from a prior $12). The 2026 rates page on the same domain only republishes Denver Water's pass-through tiers and does NOT disclose the local Infrastructure Fee — so the most recent confirmed value is from 2024. Used as-is, with a note that the 2026 fee may have changed silently.

The pre-existing rate_schedules row (id=31629) was a partial parse that captured only Tier 1 of the DW structure (no max_gal) and therefore produced bill_10ccf=$43.61 — an under-statement *below* the pure DW base of $49.58. The script overwrites the row with the full DW 3-tier structure plus the $18 Infrastructure Fee, jumping bill_10ccf from $43.61 → $67.58 (+$23.97 net).

### CO0130138 Bear Creek WSD — +$8.00/mo (newly discovered, with caveat)
$8.00/month per Single Family Residential Equivalent (SFRE), collected via Denver Water billing for water distribution system O&M. **Source conflict noted:** the /services page lists $8.00 (with prior rate of $7.00); the /billing-and-payment page still lists $7.00. Going with $8.00 (the higher and more recently described value) and documenting the discrepancy in parse_notes. Worth a re-check next year.

### CO0116552 North Lincoln WSD — +$18.00/mo CIS (newly discovered, with methodology flag)
$18.00/month per SFE Capital Improvement Surcharge, administered and billed by Denver Water on the District's behalf, **effective January 1, 2026** (newly approved at the November 2025 budget hearing).

The district also charges a separate **$110.00 per quarter ($36.67/mo) service rate** covering combined "water and sanitation operations" — this fee is NOT included in the bill update because the water/sewer split is not disclosed. Including it would significantly over-state the water bill (a true upper bound would be bill_10ccf=$67.58 + $36.67 = $104.25). Conservative call: surface this in parse_notes and leave a follow-up flag.

**Methodology flag:** the source language references the "Total Service Agreement", which per the Sprint 30 methodology should mean the district has no separate PWSID and is rolled into Denver Water Board (CO0116001). North Lincoln IS in SDWIS as CO0116552, so either the language is loose or this is a hybrid case. Worth a methodology re-review before hardening anything that depends on the Total Service vs Read-and-Bill distinction.

### CO0103186 Country Homes Land Co — no change (no web presence)
Population ~100, SDWIS name "COUNTRY HOMES LAND CO" suggests a private subdivision developer rather than a public district. WebSearch returned no `.gov`, `.org`, or `.colorado.gov` page. parse_notes annotated with the search attempt and date so the next operator doesn't repeat it. Future approaches: contact Denver Water directly (303-893-2444) or pull Arapahoe County records.

### CO0103100 Willows Water District — no change (NOT a DW pass-through)
Critical methodology finding: Willows IS a Denver Water Read-and-Bill distributor (per the loader script's RB_DISTRIBUTORS list), but its public rate page at willowswater.org publishes its OWN integrated 4-tier rate structure (base $10.96/mo, Tier 1 $5.64/1000gal — *well above* DW's $3.03 Tier 1). The PDF makes no mention of Denver Water or pass-through. **The existing high-confidence parse (id=1535, bill_10ccf=$53.16) IS the customer's full bill**, not a DW base needing a surcharge added.

Implication for the methodology: not all Read-and-Bill distributors publish their rates as "+surcharge over DW". Some (Willows, evidently) publish a fully integrated rate even though they're technically passing DW rates through. The bulk loader's "DW base + surcharge" pattern should be applied only to distributors whose published rates explicitly cite DW pass-through. Willows would have been a wrong-pattern application if it had been auto-loaded — fortunately the existing parse blocked it.

---

## Files Created/Modified

### Created
- `scripts/apply_dw_distributor_surcharges.py` — idempotent updater encoding all 5 surcharges with sources, dry-run support, and `--refresh-best-estimate` chaining
- `docs/session_summaries/2026-04-07_dw_distributor_surcharge_lookup.md` — this file

### Database
- 5 `rate_schedules` rows updated (Southgate, SW Metro, Platte Canyon, Bear Creek, North Lincoln) — fixed_charges JSONB now has a second line item for the local fee, bill_5/10/20ccf recomputed as DW base + surcharge, parse_notes re-tagged `[denver_water_read_and_bill_plus_surcharge]` with citation
- 1 `rate_schedules` row annotated only (Country Homes) — parse_notes appended with search-attempted note
- `rate_best_estimate` refreshed for CO (258 PWSIDs, 257 with estimates; the 5 updates all flowed through to the consumer view)

### Backups
- Pre-write Tier 2 snapshot taken via `ua-ops snapshot --reason dw_distributor_surcharge_update` (4 csv.gz files in `~/backups/utility-api/snapshots/`)
- Auto-snapshot from BestEstimateAgent fired during the post-update refresh — confirms the Tier 2 wiring works end-to-end

---

## Coverage Impact

| Metric | Before | After |
|---|---|---|
| CO PWSIDs with rates | 257 | 257 (unchanged — accuracy improvement, not coverage) |
| Population whose bill_10ccf is now post-surcharge | 0 | **154,133** |
| Median CO bill_10ccf | (was ~$60) | **$62.01** |
| Mean CO bill_10ccf | (was ~$74) | **$75.14** |

(Net upward shift in CO median/mean reflects the corrected pricing for 154k population that was previously under-stated.)

---

## Methodology Findings

### 1. Not all Read-and-Bill distributors publish "DW + surcharge"
Willows publishes a fully integrated rate that doesn't reference DW. This breaks the assumption that the bulk loader's pattern is universally applicable. The loader's RB_DISTRIBUTORS list should have a flag indicating which distributors have their own integrated rates (skip the loader for those). At minimum, document this so future Read-and-Bill loaders (other parent utilities like Northern Water) don't blindly apply the "base + surcharge" pattern.

### 2. The 2024 vs 2026 page inversion at Platte Canyon
The PC 2024 page documents the local Infrastructure Fee. The PC 2026 page documents only DW pass-through and does NOT republish the local fee. Lesson: scrapers landing only on the most-recent rate page may miss the local addition entirely. The bulk loader and the surcharge updater together work around this by encoding the surcharge separately.

### 3. Source freshness ranking on district websites is inconsistent
Bear Creek's /services page and /billing-and-payment page disagree ($8.00 vs $7.00). SW Metro's billing page lacks the local fee entirely; only the budget highlights page lists it. There is no consistent "rate page" pattern across districts — pickup of the right page can require multiple targeted searches per distributor.

### 4. North Lincoln's Total Service language is anomalous
The Sprint 30 methodology classified Total Service distributors as having no separate SDWIS PWSID (rolled into CO0116001). North Lincoln has its own PWSID AND uses Total Service Agreement language. Either the methodology buckets are leaky or North Lincoln is a special case. Worth re-checking before hardening any logic that depends on the Total Service / Read-and-Bill / Master Meter trichotomy.

### 5. The Tier 2 backup system saved real worry
Both the manual pre-write snapshot AND the auto-snapshot inside BestEstimateAgent fired correctly during this work. The recovery point exists if any of these updates need to be reverted. The system shipped yesterday is operationally proven on a real mutation today.

---

## Open Follow-ups

### Immediate
1. **Country Homes Land Co (CO0103186, 100 pop)** — contact Denver Water billing or Arapahoe County records to confirm whether any local fee exists. Lowest priority by population, but the only 100-pop "private water provider in SDWIS" gap.
2. **Re-verify Platte Canyon $18 Infrastructure Fee for 2026** — the most recent confirmed value is from a 2024 page; check 2025/2026 board meeting minutes or budget docs.
3. **Re-verify Bear Creek's $7 vs $8 surcharge discrepancy** — pick the canonical value and reconcile or update the stale page.

### Methodology
4. **Add an `integrated_rates_published` flag to RB_DISTRIBUTORS** in `scripts/load_denver_water_distributors.py` so future loaders skip distributors like Willows that publish their own fully integrated rates.
5. **Re-examine the Total Service vs Read-and-Bill classification** for North Lincoln, given the Total Service Agreement language on its 2026 rates page.
6. **Generalize the "base + surcharge" pattern** for other parent utilities (Northern Water, Aurora Water Wholesale, etc.) once we move beyond Denver Water.

### Out of scope (still deferred)
- Superior MD No 1 (CO0107725) — Akamai-blocked, separate chat needed
- WS2 (Loveland, Aspen) — separate chat
- WS4 (10 wrong-entity locality scrapes) — separate chat
