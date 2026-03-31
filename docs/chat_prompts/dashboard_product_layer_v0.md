# Dashboard Product Layer Redesign — v0

## Context

The UAPI dashboard currently has three data tiers visible in both Product and QA mode: **Premium** (LLM-scraped), **Free/Attributed** (Duke NIEPS, CC BY-NC-ND), and **Reference** (Duke-only, low confidence). The sidebar checkboxes, coverage bar, map choropleth, and detail panel all expose this three-tier distinction.

The problem: **the Product/QA mode distinction doesn't yet reflect the actual commercial product structure.** In Product mode, a customer sees "Premium" and "Free/Attributed" as separate tiers — but from a product perspective, both are part of the licensed commercial product. The source distinction (LLM-scraped vs Duke-attributed) is QA-level provenance detail, not product-facing information.

### The Commercial Model

- **Product tier**: All PWSIDs with usable rate data — both Premium (LLM-scraped) and Free/Attributed (Duke). This is the dataset a customer pays for. The map should show these as one unified "covered" layer.
- **Free tier**: Not yet defined in the dashboard. Future scope.
- **Reference/Low confidence**: Duke-only PWSIDs with unreliable bill estimates. Shown with a caveat.
- **No data**: No rate information at all.

### What Needs to Change

In **Product mode**, the dashboard should present a simplified two-category view:
1. **Covered** (green) — any PWSID with rate data, regardless of whether the source was LLM or Duke-attributed
2. **No data** (gray) — PWSIDs without rate data

In **QA mode**, the full source breakdown stays: Premium, Free/Attributed, Reference, No Data — with all the provenance detail, source URLs, confidence, etc.

---

## Tasks

### Task 1: Product Mode — Unified Coverage Layer

**Sidebar changes (Product mode only):**
- Replace the three tier checkboxes (Premium, Free/Attributed, Reference) with a single toggle or simplified view:
  - ☑ Covered (combines Premium + Free/Attributed)
  - ☑ Reference (stays separate — these are low-confidence Duke estimates)
  - ☐ No data (off by default)
- Coverage stats show: `Covered: X / 44,643` and `Pop: XM (X%)`
- No source-level breakdown in Product mode

**Map choropleth (Product mode):**
- Coverage view: two colors — green (covered) and gray (no data). Reference stays amber.
- Bill view: unchanged (bill amount gradient regardless of source)

**Detail panel (Product mode):**
- Do NOT show source_key, source_name, or source_tier
- DO show: utility name, PWSID, location, population, rate structure, tier table, bill estimates
- Provenance badge: just "COVERED" or "REFERENCE" — not "PREMIUM" vs "FREE"
- The reference estimate caveat stays for Duke-only PWSIDs

### Task 2: QA Mode — Full Source Breakdown (unchanged)

QA mode keeps everything as-is:
- Four tier checkboxes: Premium, Free/Attributed, Reference, No Data
- Map colors: green (free/govt), blue (premium/LLM), amber (reference), gray (no data)
- Detail panel shows full provenance: source_key, source_name, source_tier, source_url, confidence, parse_model, selection_notes
- County comparison, variance, flag button — all stay

### Task 3: CoverageBar — Mode-Aware

- Product mode: show `Covered: X` (single number), `Pop: XM (X%)`
- QA mode: show the existing breakdown with free/premium/reference segments

---

## Key Files

- `dashboard/src/components/Sidebar.jsx` — tier checkboxes, mode-aware rendering
- `dashboard/src/components/Map.jsx` — fill color expression switches by mode
- `dashboard/src/components/DetailPanel.jsx` — provenance display in Product mode
- `dashboard/src/components/CoverageBar.jsx` — mode-aware stats display
- `dashboard/src/utils/colors.js` — may need a "covered" color (or reuse green)
- `dashboard/src/hooks/useDynamicStats.js` — compute unified "covered" count for Product mode
- `dashboard/src/contexts/DashboardContext.jsx` — mode state already exists

## What NOT to Do

- Do not change the GeoJSON export or data pipeline — the `data_tier` field stays as-is (free/premium/reference/null). The Product mode simplification is purely a frontend display concern.
- Do not remove QA mode functionality — all source detail stays in QA.
- Do not add new API endpoints or data files.
- Do not change the underlying rate selection logic (best_estimate, source_priority).

## Implementation Notes

The mode toggle already exists in the sidebar (`appMode: "product" | "qa"`). The changes are:
1. Sidebar conditionally renders different checkbox sets based on `appMode`
2. Map `coverageFillExpression()` takes `appMode` as parameter — in Product mode, both `free` and `premium` tiers map to the same green color
3. DetailPanel conditionally hides source provenance in Product mode
4. CoverageBar conditionally simplifies its display
5. `useDynamicStats` optionally merges free+premium into a single "covered" count

This is a display-layer change only. The data model, export pipeline, and parse pipeline are untouched.
