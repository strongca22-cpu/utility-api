# UAPI Ingest Brief — IN IURC + KY PSC Water Rates

Two new sources to ingest. Both are state government publications, `premium` tier, no PWSIDs — utility name fuzzy matching required against EPA SDWIS.

---

## Source 1 — Indiana IURC Annual Water Bill Analysis

**What it is:** Indiana Utility Regulatory Commission annual PDF listing all IURC-regulated water utilities with monthly bill at 4,000 gal.

**Download:** `https://www.in.gov/iurc/files/2024-Water-Billing-Survey-Final.pdf`

**Format:** 4-page text-selectable PDF table. Repeated header on each page. ~78 utility rows.

**Schema (columns left to right):**
- `Utility Name` — may have trailing `*` (means fire protection surcharge included — strip for matching, flag in data)
- `Ownership` — IOU / Municipal / NFP / C.D.
- `Last Rate Case Cause No.` — IURC docket number (ignore for ingest)
- `Order Date` — date of last rate order (store as `last_rate_order_date`)
- `Average Monthly Bill for 4,000 gal.` — dollar amount (e.g., `$49.75`)

**Parsing quirks:**
- Some entries are parent rows with sub-rows (e.g., Indiana American Water has "Area One*", "West Lafayette*", "Seymour*" etc. as separate lines under the parent). Treat each sub-row as its own record; concatenate parent + sub-row name for the utility name field.
- Skip blank/whitespace rows between sections.
- Normalize `$XX.XX` → float.

**Rate data type:** `bill_at_consumption` — monthly bill at 4,000 gallons. Map to `water_rates` table as `bill_amount_per_month` with `consumption_gallons = 4000`.

**PWSID matching:** Query EPA SDWIS for Indiana CWS (state = IN). Fuzzy match `utility_name` against SDWIS system names. For Indiana American sub-rows, use the geographic descriptor in the name (e.g., "Kokomo", "Muncie") to identify the correct PWSID.

**Source key:** `in_iurc_water_billing_2024`

**State:** IN only. ~65 estimated new PWSIDs.

---

## Source 2 — Kentucky PSC Water Tariff Directory

**What it is:** Kentucky Public Service Commission IIS file server hosting individual tariff PDFs for ~127 water districts, associations, and IOUs. Each PDF contains the full rate structure (fixed charge + volumetric tiers). Directly analogous to the WV PSC pipeline already in production.

**Directory root:** `https://psc.ky.gov/tariffs/water/Districts,%20Associations,%20%26%20Privately%20Owned/`

Note: The path contains a literal `&` — encode as `%26`. Spaces encode as `%20`.

**Directory structure:**
```
/Districts, Associations, & Privately Owned/
  Bath County Water District/
    Tariff.pdf        ← target file
    Water Shortage Response Plan.pdf
    [Cancelled Tariff Pages/]
  North Marshall Water District/
    Tariff.pdf
  ...
```

Enumerate all subdirectories from the IIS listing. For each: download `Tariff.pdf`, extract text, LLM-parse rate structure.

**Rate structure in each PDF:**
- Fixed monthly base charge (minimum bill) for 5/8" residential meter
- Volumetric tiers: gallons range + rate per gallon
- Some PDFs use CCF/HCF — convert to gallons (1 CCF = 748 gal)
- Sections labeled "Monthly Rates" or "Monthly Service Rate" are the target; ignore fire protection, wholesale, tap-on sections

**Sample (North Shelby Water Company Tariff.pdf, effective 7/1/2025):**
```
5/8 x 3/4 Inch Meters:
  First 2,000 gallons:   $24.03 minimum bill
  Next 3,000 gallons:    $0.00823/gal
  Next 5,000 gallons:    $0.00681/gal
  Next 40,000 gallons:   $0.00612/gal
```

**Rate data type:** Full tier structure → `rate_schedules` table (JSONB).

**PWSID matching:** Query EPA SDWIS for Kentucky CWS (state = KY). Fuzzy match directory subdirectory name against SDWIS system names. City-owned utilities (Louisville, Lexington, Bowling Green, etc.) are NOT in this directory and will not match — that is expected and correct.

**Source key:** `ky_psc_water_tariffs_2025`

**State:** KY only. ~110 estimated new PWSIDs.

---

## Reference — Similar Existing Pipeline

Both sources should follow patterns from the existing `wv_psc_2026` module:
- `wv_psc_2026` = IIS directory crawl → per-utility Tariff.pdf → LLM parse → PWSID fuzzy match → rate_schedules ingest

The KY source is a direct clone of `wv_psc_2026`. The IN source is simpler (single PDF, bill-at-consumption, goes to `water_rates` not `rate_schedules`).
