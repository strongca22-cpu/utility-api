# Parse Failure Curation — High-Value URLs

These are URLs that successfully scraped (HTTP 200, >1K chars) but failed the LLM parse step. They are ordered by population served. Each URL points to what appears to be an actual rate page — these are the highest-value parse failures to investigate and fix.

**Action needed:** Review each URL manually. Determine whether the page actually contains parseable rate data, and if so, what's preventing the parser from extracting it (JS rendering, PDF format, rate calculator, ambiguous layout, etc.).

---

## Candidates

### 1. NYC DEP — 8.3M pop
- **PWSID:** NY7003493
- **Utility:** NEW YORK CITY SYSTEM
- **URL:** https://www.nyc.gov/site/dep/pay-my-bills/how-we-bill-you.page
- **Scraped:** 3,978 chars | Parse: failed (confidence: partial)
- **Source:** metro_research
- **Likely issue:** NYC DEP rates may be on a different subpage (rates vs "how we bill you"), or the rate structure is embedded in a format the parser didn't recognize.

### 2. Denver Water — 1.3M pop
- **PWSID:** CO0116001
- **Utility:** DENVER WATER BOARD
- **URL:** https://www.denverwater.org/business/billing-and-rates/2025-rates
- **Scraped:** 10,107 chars | Parse: not attempted
- **Source:** curated_denver_cos
- **Likely issue:** URL path says "business" — this may be commercial rates. Residential rates may be at a different URL. Also 10K chars scraped but parse never ran (batch processing issue?).

### 3. Atlanta Watershed — 1.1M pop
- **PWSID:** GA1210001
- **Utility:** ATLANTA
- **URL:** https://atlantawatershed.org/waterrates/
- **Scraped:** 2,159 chars | Parse: not attempted
- **Source:** curated_atlanta
- **Likely issue:** Only 2,159 chars scraped from a known rate page — likely JS-rendered content that Playwright didn't fully capture. The waterrates page is known to have rate tables.

### 4. Orlando Utilities Commission — 536K pop
- **PWSID:** FL3480962
- **Utility:** ORLANDO UTILITIES COMMISSION
- **URL:** https://www.ouc.com/account/service-rates-costs/
- **Scraped:** 10,921 chars | Parse: not attempted
- **Source:** metro_research
- **Likely issue:** OUC is a major utility with a known rate page. 10K chars scraped — content may include water + electric combined, making rate extraction ambiguous.

### 5. Colorado Springs Utilities — 464K pop
- **PWSID:** CO0121150
- **Utility:** COLORADO SPRINGS UTILITIES
- **URL:** https://www.csu.org/rates/
- **Scraped:** 3,633 chars | Parse: not attempted
- **Source:** curated_denver_cos
- **Likely issue:** CSU serves water, electric, gas, and wastewater from one page. Parser may need to isolate water-only rates.

### 6. Virginia Beach — 438K pop
- **PWSID:** VA3810900
- **Utility:** VIRGINIA BEACH, CITY OF
- **URL:** https://virginiabeach.gov/connect/blog/new-fees-rates-effective-july-1
- **Scraped:** 7,929 chars | Parse: failed (confidence: high)
- **Source:** curated
- **Likely issue:** Parser returned HIGH confidence but still marked failed — this suggests a validation issue (rate values outside expected bounds?) rather than a content problem. Worth investigating the actual parse output.

### 7. Liberty Utilities NY (Lynbrook) — 220K pop
- **PWSID:** NY2902835
- **Utility:** LIBERTY UTILITIES NEW YORK - LYNBROOK
- **URL:** https://new-york-water.libertyutilities.com/all/residential/rates-and-tariffs/water-rates-new-york-water.html
- **Scraped:** 6,129 chars | Parse: failed (confidence: failed)
- **Source:** metro_research
- **Likely issue:** Liberty Utilities is a regulated IOU — rates may be in a tariff PDF linked from this page rather than displayed as HTML text.

### 8. Westminster CO — 202K pop
- **PWSID:** CO0101170
- **Utility:** WESTMINSTER CITY OF
- **URL:** https://www.westminsterco.gov/317/Water-Rates
- **Scraped:** 15,042 chars | Parse: not attempted
- **Source:** curated_denver_cos
- **Likely issue:** 15K chars is substantial content. Parse was never attempted — likely a batch processing gap. This should be re-run.

### 9. Modesto CA — 219K pop
- **PWSID:** CA5010010
- **Utility:** MODESTO, CITY OF
- **URL:** https://www.modestogov.com/1056/Water-Rates
- **Scraped:** 1,377 chars | Parse: not attempted
- **Source:** curated
- **Likely issue:** Only 1,377 chars from a CivicPlus-style URL. The rate page likely uses JS/iframe to render the actual rate table. May need Playwright re-scrape.

### 10. Arvada CO — 172K pop
- **PWSID:** CO0130001
- **Utility:** ARVADA CITY OF
- **URL:** https://www.arvadaco.gov/DocumentCenter/View/467/2025-Meter-Rates-PDF
- **Scraped:** 1,108 chars | Parse: not attempted
- **Source:** curated_denver_cos
- **Likely issue:** This is a PDF link (DocumentCenter/View). The scraper may have fetched the HTML wrapper page rather than extracting the PDF content. PDF extraction pipeline needed.

### 11. Thornton CO — 226K pop
- **PWSID:** CO0101150
- **Utility:** THORNTON CITY OF
- **URL:** https://www.thorntonco.gov/city-services/utility-billing/hydrant-meter-water-sewer-rates
- **Scraped:** 1,020 chars | Parse: not attempted
- **Source:** curated_denver_cos
- **Likely issue:** Only 1,020 chars — likely a navigation page with links to actual rate documents. Deep crawl may not have followed the right link.

### 12. Santa Ana CA — 311K pop
- **PWSID:** CA3010038
- **Utility:** CITY OF SANTA ANA
- **URL:** https://www.santa-ana.org/water-rate-study/
- **Scraped:** 1,176 chars | Parse: not attempted
- **Source:** curated
- **Likely issue:** "Rate study" page may describe the methodology rather than listing actual rates. The operational rate schedule may be elsewhere.

---

## Patterns Observed

1. **"Not attempted" parses (7 of 12):** These scraped successfully but the parse step never executed. This appears to be a batch processing gap — the scrape completed but the parse tasks weren't submitted or failed silently.

2. **JS-rendered content (3 of 12):** Atlanta, Modesto, and Thornton all have low char counts from pages known to have rate tables. Playwright may need longer wait times or explicit element targeting.

3. **PDF links treated as HTML (1 of 12):** Arvada's DocumentCenter URL is a PDF that the HTML scraper can't handle. Needs PDF extraction path.

4. **Multi-utility pages (2 of 12):** Colorado Springs and OUC serve water + electric from the same page. The parser needs to isolate water rates.

5. **Wrong page variant (1 of 12):** Denver Water's "business" rates URL vs residential.

## Recommended Actions

- **Re-run parse** on items 2, 4, 5, 8, 10, 11, 12 (the "not attempted" ones)
- **Investigate Virginia Beach** (#6) — high confidence but failed validation
- **Re-scrape with Playwright** for Atlanta (#3) and Modesto (#9)
- **Add PDF extraction** for Arvada (#10) and similar DocumentCenter links
- **Find residential URL** for Denver Water (#2) and NYC (#1)
