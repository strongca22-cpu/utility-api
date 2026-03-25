# Domain Guesser — Sprint 17 Update Report

**Date:** 2026-03-25
**Module:** `src/utility_api/ops/domain_guesser.py`
**CLI:** `ua-ops domain-guess [--state VA] [--max 50] [--dry-run]`
**Reference config:** `config/domain_patterns.yaml`

---

## What Changed

Sprint 16 built the domain guesser with county-based and utility-name-based patterns only. Sprint 17 adds city-based patterns (highest priority), subdomain checks, and additional TLD coverage (.us, .net, hyphenated .gov). The city data is now available because Sprint 17 also added `CITY_NAME` from the ECHO bulk CSV to `sdwis_systems.city` (99.8% populated).

### Before (Sprint 16)

- 9 county patterns (`{county}county.gov`, etc.)
- 4 name patterns (`{name}.org`, `{name}.com`, etc.)
- No city patterns
- No subdomain checks
- **Validated coverage: 3% of 128 proven domains**

### After (Sprint 17)

- 11 city patterns (new — highest priority)
- 7 county patterns (streamlined)
- 2 name patterns
- 3 subdomain prefixes checked on confirmed base domains
- **Validated coverage: 30% of 128 proven domains**

---

## Pattern Inventory

### City-Based Patterns (11 patterns, NEW)

These are the highest-value addition. Municipal utilities almost always have a domain that incorporates their city name.

| Pattern | Example | Proven Hits | Notes |
|---------|---------|:-----------:|-------|
| `{city_slug}{state_lower}.gov` | `riversideca.gov` | 18 | **Dominant pattern.** City + 2-letter state on .gov. |
| `cityof{city_slug}.gov` | `cityofsacramento.gov` | 2 | Common for cities that distinguish from county. |
| `{city_slug}.gov` | `sandiego.gov` | 5 | Short form, larger cities with unique names. |
| `{city_hyphen}-{state_lower}.gov` | `martinsville-va.gov` | 1 | Hyphenated variant. |
| `cityof{city_slug}.org` | `cityofnapa.org` | 1 | Municipality on .org instead of .gov. |
| `{city_slug}{state_lower}.org` | (none in proven set) | 0 | Speculative but plausible. |
| `cityof{city_slug}.net` | `cityofpasadena.net` | 1 | Proven: Pasadena Water & Power. |
| `{city_slug}.{state_lower}.us` | `spotsylvania.va.us` | 1 | Legacy .us domain. Still active. |
| `ci.{city_slug}.{state_lower}.us` | `ci.roanoke.va.us` | 0 | Legacy city-of format on .us. |
| `{city_slug}water.org` | (none in proven set) | 0 | Common for water-specific utilities. |
| `{city_slug}water.com` | (none in proven set) | 0 | Less common but exists in the wild. |

**Total proven hits from city patterns alone: 35 of 128 (27%)**

The `{city_slug}{state_lower}.gov` pattern is the single most productive pattern, catching 18 proven domains by itself.

### County-Based Patterns (7 patterns, revised)

| Pattern | Example | Notes |
|---------|---------|-------|
| `{county_slug}county{state_lower}.gov` | `staffordcountyva.gov` | County + state on .gov. **New** — wasn't in Sprint 16. |
| `{county_slug}county.gov` | `fairfaxcounty.gov` | Retained from Sprint 16. |
| `{county_slug}.{state_lower}.us` | `stafford.va.us` | Legacy .us. |
| `co.{county_slug}.{state_lower}.us` | `co.stafford.va.us` | County .us prefix format. |
| `{county_slug}county.org` | `staffordcounty.org` | County on .org. |
| `{county_slug}county.com` | `staffordcounty.com` | County on .com. |
| `{county_slug}co.gov` | `staffordco.gov` | Abbreviated county. |

**Removed from Sprint 16:** `{county}countyva.gov` (VA-specific, subsumed by `{county}county{state}.gov`), `www.` prefixed duplicates (unnecessary — `www.` redirects are standard).

### Name-Based Patterns (2 patterns, trimmed)

| Pattern | Example | Notes |
|---------|---------|-------|
| `{name_slug}.org` | `fairfaxwater.org` | Utility name on .org. |
| `{name_slug}.com` | `yorkwater.com` | Utility name on .com. |

Hyphenated variants are also generated for each (`{name_hyphen}.org`, `{name_hyphen}.com`).

Name cleaning strips common suffixes before slugification: `water`, `utility`, `utilities`, `department`, `dept`, `authority`, `service`, `district`, `system`, `commission`, `co`, `inc`, `llc`.

### Subdomain Checks (3 prefixes, NEW)

After a base domain resolves via DNS, the guesser also checks:

| Subdomain | Example | Confidence |
|-----------|---------|------------|
| `utilities.{domain}` | `utilities.fairfaxcounty.gov` | high |
| `water.{domain}` | `water.seattle.gov` | high |
| `publicworks.{domain}` | `publicworks.cityofroanoke.gov` | high |

These are only checked on **confirmed base domains** (DNS already resolved). Subdomain hits are marked `confidence: high` because a utility-specific subdomain is a strong signal.

Subdomain checks are guarded: only run on domains with `<=2` dots (avoids checking subdomains of already-subdomain patterns like `ci.roanoke.va.us`).

---

## How It Works

### Per-Utility Flow

```
Input: PWSID, pws_name, county, state, owner_type, city
  │
  ├─ Skip if owner_type is F (Federal) or P (Private)
  │
  ├─ Generate city patterns (11 candidates if city available)
  ├─ Generate county patterns (7 candidates if county available)
  ├─ Generate name patterns (4 candidates if name available)
  │
  ├─ DNS A-record check each candidate (socket.getaddrinfo)
  │   └─ ~100ms per lookup, free, no rate limiting
  │
  ├─ For each live domain:
  │   ├─ Homepage candidate (confidence: medium, method: domain_guess_homepage)
  │   ├─ Top 5 rate paths (confidence: low, method: domain_guess_path)
  │   └─ 3 subdomain checks (confidence: high if DNS resolves, method: domain_guess_subdomain)
  │
  └─ Return all candidates
```

### Candidate Count Per Utility

With city + county + name all available, a single utility generates:
- City: 11 DNS lookups
- County: 7 DNS lookups
- Name: 4 DNS lookups (2 patterns × 2 slug variants)
- **Total: ~22 DNS lookups** per utility

For each live domain found (typically 1-3), an additional 3 subdomain checks + 5 path guesses are generated. The `run_domain_guessing()` function writes only homepage + subdomain candidates to `scrape_registry`; path guesses are not written (the deep crawl handles path discovery).

### Data Flow

```
utility.pwsid_coverage (uncovered PWSIDs)
  + utility.sdwis_systems (city, owner_type)
  + utility.cws_boundaries (county_served)
  │
  ▼
DomainGuesser.guess_urls()
  │
  ▼ (DNS lookups)
  │
  ▼
log_discovery() → utility.scrape_registry
  url_source = "domain_guess"
  status = "pending"
```

### SQL Query (run_domain_guessing)

The query now joins `s.city` from sdwis_systems and relaxes the filter — previously required county, now accepts city OR county:

```sql
SELECT pc.pwsid, pc.pws_name, pc.state_code, c.county_served,
       s.owner_type_code, pc.population_served, s.city
FROM utility.pwsid_coverage pc
LEFT JOIN utility.cws_boundaries c ON c.pwsid = pc.pwsid
LEFT JOIN utility.sdwis_systems s ON s.pwsid = pc.pwsid
WHERE pc.has_rate_data = FALSE
  AND pc.scrape_status = 'not_attempted'
  AND (c.county_served IS NOT NULL OR s.city IS NOT NULL)
ORDER BY pc.population_served DESC NULLS LAST
LIMIT :limit
```

**Change from Sprint 16:** `AND c.county_served IS NOT NULL` → `AND (c.county_served IS NOT NULL OR s.city IS NOT NULL)`. This means utilities without county data but with city data can still be processed (city patterns only).

---

## Validation Against Proven Domains

Tested all 128 domains in `data/proven_domains.csv` — URLs where the pipeline has successfully scraped and parsed rate data.

### Coverage Comparison

| Metric | Sprint 16 (old) | Sprint 17 (new) |
|--------|:---------------:|:---------------:|
| Patterns would generate the proven domain | 4 (3%) | 39 (30%) |
| City-only hits (new capability) | 0 | 35 |
| County/name hits | 4 | 4 |

### What City Patterns Catch

The 35 city-pattern hits include high-population utilities:

| Domain | Utility | State | Population |
|--------|---------|:-----:|----------:|
| `sandiego.gov` | San Diego, City of | CA | 1,385,379 |
| `fresno.gov` | City of Fresno | CA | 545,716 |
| `cityofsacramento.gov` | City of Sacramento Main | CA | 520,407 |
| `riversideca.gov` | Riverside, City of | CA | 298,398 |
| `virginiabeach.gov` | Virginia Beach, City of | VA | 449,974 |
| `huntingtonbeachca.gov` | City of Huntington Beach | CA | 201,000 |
| `chesterfield.gov` | Chesterfield Co Central | VA | 341,300 |
| `norfolk.gov` | Norfolk, City of | VA | 242,742 |
| `harrisonburgva.gov` | Harrisonburg, City of | VA | 54,600 |
| `martinsville-va.gov` | Martinsville, City of | VA | 13,485 |

### What DNS Guessing Cannot Catch (89 of 128 — 70%)

These are domains that no systematic pattern can predict:

- **Acronyms/abbreviations:** `ladwp.com`, `ebmud.com`, `sbmwd.org`, `srcity.org`, `ggcity.org`, `pcwa.net`
- **Regional/shared domains:** `mwdoc.com` (used by multiple cities in Orange County)
- **Third-party hosted:** `cms3.revize.com`, `indio.civicweb.net`
- **Non-utility domains:** `10news.com`, `kmph.com` (news articles about rates, not utility sites)
- **Regulator sites:** `cpuc.ca.gov` (state regulator, not utility)
- **Name mismatches:** `lbutilities.org` (not `longbeachutilities.org`)

These require SearXNG search, CCR link extraction, or manual curation — the domain guesser is not designed to find them.

---

## TLD Distribution in Proven Domains

| TLD | Count | Share | Guesser Coverage |
|-----|------:|------:|:----------------|
| `.gov` | 56 | 44% | Good — 7 city patterns, 2 county patterns target .gov |
| `.com` | 33 | 26% | Limited — mostly unpredictable domains (acronyms, third-party) |
| `.org` | 29 | 23% | Moderate — name and cityof patterns target .org |
| `.net` | 5 | 4% | Minimal — only `cityof{city}.net` pattern |
| `.us` | 4 | 3% | Covered — 3 patterns target .us |
| `.edu` | 1 | 1% | Not targeted |

---

## Performance Considerations

### DNS Lookup Speed

Each `socket.getaddrinfo()` call takes ~50-150ms. With ~22 candidates per utility:

| Batch Size | Est. DNS Lookups | Est. Time (sequential) |
|:----------:|:----------------:|:----------------------:|
| 50 | 1,100 | ~2 min |
| 500 | 11,000 | ~20 min |
| 5,000 | 110,000 | ~3 hours |
| 21,197 (full) | 466,334 | ~13 hours |

**Recommendation for full national run:** Partition by state, run in parallel tmux sessions. Or add `concurrent.futures.ThreadPoolExecutor` for DNS lookups (I/O-bound, threads work well). Not implemented yet — the current sequential approach is fine for batches of 50-500.

### False Positive Rate

DNS resolution ≠ correct website. A domain like `springfield.gov` could be Springfield, MO or Springfield, IL or Springfield, MA. The guesser will find all of them as "live" regardless of which utility it's checking.

Mitigation: The deep crawl + parse pipeline downstream validates whether the resolved domain actually contains water rate information. False positives are cheap (one wasted HTTP request) and the pipeline already handles them.

---

## Caveats and Limitations

1. **City is mailing address, not service area.** For municipal utilities this is usually the same. For small private systems it could be the owner's home address. The guesser already skips private (P) and federal (F) systems, so this mainly affects the small-system long tail.

2. **Subdomain checks multiply DNS lookups.** Each confirmed base domain triggers 3 additional lookups. If a utility has 3 confirmed base domains, that's 9 extra lookups. Acceptable for small batches; watch for scaling.

3. **No HTTP validation in the guesser.** DNS confirms a domain exists but doesn't confirm it serves HTTP(S). Dead domains, parked pages, and unrelated sites all pass DNS. The scrape pipeline handles this downstream.

4. **Pattern order doesn't affect DNS results.** Unlike search engines, DNS lookups don't benefit from prioritization — all candidates are checked regardless. The priority order in `domain_patterns.yaml` is for documentation/understanding, not runtime behavior.

5. **The `{city_slug}.gov` pattern is aggressive.** Common city names (Springfield, Franklin, Clinton) exist in many states. This pattern will produce false positives. Acceptable because the deep crawl validates, but something to monitor if writing thousands of registry entries.

---

## CLI Usage

```bash
# Preview 50 utilities, no writes
ua-ops domain-guess --dry-run --max 50

# Preview VA only
ua-ops domain-guess --dry-run --state VA --max 100

# Write results for VA (top 200 by population)
ua-ops domain-guess --state VA --max 200

# Full national sweep (recommend tmux)
# tmux new-session -d -s domain_sweep "ua-ops domain-guess --max 5000 2>&1 | tee logs/domain_guess_national.log"
```

### Output

```
Domain Guessing Results
==================================================
  Utilities checked:       50
  Live domains found:       12
  URLs written:             15
```

Written entries go to `scrape_registry` with:
- `url_source`: `domain_guess`
- `status`: `pending`
- `notes`: `Domain guess: {domain}`

---

## Files

| File | Role |
|------|------|
| `src/utility_api/ops/domain_guesser.py` | Module — patterns hardcoded for portability |
| `config/domain_patterns.yaml` | Reference documentation — pattern research, TLD distribution, state notes |
| `src/utility_api/cli/ops.py` (`domain-guess` command) | CLI entry point |
| `data/sdwis_for_guessing.csv` | Standalone export (21,197 rows with city) for offline guesser use |

---

## Recommended Next Steps

1. **Dry-run on VA** (`--state VA --max 200 --dry-run`) to see how many city-based hits appear vs county-based. VA has good proven-domain coverage to validate against.
2. **Measure false positive rate** — of the domains that resolve, how many actually serve water utility content? Run a small batch through the scrape pipeline.
3. **Consider parallelizing DNS** — `concurrent.futures.ThreadPoolExecutor(max_workers=20)` would cut a 500-utility batch from ~20 min to ~1 min. Straightforward change when needed.
4. **Monitor the `{city_slug}.gov` pattern** — if it produces too many false positives for common city names, consider requiring the full `{city_slug}{state_lower}.gov` pattern instead.
