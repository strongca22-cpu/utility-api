# Task: Ingest Denver/CO Springs Metro Water Rate URL Curation into scrape_registry

You have a curation JSON file at:
`denver_cos_metro_rate_curation.json`

And an equivalent Portland metro file at:
`portland_metro_rate_curation.json`

Your job is to process both files and insert valid entries into the database. Follow the steps below exactly, in order. Do not skip steps. Log everything.

---

## Step 1: Read and parse the curation files

Load both JSON files. Extract the `utilities[]` array from each. Confirm total entry counts match `_meta.total_entries`. If they don't match, log a warning but continue.

---

## Step 2: Filter out entries that should not be inserted

Before touching the database, remove any entry that has any of the following:

- `scraper_flag` value starting with `"DUPLICATE_"` — these are explicitly marked as duplicates of another entry
- `scraper_flag` value of `"INFO_ONLY_DO_NOT_INSERT"` — informational entries only
- `scraper_flag` value of `"SEWER_ONLY_VERIFY_SCHEMA_FIT"` — only insert if `scrape_registry` is confirmed to accept wastewater-only URLs (check schema before deciding)

Log each filtered entry with the reason it was skipped.

---

## Step 3: Resolve PWSIDs from SDWIS

For each remaining entry where `pwsid` is `null`, run the PWSID lookup query from `_meta.pwsid_lookup_sql`. Substitute:
- `:name_pattern` → `'%' || UPPER(entry.sdwis_name_hint) || '%'`
- `:city` → `UPPER(entry.sdwis_city)`

```sql
SELECT s.pwsid, s.pws_name, s.city, s.state_code, s.population_served_count
FROM utility.sdwis_systems s
WHERE s.state_code IN ('CO', 'OR', 'WA')
  AND s.pws_type_code = 'CWS'
  AND (
    UPPER(s.pws_name) LIKE :name_pattern
    OR UPPER(s.city) = :city
  )
ORDER BY s.population_served_count DESC NULLS LAST
LIMIT 5;
```

**Resolution rules:**
- If exactly 1 result returns: use that PWSID. Log it.
- If multiple results return: pick the one with the highest `population_served_count` that is plausible given the entry's `population_approx`. If ambiguous, log as `PWSID_AMBIGUOUS` and skip insertion for that entry — do not guess.
- If 0 results return: log as `PWSID_NOT_FOUND` and skip insertion for that entry.
- If `pwsid` is already set in the JSON (not null): skip the lookup and use the provided value directly.

Store the resolved PWSID back on the entry object for use in Step 4.

---

## Step 4: Check for existing coverage

Before inserting, check whether the PWSID already has rate data or a pending URL in the registry:

```sql
SELECT
  pc.pwsid,
  pc.has_rate_data,
  (SELECT COUNT(*) FROM utility.scrape_registry sr
   WHERE sr.pwsid = pc.pwsid
     AND sr.status IN ('pending', 'active')) AS pending_urls,
  (SELECT COUNT(*) FROM utility.scrape_registry sr
   WHERE sr.pwsid = pc.pwsid
     AND sr.last_parse_result = 'success') AS successful_parses
FROM utility.pwsid_coverage pc
WHERE pc.pwsid = :pwsid;
```

**Coverage rules:**
- If `has_rate_data = TRUE`: log as `ALREADY_COVERED` and skip. We have the data — no need to re-queue.
- If `pending_urls > 0` and `successful_parses = 0`: log as `ALREADY_QUEUED` — check if the existing URL is the same as our curated URL. If different and better, insert as an additional URL. If same, skip.
- If `pending_urls = 0` and `has_rate_data = FALSE`: proceed to insert.

---

## Step 5: Insert into scrape_registry

For each entry that passed Steps 2–4, insert using the template from `_meta.insert_sql_template`:

```sql
INSERT INTO utility.scrape_registry (pwsid, url, url_source, status, notes)
VALUES (
  :pwsid,
  :url,
  :url_source,         -- use value from _meta.url_source for that file
  'pending',
  :notes               -- use value from _meta.default_notes for that file
)
ON CONFLICT (pwsid, url) DO NOTHING;
```

For entries that have a `url_secondary` or `url_rate_pdf` field AND the primary URL was successfully inserted, also insert those as additional rows for the same PWSID. Use the same `url_source` and notes, but append `' [secondary]'` or `' [pdf]'` to the notes string.

For entries flagged `NEEDS_VERIFICATION`, append `' | url_unconfirmed'` to the notes string so the scraper pipeline knows to flag failures differently.

---

## Step 6: Log a summary report

After processing both files, print a structured summary:

```
=== INGESTION SUMMARY ===

FILE: portland_metro_rate_curation.json
  Total entries in file:       XX
  Filtered (duplicates/info):  XX
  PWSID resolved:              XX
  PWSID not found:             XX
  PWSID ambiguous:             XX
  Already covered (skipped):   XX
  Already queued (skipped):    XX
  Inserted (primary URL):      XX
  Inserted (secondary URLs):   XX
  Needs verification (flagged): XX

FILE: denver_cos_metro_rate_curation.json
  [same structure]

COMBINED TOTALS:
  New PWSIDs added to scrape_registry:  XX
  Total URLs inserted:                  XX
  Entries requiring manual follow-up:   XX

MANUAL FOLLOW-UP REQUIRED:
  [list each entry with PWSID_NOT_FOUND, PWSID_AMBIGUOUS, NEEDS_VERIFICATION]
  Format: {file} | id={id} | utility_name | reason
```

---

## Step 7: Trigger the pipeline for new entries (optional, confirm first)

If inserts were successful, ask for confirmation before running:

```bash
# For Oregon entries
ua-run-orchestrator --execute 30 --state OR

# For Colorado entries
ua-run-orchestrator --execute 30 --state CO

# Check results
ua-ops scrape-status --state OR
ua-ops scrape-status --state CO
ua-ops coverage-report
```

Do not run these automatically. Print the commands and ask: **"Ready to trigger the scrape pipeline for OR and CO? (yes/no)"**

---

## Important rules

1. **Never guess a PWSID.** If lookup returns 0 or ambiguous results, log and skip. A wrong PWSID poisons the registry.
2. **ON CONFLICT DO NOTHING** is your safety net — it is safe to re-run this script. Duplicates will be silently skipped.
3. **Preserve all existing data.** Do not UPDATE or DELETE any existing `scrape_registry` rows.
4. **The `url_source` values are file-specific:**
   - Portland file: `'curated_portland'`
   - Denver/CO Springs file: `'curated_denver_cos'`
5. **Log verbosely.** Every PWSID lookup, every skip, every insert should appear in the log. This run needs to be auditable.
6. **Colorado structural complexity:** The Denver/CO Springs file `_meta.colorado_structural_notes` contains important context about Denver Water wholesale vs. retail, Highlands Ranch dual-entity structure, and Lakewood fragmentation. Read these notes before processing CO entries. In particular: suburban Denver Water distributor districts each have their own PWSID — never map a distributor URL to Denver Water's PWSID.
