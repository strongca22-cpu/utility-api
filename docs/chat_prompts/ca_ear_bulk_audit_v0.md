# CA SWRCB eAR Bulk Source Audit v0

## Context
Sprint 28 established the audit-and-fix pattern for bulk data sources. Duke NIEPS was the template (explicit tier data, academic reference). EFC was the second pass (bill-curve-derived, 20 state surveys). Now applying the same pattern to the third bulk source family: CA SWRCB Electronic Annual Reports (eAR).

**eAR is structurally different from both Duke and EFC:**
- Duke: explicit tiers from academic dataset → bills calculated from tiers
- EFC: bill curves from API → tiers reverse-engineered from curves → bills are ground truth
- eAR: state-mandated utility filings with BOTH explicit tiers AND pre-computed bills — but a known tier inflation problem where utilities report tier limits in gallons instead of HCF/CCF (~748x inflation)

The EFC audit found clean JSONB and only needed confidence recalibration. The eAR audit will likely find more structural issues due to the tier inflation problem (partially addressed by `fix_ear_tier_inflation.py` but needs verification).

**Generalized scripts from EFC audit are available** — `scripts/efc_qa_analysis.py` can be adapted for eAR by changing source_key filters.

## Objective
Audit all 3 eAR vintages (2020, 2021, 2022) as a batch. Apply the audit pattern: ingest audit → head-to-head comparison → JSONB format check → fix issues → document findings.

## Target Sources

| Source Key | Records | Pre-computed Bills | Tier Data | Priority | Vintage |
|-----------|---------|-------------------|-----------|----------|---------|
| swrcb_ear_2022 | ~194 | Yes | Yes (some inflated) | 3 | 2022 |
| swrcb_ear_2021 | ~193 | Yes | Yes (some inflated) | 4 | 2021 |
| swrcb_ear_2020 | ~194 | No (tier-only) | Yes (some inflated) | 6 | 2020 |

**Total: ~581 records, CA only.** Significant overlap with scraped_llm expected (CA has strong scraping coverage).

## Tasks

### Task 1: eAR Ingest Audit
- Read `src/utility_api/ingest/ear_ingest.py` — trace full pipeline
- Source format: HydroShare Excel files → field mapping → rate_schedules
- How are tiers extracted? What columns map to volumetric_tiers JSONB?
- How are bills computed? Pre-computed (WR6/9/12/24HCFDWCharges) vs calculated?
- What does `fix_ear_tier_inflation.py` do? How many records were affected?
- What billing frequency normalization happens?
- What's the current state of eAR records in rate_schedules? (confidence distribution, NULL tiers, etc.)

### Task 2: Tier Inflation Verification
- Query rate_schedules for eAR records with tier limits > 100 CCF (748 gal * 100 = 74,800 gal)
- How many remain after the fix script ran?
- Cross-check: do inflated-tier records have reasonable pre-computed bills?
- Are there records where BOTH tiers and bills were NULLed?

### Task 3: Head-to-Head Comparison
For PWSIDs with both eAR and scraped_llm:
- Compare bill_10ccf values (% difference distribution, agreement buckets)
- eAR is CA-only, so overlap should be substantial
- Compare by eAR vintage (2020 vs 2021 vs 2022)
- Flag scraped records that include sewer charges (eAR is water-only)
- Note: eAR has bill_6ccf, bill_9ccf, bill_12ccf, bill_24ccf — compare at multiple consumption levels if scraped has them

### Task 4: JSONB Storage Format Comparability
Same checks as EFC audit:
- fixed_charges: canonical keys? Extra keys like `frequency`?
- volumetric_tiers: canonical keys? Contiguity? Duplicates?
- rate_structure_type: canonical values?
- Special attention: eAR has `frequency` key in fixed_charges (Duke had this too — needs stripping)

### Task 5: Fix Storage Issues
Apply fixes as needed:
- Strip extra JSONB keys
- Fix tier contiguity gaps
- Remove duplicate tiers
- Do NOT recalculate pre-computed bills (state-reported bills are authoritative)
- Set nuanced confidence levels
- Handle remaining tier-inflated records (NULL tiers, preserve bills)

### Task 6: Document Findings
Create `docs/ca_ear_audit_report.md` with:
- Ingest pipeline documentation
- Tier inflation analysis (before/after fix)
- Head-to-head comparison statistics
- JSONB issues found and fixed
- Recommendations for eAR data usage going forward
- Water-only vs. dual-source implications for CA cross-referencing

## Key Files
- `src/utility_api/ingest/ear_ingest.py` — eAR ingest module
- `scripts/fix_ear_tier_inflation.py` — tier inflation fix (already run?)
- `scripts/analyze_ear_rate_changes.py` — rate change analysis
- `scripts/efc_qa_analysis.py` — adapt for eAR (change source_key filters)
- `scripts/migrate_efc_to_comparable.py` — adapt for eAR (confidence + JSONB fixes)
- `config/source_priority.yaml` — eAR priority/confidence config
- `data/raw/swrcb_ear/` — source Excel files

## What NOT to Change
- Do not modify scraped_llm data
- Do not re-run eAR ingest — this is a patch to existing records
- Do not overwrite pre-computed bills with tier-recalculated bills (state-reported bills are authoritative, same principle as EFC curve-interpolated bills)
- Do not write to water_rates table — all changes target rate_schedules

## Key Differences from EFC Audit
1. **eAR has BOTH explicit tiers AND pre-computed bills** — unlike EFC (bills only) or Duke (tiers only in some vintages)
2. **Tier inflation is a known problem** — some records have tier limits 748x too high
3. **CA-only** — overlap with scraped_llm should be much larger than EFC's 36-pair overlap
4. **Water-only scope** — useful for catching scraped records that accidentally include sewer charges
5. **Multiple bill benchmarks** — bill_6ccf, bill_9ccf, bill_12ccf, bill_24ccf available (not just 5/10/20)
