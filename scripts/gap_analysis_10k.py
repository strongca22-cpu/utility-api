#!/usr/bin/env python3
"""
Gap Analysis: High-Population PWSIDs Without Rate Data

Purpose:
    Deep analytical investigation into PWSIDs >= 10k population that lack
    rate_best_estimate data. Categorizes failure modes to guide pipeline improvements.

Author: AI-Generated
Created: 2026-04-02
Modified: 2026-04-02

Dependencies:
    - sqlalchemy
    - pandas
    - utility_api (local package)

Usage:
    python3 scripts/gap_analysis_10k.py
"""

import pandas as pd
from sqlalchemy import text
from utility_api.db import engine
from utility_api.config import settings

S = settings.utility_schema
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 250)
pd.set_option('display.max_colwidth', 120)
pd.set_option('display.max_rows', 500)


def q1_full_pipeline_status():
    """Query 1: All gap PWSIDs >= 10k with full pipeline status."""
    print("=" * 120)
    print("QUERY 1: All gap PWSIDs >= 10k population with full pipeline status")
    print("=" * 120)

    sql = text(f"""
        WITH gap AS (
            SELECT ss.pwsid, ss.pws_name, ss.state_code, ss.population_served_count AS pop
            FROM {S}.sdwis_systems ss
            WHERE ss.population_served_count >= 10000
              AND NOT EXISTS (
                  SELECT 1 FROM {S}.rate_best_estimate rbe WHERE rbe.pwsid = ss.pwsid
              )
        ),
        scrape_stats AS (
            SELECT
                sr.pwsid,
                COUNT(*) AS total_urls,
                COUNT(*) FILTER (WHERE LENGTH(sr.scraped_text) >= 100) AS urls_with_text,
                MAX(sr.last_content_length) AS max_content_length,
                MAX(sr.last_parse_result) AS best_parse_result
            FROM {S}.scrape_registry sr
            WHERE sr.pwsid IN (SELECT pwsid FROM gap)
            GROUP BY sr.pwsid
        ),
        best_url AS (
            SELECT DISTINCT ON (sr.pwsid)
                sr.pwsid,
                sr.url AS best_url,
                sr.content_type AS best_content_type,
                sr.last_content_length AS best_content_len,
                sr.last_parse_result AS best_url_parse_result,
                sr.last_parse_confidence AS best_url_confidence,
                LEFT(sr.last_parse_raw_response, 2000) AS raw_response_excerpt,
                sr.notes
            FROM {S}.scrape_registry sr
            WHERE sr.pwsid IN (SELECT pwsid FROM gap)
              AND sr.scraped_text IS NOT NULL
              AND LENGTH(sr.scraped_text) >= 100
            ORDER BY sr.pwsid, sr.last_content_length DESC NULLS LAST
        )
        SELECT
            g.pwsid, g.pws_name, g.state_code, g.pop,
            COALESCE(ss.total_urls, 0) AS total_urls,
            COALESCE(ss.urls_with_text, 0) AS urls_with_text,
            ss.best_parse_result,
            ss.max_content_length,
            bu.best_content_type,
            bu.best_url_parse_result,
            bu.best_url_confidence,
            bu.notes AS best_url_notes,
            bu.best_url,
            bu.raw_response_excerpt
        FROM gap g
        LEFT JOIN scrape_stats ss ON ss.pwsid = g.pwsid
        LEFT JOIN best_url bu ON bu.pwsid = g.pwsid
        ORDER BY g.pop DESC
    """)

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)

    print(f"\nTotal gap PWSIDs >= 10k: {len(df)}")
    print(f"Total population unserved: {df['pop'].sum():,}")
    print(f"\nWith any URLs in scrape_registry: {(df['total_urls'] > 0).sum()}")
    print(f"With substantive text (>=100 chars): {(df['urls_with_text'] > 0).sum()}")
    print(f"With zero URLs: {(df['total_urls'] == 0).sum()}")
    print()

    # Show condensed view of all
    display_cols = ['pwsid', 'pws_name', 'state_code', 'pop', 'total_urls',
                    'urls_with_text', 'best_parse_result', 'max_content_length',
                    'best_content_type', 'best_url_parse_result', 'best_url_confidence']
    print(df[display_cols].to_string(index=False))
    print()

    return df


def q2_categorize_failures(df):
    """Query 2: Categorize each gap PWSID into a failure mode."""
    print("\n" + "=" * 120)
    print("QUERY 2: Failure mode categorization")
    print("=" * 120)

    def categorize(row):
        notes = str(row.get('best_url_notes', '') or '') + ' ' + str(row.get('raw_response_excerpt', '') or '')
        notes_lower = notes.lower()
        urls_with_text = row.get('urls_with_text', 0) or 0
        total_urls = row.get('total_urls', 0) or 0
        parse_result = str(row.get('best_url_parse_result', '') or '').lower()
        confidence = str(row.get('best_url_confidence', '') or '').lower()
        content_type = str(row.get('best_content_type', '') or '').lower()
        max_len = row.get('max_content_length', 0) or 0

        # No URLs at all
        if total_urls == 0:
            return 'scrape_failed'

        # Has text but never parsed
        if urls_with_text > 0 and (parse_result == '' or parse_result == 'none' or parse_result == 'nan'):
            return 'never_parsed'

        # Auth / paywall
        for kw in ['login', 'sign in', 'sign-in', 'subscribe', 'access denied', 'authentication', 'log in']:
            if kw in notes_lower:
                return 'auth_paywall'

        # JS wall
        if any(kw in notes_lower for kw in ['javascript', 'loading', 'dynamic content', 'client-side']):
            return 'js_wall'
        if urls_with_text == 0 and total_urls > 0:
            # All scrapes returned empty/thin
            return 'scrape_failed'
        if max_len and max_len < 200 and content_type == 'html':
            return 'js_wall'

        # Water/sewer combined
        for kw in ['combined', 'cannot separate', 'water and sewer', 'water & sewer', 'includes sewer',
                    'bundled', 'water/sewer', 'separate water']:
            if kw in notes_lower:
                return 'water_sewer_combined'

        # Rates behind link
        for kw in ['link', 'download', 'navigate', 'click', 'redirect', 'another page',
                    'separate document', 'pdf available']:
            if kw in notes_lower:
                return 'rates_behind_link'

        # Partial extraction
        if confidence == 'low' and parse_result not in ('failed', ''):
            return 'partial_extraction'

        # PDF extraction failed
        if 'pdf' in content_type:
            if any(kw in notes_lower for kw in ['garbled', 'unreadable', 'scanned', 'image-based', 'ocr']):
                return 'pdf_extraction_failed'
            if parse_result == 'failed':
                return 'pdf_extraction_failed'

        # Wrong URL
        if parse_result == 'failed' or parse_result == 'skipped':
            for kw in ['no rate', 'does not contain', 'not a water utility', 'no water rate',
                       'not contain', 'unrelated', 'no pricing', 'no utility rate',
                       'does not appear', 'no relevant', 'couldn\'t find', 'could not find',
                       'unable to find', 'unable to extract', 'no specific rate',
                       'does not provide', 'no usable']:
                if kw in notes_lower:
                    return 'wrong_url_all_ranks'
            return 'wrong_url_all_ranks'  # failed parse with text = wrong URL

        # Has text, parsed, but still no rate_best_estimate — might be partial or other
        if urls_with_text > 0 and parse_result not in ('failed', 'skipped', '', 'none', 'nan'):
            return 'partial_extraction'

        return 'other'

    df['failure_mode'] = df.apply(categorize, axis=1)

    summary = df.groupby('failure_mode').agg(
        count=('pwsid', 'count'),
        total_pop=('pop', 'sum')
    ).sort_values('total_pop', ascending=False)
    summary['pct_count'] = (summary['count'] / summary['count'].sum() * 100).round(1)
    summary['pct_pop'] = (summary['total_pop'] / summary['total_pop'].sum() * 100).round(1)

    print("\nFailure Mode Summary:")
    print(summary.to_string())
    print(f"\nTotal: {summary['count'].sum()} PWSIDs, {summary['total_pop'].sum():,} population")

    return df


def q3_wrong_url_domains(df):
    """Query 3: For wrong_url_all_ranks, what domains are these URLs on?"""
    print("\n" + "=" * 120)
    print("QUERY 3: Wrong URL domains — potential blacklist candidates")
    print("=" * 120)

    wrong = df[df['failure_mode'] == 'wrong_url_all_ranks']
    if wrong.empty:
        print("No PWSIDs in this category.")
        return

    sql = text(f"""
        SELECT sr.pwsid, sr.url, sr.last_content_length, sr.last_parse_result
        FROM {S}.scrape_registry sr
        WHERE sr.pwsid = ANY(:pwsids)
        AND sr.scraped_text IS NOT NULL
        AND LENGTH(sr.scraped_text) >= 100
        ORDER BY sr.pwsid, sr.last_content_length DESC
    """)

    with engine.connect() as conn:
        urls_df = pd.read_sql(sql, conn, params={'pwsids': wrong['pwsid'].tolist()})

    if urls_df.empty:
        print("No URLs with text found for these PWSIDs.")
        return

    # Extract domains
    import re
    def extract_domain(url):
        m = re.search(r'https?://([^/]+)', str(url))
        return m.group(1) if m else 'unknown'

    urls_df['domain'] = urls_df['url'].apply(extract_domain)

    domain_counts = urls_df.groupby('domain').agg(
        url_count=('url', 'count'),
        pwsid_count=('pwsid', 'nunique')
    ).sort_values('pwsid_count', ascending=False)

    print(f"\nDomains serving wrong URLs ({len(domain_counts)} unique domains):")
    print(domain_counts.head(40).to_string())


def q4_rates_behind_link(df):
    """Query 4: Sample 20 'rates_behind_link' with URLs and notes."""
    print("\n" + "=" * 120)
    print("QUERY 4: Rates behind link — sample 20 (nav crawl recovery candidates)")
    print("=" * 120)

    behind = df[df['failure_mode'] == 'rates_behind_link'].head(20)
    if behind.empty:
        print("No PWSIDs in this category.")
        return

    for _, row in behind.iterrows():
        print(f"\n{'─' * 100}")
        print(f"PWSID: {row['pwsid']}  |  {row['pws_name']}  |  {row['state_code']}  |  Pop: {row['pop']:,}")
        print(f"URL: {row.get('best_url', 'N/A')}")
        notes = str(row.get('raw_response_excerpt', '') or '')[:500]
        print(f"LLM Notes: {notes}")


def q5_water_sewer_combined(df):
    """Query 5: Sample 15 'water_sewer_combined' with notes."""
    print("\n" + "=" * 120)
    print("QUERY 5: Water/sewer combined — sample 15")
    print("=" * 120)

    combined = df[df['failure_mode'] == 'water_sewer_combined'].head(15)
    if combined.empty:
        print("No PWSIDs in this category.")
        return

    for _, row in combined.iterrows():
        print(f"\n{'─' * 100}")
        print(f"PWSID: {row['pwsid']}  |  {row['pws_name']}  |  {row['state_code']}  |  Pop: {row['pop']:,}")
        print(f"URL: {row.get('best_url', 'N/A')}")
        notes = str(row.get('raw_response_excerpt', '') or '')[:600]
        print(f"LLM Notes: {notes}")


def q6_partial_extraction(df):
    """Query 6: For partial_extraction, what fields were extracted?"""
    print("\n" + "=" * 120)
    print("QUERY 6: Partial extraction — what fields are present?")
    print("=" * 120)

    partial = df[df['failure_mode'] == 'partial_extraction']
    if partial.empty:
        print("No PWSIDs in this category.")
        return

    sql = text(f"""
        SELECT rs.pwsid, rs.source_key, rs.confidence, rs.rate_structure_type,
               rs.fixed_charges IS NOT NULL AS has_fixed,
               rs.volumetric_tiers IS NOT NULL AS has_tiers,
               rs.bill_5ccf, rs.bill_10ccf, rs.bill_6ccf, rs.bill_12ccf,
               rs.tier_count, rs.customer_class, rs.billing_frequency,
               rs.parse_notes, rs.review_reason
        FROM {S}.rate_schedules rs
        WHERE rs.pwsid = ANY(:pwsids)
        ORDER BY rs.pwsid
    """)

    with engine.connect() as conn:
        rs_df = pd.read_sql(sql, conn, params={'pwsids': partial['pwsid'].tolist()})

    if rs_df.empty:
        print(f"No rate_schedules found for {len(partial)} partial PWSIDs.")
        print("These may have been parsed but not inserted into rate_schedules.")

        # Check water_rates instead
        sql2 = text(f"""
            SELECT wr.pwsid, wr.source, wr.parse_confidence,
                   wr.fixed_charge_monthly, wr.tier_1_rate, wr.tier_2_rate,
                   wr.bill_5ccf, wr.bill_10ccf, wr.parse_notes
            FROM {S}.water_rates wr
            WHERE wr.pwsid = ANY(:pwsids)
            ORDER BY wr.pwsid
        """)
        wr_df = pd.read_sql(sql2, conn, params={'pwsids': partial['pwsid'].tolist()})
        if not wr_df.empty:
            print(f"\nFound {len(wr_df)} rows in water_rates for these PWSIDs:")
            print(wr_df.to_string(index=False))
        return

    print(f"\nFound {len(rs_df)} rate_schedule rows for {rs_df['pwsid'].nunique()} partial PWSIDs:")
    print(f"\nField presence:")
    print(f"  has_fixed_charges: {rs_df['has_fixed'].sum()}/{len(rs_df)}")
    print(f"  has_volumetric_tiers: {rs_df['has_tiers'].sum()}/{len(rs_df)}")
    print(f"  has_bill_5ccf: {rs_df['bill_5ccf'].notna().sum()}/{len(rs_df)}")
    print(f"  has_bill_10ccf: {rs_df['bill_10ccf'].notna().sum()}/{len(rs_df)}")
    print(f"  has_bill_6ccf: {rs_df['bill_6ccf'].notna().sum()}/{len(rs_df)}")
    print(f"  has_bill_12ccf: {rs_df['bill_12ccf'].notna().sum()}/{len(rs_df)}")

    print(f"\nRate structure types:")
    print(rs_df['rate_structure_type'].value_counts().to_string())

    print(f"\nConfidence levels:")
    print(rs_df['confidence'].value_counts().to_string())

    print(f"\nSample parse_notes:")
    for _, row in rs_df.head(15).iterrows():
        print(f"  {row['pwsid']}: {str(row['parse_notes'])[:150]}")


def q7_state_failure_distribution(df):
    """Query 7: State-level failure mode distribution."""
    print("\n" + "=" * 120)
    print("QUERY 7: State-level failure mode distribution")
    print("=" * 120)

    ct = pd.crosstab(df['state_code'], df['failure_mode'], margins=True)
    print("\nCount by state x failure_mode:")
    print(ct.to_string())

    # Also show by population
    print("\n\nPopulation by state x failure_mode:")
    pop_ct = df.pivot_table(index='state_code', columns='failure_mode',
                            values='pop', aggfunc='sum', fill_value=0, margins=True)
    # Format as integers
    print(pop_ct.astype(int).to_string())


def q8_top50_individual(df):
    """Query 8: Top 50 by population — individual case analysis."""
    print("\n" + "=" * 120)
    print("QUERY 8: Top 50 gap PWSIDs by population — individual case analysis")
    print("=" * 120)

    top50 = df.nlargest(50, 'pop')

    for i, (_, row) in enumerate(top50.iterrows(), 1):
        notes = str(row.get('raw_response_excerpt', '') or '')[:200]
        print(f"\n{'─' * 100}")
        print(f"#{i}  PWSID: {row['pwsid']}  |  {row['pws_name']}")
        print(f"     State: {row['state_code']}  |  Pop: {row['pop']:,}  |  Failure: {row['failure_mode']}")
        print(f"     URLs: {row['total_urls']}  |  With text: {row['urls_with_text']}  |  "
              f"Parse: {row.get('best_url_parse_result', 'N/A')}  |  Confidence: {row.get('best_url_confidence', 'N/A')}")
        print(f"     Best URL: {row.get('best_url', 'N/A')}")
        if notes and notes != 'None' and notes != 'nan':
            print(f"     Notes: {notes}")


def main():
    print("Gap Analysis: High-Population PWSIDs Without Rate Data")
    print("=" * 120)

    df = q1_full_pipeline_status()
    df = q2_categorize_failures(df)
    q3_wrong_url_domains(df)
    q4_rates_behind_link(df)
    q5_water_sewer_combined(df)
    q6_partial_extraction(df)
    q7_state_failure_distribution(df)
    q8_top50_individual(df)

    print("\n" + "=" * 120)
    print("ANALYSIS COMPLETE")
    print("=" * 120)


if __name__ == "__main__":
    main()
