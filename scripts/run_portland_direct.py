#!/usr/bin/env python3
"""Run direct scrape+parse on remaining Portland curated URLs."""

import time
from sqlalchemy import text
from utility_api.db import engine
from utility_api.config import settings
from utility_api.agents.scrape import ScrapeAgent
from utility_api.agents.parse import ParseAgent
from loguru import logger

schema = settings.utility_schema

with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT sr.pwsid, sr.url, s.pws_name
        FROM utility.scrape_registry sr
        JOIN utility.sdwis_systems s ON sr.pwsid = s.pwsid
        WHERE sr.url_source = 'curated_portland'
          AND (sr.last_parse_result IS NULL OR sr.last_parse_result != 'success')
        ORDER BY s.population_served_count DESC NULLS LAST
    """)).fetchall()

logger.info(f"Portland direct: {len(rows)} URLs to process")

scraper = ScrapeAgent()
parser = ParseAgent()
successes = 0
failures = 0

for i, (pwsid, url, name) in enumerate(rows):
    logger.info(f"[{i+1}/{len(rows)}] {pwsid} — {name}")
    logger.info(f"  URL: {url}")
    try:
        sr = scraper.run(pwsid=pwsid, url=url)
        chars = sr.get("chars", 0) if sr else 0
        if chars > 100:
            pr = parser.run(pwsid=pwsid)
            conf = pr.get("confidence", "failed") if pr else "failed"
            if conf in ("high", "medium"):
                successes += 1
                logger.info(f"  SUCCESS: {conf} | bill@10CCF=${pr.get('bill_10ccf', '?')}")
            else:
                failures += 1
                logger.info(f"  Parse failed: {conf}")
        else:
            failures += 1
            logger.info(f"  Scrape: insufficient content ({chars} chars)")
    except Exception as e:
        logger.error(f"  Error: {e}")
        failures += 1

    time.sleep(1.5)

logger.info(f"\n=== Portland Direct Pipeline Complete ===")
logger.info(f"Successes: {successes}, Failures: {failures}, Total: {len(rows)}")
