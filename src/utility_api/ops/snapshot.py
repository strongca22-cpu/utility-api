#!/usr/bin/env python3
"""
Pre-Write Snapshot Module (Tier 2 of Backup System)

Purpose:
    Take fast, gzipped CSV snapshots of the critical rate-intelligence tables
    BEFORE any destructive operation (rebuild, truncate, mass UPDATE). Tier 2
    of the layered backup system; complements the daily pg_dump (Tier 1).

    Trigger this from any code path that mutates the protected tables in
    bulk. The cost is small (a few seconds; tens of MB) and the recovery
    value when something goes sideways is enormous.

    Critical tables:
        - rate_schedules
        - rate_best_estimate
        - pwsid_coverage
        - scrape_registry  (excluding scraped_text + last_parse_raw_response,
                            which are huge and rarely change in a rebuild)

Author: AI-Generated
Created: 2026-04-06
Modified: 2026-04-06

Dependencies:
    - sqlalchemy
    - psycopg (raw COPY via the SQLAlchemy connection)
    - loguru

Usage:
    from utility_api.ops.snapshot import snapshot_critical_tables

    paths = snapshot_critical_tables(reason="best_estimate_rebuild")
    for table, path in paths.items():
        logger.info(f"Snapshot {table}: {path}")

    # CLI:
    #   ua-ops snapshot --reason "manual_pre_migration"

Notes:
    - Output dir: ~/backups/utility-api/snapshots/
    - Filename:   <table>_<reason>_<UTC-timestamp>.csv.gz
    - Output is OUTSIDE the project directory by design (see backup_system.md).
    - Retention: snapshots older than 30 days in the output dir are deleted
      at the end of each call. The daily pg_dump (Tier 1) is the long-term
      record.
    - Uses psycopg's COPY (...) TO STDOUT WITH (FORMAT csv, HEADER) streamed
      through gzip. Faster and lower-memory than pandas for large tables.
"""

from __future__ import annotations

import gzip
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from loguru import logger

from utility_api.config import settings
from utility_api.db import engine


# --- Constants ---

BACKUP_ROOT = Path.home() / "backups" / "utility-api"
SNAPSHOT_DIR = BACKUP_ROOT / "snapshots"
RETENTION_DAYS = 30

# Tables to snapshot, with the columns to omit (huge / rarely-mutated blobs).
# Empty omit_cols → SELECT *.
CRITICAL_TABLES: dict[str, list[str]] = {
    "rate_schedules": [],
    "rate_best_estimate": [],
    "pwsid_coverage": [],
    "scrape_registry": ["scraped_text", "last_parse_raw_response"],
}


# --- Helpers ---

_REASON_RE = re.compile(r"[^a-zA-Z0-9_-]+")


def _sanitize_reason(reason: str) -> str:
    """Make `reason` safe for use in a filename."""
    cleaned = _REASON_RE.sub("_", reason.strip()).strip("_")
    return cleaned or "unspecified"


def _get_columns(conn, schema: str, table: str) -> list[str]:
    """Return the column names for `schema.table`, in ordinal order."""
    from sqlalchemy import text

    rows = conn.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
            """
        ),
        {"schema": schema, "table": table},
    ).fetchall()
    return [r[0] for r in rows]


def _copy_table_to_gzip(
    conn,
    schema: str,
    table: str,
    omit_cols: list[str],
    out_path: Path,
) -> int:
    """Stream `schema.table` (minus omit_cols) to a gzipped CSV.

    Uses psycopg's COPY ... TO STDOUT WITH (FORMAT csv, HEADER) for speed.
    Returns the number of rows written.
    """
    cols = _get_columns(conn, schema, table)
    if not cols:
        raise RuntimeError(f"No columns found for {schema}.{table}")
    keep = [c for c in cols if c not in set(omit_cols)]
    if not keep:
        raise RuntimeError(f"All columns omitted for {schema}.{table}")

    # Identifier-quote columns to be safe.
    col_list = ", ".join(f'"{c}"' for c in keep)
    copy_sql = (
        f'COPY (SELECT {col_list} FROM "{schema}"."{table}") '
        f'TO STDOUT WITH (FORMAT csv, HEADER)'
    )

    # SQLAlchemy 2.x exposes the underlying psycopg connection via .connection;
    # psycopg3 provides cursor.copy() as a context manager.
    raw_conn = conn.connection  # DBAPI connection (psycopg)
    row_count = 0
    with gzip.open(out_path, "wb") as gz:
        with raw_conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                for chunk in copy:
                    if chunk:
                        gz.write(chunk)
        # COPY does not return rowcount on the cursor reliably; do a fast count.
        cnt = conn.execute(
            __import__("sqlalchemy").text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        ).scalar()
        row_count = int(cnt or 0)

    return row_count


def _prune_old_snapshots(snapshot_dir: Path, retention_days: int) -> int:
    """Delete *.csv.gz files older than `retention_days` in `snapshot_dir`.

    Returns the number of files deleted.
    """
    if not snapshot_dir.exists():
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    deleted = 0
    for f in snapshot_dir.glob("*.csv.gz"):
        try:
            mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
        except OSError:
            continue
        if mtime < cutoff:
            try:
                f.unlink()
                deleted += 1
            except OSError as e:
                logger.warning(f"Failed to delete old snapshot {f.name}: {e}")
    return deleted


# --- Public API ---

def snapshot_critical_tables(reason: str) -> dict[str, Path]:
    """Snapshot the critical rate-intelligence tables to gzipped CSVs.

    Parameters
    ----------
    reason : str
        Short tag for what triggered the snapshot. Used in the filename.
        Examples: "best_estimate_rebuild", "manual_pre_migration".

    Returns
    -------
    dict[str, Path]
        Mapping of table_name → output file path. Tables that fail to
        snapshot are omitted (with a warning logged).

    Notes
    -----
    Output goes to ~/backups/utility-api/snapshots/. The directory is
    created if missing. Snapshots older than 30 days are pruned at the end.
    """
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    schema = settings.utility_schema
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_reason = _sanitize_reason(reason)

    logger.info(
        f"Tier 2 snapshot: reason={safe_reason} schema={schema} "
        f"dir={SNAPSHOT_DIR}"
    )

    out: dict[str, Path] = {}

    # Open one transaction so all snapshots are from the same instant.
    with engine.connect() as conn:
        for table, omit_cols in CRITICAL_TABLES.items():
            out_path = SNAPSHOT_DIR / f"{table}_{safe_reason}_{timestamp}.csv.gz"
            try:
                rows = _copy_table_to_gzip(conn, schema, table, omit_cols, out_path)
                size = out_path.stat().st_size
                logger.info(
                    f"  ✓ {table}: {rows:,} rows → {out_path.name} ({size/1024:.0f} KB)"
                )
                out[table] = out_path
            except Exception as e:
                logger.warning(f"  ✗ {table}: snapshot failed: {e}")
                # Clean up partial file if any
                if out_path.exists():
                    try:
                        out_path.unlink()
                    except OSError:
                        pass

    pruned = _prune_old_snapshots(SNAPSHOT_DIR, RETENTION_DAYS)
    if pruned:
        logger.info(f"Pruned {pruned} snapshot(s) older than {RETENTION_DAYS} days")

    return out
