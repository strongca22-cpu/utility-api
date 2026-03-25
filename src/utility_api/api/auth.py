#!/usr/bin/env python3
"""
API Authentication Middleware

Purpose:
    API key validation and rate limiting for the Utility Intelligence API.
    Keys are passed via X-API-Key header. Rate limits are tier-based:
    free=100/day, basic=1000/day, premium=10000/day.

Author: AI-Generated
Created: 2026-03-25
Modified: 2026-03-25

Dependencies:
    - fastapi
    - sqlalchemy

Usage:
    # In app.py:
    from utility_api.api.auth import require_api_key
    app.include_router(router, dependencies=[Depends(require_api_key)])

Notes:
    - /health endpoint is exempt from auth
    - /docs and /openapi.json are exempt from auth
    - Keys are stored as SHA-256 hashes (never plaintext in DB)
    - Daily request counts reset on first request of a new day
    - Usage tracking (request_count, last_used_at) updates on every request
"""

import hashlib
from datetime import date, datetime, timezone

from fastapi import Depends, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from utility_api.api.dependencies import get_db
from utility_api.config import settings

# Rate limits by tier (requests per day)
TIER_LIMITS = {
    "free": 100,
    "basic": 1000,
    "premium": 10000,
}

# Paths that don't require authentication
EXEMPT_PATHS = {"/health", "/docs", "/openapi.json", "/redoc"}


def hash_api_key(key: str) -> str:
    """Hash an API key with SHA-256 for storage/lookup."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def require_api_key(request: Request, db: Session = Depends(get_db)):
    """FastAPI dependency that validates API key and enforces rate limits.

    Checks X-API-Key header, validates against api_keys table, enforces
    daily rate limits per tier, and updates usage tracking.

    Raises HTTPException 401 for missing/invalid keys, 429 for rate limit exceeded.
    """
    # Exempt certain paths
    if request.url.path in EXEMPT_PATHS:
        return None

    # Extract key from header
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing API key. Pass via X-API-Key header.",
        )

    # Look up key hash
    key_hash = hash_api_key(api_key)
    schema = settings.utility_schema

    row = db.execute(text(f"""
        SELECT id, name, tier, is_active, daily_request_count, daily_reset_date
        FROM {schema}.api_keys
        WHERE key_hash = :key_hash
    """), {"key_hash": key_hash}).mappings().first()

    if not row:
        raise HTTPException(status_code=401, detail="Invalid API key.")

    if not row["is_active"]:
        raise HTTPException(status_code=401, detail="API key has been revoked.")

    # Check rate limit
    tier = row["tier"]
    limit = TIER_LIMITS.get(tier, 100)
    today = date.today()

    daily_count = row["daily_request_count"]
    if row["daily_reset_date"] != today:
        # New day — reset counter
        daily_count = 0

    if daily_count >= limit:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded. {tier} tier allows {limit} requests/day.",
            headers={"Retry-After": "3600"},
        )

    # Update usage tracking
    db.execute(text(f"""
        UPDATE {schema}.api_keys SET
            request_count = request_count + 1,
            daily_request_count = CASE
                WHEN daily_reset_date = :today THEN daily_request_count + 1
                ELSE 1
            END,
            daily_reset_date = :today,
            last_used_at = :now
        WHERE id = :key_id
    """), {
        "today": today,
        "now": datetime.now(timezone.utc),
        "key_id": row["id"],
    })
    db.commit()

    # Store key info on request state for downstream use
    request.state.api_key_name = row["name"]
    request.state.api_key_tier = tier

    return row["name"]
