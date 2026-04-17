"""
layer_cache_helper.py
=====================
Shared Postgres-backed cache helper for all PropIQ feature layers.

Problem (H-7): Every layer writes to /tmp which is wiped on every Railway
redeploy.  A deploy during the 9 AM dispatch window silently zeroes out all
15+ feature slots for the rest of the day — worse predictions, no log noise.

Fix: two-line drop-in for any layer's _load_cache / _save_cache:

    from layer_cache_helper import pg_cache_get, pg_cache_set

    # In _load_cache():
    data = pg_cache_get("cv_consistency", cache_key, today)

    # In _save_cache():
    pg_cache_set("cv_consistency", cache_key, today, data)

Table: layer_cache (V37 — auto-created on first call)
  layer_name  VARCHAR(40) — identifies which layer (cv_consistency, bayesian, …)
  cache_key   TEXT        — player_id, date, or any string key
  cache_date  DATE        — today's PT date (for TTL pruning)
  data        TEXT        — JSON-serialised value
  UNIQUE(layer_name, cache_key, cache_date)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

_log = logging.getLogger(__name__)
_DB  = os.getenv("DATABASE_URL", "")
_TZ  = ZoneInfo("America/Los_Angeles")

# In-memory L1 cache so the Postgres round-trip only fires once per process
_MEM: dict[tuple, object] = {}


def _today() -> str:
    return datetime.now(_TZ).strftime("%Y-%m-%d")


def _ensure_table(cur) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS layer_cache (
            id          SERIAL      PRIMARY KEY,
            layer_name  VARCHAR(40) NOT NULL,
            cache_key   TEXT        NOT NULL,
            cache_date  DATE        NOT NULL,
            data        TEXT        NOT NULL,
            saved_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (layer_name, cache_key, cache_date)
        )
    """)
    # Prune rows older than 3 days to keep table small
    cur.execute("""
        DELETE FROM layer_cache
        WHERE cache_date < CURRENT_DATE - INTERVAL '3 days'
    """)


def pg_cache_get(layer: str, key: str, date: str | None = None) -> object | None:
    """
    Return cached value for (layer, key, date) or None on miss.
    Checks L1 memory first, then Postgres.
    """
    date = date or _today()
    mem_key = (layer, key, date)
    if mem_key in _MEM:
        return _MEM[mem_key]
    if not _DB:
        return None
    try:
        import psycopg2  # noqa: PLC0415
        conn = psycopg2.connect(_DB)
        cur  = conn.cursor()
        _ensure_table(cur)
        conn.commit()
        cur.execute(
            "SELECT data FROM layer_cache "
            "WHERE layer_name=%s AND cache_key=%s AND cache_date=%s LIMIT 1",
            (layer, str(key), date),
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        if row:
            val = json.loads(row[0])
            _MEM[mem_key] = val
            return val
    except Exception as exc:  # noqa: BLE001
        _log.debug("[layer_cache] get %s/%s failed: %s", layer, key, exc)
    return None


def pg_cache_set(layer: str, key: str, value: object, date: str | None = None) -> None:
    """
    Upsert (layer, key, date) -> JSON(value) into Postgres.
    Also updates L1 memory cache.
    """
    date = date or _today()
    _MEM[(layer, key, date)] = value
    if not _DB:
        return
    try:
        import psycopg2  # noqa: PLC0415
        conn = psycopg2.connect(_DB)
        cur  = conn.cursor()
        _ensure_table(cur)
        cur.execute(
            """
            INSERT INTO layer_cache (layer_name, cache_key, cache_date, data)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (layer_name, cache_key, cache_date) DO UPDATE
                SET data = EXCLUDED.data, saved_at = NOW()
            """,
            (layer, str(key), date, json.dumps(value)),
        )
        conn.commit()
        cur.close(); conn.close()
    except Exception as exc:  # noqa: BLE001
        _log.debug("[layer_cache] set %s/%s failed: %s", layer, key, exc)
