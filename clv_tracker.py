"""
clv_tracker.py
==============
Closing Line Value (CLV) summary helper.

Reads CLV records from the Postgres clv_records table (V36 migration).
Falls back to SQLite line_stream.db if Postgres is unavailable.

Public API
----------
    get_daily_clv_summary(date_str) -> dict

    Return dict keys:
        available  bool   -- True if CLV records exist for this date
        beat_close int    -- count of legs that beat the closing line
        total_legs int    -- total CLV records for this date
        beat_pct   float  -- beat_close / total_legs * 100
        avg_clv_pts float -- mean CLV points across all legs
"""

from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

_DB_PATH = Path(
    os.getenv("LINE_STREAM_DB_PATH")
    or os.getenv("CLV_DB_PATH")
    or "/app/data/line_stream.db"
)

_EMPTY_SUMMARY: dict = {
    "available":   False,
    "beat_close":  0,
    "total_legs":  0,
    "beat_pct":    0.0,
    "avg_clv_pts": 0.0,
}


def _ensure_pg_table() -> None:
    """Create clv_records Postgres table if it doesn't exist (V36 migration)."""
    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        return
    try:
        import psycopg2
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS clv_records (
                        id           SERIAL PRIMARY KEY,
                        game_date    DATE NOT NULL,
                        agent_name   TEXT,
                        player_name  TEXT,
                        prop_type    TEXT,
                        side         TEXT,
                        pick_line    FLOAT,
                        closing_line FLOAT,
                        clv_pts      FLOAT,
                        beat_close   INTEGER,
                        recorded_at  TIMESTAMPTZ DEFAULT NOW()
                    );
                    CREATE INDEX IF NOT EXISTS idx_clv_game_date ON clv_records (game_date);
                """)
            conn.commit()
    except Exception as exc:
        logger.debug("[CLV] _ensure_pg_table: %s", exc)


def insert_clv_record(
    game_date: str,
    player_name: str,
    prop_type: str,
    side: str,
    pick_line: float,
    closing_line: float,
    clv_pts: float,
    beat_close: int,
    agent_name: str = "",
) -> None:
    """Write one CLV record to Postgres. Called by line_stream.py at CLV compute time."""
    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        return
    try:
        import psycopg2
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO clv_records
                        (game_date, agent_name, player_name, prop_type, side,
                         pick_line, closing_line, clv_pts, beat_close)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (game_date, agent_name, player_name, prop_type, side,
                      pick_line, closing_line, clv_pts, beat_close))
            conn.commit()
    except Exception as exc:
        logger.warning("[CLV] insert_clv_record failed: %s", exc)


def _query_pg(date_str: str) -> dict | None:
    """Try Postgres first — returns None if unavailable or no rows."""
    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        return None
    try:
        import psycopg2
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) AS total,
                           SUM(beat_close) AS beats,
                           AVG(clv_pts) AS avg_clv
                    FROM clv_records
                    WHERE game_date = %s
                """, (date_str,))
                row = cur.fetchone()
        if not row or not row[0]:
            return None
        total = int(row[0] or 0)
        if total == 0:
            return None
        beats   = int(row[1] or 0)
        avg_clv = float(row[2] or 0.0)
        return {
            "available":   True,
            "beat_close":  beats,
            "total_legs":  total,
            "beat_pct":    round(beats / total * 100, 1),
            "avg_clv_pts": round(avg_clv, 3),
        }
    except Exception as exc:
        logger.debug("[CLV] Postgres query failed: %s", exc)
        return None


def _query_sqlite(date_str: str) -> dict | None:
    """SQLite fallback — still works when Postgres is down or on local dev."""
    if not _DB_PATH.exists():
        return None
    try:
        conn = sqlite3.connect(str(_DB_PATH))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT COUNT(*) AS total, SUM(beat_close) AS beats, AVG(clv_pts) AS avg_clv
            FROM clv_records WHERE game_date = ?
            """,
            (date_str,),
        ).fetchone()
        conn.close()
        total = int(row["total"] or 0)
        if total == 0:
            return None
        beats   = int(row["beats"] or 0)
        avg_clv = float(row["avg_clv"] or 0.0)
        return {
            "available":   True,
            "beat_close":  beats,
            "total_legs":  total,
            "beat_pct":    round(beats / total * 100, 1),
            "avg_clv_pts": round(avg_clv, 3),
        }
    except Exception as exc:
        logger.warning("[CLV] SQLite query failed: %s", exc)
        return None


def get_daily_clv_summary(date_str: str) -> dict:
    """
    Return CLV summary for a given date.

    date_str: 'YYYY-MM-DD'
    Tries Postgres first, falls back to SQLite.
    Returns _EMPTY_SUMMARY (available=False) if no records exist.
    """
    result = _query_pg(date_str) or _query_sqlite(date_str)
    if result:
        return result
    logger.debug("[CLV] No CLV records found for %s", date_str)
    return _EMPTY_SUMMARY.copy()
