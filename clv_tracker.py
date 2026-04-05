"""
clv_tracker.py
==============
Closing Line Value (CLV) summary helper.

Reads CLV records from the line_stream SQLite database written by
line_stream.py during the IN_PROGRESS / FINAL phase of each day.

Public API
----------
    get_daily_clv_summary(date_str) → dict

    Return dict keys:
        available  bool   — True if CLV records exist for this date
        beat_close int    — count of legs that beat the closing line
        total_legs int    — total CLV records for this date
        beat_pct   float  — beat_close / total_legs * 100
        avg_clv_pts float — mean CLV points across all legs
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

# line_stream.py writes to LINE_STREAM_DB_PATH (default /tmp/line_stream.db in root,
# /agent/home/line_stream.db in ml_engine/). Read the same env var so both sides agree.
# /app/data is created by Dockerfile and survives within a deployment longer than /tmp,
# though a full Railway volume (set LINE_STREAM_DB_PATH in env) is needed for true persistence.
import os as _os
_DB_PATH = Path(
    _os.getenv("LINE_STREAM_DB_PATH")          # primary — matches line_stream.py writer
    or _os.getenv("CLV_DB_PATH")               # legacy fallback
    or "/app/data/line_stream.db"              # better than /tmp; persists within deploy
)

_EMPTY_SUMMARY: dict = {
    "available":   False,
    "beat_close":  0,
    "total_legs":  0,
    "beat_pct":    0.0,
    "avg_clv_pts": 0.0,
}


def get_daily_clv_summary(date_str: str) -> dict:
    """
    Return CLV summary for a given date.

    date_str: 'YYYY-MM-DD'
    Returns _EMPTY_SUMMARY (available=False) if no records exist.
    """
    if not _DB_PATH.exists():
        logger.debug("[CLV] line_stream.db not found — CLV unavailable")
        return _EMPTY_SUMMARY.copy()

    try:
        conn = sqlite3.connect(str(_DB_PATH))
        conn.row_factory = sqlite3.Row

        row = conn.execute(
            """
            SELECT
                COUNT(*)          AS total,
                SUM(beat_close)   AS beats,
                AVG(clv_pts)      AS avg_clv
              FROM clv_records
             WHERE game_date = ?
            """,
            (date_str,),
        ).fetchone()

        conn.close()

        total = int(row["total"] or 0)
        if total == 0:
            return _EMPTY_SUMMARY.copy()

        beats   = int(row["beats"] or 0)
        avg_clv = float(row["avg_clv"] or 0.0)

        return {
            "available":   True,
            "beat_close":  beats,
            "total_legs":  total,
            "beat_pct":    round(beats / total * 100, 1) if total > 0 else 0.0,
            "avg_clv_pts": round(avg_clv, 3),
        }

    except Exception as exc:
        logger.warning("[CLV] get_daily_clv_summary failed: %s", exc)
        return _EMPTY_SUMMARY.copy()
