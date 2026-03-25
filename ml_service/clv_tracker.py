"""
clv_tracker.py
==============
Query utilities for CLV (Closing Line Value) records stored by line_stream.py.

Used by nightly_recap.py to include CLV stats in the nightly Discord recap,
and available standalone for ad-hoc CLV analysis.

CLV is computed in line_stream.py and stored in /agent/home/line_stream.db.
This module is read-only — it never writes to the database.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

_DB_PATH = Path("/agent/home/line_stream.db")


def _get_db() -> sqlite3.Connection | None:
    """Return a read-only connection to line_stream.db, or None if absent."""
    if not _DB_PATH.exists():
        return None
    try:
        conn = sqlite3.connect(str(_DB_PATH))
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as exc:
        logger.warning("[CLVTracker] Cannot open DB: %s", exc)
        return None


def get_daily_clv_records(date_str: str) -> list[dict]:
    """
    Return all CLV records stored for a given date.

    Each record dict:
        player_name, prop_type, side, pick_line, closing_line,
        clv_pts, beat_close (bool)
    """
    conn = _get_db()
    if conn is None:
        return []
    try:
        rows = conn.execute(
            """
            SELECT player_name, prop_type, side, pick_line,
                   closing_line, clv_pts, beat_close
            FROM clv_records
            WHERE game_date = ?
            ORDER BY ABS(clv_pts) DESC
            """,
            (date_str,),
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception as exc:
        logger.warning("[CLVTracker] get_daily_clv_records failed: %s", exc)
        return []
    finally:
        conn.close()


def get_daily_clv_summary(date_str: str) -> dict:
    """
    Return aggregate CLV stats for a given date.

    Returns:
        {
            "total_legs":   int,
            "beat_close":   int,
            "missed_close": int,
            "beat_pct":     float,   # 0-100
            "avg_clv_pts":  float,
            "available":    bool     # False if no CLV data for this date
        }
    """
    empty = {
        "total_legs": 0,
        "beat_close": 0,
        "missed_close": 0,
        "beat_pct": 0.0,
        "avg_clv_pts": 0.0,
        "available": False,
    }
    conn = _get_db()
    if conn is None:
        return empty
    try:
        row = conn.execute(
            """
            SELECT
                COUNT(*)              AS total_legs,
                SUM(beat_close)       AS beat_close,
                AVG(clv_pts)          AS avg_clv_pts
            FROM clv_records
            WHERE game_date = ?
            """,
            (date_str,),
        ).fetchone()

        if not row or not row["total_legs"]:
            return empty

        total = row["total_legs"]
        beats = int(row["beat_close"] or 0)
        avg = round(float(row["avg_clv_pts"] or 0.0), 2)
        beat_pct = round(beats / total * 100, 1) if total else 0.0

        return {
            "total_legs":   total,
            "beat_close":   beats,
            "missed_close": total - beats,
            "beat_pct":     beat_pct,
            "avg_clv_pts":  avg,
            "available":    True,
        }
    except Exception as exc:
        logger.warning("[CLVTracker] get_daily_clv_summary failed: %s", exc)
        return empty
    finally:
        conn.close()


def get_season_clv_summary() -> dict:
    """
    Return aggregate CLV stats across all recorded dates.

    Returns same shape as get_daily_clv_summary plus 'dates_tracked'.
    """
    empty = {
        "total_legs": 0,
        "beat_close": 0,
        "missed_close": 0,
        "beat_pct": 0.0,
        "avg_clv_pts": 0.0,
        "dates_tracked": 0,
        "available": False,
    }
    conn = _get_db()
    if conn is None:
        return empty
    try:
        row = conn.execute(
            """
            SELECT
                COUNT(*)                  AS total_legs,
                SUM(beat_close)           AS beat_close,
                AVG(clv_pts)              AS avg_clv_pts,
                COUNT(DISTINCT game_date) AS dates_tracked
            FROM clv_records
            """,
        ).fetchone()

        if not row or not row["total_legs"]:
            return empty

        total = row["total_legs"]
        beats = int(row["beat_close"] or 0)
        avg = round(float(row["avg_clv_pts"] or 0.0), 2)
        beat_pct = round(beats / total * 100, 1) if total else 0.0

        return {
            "total_legs":    total,
            "beat_close":    beats,
            "missed_close":  total - beats,
            "beat_pct":      beat_pct,
            "avg_clv_pts":   avg,
            "dates_tracked": int(row["dates_tracked"] or 0),
            "available":     True,
        }
    except Exception as exc:
        logger.warning("[CLVTracker] get_season_clv_summary failed: %s", exc)
        return empty
    finally:
        conn.close()
