"""
season_record.py
================
Thin wrapper around the agent's built-in SQL database for tracking
PropIQ parlay results throughout the season.

Table: propiq_season_record
    id            INTEGER PK AUTOINCREMENT
    date          TEXT        e.g. "2026-04-01"
    agent_name    TEXT        e.g. "EVHunter"
    num_legs      INTEGER     2-4
    confidence    REAL        0.0-10.0
    ev_pct        REAL        overall parlay EV %
    stake         REAL        default 20.0
    status        TEXT        PENDING | WIN | LOSS | PUSH
    units_profit  REAL        +/- units (stake=1 unit)
    legs_json     TEXT        JSON array of leg dicts (player_name, prop_type, side, line)
    created_at    TEXT        UTC datetime

Usage
-----
    from season_record import record_parlay, get_agent_season_stats

    record_parlay(date="2026-04-01", agent="EVHunter",
                  num_legs=4, confidence=9.2, ev_pct=14.3)

    stats = get_agent_season_stats("EVHunter")
    # {"wins": 7, "losses": 3, "pushes": 0, "pending": 1,
    #  "units_profit": 4.2, "roi_pct": 21.0, "record": "7W-3L-0P"}
"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# The agent SQL DB is accessible at this path when running inside the agent.
# Outside agent context (e.g. backtest) this falls back to /tmp/propiq.db.
# ---------------------------------------------------------------------------
_DB_PATHS = [
    Path("/agent/data/memory.db"),   # Primary agent DB path
    Path("/agent/memory.db"),        # Alternative location
    Path("/tmp/propiq_season.db"),   # Fallback for local runs
]

_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS propiq_season_record (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    date          TEXT    NOT NULL,
    agent_name    TEXT    NOT NULL,
    num_legs      INTEGER NOT NULL DEFAULT 0,
    confidence    REAL    NOT NULL DEFAULT 0.0,
    ev_pct        REAL    NOT NULL DEFAULT 0.0,
    stake         REAL    NOT NULL DEFAULT 20.0,
    status        TEXT    NOT NULL DEFAULT 'PENDING',
    units_profit  REAL    NOT NULL DEFAULT 0.0,
    legs_json     TEXT    NOT NULL DEFAULT '[]',
    created_at    TEXT    NOT NULL DEFAULT (datetime('now'))
)
"""

_MIGRATE_DDL = """
ALTER TABLE propiq_season_record ADD COLUMN legs_json TEXT NOT NULL DEFAULT '[]'
"""


def _get_conn() -> Optional[sqlite3.Connection]:
    """Return a sqlite3 connection to the first found DB path, or None."""
    for p in _DB_PATHS:
        if p.exists():
            try:
                conn = sqlite3.connect(str(p))
                conn.row_factory = sqlite3.Row
                # Ensure table exists
                conn.execute(_TABLE_DDL)
                # Migrate: add legs_json column if absent
                try:
                    conn.execute(_MIGRATE_DDL)
                    conn.commit()
                except sqlite3.OperationalError:
                    pass  # Column already exists
                conn.commit()
                return conn
            except Exception as exc:
                logger.debug("season_record: cannot open %s — %s", p, exc)
    logger.warning("season_record: no DB path found — season tracking disabled")
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def record_parlay(
    date: str,
    agent: str,
    num_legs: int,
    confidence: float,
    ev_pct: float,
    stake: float = 20.0,
    legs: list[dict] | None = None,
) -> bool:
    """
    Insert a new PENDING parlay record.
    legs: list of dicts with keys player_name, prop_type, side, line.
    Returns True on success, False if DB unavailable.
    """
    conn = _get_conn()
    if conn is None:
        return False
    try:
        legs_json = json.dumps(legs or [])
        conn.execute(
            """
            INSERT INTO propiq_season_record
                (date, agent_name, num_legs, confidence, ev_pct, stake, status, units_profit, legs_json)
            VALUES (?, ?, ?, ?, ?, ?, 'PENDING', 0.0, ?)
            """,
            (date, agent, num_legs, confidence, ev_pct, stake, legs_json),
        )
        conn.commit()
        return True
    except Exception as exc:
        logger.warning("season_record.record_parlay: %s", exc)
        return False
    finally:
        conn.close()


def get_pending_parlays(date: str) -> list[dict]:
    """
    Return all PENDING parlays for a given date as a list of dicts.
    Each dict includes: id, agent_name, stake, legs (parsed from legs_json).
    """
    conn = _get_conn()
    if conn is None:
        return []
    try:
        rows = conn.execute(
            """
            SELECT id, agent_name, stake, legs_json
            FROM propiq_season_record
            WHERE date = ? AND status = 'PENDING'
            ORDER BY id
            """,
            (date,),
        ).fetchall()
        result = []
        for row in rows:
            try:
                legs = json.loads(row["legs_json"] or "[]")
            except Exception:
                legs = []
            result.append({
                "id":         row["id"],
                "agent_name": row["agent_name"],
                "stake":      row["stake"],
                "legs":       legs,
            })
        return result
    except Exception as exc:
        logger.warning("season_record.get_pending_parlays: %s", exc)
        return []
    finally:
        conn.close()


def settle_parlay_record(
    parlay_id: int,
    status: str,
    units_profit: float,
) -> bool:
    """Update a parlay record by ID with WIN/LOSS/PUSH outcome."""
    if status not in ("WIN", "LOSS", "PUSH"):
        return False
    conn = _get_conn()
    if conn is None:
        return False
    try:
        conn.execute(
            """
            UPDATE propiq_season_record
            SET status = ?, units_profit = ?
            WHERE id = ? AND status = 'PENDING'
            """,
            (status, units_profit, parlay_id),
        )
        conn.commit()
        return True
    except Exception as exc:
        logger.warning("season_record.settle_parlay_record: %s", exc)
        return False
    finally:
        conn.close()


def update_parlay_result(
    date: str,
    agent: str,
    status: str,          # "WIN" | "LOSS" | "PUSH"
    units_profit: float,
) -> bool:
    """
    Update the most recent matching PENDING record for date+agent.
    Returns True on success.
    """
    if status not in ("WIN", "LOSS", "PUSH"):
        logger.warning("season_record: invalid status '%s'", status)
        return False
    conn = _get_conn()
    if conn is None:
        return False
    try:
        conn.execute(
            """
            UPDATE propiq_season_record
            SET status = ?, units_profit = ?
            WHERE date = ? AND agent_name = ? AND status = 'PENDING'
            ORDER BY id DESC
            LIMIT 1
            """,
            (status, units_profit, date, agent),
        )
        conn.commit()
        return True
    except Exception as exc:
        logger.warning("season_record.update_parlay_result: %s", exc)
        return False
    finally:
        conn.close()


def get_agent_season_stats(agent: str) -> dict:
    """
    Return current season stats for one agent.

    Returns
    -------
    {
        "wins": int, "losses": int, "pushes": int, "pending": int,
        "units_profit": float, "roi_pct": float, "record": str,
        "last_result": str   # "WIN" | "LOSS" | "PUSH" | "PENDING"
    }
    """
    empty = {
        "wins": 0, "losses": 0, "pushes": 0, "pending": 0,
        "units_profit": 0.0, "roi_pct": 0.0,
        "record": "0W-0L-0P", "last_result": "—",
    }
    conn = _get_conn()
    if conn is None:
        return empty
    try:
        rows = conn.execute(
            """
            SELECT status, SUM(units_profit) AS pnl, COUNT(*) AS cnt
            FROM propiq_season_record
            WHERE agent_name = ?
            GROUP BY status
            """,
            (agent,),
        ).fetchall()

        stats: dict[str, int | float] = {
            "wins": 0, "losses": 0, "pushes": 0, "pending": 0,
            "units_profit": 0.0,
        }
        for row in rows:
            s = row["status"]
            if s == "WIN":
                stats["wins"] = row["cnt"]
                stats["units_profit"] = stats["units_profit"] + (row["pnl"] or 0.0)
            elif s == "LOSS":
                stats["losses"] = row["cnt"]
                stats["units_profit"] = stats["units_profit"] + (row["pnl"] or 0.0)
            elif s == "PUSH":
                stats["pushes"] = row["cnt"]
            else:
                stats["pending"] = row["cnt"]

        total_resolved = stats["wins"] + stats["losses"] + stats["pushes"]
        roi = (stats["units_profit"] / (total_resolved or 1)) * 100

        last_row = conn.execute(
            """
            SELECT status FROM propiq_season_record
            WHERE agent_name = ?
            ORDER BY id DESC LIMIT 1
            """,
            (agent,),
        ).fetchone()
        last_result = last_row["status"] if last_row else "—"

        return {
            **stats,
            "roi_pct": round(roi, 1),
            "record": f"{stats['wins']}W-{stats['losses']}L-{stats['pushes']}P",
            "last_result": last_result,
        }
    except Exception as exc:
        logger.warning("season_record.get_agent_season_stats: %s", exc)
        return empty
    finally:
        conn.close()


def get_overall_season_stats() -> dict:
    """
    Return aggregate season stats across ALL agents.

    Returns
    -------
    {
        "wins": int, "losses": int, "pushes": int, "pending": int,
        "units_profit": float, "roi_pct": float, "record": str,
        "parlays_sent": int
    }
    """
    empty = {
        "wins": 0, "losses": 0, "pushes": 0, "pending": 0,
        "units_profit": 0.0, "roi_pct": 0.0,
        "record": "0W-0L-0P", "parlays_sent": 0,
    }
    conn = _get_conn()
    if conn is None:
        return empty
    try:
        rows = conn.execute(
            """
            SELECT status, SUM(units_profit) AS pnl, COUNT(*) AS cnt
            FROM propiq_season_record
            GROUP BY status
            """
        ).fetchall()

        stats: dict = {
            "wins": 0, "losses": 0, "pushes": 0, "pending": 0,
            "units_profit": 0.0,
        }
        for row in rows:
            s = row["status"]
            if s == "WIN":
                stats["wins"] = row["cnt"]
                stats["units_profit"] += row["pnl"] or 0.0
            elif s == "LOSS":
                stats["losses"] = row["cnt"]
                stats["units_profit"] += row["pnl"] or 0.0
            elif s == "PUSH":
                stats["pushes"] = row["cnt"]
            else:
                stats["pending"] = row["cnt"]

        total_resolved = stats["wins"] + stats["losses"] + stats["pushes"]
        roi = (stats["units_profit"] / (total_resolved or 1)) * 100

        return {
            **stats,
            "roi_pct": round(roi, 1),
            "record": f"{stats['wins']}W-{stats['losses']}L-{stats['pushes']}P",
            "parlays_sent": total_resolved + stats["pending"],
        }
    except Exception as exc:
        logger.warning("season_record.get_overall_season_stats: %s", exc)
        return empty
    finally:
        conn.close()
