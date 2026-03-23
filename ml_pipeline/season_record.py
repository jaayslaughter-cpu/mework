"""
season_record.py
================
Thin wrapper around Postgres (via DATABASE_URL / POSTGRES_URL) for tracking
PropIQ parlay results throughout the season.

Connection priority:
    1. POSTGRES_URL env var
    2. DATABASE_URL env var  (Railway default)
    3. SQLite fallback at /tmp/propiq_season.db  (local dev / backtest only)

Table: propiq_season_record
    id            SERIAL PK
    date          TEXT        e.g. "2026-04-01"
    agent_name    TEXT        e.g. "EVHunter"
    num_legs      INTEGER     2-4
    confidence    REAL        0.0-10.0
    ev_pct        REAL        overall parlay EV %
    stake         REAL        default 20.0
    platform      TEXT        PrizePicks | Underdog
    status        TEXT        PENDING | WIN | LOSS | PUSH
    units_profit  REAL        +/- units (stake=1 unit)
    legs_json     TEXT        JSON array of leg dicts
    created_at    TIMESTAMPTZ
    settled_at    TIMESTAMPTZ

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
import os
import sqlite3
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connection helpers — Postgres primary, SQLite fallback
# ---------------------------------------------------------------------------

_SQLITE_FALLBACK = Path("/tmp/propiq_season.db")

_PG_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS propiq_season_record (
    id           SERIAL PRIMARY KEY,
    date         TEXT             NOT NULL,
    agent_name   TEXT             NOT NULL,
    num_legs     INTEGER          NOT NULL DEFAULT 0,
    confidence   DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    ev_pct       DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    stake        DOUBLE PRECISION NOT NULL DEFAULT 20.0,
    platform     TEXT             NOT NULL DEFAULT 'Underdog',
    status       TEXT             NOT NULL DEFAULT 'PENDING',
    units_profit DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    legs_json    TEXT             NOT NULL DEFAULT '[]',
    created_at   TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    settled_at   TIMESTAMPTZ
)
"""

_PG_MIGRATE_DDLS = [
    "ALTER TABLE propiq_season_record ADD COLUMN IF NOT EXISTS platform TEXT NOT NULL DEFAULT 'Underdog'",
    "ALTER TABLE propiq_season_record ADD COLUMN IF NOT EXISTS settled_at TIMESTAMPTZ",
    "ALTER TABLE propiq_season_record ADD COLUMN IF NOT EXISTS legs_json TEXT NOT NULL DEFAULT '[]'",
]

_SQLITE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS propiq_season_record (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    date         TEXT    NOT NULL,
    agent_name   TEXT    NOT NULL,
    num_legs     INTEGER NOT NULL DEFAULT 0,
    confidence   REAL    NOT NULL DEFAULT 0.0,
    ev_pct       REAL    NOT NULL DEFAULT 0.0,
    stake        REAL    NOT NULL DEFAULT 20.0,
    platform     TEXT    NOT NULL DEFAULT 'Underdog',
    status       TEXT    NOT NULL DEFAULT 'PENDING',
    units_profit REAL    NOT NULL DEFAULT 0.0,
    legs_json    TEXT    NOT NULL DEFAULT '[]',
    created_at   TEXT    NOT NULL DEFAULT (datetime('now')),
    settled_at   TEXT
)
"""


def _get_pg_conn():
    """Return a live psycopg2 connection or None."""
    url = os.getenv("POSTGRES_URL", os.getenv("DATABASE_URL", ""))
    if not url:
        return None
    try:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(url)
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(_PG_TABLE_DDL)
            for ddl in _PG_MIGRATE_DDLS:
                try:
                    cur.execute(ddl)
                except Exception:
                    conn.rollback()
                    with conn.cursor() as cur2:
                        cur2.execute(_PG_TABLE_DDL)
        conn.commit()
        return conn
    except ImportError:
        logger.warning("season_record: psycopg2 not installed — falling back to SQLite")
        return None
    except Exception as exc:
        logger.warning("season_record: Postgres connection failed (%s) — falling back to SQLite", exc)
        return None


def _get_sqlite_conn() -> sqlite3.Connection:
    """Return a SQLite connection (always succeeds — used for local dev/backtest)."""
    conn = sqlite3.connect(str(_SQLITE_FALLBACK))
    conn.row_factory = sqlite3.Row
    conn.execute(_SQLITE_TABLE_DDL)
    for col in ("platform", "settled_at", "legs_json"):
        try:
            conn.execute(f"ALTER TABLE propiq_season_record ADD COLUMN {col} TEXT")
        except sqlite3.OperationalError:
            pass
    conn.commit()
    return conn


def _get_conn():
    """Return (conn, is_postgres). Always returns a valid connection."""
    pg = _get_pg_conn()
    if pg is not None:
        return pg, True
    logger.info("season_record: using SQLite fallback at %s", _SQLITE_FALLBACK)
    return _get_sqlite_conn(), False


def _dict_rows(conn, is_pg: bool, rows) -> list[dict]:
    """Normalise rows to list of plain dicts regardless of driver."""
    if is_pg:
        import psycopg2.extras  # noqa: F401
        return [dict(r) for r in rows]
    return [dict(r) for r in rows]


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
    platform: str = "Underdog",
    legs: list[dict] | None = None,
) -> bool:
    """
    Insert a new PENDING parlay record.
    legs: list of dicts with keys player_name, prop_type, side, line.
    Returns True on success, False on error.
    """
    conn, is_pg = _get_conn()
    ph = "%s" if is_pg else "?"
    try:
        legs_json = json.dumps(legs or [])
        sql = f"""
            INSERT INTO propiq_season_record
                (date, agent_name, num_legs, confidence, ev_pct, stake, platform, status, units_profit, legs_json)
            VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, 'PENDING', 0.0, {ph})
        """
        if is_pg:
            with conn.cursor() as cur:
                cur.execute(sql, (date, agent, num_legs, confidence, ev_pct, stake, platform, legs_json))
        else:
            conn.execute(sql, (date, agent, num_legs, confidence, ev_pct, stake, platform, legs_json))
        conn.commit()
        return True
    except Exception as exc:
        logger.warning("season_record.record_parlay: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        conn.close()


def get_pending_parlays(date: str) -> list[dict]:
    """
    Return all PENDING parlays for a given date as a list of dicts.
    Each dict includes: id, agent_name, stake, platform, legs (parsed).
    """
    conn, is_pg = _get_conn()
    ph = "%s" if is_pg else "?"
    try:
        sql = f"""
            SELECT id, agent_name, stake, platform, legs_json
            FROM propiq_season_record
            WHERE date = {ph} AND status = 'PENDING'
            ORDER BY id
        """
        if is_pg:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (date,))
                rows = cur.fetchall()
        else:
            rows = conn.execute(sql, (date,)).fetchall()

        result = []
        for row in rows:
            row = dict(row)
            try:
                legs = json.loads(row.get("legs_json") or "[]")
            except Exception:
                legs = []
            result.append({
                "id":         row["id"],
                "agent_name": row["agent_name"],
                "stake":      row["stake"],
                "platform":   row.get("platform", "Underdog"),
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
    conn, is_pg = _get_conn()
    ph = "%s" if is_pg else "?"
    try:
        sql = f"""
            UPDATE propiq_season_record
            SET status = {ph}, units_profit = {ph}, settled_at = {'NOW()' if is_pg else "datetime('now')"}
            WHERE id = {ph} AND status = 'PENDING'
        """
        if is_pg:
            with conn.cursor() as cur:
                cur.execute(sql, (status, units_profit, parlay_id))
        else:
            conn.execute(sql, (status, units_profit, parlay_id))
        conn.commit()
        return True
    except Exception as exc:
        logger.warning("season_record.settle_parlay_record: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        conn.close()


def update_parlay_result(
    date: str,
    agent: str,
    status: str,
    units_profit: float,
) -> bool:
    """
    Update the most recent PENDING record for date+agent.
    Postgres: uses subquery (no ORDER BY in UPDATE).
    SQLite: uses ORDER BY + LIMIT.
    Returns True on success.
    """
    if status not in ("WIN", "LOSS", "PUSH"):
        logger.warning("season_record: invalid status '%s'", status)
        return False
    conn, is_pg = _get_conn()
    ph = "%s" if is_pg else "?"
    try:
        if is_pg:
            sql = f"""
                UPDATE propiq_season_record
                SET status = {ph}, units_profit = {ph}, settled_at = NOW()
                WHERE id = (
                    SELECT id FROM propiq_season_record
                    WHERE date = {ph} AND agent_name = {ph} AND status = 'PENDING'
                    ORDER BY id DESC
                    LIMIT 1
                )
            """
            with conn.cursor() as cur:
                cur.execute(sql, (status, units_profit, date, agent))
        else:
            sql = f"""
                UPDATE propiq_season_record
                SET status = {ph}, units_profit = {ph}, settled_at = datetime('now')
                WHERE date = {ph} AND agent_name = {ph} AND status = 'PENDING'
                ORDER BY id DESC
                LIMIT 1
            """
            conn.execute(sql, (status, units_profit, date, agent))
        conn.commit()
        return True
    except Exception as exc:
        logger.warning("season_record.update_parlay_result: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
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
        "last_result": str
    }
    """
    empty: dict[str, Any] = {
        "wins": 0, "losses": 0, "pushes": 0, "pending": 0,
        "units_profit": 0.0, "roi_pct": 0.0,
        "record": "0W-0L-0P", "last_result": "—",
    }
    conn, is_pg = _get_conn()
    ph = "%s" if is_pg else "?"
    try:
        sql = f"""
            SELECT status, SUM(units_profit) AS pnl, COUNT(*) AS cnt
            FROM propiq_season_record
            WHERE agent_name = {ph}
            GROUP BY status
        """
        if is_pg:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (agent,))
                rows = [dict(r) for r in cur.fetchall()]
        else:
            rows = [dict(r) for r in conn.execute(sql, (agent,)).fetchall()]

        stats: dict[str, Any] = {
            "wins": 0, "losses": 0, "pushes": 0, "pending": 0,
            "units_profit": 0.0,
        }
        for row in rows:
            s = row["status"]
            if s == "WIN":
                stats["wins"] = row["cnt"]
                stats["units_profit"] += float(row["pnl"] or 0.0)
            elif s == "LOSS":
                stats["losses"] = row["cnt"]
                stats["units_profit"] += float(row["pnl"] or 0.0)
            elif s == "PUSH":
                stats["pushes"] = row["cnt"]
            else:
                stats["pending"] = row["cnt"]

        total_resolved = stats["wins"] + stats["losses"] + stats["pushes"]
        roi = (stats["units_profit"] / (total_resolved or 1)) * 100

        last_sql = f"""
            SELECT status FROM propiq_season_record
            WHERE agent_name = {ph}
            ORDER BY id DESC LIMIT 1
        """
        if is_pg:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(last_sql, (agent,))
                last_row = cur.fetchone()
        else:
            last_row = conn.execute(last_sql, (agent,)).fetchone()

        last_result = dict(last_row)["status"] if last_row else "—"

        return {
            **stats,
            "roi_pct":     round(roi, 1),
            "record":      f"{stats['wins']}W-{stats['losses']}L-{stats['pushes']}P",
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
    empty: dict[str, Any] = {
        "wins": 0, "losses": 0, "pushes": 0, "pending": 0,
        "units_profit": 0.0, "roi_pct": 0.0,
        "record": "0W-0L-0P", "parlays_sent": 0,
    }
    conn, is_pg = _get_conn()
    try:
        sql = """
            SELECT status, SUM(units_profit) AS pnl, COUNT(*) AS cnt
            FROM propiq_season_record
            GROUP BY status
        """
        if is_pg:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                rows = [dict(r) for r in cur.fetchall()]
        else:
            rows = [dict(r) for r in conn.execute(sql).fetchall()]

        stats: dict[str, Any] = {
            "wins": 0, "losses": 0, "pushes": 0, "pending": 0,
            "units_profit": 0.0,
        }
        for row in rows:
            s = row["status"]
            if s == "WIN":
                stats["wins"] = row["cnt"]
                stats["units_profit"] += float(row["pnl"] or 0.0)
            elif s == "LOSS":
                stats["losses"] = row["cnt"]
                stats["units_profit"] += float(row["pnl"] or 0.0)
            elif s == "PUSH":
                stats["pushes"] = row["cnt"]
            else:
                stats["pending"] = row["cnt"]

        total_resolved = stats["wins"] + stats["losses"] + stats["pushes"]
        roi = (stats["units_profit"] / (total_resolved or 1)) * 100

        return {
            **stats,
            "roi_pct":      round(roi, 1),
            "record":       f"{stats['wins']}W-{stats['losses']}L-{stats['pushes']}P",
            "parlays_sent": total_resolved + stats["pending"],
        }
    except Exception as exc:
        logger.warning("season_record.get_overall_season_stats: %s", exc)
        return empty
    finally:
        conn.close()
