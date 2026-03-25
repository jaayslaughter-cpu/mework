"""
season_record.py
================
Postgres-backed season record tracker for PropIQ parlays.

Schema: propiq_season_record
  id          SERIAL PRIMARY KEY
  date        DATE
  agent_name  TEXT
  parlay_legs INTEGER
  platform    TEXT
  stake       NUMERIC(10,2)
  payout      NUMERIC(10,2)
  confidence  NUMERIC(4,2)
  status      TEXT   ('PENDING' | 'WIN' | 'LOSS' | 'PUSH')
  legs_json   TEXT   (JSON array of leg dicts)
  created_at  TIMESTAMPTZ
  settled_at  TIMESTAMPTZ

All functions degrade gracefully when DATABASE_URL is not set.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_DEFAULT_STAKE  = 20.0   # dollars per parlay
_DEFAULT_PAYOUT = 40.0   # 2× on winning FLEX


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_conn():
    """Return a psycopg2 connection, or None if DATABASE_URL is unset."""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logger.debug("[SeasonRecord] DATABASE_URL not set — Postgres unavailable")
        return None
    try:
        import psycopg2  # noqa: PLC0415
        return psycopg2.connect(db_url)
    except Exception as exc:
        logger.warning("[SeasonRecord] Connection failed: %s", exc)
        return None


def _ensure_table(conn) -> None:
    """Create propiq_season_record if it does not already exist."""
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS propiq_season_record (
                id          SERIAL PRIMARY KEY,
                date        DATE            NOT NULL,
                agent_name  TEXT            NOT NULL,
                parlay_legs INTEGER         DEFAULT 0,
                platform    TEXT            DEFAULT 'Mixed',
                stake       NUMERIC(10, 2)  DEFAULT 20.00,
                payout      NUMERIC(10, 2)  DEFAULT  0.00,
                confidence  NUMERIC(4,  2)  DEFAULT  0.00,
                status      TEXT            DEFAULT 'PENDING',
                legs_json   TEXT,
                created_at  TIMESTAMPTZ     DEFAULT NOW(),
                settled_at  TIMESTAMPTZ
            )
        """)
        conn.commit()
        cur.close()
    except Exception as exc:
        logger.warning("[SeasonRecord] Table creation failed: %s", exc)


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

def record_parlay(
    date: str,
    agent: str,
    num_legs: int,
    confidence: float,
    _ev_pct: float = 0.0,
    platform: str = "Mixed",
    stake: float = _DEFAULT_STAKE,
    legs: Optional[list] = None,
) -> bool:
    """Insert a new PENDING parlay into the season record."""
    conn = _get_conn()
    if not conn:
        return False
    try:
        _ensure_table(conn)
        legs_json = json.dumps(legs) if legs else None
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO propiq_season_record
                (date, agent_name, parlay_legs, platform, stake,
                 payout, confidence, status, legs_json, created_at)
            VALUES (%s, %s, %s, %s, %s, 0.00, %s, 'PENDING', %s, %s)
            """,
            (
                date, agent, num_legs, platform, stake,
                confidence, legs_json,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        cur.close()
        return True
    except Exception as exc:
        logger.warning("[SeasonRecord] record_parlay failed: %s", exc)
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def settle_parlay_record(
    parlay_id: int,
    status: str,
    units_profit: float,
    payout: Optional[float] = None,
) -> bool:
    """Update a parlay record with its WIN/LOSS/PUSH outcome."""
    conn = _get_conn()
    if not conn:
        return False
    try:
        payout_val = (
            payout
            if payout is not None
            else max(0.0, units_profit + _DEFAULT_STAKE)
        )
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE propiq_season_record
               SET status = %s,
                   payout = %s,
                   settled_at = %s
             WHERE id = %s
            """,
            (
                status,
                payout_val,
                datetime.now(timezone.utc).isoformat(),
                parlay_id,
            ),
        )
        conn.commit()
        cur.close()
        return True
    except Exception as exc:
        logger.warning("[SeasonRecord] settle_parlay_record failed: %s", exc)
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------------

def get_pending_parlays(date: str) -> list[dict]:
    """Return all PENDING parlays for a given date (YYYY-MM-DD)."""
    conn = _get_conn()
    if not conn:
        return []
    try:
        _ensure_table(conn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, date, agent_name, parlay_legs, platform,
                   stake, payout, confidence, status, legs_json
              FROM propiq_season_record
             WHERE date = %s AND status = 'PENDING'
             ORDER BY id
            """,
            (date,),
        )
        rows = cur.fetchall()
        cur.close()
        results = []
        for row in rows:
            legs: list = []
            if row[9]:
                try:
                    legs = json.loads(row[9])
                except Exception:
                    pass
            results.append({
                "id":          row[0],
                "date":        str(row[1]),
                "agent_name":  row[2],
                "parlay_legs": row[3],
                "platform":    row[4],
                "stake":       float(row[5] or 0),
                "payout":      float(row[6] or 0),
                "confidence":  float(row[7] or 0),
                "status":      row[8],
                "legs":        legs,
            })
        return results
    except Exception as exc:
        logger.warning("[SeasonRecord] get_pending_parlays failed: %s", exc)
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_agent_season_stats(agent: str) -> dict:
    """Return season W/L record and profit for a specific agent."""
    conn = _get_conn()
    if not conn:
        return {}
    try:
        _ensure_table(conn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE status = 'WIN')     AS wins,
                COUNT(*) FILTER (WHERE status = 'LOSS')    AS losses,
                COUNT(*) FILTER (WHERE status = 'PUSH')    AS pushes,
                COUNT(*) FILTER (WHERE status = 'PENDING') AS pending,
                COALESCE(SUM(payout) FILTER (WHERE status = 'WIN'), 0)
                    - COALESCE(SUM(stake), 0)               AS net_profit
              FROM propiq_season_record
             WHERE agent_name = %s
            """,
            (agent,),
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            return {}
        wins, losses, pushes, pending, net = row
        wins    = int(wins    or 0)
        losses  = int(losses  or 0)
        pushes  = int(pushes  or 0)
        pending = int(pending or 0)
        net     = float(net   or 0)
        total   = wins + losses
        return {
            "wins":       wins,
            "losses":     losses,
            "pushes":     pushes,
            "pending":    pending,
            "record":     f"{wins}W-{losses}L-{pushes}P",
            "net_profit": round(net, 2),
            "win_pct":    round(wins / total * 100, 1) if total > 0 else 0.0,
        }
    except Exception as exc:
        logger.warning("[SeasonRecord] get_agent_season_stats failed: %s", exc)
        return {}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_overall_season_stats() -> dict:
    """Return combined season stats across all agents."""
    conn = _get_conn()
    if not conn:
        return {"record": "0W-0L-0P", "units_profit": 0.0, "roi_pct": 0.0, "pending": 0}
    try:
        _ensure_table(conn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE status = 'WIN')     AS wins,
                COUNT(*) FILTER (WHERE status = 'LOSS')    AS losses,
                COUNT(*) FILTER (WHERE status = 'PUSH')    AS pushes,
                COUNT(*) FILTER (WHERE status = 'PENDING') AS pending,
                COALESCE(SUM(payout) FILTER (WHERE status = 'WIN'), 0) AS total_payout,
                COALESCE(SUM(stake),  0)                               AS total_staked
              FROM propiq_season_record
            """
        )
        row = cur.fetchone()
        cur.close()
        wins, losses, pushes, pending, total_payout, total_staked = row
        wins         = int(wins         or 0)
        losses       = int(losses       or 0)
        pushes       = int(pushes       or 0)
        pending      = int(pending      or 0)
        total_payout = float(total_payout or 0)
        total_staked = float(total_staked or 0)
        units_profit = total_payout - total_staked
        roi_pct = (
            units_profit / total_staked * 100 if total_staked > 0 else 0.0
        )
        return {
            "wins":         wins,
            "losses":       losses,
            "pushes":       pushes,
            "pending":      pending,
            "record":       f"{wins}W-{losses}L-{pushes}P",
            "units_profit": round(units_profit, 2),
            "roi_pct":      round(roi_pct, 1),
        }
    except Exception as exc:
        logger.warning("[SeasonRecord] get_overall_season_stats failed: %s", exc)
        return {"record": "0W-0L-0P", "units_profit": 0.0, "roi_pct": 0.0, "pending": 0}
    finally:
        try:
            conn.close()
        except Exception:
            pass
