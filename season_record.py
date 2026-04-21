# season_record.py — FIXED: renamed _ev_pct -> ev_pct (Problem 1 & 17)
# Phase 104: added sslmode=require to _get_conn() -- Railway Postgres requires SSL
# PR #393: Fixed get_overall_season_stats() SQL — was only summing WIN payouts,
#          missing loss amounts from net profit. Renamed total_payout -> net_profit.

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_DEFAULT_STAKE = 5.0

# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

def _get_conn():
    """Return a psycopg2 connection, or None if DATABASE_URL is unset."""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logger.debug("[SeasonRecord] DATABASE_URL not set — Postgres unavailable")
        return None
    try:
        import psycopg2  # noqa: PLC0415
        return psycopg2.connect(db_url, sslmode="require")
    except Exception as exc:
        logger.warning("[SeasonRecord] DB connect failed: %s", exc)
        return None


def _ensure_table(conn) -> None:
    """Create propiq_season_record table if it doesn't exist."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS propiq_season_record (
                    id          SERIAL PRIMARY KEY,
                    date        TEXT NOT NULL,
                    agent_name  TEXT NOT NULL,
                    parlay_legs INTEGER NOT NULL,
                    platform    TEXT NOT NULL DEFAULT 'Mixed',
                    stake       NUMERIC(8,2) NOT NULL DEFAULT 5.00,
                    payout      NUMERIC(8,2) NOT NULL DEFAULT 0.00,
                    confidence  NUMERIC(5,2) NOT NULL DEFAULT 0.00,
                    status      TEXT NOT NULL DEFAULT 'PENDING',
                    legs_json   TEXT,
                    created_at  TEXT NOT NULL,
                    discord_sent BOOLEAN NOT NULL DEFAULT TRUE
                )
            """)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE propiq_season_record
                ADD COLUMN IF NOT EXISTS discord_sent BOOLEAN NOT NULL DEFAULT TRUE
            """)
        conn.commit()
    except Exception as exc:
        logger.warning("[SeasonRecord] _ensure_table failed: %s", exc)


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

def record_parlay(
    date: str,
    agent: str,
    num_legs: int,
    confidence: float,
    ev_pct: float = 0.0,
    platform: str = "Mixed",
    stake: float = _DEFAULT_STAKE,
    legs: Optional[list] = None,
) -> bool:
    """Insert a new PENDING parlay into the season record."""
    conn = _get_conn()
    if not conn:
        logger.warning("[SeasonRecord] record_parlay skipped — no DB connection")
        return False
    try:
        _ensure_table(conn)
        legs_json = json.dumps(legs) if legs else None
        cur = conn.cursor()

        cur.execute(
            """
            SELECT 1 FROM propiq_season_record
            WHERE date = %s AND agent_name = %s AND legs_json = %s
            LIMIT 1
            """,
            (date, agent, legs_json),
        )
        if cur.fetchone():
            logger.debug("[SeasonRecord] Duplicate parlay skipped: %s %s", date, agent)
            cur.close()
            return True

        cur.execute(
            """
            INSERT INTO propiq_season_record
                (date, agent_name, parlay_legs, platform, stake,
                 payout, confidence, status, legs_json, created_at, discord_sent)
            VALUES (%s, %s, %s, %s, %s, 0.00, %s, 'PENDING', %s, %s, TRUE)
            """,
            (
                date, agent, num_legs, platform, stake,
                confidence, legs_json,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        cur.close()
        logger.info("[SeasonRecord] Recorded parlay: %s %s %d-leg conf=%.1f",
                    date, agent, num_legs, confidence)
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
    units_profit: float = 0.0,
) -> bool:
    """Update a parlay record with WIN/LOSS/PUSH and profit."""
    conn = _get_conn()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE propiq_season_record
                SET status = %s, payout = %s
                WHERE id = %s
                """,
                (status, units_profit, parlay_id),
            )
        conn.commit()
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
    """Return all PENDING parlays for a given date."""
    conn = _get_conn()
    if not conn:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, agent_name, stake, legs_json
                FROM propiq_season_record
                WHERE status = 'PENDING' AND date = %s
                """,
                (date,),
            )
            rows = cur.fetchall()
        results = []
        for row in rows:
            pid, agent, stake, legs_json = row
            legs = json.loads(legs_json) if legs_json else []
            results.append({
                "id": pid,
                "agent_name": agent,
                "stake": float(stake),
                "legs": legs,
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


def get_all_pending_parlays() -> list[dict]:
    """Return ALL PENDING parlays across all dates (for rollover settlement)."""
    conn = _get_conn()
    if not conn:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, date, agent_name, stake, legs_json
                FROM   propiq_season_record
                WHERE  status = 'PENDING'
                ORDER  BY date ASC
                """
            )
            rows = cur.fetchall()
        results = []
        for row in rows:
            pid, date, agent, stake, legs_json = row
            legs = json.loads(legs_json) if legs_json else []
            results.append({
                "id": pid,
                "date": str(date),
                "agent_name": agent,
                "stake": float(stake),
                "legs": legs,
            })
        return results
    except Exception as exc:
        logger.warning("[SeasonRecord] get_all_pending_parlays failed: %s", exc)
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_overall_season_stats() -> dict:
    """Return aggregate W/L/ROI stats for the full season.

    PR #393 FIX: SQL now sums payout for WIN+LOSS rows to get true net_profit.
    Previous version only summed WIN payouts, ignoring the -5u on every loss,
    which inflated season_units and ROI in the Discord footer.
    """
    conn = _get_conn()
    if not conn:
        return {}
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'WIN')     AS wins,
                    COUNT(*) FILTER (WHERE status = 'LOSS')    AS losses,
                    COUNT(*) FILTER (WHERE status = 'PUSH')    AS pushes,
                    COUNT(*) FILTER (WHERE status = 'PENDING') AS pending,
                    -- PR #393: sum WIN+LOSS payouts for true net profit
                    -- (payout stores net units_profit: positive for WIN, negative for LOSS)
                    COALESCE(SUM(payout) FILTER (WHERE status IN ('WIN','LOSS')), 0) AS net_profit,
                    COALESCE(SUM(stake) FILTER (WHERE status IN ('WIN','LOSS')), 0) AS total_staked
                FROM propiq_season_record
            """)
            row = cur.fetchone()
        if not row:
            return {}
        wins, losses, pushes, pending, net_profit, total_staked = row
        roi = (
            float(net_profit) / float(total_staked) * 100
            if total_staked and float(total_staked) > 0 else 0.0
        )
        return {
            "wins":         wins,
            "losses":       losses,
            "pushes":       pushes,
            "pending":      pending,
            "roi_pct":      round(roi, 2),
            "net_profit":   float(net_profit),
            "total_staked": float(total_staked),
        }
    except Exception as exc:
        logger.warning("[SeasonRecord] get_overall_season_stats failed: %s", exc)
        return {}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_agent_season_stats(agent_name: str) -> dict:
    """Return W/L/ROI for a specific agent."""
    conn = _get_conn()
    if not conn:
        return {}
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'WIN')  AS wins,
                    COUNT(*) FILTER (WHERE status = 'LOSS') AS losses,
                    COUNT(*) FILTER (WHERE status = 'PUSH') AS pushes,
                    COALESCE(SUM(payout) FILTER (WHERE status IN ('WIN','LOSS')), 0) AS net_profit,
                    COALESCE(SUM(stake) FILTER (WHERE status IN ('WIN','LOSS')), 0) AS total_staked
                FROM propiq_season_record
                WHERE agent_name = %s
            """, (agent_name,))
            row = cur.fetchone()
        if not row:
            return {}
        wins, losses, pushes, net_profit, total_staked = row
        total_graded = (wins or 0) + (losses or 0)
        win_rate = wins / total_graded * 100 if total_graded > 0 else 0.0
        roi = (
            float(net_profit) / float(total_staked) * 100
            if total_staked and float(total_staked) > 0 else 0.0
        )
        return {
            "wins":     wins,
            "losses":   losses,
            "pushes":   pushes,
            "win_rate": round(win_rate, 1),
            "roi_pct":  round(roi, 2),
        }
    except Exception as exc:
        logger.warning("[SeasonRecord] get_agent_season_stats failed: %s", exc)
        return {}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_daily_clv_summary(date: str) -> dict:
    """Return CLV summary stub — extends in future sprint."""
    return {"date": date, "avg_clv": 0.0, "positive_clv_pct": 0.0}
