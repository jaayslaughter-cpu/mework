"""
agent_diagnostics.py
====================
PropIQ — Per-agent nightly diagnostics.

Computes rolling 30-day metrics per agent after each grading run:
  - win_rate:   wins / (wins + losses)   [pushes excluded]
  - roi:        sum(profit_loss) / sum(stake)
  - brier:      mean((model_prob/100 - actual_outcome)^2) for graded rows
  - n_graded:   total discord_sent=TRUE + status IN ('WIN','LOSS','PUSH')

Freeze logic:
  - Agent is FROZEN if ROI < 0.0 for 20+ consecutive calendar days.
  - Frozen agents are skipped by job_agents() until manually unfrozen OR
    they record a positive-ROI day.
  - Freeze status is stored in `agent_freeze_log` table.

DB Tables created by this module (IF NOT EXISTS):
  - agent_diagnostics (id, agent_name, snapshot_date, win_rate, roi, brier, n_graded, frozen)
  - agent_freeze_log  (id, agent_name, freeze_date, unfreeze_date, freeze_reason)

Wire into run_grading_tasklet() AFTER the existing grading loop:

    try:
        from agent_diagnostics import run_agent_diagnostics as _run_diag
        _run_diag()
        logger.info("[Grading] Agent diagnostics completed.")
    except Exception as _diag_err:
        logger.warning("[Grading] Agent diagnostics failed: %s", _diag_err)
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger("propiq.agent_diagnostics")

AGENT_NAMES = [
    "EVHunter", "UnderMachine", "UmpireAgent", "F5Agent",
    "FadeAgent", "LineValueAgent", "BullpenAgent", "WeatherAgent",
    "MLEdgeAgent", "UnderDogAgent", "StackSmithAgent", "ChalkBusterAgent",
    "SharpFadeAgent", "CorrelatedParlayAgent", "PropCycleAgent",
    "LineupChaseAgent", "LineDriftAgent",
]

FREEZE_CONSECUTIVE_DAYS = 20     # freeze after this many negative-ROI days in a row
ROLLING_WINDOW_DAYS     = 30     # window for win_rate, roi, brier metrics


def _get_pg_conn():
    """Open a psycopg2 connection using DATABASE_URL."""
    import psycopg2
    url = os.getenv("DATABASE_URL", "")
    if url:
        return psycopg2.connect(url)
    return psycopg2.connect(
        host     = os.getenv("POSTGRES_HOST",     "postgres"),
        port     = int(os.getenv("POSTGRES_PORT", 5432)),
        dbname   = os.getenv("POSTGRES_DB",       "propiq"),
        user     = os.getenv("POSTGRES_USER",     "propiq"),
        password = os.getenv("POSTGRES_PASSWORD", "propiq"),
    )


def _ensure_tables(conn) -> None:
    """Create agent_diagnostics and agent_freeze_log tables if they don't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agent_diagnostics (
                id            SERIAL PRIMARY KEY,
                agent_name    VARCHAR(80)   NOT NULL,
                snapshot_date DATE          NOT NULL,
                win_rate      FLOAT,
                roi           FLOAT,
                brier         FLOAT,
                n_graded      INTEGER       DEFAULT 0,
                frozen        BOOLEAN       DEFAULT FALSE,
                created_at    TIMESTAMPTZ   DEFAULT NOW(),
                UNIQUE (agent_name, snapshot_date)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agent_freeze_log (
                id            SERIAL PRIMARY KEY,
                agent_name    VARCHAR(80)   NOT NULL UNIQUE,
                freeze_date   DATE          NOT NULL,
                unfreeze_date DATE,
                freeze_reason TEXT,
                created_at    TIMESTAMPTZ   DEFAULT NOW()
            )
        """)
        # Heal: add unique constraint on existing deployments
        try:
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS agent_freeze_log_agent_name_uidx
                ON agent_freeze_log (agent_name)
            """)
        except Exception:
            pass
    conn.commit()


def _today_pt():
    import datetime
    from zoneinfo import ZoneInfo
    return datetime.datetime.now(ZoneInfo("America/Los_Angeles")).date()


def _compute_agent_metrics(conn, agent_name: str, today) -> dict:
    """
    Compute 30-day rolling metrics for one agent.
    Returns dict with win_rate, roi, brier, n_graded.

    PR #400 fix: bet_ledger uses `status` column (WIN/LOSS/PUSH), NOT `result`.
    The `result` column exists but is always NULL — querying it returned 0 rows
    for every agent, making win_rate/roi/brier all NULL.
    """
    import datetime
    window_start = today - datetime.timedelta(days=ROLLING_WINDOW_DAYS)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                status,
                COALESCE(profit_loss, 0.0)  AS pl,
                COALESCE(units_wagered, ABS(kelly_units), 1.0)  AS stake,
                COALESCE(model_prob,  50.0)  AS mp,
                CASE WHEN status = 'WIN'  THEN 1
                     WHEN status = 'LOSS' THEN 0
                     ELSE NULL END            AS outcome
            FROM bet_ledger
            WHERE agent_name   = %s
              AND discord_sent = TRUE
              AND status       IN ('WIN', 'LOSS', 'PUSH')
              AND bet_date      >= %s
              AND bet_date      <= %s
        """, (agent_name, window_start, today))
        rows = cur.fetchall()

    if not rows:
        return {"win_rate": None, "roi": None, "brier": None, "n_graded": 0}

    wins      = sum(1 for r in rows if r[0] == "WIN")
    losses    = sum(1 for r in rows if r[0] == "LOSS")
    total_pl  = sum(r[1] for r in rows)
    total_stk = sum(r[2] for r in rows) or 1.0

    win_rate  = wins / max(wins + losses, 1)
    roi       = total_pl / total_stk

    # Brier score: only for WIN/LOSS rows (outcome = 0 or 1)
    brier_rows = [(r[3] / 100.0, r[4]) for r in rows if r[4] is not None]
    brier = (sum((mp - out) ** 2 for mp, out in brier_rows) / len(brier_rows)
             if brier_rows else None)

    return {
        "win_rate": round(win_rate, 4),
        "roi":      round(roi,      4),
        "brier":    round(brier, 4) if brier is not None else None,
        "n_graded": len(rows),
    }


def _negative_roi_streak(conn, agent_name: str, today) -> int:
    """
    Count consecutive calendar days (ending today) where agent had negative ROI.
    Returns 0 if no negative-ROI days at the end of the sequence.

    PR #400 fix: uses `status` column (was incorrectly using `result`).
    """
    import datetime
    # Pull daily ROI for last 60 days to check streak
    window_start = today - datetime.timedelta(days=60)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                bet_date,
                SUM(COALESCE(profit_loss, 0.0))  AS daily_pl,
                SUM(COALESCE(units_wagered, ABS(kelly_units), 1.0))  AS daily_stk
            FROM bet_ledger
            WHERE agent_name   = %s
              AND discord_sent = TRUE
              AND status       IN ('WIN', 'LOSS', 'PUSH')
              AND bet_date      >= %s
              AND bet_date      <= %s
            GROUP BY bet_date
            ORDER BY bet_date DESC
        """, (agent_name, window_start, today))
        rows = cur.fetchall()

    if not rows:
        return 0

    streak = 0
    for _, pl, stk in rows:
        daily_roi = (pl / stk) if stk else 0.0
        if daily_roi < 0.0:
            streak += 1
        else:
            break   # streak broken

    return streak


def _is_currently_frozen(conn, agent_name: str) -> bool:
    """Return True if the agent has an open freeze record (unfreeze_date IS NULL)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM agent_freeze_log
            WHERE agent_name    = %s
              AND unfreeze_date IS NULL
            LIMIT 1
        """, (agent_name,))
        return cur.fetchone() is not None


def _freeze_agent(conn, agent_name: str, today, reason: str) -> None:
    """Insert a freeze record for the agent."""
    with conn.cursor() as cur:
        # Close any open freeze first (idempotent)
        cur.execute("""
            UPDATE agent_freeze_log
               SET unfreeze_date = %s
             WHERE agent_name    = %s
               AND unfreeze_date IS NULL
        """, (today, agent_name))
        cur.execute("""
            INSERT INTO agent_freeze_log (agent_name, freeze_date, freeze_reason)
            VALUES (%s, %s, %s)
        """, (agent_name, today, reason))
    conn.commit()
    logger.warning("[Diagnostics] FROZEN %s — %s", agent_name, reason)


def _unfreeze_agent(conn, agent_name: str, today) -> None:
    """Close all open freeze records for the agent."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE agent_freeze_log
               SET unfreeze_date = %s
             WHERE agent_name    = %s
               AND unfreeze_date IS NULL
        """, (today, agent_name))
    conn.commit()
    logger.info("[Diagnostics] UNFROZEN %s — positive ROI streak restored", agent_name)


def _upsert_snapshot(conn, agent_name: str, today, metrics: dict, frozen: bool) -> None:
    """Insert or update today's diagnostic snapshot."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO agent_diagnostics
                (agent_name, snapshot_date, win_rate, roi, brier, n_graded, frozen)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (agent_name, snapshot_date) DO UPDATE
                SET win_rate  = EXCLUDED.win_rate,
                    roi       = EXCLUDED.roi,
                    brier     = EXCLUDED.brier,
                    n_graded  = EXCLUDED.n_graded,
                    frozen    = EXCLUDED.frozen
        """, (
            agent_name, today,
            metrics.get("win_rate"),
            metrics.get("roi"),
            metrics.get("brier"),
            metrics.get("n_graded", 0),
            frozen,
        ))
    conn.commit()


def run_agent_diagnostics() -> None:
    """
    Main entry point — called from run_grading_tasklet() at 2 AM.

    For each of the 17 agents:
    1. Compute 30-day rolling metrics from bet_ledger.
    2. Check consecutive-day negative ROI streak.
    3. Freeze / unfreeze as appropriate.
    4. Upsert today's snapshot into agent_diagnostics.

    Logs a summary table at INFO level.
    """
    today = _today_pt()
    conn  = _get_pg_conn()
    try:
        _ensure_tables(conn)

        summary_lines = []
        for agent_name in AGENT_NAMES:
            metrics  = _compute_agent_metrics(conn, agent_name, today)
            neg_streak = _negative_roi_streak(conn, agent_name, today)
            currently_frozen = _is_currently_frozen(conn, agent_name)

            # Freeze decision
            should_freeze = (
                metrics.get("roi") is not None
                and metrics["roi"] < 0.0
                and neg_streak >= FREEZE_CONSECUTIVE_DAYS
            )
            if should_freeze and not currently_frozen:
                _freeze_agent(
                    conn, agent_name, today,
                    f"ROI={metrics['roi']:.1%} for {neg_streak} consecutive days",
                )
                frozen = True
            elif currently_frozen and (metrics.get("roi", -1) or 0) >= 0.0:
                # Positive-ROI day — unfreeze
                _unfreeze_agent(conn, agent_name, today)
                frozen = False
            else:
                frozen = currently_frozen

            _upsert_snapshot(conn, agent_name, today, metrics, frozen)

            n      = metrics.get("n_graded", 0)
            roi    = metrics.get("roi")
            wr     = metrics.get("win_rate")
            brier  = metrics.get("brier")
            flag   = " ❄️ FROZEN" if frozen else ""
            if n > 0:
                summary_lines.append(
                    f"  {agent_name:<26} | n={n:>3} | "
                    f"ROI={roi:+.1%} | WR={wr:.1%} | "
                    f"Brier={'%.3f' % brier if brier is not None else 'N/A'}{flag}"
                )
            else:
                summary_lines.append(f"  {agent_name:<26} | no graded rows{flag}")

        logger.info(
            "[Diagnostics] 30-day agent snapshot for %s:\n%s",
            today.isoformat(),
            "\n".join(summary_lines),
        )
    finally:
        conn.close()


def get_frozen_agents() -> set:
    """
    Return set of currently-frozen agent names.
    Called by job_agents() to skip frozen agents.
    Returns empty set on any DB error (fail-open — never block dispatch on diagnostics error).
    """
    try:
        conn = _get_pg_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT agent_name FROM agent_freeze_log
                WHERE unfreeze_date IS NULL
            """)
            frozen = {r[0] for r in cur.fetchall()}
        conn.close()
        return frozen
    except Exception as exc:
        logger.debug("[Diagnostics] get_frozen_agents failed (fail-open): %s", exc)
        return set()
