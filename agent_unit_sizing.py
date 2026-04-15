"""
agent_unit_sizing.py — Phase 43
Per-agent dynamic unit sizing with 5-tier ladder (17 core agents + StreakAgent = 18 total).

Tier ladder:
  Tier 1 → $5   (floor, all agents start here)
  Tier 2 → $8   (3 consecutive wins from tier 1)
  Tier 3 → $12  (3 consecutive wins from tier 2)
  Tier 4 → $16  (3 consecutive wins from tier 3)
  Tier 5 → $20  (3 consecutive wins from tier 4)

Step-down: 3 consecutive losses → -1 tier (never below tier 1)
Streak resets when direction changes.
"""

import os
import psycopg2
from datetime import datetime, timezone

TIER_DOLLARS = {1: 5.0, 2: 8.0, 3: 12.0, 4: 16.0, 5: 20.0}
WINS_TO_TIER_UP = 3
LOSSES_TO_TIER_DOWN = 3
MAX_TIER = 5
MIN_TIER = 1

DATABASE_URL = os.environ.get("DATABASE_URL")


def _get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def _ensure_table() -> None:
    """Auto-create agent_unit_sizing table if migration hasn't run yet.
    Safe to call on every startup — CREATE TABLE IF NOT EXISTS is idempotent.

    PR #325 — Schema healing:
    If the table was created by an older migration (V33) that used `stake`
    instead of `unit_dollars`, the CREATE TABLE above is a no-op (table
    already exists) and unit_dollars is still missing. The ALTER TABLE block
    below heals the live DB in-process, eliminating the 1,000+/day
    "column unit_dollars does not exist" Postgres errors without requiring
    a manual console fix or Flyway migration runner.
    """
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agent_unit_sizing (
                id                  SERIAL PRIMARY KEY,
                agent_name          VARCHAR(100) NOT NULL UNIQUE,
                tier                INTEGER      NOT NULL DEFAULT 1,
                unit_dollars        REAL         NOT NULL DEFAULT 5.0,
                consecutive_wins    INTEGER      NOT NULL DEFAULT 0,
                consecutive_losses  INTEGER      NOT NULL DEFAULT 0,
                last_result         VARCHAR(1),
                temperature         REAL         NOT NULL DEFAULT 1.5,
                updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
            )
        """)

        # ── PR #325: Schema healing — add unit_dollars if missing ─────────────
        # ALTER TABLE ... ADD COLUMN IF NOT EXISTS is idempotent; safe to run
        # every startup. When the column already exists this is a metadata-only
        # no-op that Postgres resolves in microseconds.
        cur.execute(
            "ALTER TABLE agent_unit_sizing "
            "ADD COLUMN IF NOT EXISTS unit_dollars REAL NOT NULL DEFAULT 5.0"
        )
        # Backfill unit_dollars from legacy 'stake' column (V33 schema) if both
        # exist and unit_dollars is still at the default floor value.
        cur.execute("""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE  table_name  = 'agent_unit_sizing'
                      AND  column_name = 'stake'
                ) THEN
                    UPDATE agent_unit_sizing
                    SET    unit_dollars = stake
                    WHERE  unit_dollars = 5.0
                      AND  stake IS NOT NULL
                      AND  stake <> 5.0;
                END IF;
            END $$;
        """)
        # ── End schema healing ─────────────────────────────────────────────────

        # Seed all 17 agents at tier 1 if missing
        _ALL_AGENTS = [
            "EVHunter", "UnderMachine", "UmpireAgent", "F5Agent", "FadeAgent",
            "LineValueAgent", "BullpenAgent", "WeatherAgent", "MLEdgeAgent",
            "UnderDogAgent", "StackSmithAgent", "ChalkBusterAgent", "SharpFadeAgent",
            "CorrelatedParlayAgent", "PropCycleAgent", "LineupChaseAgent", "LineDriftAgent",
        ]
        for _ag in _ALL_AGENTS:
            cur.execute(
                """
                INSERT INTO agent_unit_sizing (agent_name, tier, unit_dollars)
                VALUES (%s, 1, 5.0)
                ON CONFLICT (agent_name) DO NOTHING
                """,
                (_ag,),
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[UnitSizing] _ensure_table error: {e}")


def get_unit(agent_name: str) -> float:
    """Return current unit dollar amount for a given agent. Defaults to $5 if missing."""
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT unit_dollars FROM agent_unit_sizing WHERE agent_name = %s",
            (agent_name,),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return float(row[0])
    except Exception as e:
        print(f"[UnitSizing] get_unit error for {agent_name}: {e}")
    return 5.0


def get_all_units() -> dict:
    """Return {agent_name: unit_dollars} for all 17 agents.
    Auto-creates the table and seeds agents at tier 1 if missing.
    """
    _ensure_table()
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("SELECT agent_name, unit_dollars FROM agent_unit_sizing")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {row[0]: float(row[1]) for row in rows}
    except Exception as e:
        print(f"[UnitSizing] get_all_units error: {e}")
    return {}


def record_result(agent_name: str, result: str) -> dict:
    """
    Record a W or L for an agent and update tier if threshold hit.

    Args:
        agent_name: One of the 18 agent names.
        result: 'W' for win, 'L' for loss, 'P' for push (push is ignored — no streak change).

    Returns:
        dict with new tier, unit_dollars, and any tier_change message.
    """
    if result not in ("W", "L", "P"):
        raise ValueError(f"result must be W, L, or P — got: {result}")

    try:
        conn = _get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT tier, unit_dollars, consecutive_wins, consecutive_losses
            FROM agent_unit_sizing
            WHERE agent_name = %s
            """,
            (agent_name,),
        )
        row = cur.fetchone()
        if not row:
            # Agent not in table — insert at tier 1
            cur.execute(
                """
                INSERT INTO agent_unit_sizing
                    (agent_name, tier, unit_dollars, consecutive_wins, consecutive_losses, last_result, updated_at)
                VALUES (%s, 1, 5.0, 0, 0, NULL, %s)
                """,
                (agent_name, datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
            row = (1, 5.0, 0, 0)

        tier, unit_dollars, consec_wins, consec_losses = row

        tier_change = None

        if result == "P":
            # Push — no streak impact
            new_tier = tier
            new_wins = consec_wins
            new_losses = consec_losses
        elif result == "W":
            new_wins = consec_wins + 1
            new_losses = 0  # streak resets
            if new_wins >= WINS_TO_TIER_UP and tier < MAX_TIER:
                new_tier = tier + 1
                new_wins = 0  # reset streak after promotion
                tier_change = f"📈 {agent_name} promoted: Tier {tier} → Tier {new_tier} (${TIER_DOLLARS[tier]:.0f} → ${TIER_DOLLARS[new_tier]:.0f}/unit)"
            else:
                new_tier = tier
        else:  # L
            new_losses = consec_losses + 1
            new_wins = 0  # streak resets
            if new_losses >= LOSSES_TO_TIER_DOWN and tier > MIN_TIER:
                new_tier = tier - 1
                new_losses = 0  # reset streak after demotion
                tier_change = f"📉 {agent_name} demoted: Tier {tier} → Tier {new_tier} (${TIER_DOLLARS[tier]:.0f} → ${TIER_DOLLARS[new_tier]:.0f}/unit)"
            else:
                new_tier = tier

        new_unit = TIER_DOLLARS[new_tier]

        cur.execute(
            """
            UPDATE agent_unit_sizing
            SET tier = %s,
                unit_dollars = %s,
                consecutive_wins = %s,
                consecutive_losses = %s,
                last_result = %s,
                updated_at = %s
            WHERE agent_name = %s
            """,
            (
                new_tier,
                new_unit,
                new_wins,
                new_losses,
                result,
                datetime.now(timezone.utc).isoformat(),
                agent_name,
            ),
        )
        conn.commit()
        cur.close()
        conn.close()

        return {
            "agent_name": agent_name,
            "old_tier": tier,
            "new_tier": new_tier,
            "unit_dollars": new_unit,
            "consecutive_wins": new_wins,
            "consecutive_losses": new_losses,
            "tier_change": tier_change,
        }

    except Exception as e:
        print(f"[UnitSizing] record_result error for {agent_name}: {e}")
        return {"agent_name": agent_name, "unit_dollars": 5.0, "tier_change": None}


def get_tier_summary() -> list:
    """
    Return full tier table for all agents, sorted by tier desc then name.
    Used by monthly leaderboard.
    """
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT agent_name, tier, unit_dollars, consecutive_wins, consecutive_losses, last_result, updated_at
            FROM agent_unit_sizing
            ORDER BY tier DESC, agent_name ASC
            """
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [
            {
                "agent_name": row[0],
                "tier": row[1],
                "unit_dollars": row[2],
                "consecutive_wins": row[3],
                "consecutive_losses": row[4],
                "last_result": row[5],
                "updated_at": row[6],
            }
            for row in rows
        ]
    except Exception as e:
        print(f"[UnitSizing] get_tier_summary error: {e}")
        return []
