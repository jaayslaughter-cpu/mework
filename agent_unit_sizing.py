"""
agent_unit_sizing.py — Phase 43
Per-agent dynamic unit sizing with 5-tier ladder.

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
    """Return {agent_name: unit_dollars} for all 18 agents."""
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
