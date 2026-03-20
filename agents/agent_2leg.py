"""
agents/agent_2leg.py
Agent 2-Leg: Finds the two highest-edge props from the same game (correlated 2-leg parlay).
Targets: +EV edge >= 5%, odds between -130 and +250.
"""
import logging
import psycopg2
import os
from datetime import datetime

logger = logging.getLogger(__name__)

DB_CONN = {
    "dbname": os.environ.get("POSTGRES_DB", "propiq"),
    "user": os.environ.get("POSTGRES_USER", "propiq_admin"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": 5432,
}

MIN_EDGE = 5.0
MIN_ODDS = -130
MAX_ODDS = 250


def _american_to_decimal(odds: int) -> float:
    if odds >= 0:
        return (odds / 100) + 1
    else:
        return (100 / abs(odds)) + 1


def generate_ticket(date: str, projections: list) -> dict | None:
    """
    Selects the best correlated 2-leg parlay from today's projections.
    Returns the ticket dict or None if no qualifying combo found.
    """
    # Filter qualifying props
    qualifying = [
        p for p in projections
        if p.get("edge_percentage", 0) >= MIN_EDGE
        and MIN_ODDS <= p.get("over_odds", 0) <= MAX_ODDS
    ]

    if len(qualifying) < 2:
        logger.info("[Agent2Leg] Not enough qualifying props for a 2-leg ticket.")
        return None

    best_ticket = None
    best_joint_prob = 0.0

    # Find best correlated pair (same game)
    for i, leg1 in enumerate(qualifying):
        for leg2 in qualifying[i + 1:]:
            if leg1.get("game_id") != leg2.get("game_id"):
                continue  # Must be same game for correlation

            joint_prob = (leg1["model_projected_over"] / 100) * (leg2["model_projected_over"] / 100)
            parlay_odds = (
                _american_to_decimal(leg1["over_odds"]) *
                _american_to_decimal(leg2["over_odds"]) - 1
            ) * 100  # Approximate parlay odds

            if joint_prob > best_joint_prob:
                best_joint_prob = joint_prob
                best_ticket = {
                    "agent": "Agent_2Leg",
                    "date": date,
                    "legs": [leg1, leg2],
                    "joint_probability": round(joint_prob * 100, 2),
                    "estimated_parlay_odds": round(parlay_odds),
                    "generated_at": datetime.utcnow().isoformat(),
                }

    if best_ticket:
        logger.info(f"[Agent2Leg] Ticket generated: {best_ticket['joint_probability']}% joint prob")
        _save_ticket(best_ticket)

    return best_ticket


def _save_ticket(ticket: dict) -> None:
    try:
        conn = psycopg2.connect(**DB_CONN)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO bets_log (agent, bet_date, legs_json, joint_probability, estimated_odds, created_at)
            VALUES (%s, %s, %s::jsonb, %s, %s, NOW())
            ON CONFLICT DO NOTHING;
        """, (
            ticket["agent"],
            ticket["date"],
            str(ticket["legs"]).replace("'", '"'),
            ticket["joint_probability"],
            ticket["estimated_parlay_odds"],
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"[Agent2Leg] Failed to save ticket: {e}")
