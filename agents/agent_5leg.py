"""
agents/agent_5leg.py
Agent 5-Leg: High-ceiling parlay — 5 independent props each with edge >= 4%.
Maximizes expected value for lottery-style tickets.
"""
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

MIN_EDGE = 4.0
TARGET_LEGS = 5


def generate_ticket(date: str, projections: list) -> dict | None:
    qualifying = [p for p in projections if p.get("edge_percentage", 0) >= MIN_EDGE]

    if len(qualifying) < TARGET_LEGS:
        logger.info("[Agent5Leg] Only %s qualifying props, need %s.", len(qualifying), TARGET_LEGS)
        return None

    # Pick 5 with highest edge, prefer diversity across games
    seen_games = set()
    diverse_legs = []
    for p in sorted(qualifying, key=lambda x: x["edge_percentage"], reverse=True):
        gid = p.get("game_id", "unknown")
        if gid not in seen_games:
            diverse_legs.append(p)
            seen_games.add(gid)
        if len(diverse_legs) == TARGET_LEGS:
            break

    # If we can't get 5 diverse, just take top 5
    if len(diverse_legs) < TARGET_LEGS:
        diverse_legs = sorted(qualifying, key=lambda x: x["edge_percentage"], reverse=True)[:TARGET_LEGS]

    joint = 1.0
    for leg in diverse_legs:
        joint *= leg["model_projected_over"] / 100

    ticket = {
        "agent": "Agent_5Leg",
        "date": date,
        "legs": diverse_legs,
        "joint_probability": round(joint * 100, 2),
        "generated_at": datetime.utcnow().isoformat(),
    }

    logger.info("[Agent5Leg] 5-leg ticket: %.1f%% joint prob", joint * 100)
    return ticket
