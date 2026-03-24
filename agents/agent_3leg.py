"""
agents/agent_3leg.py
Agent 3-Leg: Builds a 3-leg correlated parlay — requires all 3 legs from same game or same team stack.
"""
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

MIN_EDGE = 5.0
MAX_LEGS = 3


def generate_ticket(date: str, projections: list) -> dict | None:
    qualifying = [p for p in projections if p.get("edge_percentage", 0) >= MIN_EDGE]

    if len(qualifying) < 3:
        return None

    # Group by game
    by_game: dict = {}
    for p in qualifying:
        gid = p.get("game_id", "unknown")
        by_game.setdefault(gid, []).append(p)

    best_ticket = None
    best_joint = 0.0

    for game_id, legs in by_game.items():
        if len(legs) < 3:
            continue
        # Take top 3 by edge
        top3 = sorted(legs, key=lambda x: x["edge_percentage"], reverse=True)[:3]
        joint = 1.0
        for leg in top3:
            joint *= leg["model_projected_over"] / 100
        if joint > best_joint:
            best_joint = joint
            best_ticket = {
                "agent": "Agent_3Leg",
                "date": date,
                "game_id": game_id,
                "legs": top3,
                "joint_probability": round(joint * 100, 2),
                "generated_at": datetime.utcnow().isoformat(),
            }

    if best_ticket:
        logger.info("[Agent3Leg] Ticket: %s%% joint prob", best_ticket['joint_probability'])

    return best_ticket
