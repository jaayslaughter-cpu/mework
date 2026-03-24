"""
agents/agent_best.py
Agent Best: Finds the single highest-edge prop on the board each day.
Targets: Highest edge_percentage overall, any prop category.
"""
import logging
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

MIN_EDGE = 3.0


def generate_ticket(date: str, projections: list) -> dict | None:
    """Returns the single best prop on the board today."""
    qualifying = [p for p in projections if p.get("edge_percentage", 0) >= MIN_EDGE]

    if not qualifying:
        logger.info("[AgentBest] No qualifying props today.")
        return None

    best = max(qualifying, key=lambda x: x.get("edge_percentage", 0))

    ticket = {
        "agent": "Agent_Best",
        "date": date,
        "legs": [best],
        "edge_percentage": best["edge_percentage"],
        "model_projected_over": best["model_projected_over"],
        "vegas_implied_over": best["vegas_implied_over"],
        "generated_at": datetime.utcnow().isoformat(),
    }

    logger.info("[AgentBest] Best prop: %s edge=%s%%", best.get('prop_category'), best['edge_percentage'])
    return ticket
