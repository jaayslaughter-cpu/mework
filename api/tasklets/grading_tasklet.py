"""
Grading Tasklet — Runs daily at 1:05AM
----------------------------------------
Settles all pending bets from the previous day using boxscore data.
Updates agent stats + calibration error store.
"""
from __future__ import annotations
import logging
from datetime import date, timedelta

from ..agents.grading_agent import GradingAgent

logger = logging.getLogger("propiq.tasklet.grading")

_grading_agent: GradingAgent | None = None


def _get_grading_agent() -> GradingAgent:
    global _grading_agent
    if _grading_agent is None:
        _grading_agent = GradingAgent()
    return _grading_agent


def run_grading_tasklet(game_date: str | None = None) -> dict:
    """
    Grade yesterday's bets by default, or a specific date.
    Runs at 1:05AM to allow box scores to fully finalize.
    """
    target_date = game_date or (date.today() - timedelta(days=1)).isoformat()
    logger.info("[grading] Starting grading for %s", target_date)

    agent = _get_grading_agent()
    summary = agent.grade_all_pending(game_date=target_date)

    logger.info(
        "[grading] Done — %s bets settled | W:%s L:%s P:%s",
        summary.get('graded', 0),
        summary.get('wins', 0),
        summary.get('losses', 0),
        summary.get('pushes', 0)
    )
    return summary
