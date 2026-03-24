"""
Agent Tasklet — Runs every 30 seconds
---------------------------------------
Runs all 7 agents against the latest hub data.
Outputs bet slips to the queue.
"""
from __future__ import annotations
import json
import logging
import time
from datetime import datetime
from pathlib import Path

from .data_hub_tasklet import read_hub
from ..agents import EVHunter, UnderMachine, ThreeLeg, ParlayAgent, LiveAgent, ArbAgent, GradingAgent

logger = logging.getLogger("propiq.tasklet.agent")

BET_QUEUE_PATH = Path(__file__).parent.parent / "data" / "bet_queue.jsonl"
BET_QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)

# Singleton agent instances (stateful — LiveAgent keeps history)
_agents = {
    "ev_hunter": EVHunter(),
    "under_machine": UnderMachine(),
    "three_leg": ThreeLeg(),
    "parlay": ParlayAgent(),
    "live": LiveAgent(),
    "arb": ArbAgent(),
    "grading": GradingAgent(),  # Passive — grades via scheduled task
}


def get_agents() -> dict:
    return _agents


def run_agent_tasklet() -> dict:
    """Run all 7 agents and write slips to bet_queue.jsonl."""
    start = time.time()
    hub_data = read_hub()

    if not hub_data:
        logger.warning("[agents] No hub data — skipping agent run")
        return {"status": "no_data", "slips": 0}

    all_slips = []
    agent_results = {}

    for name, agent in _agents.items():
        if name == "grading":
            continue  # Grading runs on schedule, not every 30s

        try:
            slips = agent.run(hub_data)
            agent_results[name] = {
                "slips_filed": len(slips),
                "top_ev": max((s.expected_value for s in slips), default=0.0),
            }
            all_slips.extend(slips)
        except Exception as e:
            logger.error("[agents] %s crashed: %s", name, e)
            agent_results[name] = {"error": str(e)}

    # Append to bet queue
    if all_slips:
        with open(BET_QUEUE_PATH, "a") as f:
            for slip in all_slips:
                f.write(json.dumps(slip.to_dict()) + "\n")

    elapsed = time.time() - start
    logger.info(
        "[agents] %d slips filed across %d agents in %.2fs",
        len(all_slips),
        len([k for k, v in agent_results.items() if 'error' not in v]),
        elapsed,
    )

    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "total_slips": len(all_slips),
        "agents": agent_results,
    }
