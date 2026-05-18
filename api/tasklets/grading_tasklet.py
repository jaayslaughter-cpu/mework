"""
Grading Tasklet — Runs daily at 2:00 AM PT
-------------------------------------------
DELEGATOR — this file is intentionally thin.

The canonical grading logic lives in tasklets.py as run_grading_tasklet().
That function reads OPEN rows from Postgres bet_ledger (discord_sent=TRUE),
fetches ESPN boxscores, writes actual_outcome / status / profit_loss / clv,
and rebuilds features_json for XGBoost retraining.

Historical bug: This file previously wrapped GradingAgent (api/agents/grading_agent.py)
whose AgentDB class uses sqlite3 against api/data/agent_army.db — a file that
has no live bets. It was silently returning {"graded": 0} on every run.

DO NOT reimport GradingAgent or sqlite3 here.
"""
from __future__ import annotations
import logging
import pathlib
import sys

log = logging.getLogger("propiq.tasklet.grading")


def _ensure_root_on_path() -> None:
    """Ensure repo root is on sys.path regardless of working directory.

    Railway sets WORKDIR /app/api, so a bare `import tasklets` resolves to
    api/tasklets.py (the thin re-export shim) instead of the root monolith.
    Inserting the repo root fixes this for all delegator imports.
    """
    root = str(pathlib.Path(__file__).resolve().parents[2])
    if root not in sys.path:
        sys.path.insert(0, root)


def run_grading_tasklet(game_date: str | None = None) -> dict:
    """Delegate to canonical Postgres-backed grader in tasklets.py."""
    try:
        _ensure_root_on_path()
        import importlib
        _mod = importlib.import_module("tasklets")
        _mod.run_grading_tasklet()
        return {"status": "ok", "source": "tasklets.run_grading_tasklet"}
    except Exception as exc:
        log.error("[Grading delegator] run_grading_tasklet failed: %s", exc)
        return {"status": "error", "error": str(exc)}
