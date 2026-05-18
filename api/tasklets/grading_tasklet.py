"""
Delegator to canonical Postgres-backed grading in tasklets.py.
DO NOT use GradingAgent (SQLite) here.

Root cause fixed (PR #580 Bug 1): original file called GradingAgent which
uses sqlite3 against api/data/agent_army.db — no live bets there.
The live grader is run_grading_tasklet() in root tasklets.py (Postgres).
"""
from __future__ import annotations
import logging
import os
import sys
import pathlib

log = logging.getLogger("propiq.tasklet.grading")


def _ensure_root_on_path() -> None:
    """Insert repo root into sys.path so `import tasklets` resolves correctly
    regardless of Railway WORKDIR."""
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
        log.error("[Grading delegator] failed: %s", exc)
        return {"status": "error", "error": str(exc)}
