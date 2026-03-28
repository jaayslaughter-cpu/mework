"""
api/tasklets/__init__.py

FIX BUG 9: Previously imported ALL tasklets eagerly at module load time,
including backtest_tasklet which has a large dependency chain (numpy, requests,
api.services.prop_model, api.services.strikeout_model).

If ANY of those imports fail (e.g. numpy not installed, prop_model not yet
present), the ENTIRE tasklets package fails to import — killing
run_agent_tasklet, run_data_hub_tasklet, etc. which are needed for production.

Fix: Wrap backtest_tasklet (and xgboost_tasklet) in try/except.
  Core production tasklets (data_hub, agent, leaderboard, grading) always load.
  If backtest/xgboost deps fail, a safe stub raises RuntimeError on call.
"""

from .data_hub_tasklet import run_data_hub_tasklet, read_hub
from .agent_tasklet import run_agent_tasklet, get_agents
from .leaderboard_tasklet import run_leaderboard_tasklet, read_leaderboard
from .grading_tasklet import run_grading_tasklet

# FIX BUG 9: Graceful fallback for heavy-dependency tasklets
try:
    from .backtest_tasklet import run_backtest_tasklet
except Exception as _bt_err:
    import logging as _logging
    _logging.getLogger(__name__).warning(
        "[tasklets] backtest_tasklet unavailable: %s", _bt_err
    )

    def run_backtest_tasklet(*args, **kwargs):  # type: ignore[misc]
        raise RuntimeError(
            f"backtest_tasklet could not be loaded: {_bt_err}"
        )

try:
    from .xgboost_tasklet import run_xgboost_tasklet
except Exception as _xgb_err:
    import logging as _logging
    _logging.getLogger(__name__).warning(
        "[tasklets] xgboost_tasklet unavailable: %s", _xgb_err
    )

    def run_xgboost_tasklet(*args, **kwargs):  # type: ignore[misc]
        raise RuntimeError(
            f"xgboost_tasklet could not be loaded: {_xgb_err}"
        )

__all__ = [
    "run_data_hub_tasklet", "read_hub",
    "run_agent_tasklet", "get_agents",
    "run_leaderboard_tasklet", "read_leaderboard",
    "run_backtest_tasklet",
    "run_grading_tasklet",
    "run_xgboost_tasklet",
]
