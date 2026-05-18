"""
api/tasklets/xgboost_tasklet.py
================================
DELEGATOR — this file is intentionally thin.

The canonical XGBoost retrain lives in tasklets.py as run_xgboost_tasklet().
That function reads from Postgres bet_ledger (discord_sent=TRUE rows) and
uses the correct 27-slot feature vector built by _build_feature_vector().

Historical bug (PR #343): This file previously connected to
api/data/agent_army.db (SQLite) and trained on 12 features that never
matched the live 27-slot feature schema. It has been replaced with this
delegator so callers in api_server.py continue to work.

Historical bug 2: The bare `from tasklets import ...` resolved to
api/tasklets.py (the thin shim) instead of the root monolith when Railway
sets WORKDIR /app/api. Fixed by _ensure_root_on_path().

DO NOT reimport sqlite3 or reopen agent_army.db here.
"""
from __future__ import annotations

import importlib
import logging
import pathlib
import sys
from pathlib import Path

log = logging.getLogger("propiq.tasklet.xgboost")


# ── Path helper ────────────────────────────────────────────────────────────────

def _ensure_root_on_path() -> None:
    """Ensure repo root is on sys.path regardless of working directory.

    Railway sets WORKDIR /app/api, so a bare `import tasklets` resolves to
    api/tasklets.py (the thin re-export shim) instead of the root monolith.
    """
    root = str(pathlib.Path(__file__).resolve().parents[2])
    if root not in sys.path:
        sys.path.insert(0, root)


# ── Public surface ─────────────────────────────────────────────────────────────

def run_xgboost_tasklet() -> dict:
    """Delegate to canonical Postgres-backed retrainer in tasklets.py."""
    try:
        _ensure_root_on_path()
        _mod = importlib.import_module("tasklets")
        _mod.run_xgboost_tasklet()
        return {"status": "ok", "source": "tasklets.run_xgboost_tasklet"}
    except Exception as exc:
        log.error("[XGBoost delegator] run_xgboost_tasklet failed: %s", exc)
        return {"status": "error", "error": str(exc)}


def load_model() -> tuple:
    """
    Load the XGBoost model.
    Returns (model, accuracy) or (None, 0.0) if not available.
    Checks both disk locations (canonical path from tasklets.py, then legacy path).
    """
    import json
    import os

    _ensure_root_on_path()

    # Try canonical path first (matches tasklets.py XGB_MODEL_PATH)
    canonical = Path(os.getenv("XGB_MODEL_PATH", "/app/api/models/prop_model_v1.json"))
    legacy = Path(__file__).parent.parent / "models" / "xgboost_props.pkl"

    # Canonical JSON path (XGBoost native format)
    if canonical.exists():
        try:
            import xgboost as xgb
            model = xgb.Booster()
            model.load_model(str(canonical))
            log.info("[XGBoost] Loaded model from canonical path: %s", canonical)
            return model, 0.0
        except Exception as exc:
            log.warning("[XGBoost] Failed to load from %s: %s", canonical, exc)

    # Legacy pickle path (backward compat)
    if legacy.exists():
        try:
            import pickle
            with open(legacy, "rb") as f:
                data = pickle.load(f)
            log.info("[XGBoost] Loaded model from legacy pickle: %s", legacy)
            return data.get("model"), data.get("accuracy", 0.0)
        except Exception as exc:
            log.warning("[XGBoost] Failed to load from legacy pickle: %s", exc)

    log.info("[XGBoost] No model file found — fallback to heuristic pipeline")
    return None, 0.0
