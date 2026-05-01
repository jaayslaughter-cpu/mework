"""
xgb_k_layer.py — Per-Line XGBoost K & Hit Prop Scorer
=======================================================
Adapted from mlb-analytics-hub/xgb_prop_scorer.py
Source: github.com/johnmsimo/mlb-analytics-hub

Architecture insight (Layne/Cato 2024):
  K over 3.5 and K over 7.5 are fundamentally different propositions.
  Feature importance differs: 3.5 is dominated by SwStr% and platoon;
  7.5 is dominated by opp_lineup_xwoba and L10 avg. A single model
  produces mediocre predictions at every line. Separate per-line models
  + Platt calibration close the gap materially.

Model files (produced by scripts/xgb_k_training.py):
  models/xgb_k_3_5.pkl         — K > 3.5 strikeouts
  models/xgb_k_4_5.pkl         — K > 4.5 strikeouts
  models/xgb_k_5_5.pkl         — K > 5.5 strikeouts
  models/xgb_k_6_5.pkl         — K > 6.5 strikeouts
  models/xgb_hits.pkl           — batter ≥1 hit
  models/xgb_feature_cols.json — feature column order per model key

Wiring (F5Agent, tasklets.py):
  After all K adjustments (swstr, opp_k, platoon, lambda_gap, line_move):

      if prop_type == "strikeouts":
          try:
              from xgb_k_layer import xgb_k_ready, xgb_k_prob as _xgb_k_prob
              if xgb_k_ready():
                  _xkp = _xgb_k_prob(prop, line=float(prop.get("line", 4.5)))
                  if _xkp is not None:
                      model_prob = round(0.80 * model_prob + 0.20 * _xkp * 100, 2)
                      model_prob = max(5.0, min(95.0, model_prob))
          except ImportError:
              pass

  For batter hit props (EVHunter / future HitAgent):

      if prop_type in ("hits", "fantasy_score") and xgb_hit_ready():
          _xhp = xgb_hit_prob(prop, pitcher_dict)
          if _xhp is not None:
              model_prob = round(0.70 * model_prob + 0.30 * _xhp * 100, 2)

All functions return None if models not loaded — existing formula runs unchanged.

Blend schedule (matching Confidence Gate review):
  Now (Brier ~0.248):       80% formula / 20% per-line XGBoost
  After 200+ graded + Brier < 0.20:  shift to 60/40 or 50/50
"""

from __future__ import annotations

import json
import logging
import os
import threading
import traceback
from typing import Optional

import numpy as np

logger = logging.getLogger("propiq.xgb_k")

# ── Model file paths ────────────────────────────────────────────────────────
_HERE      = os.path.dirname(os.path.abspath(__file__))
_MODEL_DIR = os.path.join(_HERE, "models")
_FEAT_FILE = os.path.join(_MODEL_DIR, "xgb_feature_cols.json")

_MODEL_PATHS: dict[str, str] = {
    "k_3.5": os.path.join(_MODEL_DIR, "xgb_k_3_5.pkl"),
    "k_4.5": os.path.join(_MODEL_DIR, "xgb_k_4_5.pkl"),
    "k_5.5": os.path.join(_MODEL_DIR, "xgb_k_5_5.pkl"),
    "k_6.5": os.path.join(_MODEL_DIR, "xgb_k_6_5.pkl"),
    "hits":  os.path.join(_MODEL_DIR, "xgb_hits.pkl"),
}

# ── Registry ────────────────────────────────────────────────────────────────
_lock:      threading.Lock  = threading.Lock()
_models:    dict            = {}   # key → XGBClassifier (Platt-calibrated)
_feat_cols: dict            = {}   # key → list[str]
_loaded:    bool            = False


def _load_models() -> None:
    """Lazy-load all available .pkl files once at first call."""
    global _loaded
    with _lock:
        if _loaded:
            return
        try:
            import pickle
            feat_map: dict = {}
            if os.path.exists(_FEAT_FILE):
                with open(_FEAT_FILE) as f:
                    feat_map = json.load(f)

            for key, path in _MODEL_PATHS.items():
                if os.path.exists(path):
                    with open(path, "rb") as f:
                        _models[key] = pickle.load(f)
                    _feat_cols[key] = feat_map.get(key, [])
                    logger.info("[xgb_k] loaded %s from %s", key, path)
                else:
                    logger.debug("[xgb_k] model not found: %s", path)

            if _models:
                logger.info("[xgb_k] %d model(s) ready: %s",
                            len(_models), sorted(_models))
            else:
                logger.info("[xgb_k] no models found in %s — "
                            "run scripts/xgb_k_training.py to generate", _MODEL_DIR)

        except Exception:
            logger.warning("[xgb_k] model load failed:\n%s", traceback.format_exc())
        finally:
            _loaded = True


# ── Ready checks ────────────────────────────────────────────────────────────

def xgb_k_ready() -> bool:
    """True if at least one per-line K model is loaded."""
    if not _loaded:
        _load_models()
    return any(k.startswith("k_") for k in _models)


def xgb_hit_ready() -> bool:
    """True if the batter-hit model is loaded."""
    if not _loaded:
        _load_models()
    return "hits" in _models


# ── Feature helpers ──────────────────────────────────────────────────────────

def _sf(d: dict, *keys, default: float = 0.0) -> float:
    """Safe float — tries each key in order, returns default."""
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        try:
            f = float(v)
            if not (f != f) and not (f == float("inf")) and not (f == float("-inf")):
                return f
        except (TypeError, ValueError):
            continue
    return default


# ── Feature lists (must match xgb_k_training.py exactly) ───────────────────

# Pitcher strikeout features — 10 dimensions
K_FEATURES = [
    "sv_xera",          # Statcast xERA (expected ERA from quality of contact)
    "fg_era",           # FanGraphs ERA (season)
    "fg_kpct",          # FanGraphs K% (season, 0–100)
    "fg_bbpct",         # FanGraphs BB% (season, 0–100)
    "sv_swstr_pct",     # Statcast SwStr% / whiff rate (0–100)
    "l5_ks",            # Rolling L5-start avg strikeouts
    "l5_k_rate",        # Rolling L5-start K rate (0–100)
    "l10_ks",           # Rolling L10-start avg strikeouts
    "opp_k_pct",        # Opposing lineup avg K% (regressed, 0–100)
    "opp_xwoba",        # Opposing lineup avg xwOBA
]

# Batter hit features — 19 dimensions
HITS_FEATURES = [
    "sv_xba",           # Statcast xBA
    "sv_xwoba",         # Statcast xwOBA
    "sv_xslg",          # Statcast xSLG
    "sv_ev",            # Avg exit velocity
    "sv_brl_pct",       # Barrel % (0–100)
    "sv_hh_pct",        # Hard-hit % (0–100)
    "sv_swstr_pct",     # SwStr% (batter)
    "sv_la",            # Launch angle (degrees)
    "fg_kpct",          # FanGraphs batter K% (0–100)
    "fg_bbpct",         # FanGraphs batter BB% (0–100)
    "opp_xera",         # Opposing pitcher xERA
    "opp_k_pct",        # Pitcher K% (0–100)
    "opp_bb_pct",       # Pitcher BB% (0–100)
    "opp_swstr_pct",    # Pitcher SwStr%
    "bats_L",           # 1 = left-handed batter
    "throws_R",         # 1 = right-handed pitcher
    "platoon_adv",      # 1 = favorable handedness matchup
    "l7_hits",          # L7-game rolling hit total
    "l7_hit_rate",      # L7-game rolling hit rate (0–1)
]


def _build_k_features(prop: dict, feat_order: list) -> Optional[np.ndarray]:
    """
    Build the K feature vector from a PropIQ enriched prop dict.
    All our prop dicts use underscore_separated keys — no translation needed.
    Percentage columns stored as 0–1 are scaled to 0–100.
    """
    raw: dict[str, float] = {
        "sv_xera":      _sf(prop, "sv_xera",     "fg_era",    default=4.50),
        "fg_era":       _sf(prop, "fg_era",       "sv_era_p",  default=4.50),
        "fg_kpct":      _sf(prop, "fg_kpct",                   default=22.0),
        "fg_bbpct":     _sf(prop, "fg_bbpct",                  default=8.0),
        "sv_swstr_pct": _sf(prop, "sv_swstr_pct", "swstr_pct", "csw_pct",
                            default=24.0),
        "l5_ks":        _sf(prop, "l5_ks",        "_l5_ks",   default=4.5),
        "l5_k_rate":    _sf(prop, "l5_k_rate",    "_l5_k_rate", default=22.0),
        "l10_ks":       _sf(prop, "l10_ks",       "_l10_ks",  default=4.5),
        "opp_k_pct":    _sf(prop, "_opp_avg_k_pct", "opp_k_rate",
                            "opp_k_pct", default=22.0),
        "opp_xwoba":    _sf(prop, "_opp_avg_xwoba", "opp_xwoba", default=0.320),
    }

    # Scale fractions → percent
    for pct_key in ("fg_kpct", "fg_bbpct", "sv_swstr_pct",
                    "l5_k_rate", "opp_k_pct"):
        if 0.0 < raw[pct_key] <= 1.0:
            raw[pct_key] *= 100.0

    cols = feat_order if feat_order else K_FEATURES
    try:
        return np.array([[raw.get(c, 0.0) for c in cols]], dtype=np.float32)
    except Exception:
        logger.debug("[xgb_k] K feature build error", exc_info=True)
        return None


def _build_hit_features(prop: dict, pitcher: dict,
                         feat_order: list) -> Optional[np.ndarray]:
    """
    Build the batter-hit feature vector.
    prop = batter prop dict (enriched); pitcher = enriched pitcher sub-dict.
    """
    bat_side = str(prop.get("batter_hand", prop.get("bats", "R")) or "R").upper()[:1]
    pit_hand = str(pitcher.get("_pitcher_hand", pitcher.get("pitcher_hand",
                   pitcher.get("pitchHand", "R"))) or "R").upper()[:1]
    platoon = 1 if (bat_side == "L" and pit_hand == "R") or \
                   (bat_side == "R" and pit_hand == "L") else 0

    raw: dict[str, float] = {
        # Batter Statcast
        "sv_xba":       _sf(prop, "sv_xba",      default=0.250),
        "sv_xwoba":     _sf(prop, "sv_xwoba",    "fg_woba",  default=0.320),
        "sv_xslg":      _sf(prop, "sv_xslg",     "fg_slg",   default=0.400),
        "sv_ev":        _sf(prop, "sv_ev",                    default=88.0),
        "sv_brl_pct":   _sf(prop, "sv_brl_pct",              default=4.0),
        "sv_hh_pct":    _sf(prop, "sv_hh_pct",               default=35.0),
        "sv_swstr_pct": _sf(prop, "sv_swstr_pct", "swstr_pct", default=10.0),
        "sv_la":        _sf(prop, "sv_la",                    default=12.0),
        "fg_kpct":      _sf(prop, "fg_kpct",                  default=22.0),
        "fg_bbpct":     _sf(prop, "fg_bbpct",                 default=8.0),
        # Pitcher opposition metrics
        "opp_xera":     _sf(pitcher, "sv_xera",  "fg_era",   default=4.50),
        "opp_k_pct":    _sf(pitcher, "fg_kpct",              default=22.0),
        "opp_bb_pct":   _sf(pitcher, "fg_bbpct",             default=8.0),
        "opp_swstr_pct":_sf(pitcher, "sv_swstr_pct", "swstr_pct", default=24.0),
        # Platoon flags
        "bats_L":       1.0 if bat_side == "L" else 0.0,
        "throws_R":     1.0 if pit_hand == "R" else 0.0,
        "platoon_adv":  float(platoon),
        # Rolling form
        "l7_hits":      _sf(prop, "l7_hits",      "_l7_hits", default=1.5),
        "l7_hit_rate":  _sf(prop, "l7_hit_rate",  "_l7_hit_rate", default=0.50),
    }

    # Scale fractions → percent (pct columns expected in 0–100)
    for pct_key in ("fg_kpct", "fg_bbpct", "sv_swstr_pct",
                    "sv_brl_pct", "sv_hh_pct",
                    "opp_k_pct", "opp_bb_pct", "opp_swstr_pct"):
        if 0.0 < raw[pct_key] <= 1.0:
            raw[pct_key] *= 100.0

    cols = feat_order if feat_order else HITS_FEATURES
    try:
        return np.array([[raw.get(c, 0.0) for c in cols]], dtype=np.float32)
    except Exception:
        logger.debug("[xgb_k] hit feature build error", exc_info=True)
        return None


# ── Public API ───────────────────────────────────────────────────────────────

def xgb_k_prob(prop: dict, line: float = 4.5) -> Optional[float]:
    """
    Returns P(Over | K line) for a pitcher strikeout prop.

    Args:
        prop: enriched PropIQ prop dict (from prop_enrichment_layer)
        line: the current UD/PP line (3.5, 4.5, 5.5, 6.5)

    Returns:
        float [0, 1] — probability of Over, or None if model not loaded.
    """
    if not _loaded:
        _load_models()

    # Nearest supported line
    supported = [3.5, 4.5, 5.5, 6.5]
    nearest   = min(supported, key=lambda x: abs(x - line))
    key       = f"k_{nearest}"

    if key not in _models:
        # Try any available K model
        for fallback in ("k_4.5", "k_3.5", "k_5.5", "k_6.5"):
            if fallback in _models:
                key = fallback
                break
        else:
            return None

    try:
        feat_order = _feat_cols.get(key, [])
        X = _build_k_features(prop, feat_order)
        if X is None:
            return None
        prob = float(_models[key].predict_proba(X)[0, 1])
        return round(min(0.97, max(0.03, prob)), 4)
    except Exception:
        logger.debug("[xgb_k] xgb_k_prob error", exc_info=True)
        return None


def xgb_hit_prob(prop: dict, pitcher: Optional[dict] = None) -> Optional[float]:
    """
    Returns P(batter records ≥1 hit) for a batter hit prop.

    Args:
        prop:    enriched batter prop dict
        pitcher: enriched pitcher sub-dict (or pass None to use prop itself
                 for pitcher keys already merged in)

    Returns:
        float [0, 1] — probability of at least 1 hit, or None if not loaded.
    """
    if not _loaded:
        _load_models()
    if "hits" not in _models:
        return None

    try:
        feat_order = _feat_cols.get("hits", [])
        X = _build_hit_features(prop, pitcher or prop, feat_order)
        if X is None:
            return None
        prob = float(_models["hits"].predict_proba(X)[0, 1])
        return round(min(0.97, max(0.03, prob)), 4)
    except Exception:
        logger.debug("[xgb_k] xgb_hit_prob error", exc_info=True)
        return None


def xgb_k_prob_bulk(props: list[dict]) -> dict[str, float]:
    """
    Batch K prob prediction. Returns {player_name: prob_over} dict.
    Uses the 4.5 model (most common line) for all props in the batch.
    """
    if not _loaded:
        _load_models()
    model = _models.get("k_4.5") or next(
        (m for k, m in _models.items() if k.startswith("k_")), None)
    if model is None or not props:
        return {}

    key        = next((k for k in _models if k.startswith("k_")), "k_4.5")
    feat_order = _feat_cols.get(key, [])
    rows, names = [], []
    for p in props:
        X = _build_k_features(p, feat_order)
        if X is not None:
            rows.append(X[0])
            names.append(p.get("player", ""))

    if not rows:
        return {}
    try:
        probs = model.predict_proba(
            np.array(rows, dtype=np.float32))[:, 1]
        return {
            name: round(min(0.97, max(0.03, float(p))), 4)
            for name, p in zip(names, probs)
        }
    except Exception:
        logger.debug("[xgb_k] bulk predict error", exc_info=True)
        return {}


# ── Diagnostic ───────────────────────────────────────────────────────────────

def xgb_k_status() -> dict:
    """Returns model load status — wired into 10 AM bug_checker embed."""
    if not _loaded:
        _load_models()
    return {
        "models_loaded": sorted(_models.keys()),
        "k_ready":       xgb_k_ready(),
        "hit_ready":     xgb_hit_ready(),
        "model_dir":     _MODEL_DIR,
        "k_features":    K_FEATURES,
        "hits_features": HITS_FEATURES,
    }


if __name__ == "__main__":
    status = xgb_k_status()
    print("[xgb_k_layer] Status:", status)
    if not status["k_ready"]:
        print("  → No models found. Run: uv run --with xgboost,scikit-learn "
              "python3 scripts/xgb_k_training.py")
    else:
        print("  → K models ready:", status["models_loaded"])
