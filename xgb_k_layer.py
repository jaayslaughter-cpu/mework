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

PR #562: Models now loaded from xgb_model_store DB table as fallback
when filesystem PKLs are missing (Railway restart wipes ephemeral FS).
Training script persists models to DB after each successful train.

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
              model_prob = round(0.90 * model_prob + 0.10 * _xhp * 100, 2)  # Brier 0.2668 > null 0.25 — reduced to 10%

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


def _load_models_from_db() -> int:
    """
    PR #562: Load models from xgb_model_store Postgres table.
    Called as fallback when filesystem PKLs are missing (e.g., Railway restart).
    Returns number of models loaded.
    """
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        return 0
    try:
        import base64
        import pickle as _pickle
        import psycopg2
        with psycopg2.connect(db_url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT prop_type, model_json, feature_names
                    FROM xgb_model_store
                    WHERE trained_at > NOW() - INTERVAL '14 days'
                    ORDER BY trained_at DESC
                """)
                rows = cur.fetchall()
        if not rows:
            logger.info("[xgb_k] xgb_model_store: no recent models (< 14 days)")
            return 0
        loaded = 0
        for prop_type, model_b64, feat_names_raw in rows:
            try:
                model = _pickle.loads(base64.b64decode(model_b64))
                # prop_type in DB matches _models keys: "k_3.5", "k_4.5", "k_5.5", "k_6.5", "hits"
                key = str(prop_type)
                _models[key] = model
                if feat_names_raw:
                    if isinstance(feat_names_raw, str):
                        _feat_cols[key] = json.loads(feat_names_raw)
                    elif isinstance(feat_names_raw, list):
                        _feat_cols[key] = feat_names_raw
                logger.info("[xgb_k] Loaded '%s' from xgb_model_store (DB fallback)", key)
                loaded += 1
            except Exception as exc:
                logger.warning("[xgb_k] Failed to deserialize '%s' from DB: %s", prop_type, exc)
        return loaded
    except Exception as exc:
        logger.debug("[xgb_k] DB model load skipped: %s", exc)
        return 0


def _load_models() -> None:
    """Lazy-load all available .pkl files once at first call.
    PR #562: Falls back to xgb_model_store DB table if filesystem is empty.
    """
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
                logger.info("[xgb_k] %d model(s) ready from filesystem: %s",
                            len(_models), sorted(_models))
            else:
                # ── PR #562: Filesystem empty → try DB ──────────────────────
                logger.info("[xgb_k] No filesystem models found in %s — "
                            "trying xgb_model_store DB table...", _MODEL_DIR)
                n_db = _load_models_from_db()
                if n_db > 0:
                    logger.info("[xgb_k] %d model(s) loaded from DB: %s",
                                n_db, sorted(_models))
                else:
                    logger.info("[xgb_k] No models found anywhere. "
                                "Run scripts/xgb_k_training.py to generate.")

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

# TRAINING_ALIGNED — feature names match xgb_training_pipeline.py
# K model feature names — must match xgb_training_pipeline.py exactly
K_FEATURES = [
    "sv_xera",                  # Statcast xERA
    "sv_era",                   # ERA (FanGraphs, stored as sv_era in training)
    "sv_k_pct",                 # K% (0-100 scale)
    "sv_bb_pct",                # BB% (0-100 scale)
    "sv_whiff_pct",             # SwStr% (0-100 scale)
    "l3_ks",                    # L3-start avg strikeouts
    "l5_ks",                    # L5-start avg strikeouts
    "l10_ks",                   # L10-start avg strikeouts
    "l3_ip",                    # L3-start avg IP
    "l5_ip",                    # L5-start avg IP
    "days_rest",                # Days since last start
    "opp_lineup_k_pct_proxy",   # Opposing lineup K% (0-100)
    "opp_lineup_xwoba_proxy",   # Opposing lineup xwOBA
]

# Hit model feature names — must match xgb_training_pipeline.py exactly
HITS_FEATURES = [
    "sv_xba",       # Statcast xBA
    "sv_xwoba",     # Statcast xwOBA
    "sv_xslg",      # Statcast xSLG
    "sv_ev",        # Exit velocity
    "sv_brl_pct",   # Barrel %
    "sv_hh_pct",    # Hard-hit %
    "sv_ss_pct",    # SwStr% (NOTE: training key is sv_ss_pct, not sv_swstr_pct)
    "sv_la",        # Launch angle
    "sv_k_pct",     # Batter K% (training key is sv_k_pct, not fg_kpct)
    "sv_bb_pct",    # Batter BB% (training key is sv_bb_pct, not fg_bbpct)
    "opp_xera",     # Pitcher xERA
    "opp_k_pct",    # Pitcher K%
    "opp_bb_pct",   # Pitcher BB%
    "opp_whiff",    # Pitcher SwStr% (was missing — always 0.0 before)
    "bats_L",       # 1 = left-handed batter
    "throws_R",     # 1 = right-handed pitcher
    "platoon_adv",  # 1 = favorable platoon matchup
    "l7_hits",      # L7-game hit total
    "l7_hit_rate",  # L7-game hit rate
]



def _build_k_features(prop: dict, feat_order: list) -> Optional[np.ndarray]:
    """
    Build the K feature vector — column names match xgb_training_pipeline.py exactly.

    Mapping from PropIQ prop dict keys to training column names:
      fg_era / sv_era_p    → sv_era          (ERA stored as sv_era in training)
      fg_kpct / sv_kpct    → sv_k_pct        (K% in 0-100 scale)
      fg_bbpct             → sv_bb_pct       (BB% in 0-100 scale)
      sv_swstr_pct / csw   → sv_whiff_pct    (SwStr% in 0-100 scale)
      _l3_ks / l3_ks       → l3_ks           (L3-start avg Ks — was missing)
      _l3_ip / l3_ip       → l3_ip           (L3-start avg IP — was missing)
      _l5_ip / l5_ip       → l5_ip           (L5-start avg IP — was missing)
      _days_rest           → days_rest       (days since last start — was missing)
      _opp_avg_k_pct       → opp_lineup_k_pct_proxy
      _opp_avg_xwoba       → opp_lineup_xwoba_proxy
    """
    raw: dict[str, float] = {
        "sv_xera":                  _sf(prop, "sv_xera",           default=4.50),
        "sv_era":                   _sf(prop, "fg_era", "sv_era_p", "era",
                                        default=4.50),
        "sv_k_pct":                 _sf(prop, "fg_kpct", "sv_kpct", "k_pct",
                                        default=22.0),
        "sv_bb_pct":                _sf(prop, "fg_bbpct", "sv_bbpct", "bb_pct",
                                        default=8.0),
        "sv_whiff_pct":             _sf(prop, "sv_swstr_pct", "swstr_pct",
                                        "csw_pct", "sv_whiff_pct", default=24.0),
        "l3_ks":                    _sf(prop, "l3_ks", "_l3_ks",   default=4.5),
        "l5_ks":                    _sf(prop, "l5_ks", "_l5_ks",   default=4.5),
        "l10_ks":                   _sf(prop, "l10_ks", "_l10_ks", default=4.5),
        "l3_ip":                    _sf(prop, "l3_ip", "_l3_ip",   default=5.0),
        "l5_ip":                    _sf(prop, "l5_ip", "_l5_ip",   default=5.0),
        "days_rest":                _sf(prop, "days_rest", "_days_rest",
                                        "rest_days",               default=5.0),
        "opp_lineup_k_pct_proxy":   _sf(prop, "_opp_avg_k_pct", "opp_k_pct",
                                        "opp_lineup_k_pct_proxy",  default=22.0),
        "opp_lineup_xwoba_proxy":   _sf(prop, "_opp_avg_xwoba", "opp_xwoba",
                                        "opp_lineup_xwoba_proxy",  default=0.320),
    }

    # Scale fractions → percent (training data used 0-100 scale for pct cols)
    for pct_key in ("sv_k_pct", "sv_bb_pct", "sv_whiff_pct", "opp_lineup_k_pct_proxy"):
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
    Build the batter-hit feature vector — column names match xgb_training_pipeline.py.

    Key corrections vs prior version:
      sv_swstr_pct → sv_ss_pct  (training used sv_ss_pct for SwStr%)
      fg_kpct      → sv_k_pct   (training used sv_k_pct, not fg_kpct)
      fg_bbpct     → sv_bb_pct  (training used sv_bb_pct, not fg_bbpct)
      opp_whiff now populated from pitcher dict (was always 0.0 before)
    """
    bat_side = str(prop.get("batter_hand", prop.get("bats", "R")) or "R").upper()[:1]
    pit_hand = str(pitcher.get("_pitcher_hand", pitcher.get("pitcher_hand",
                   pitcher.get("pitchHand", "R"))) or "R").upper()[:1]
    platoon = 1 if (bat_side == "L" and pit_hand == "R") or \
                   (bat_side == "R" and pit_hand == "L") else 0

    raw: dict[str, float] = {
        # Batter Statcast — use sv_ prefix to match training column names
        "sv_xba":       _sf(prop, "sv_xba",                       default=0.250),
        "sv_xwoba":     _sf(prop, "sv_xwoba",    "fg_woba",        default=0.320),
        "sv_xslg":      _sf(prop, "sv_xslg",     "fg_slg",         default=0.400),
        "sv_ev":        _sf(prop, "sv_ev",                         default=88.0),
        "sv_brl_pct":   _sf(prop, "sv_brl_pct",                   default=4.0),
        "sv_hh_pct":    _sf(prop, "sv_hh_pct",                    default=35.0),
        # sv_ss_pct = SwStr% (training key) — was wrongly keyed as sv_swstr_pct
        "sv_ss_pct":    _sf(prop, "sv_swstr_pct", "sv_ss_pct",
                            "swstr_pct",                           default=10.0),
        "sv_la":        _sf(prop, "sv_la",                         default=12.0),
        # sv_k_pct and sv_bb_pct — training used sv_ prefix, not fg_
        "sv_k_pct":     _sf(prop, "fg_kpct", "sv_k_pct", "k_pct", default=22.0),
        "sv_bb_pct":    _sf(prop, "fg_bbpct", "sv_bb_pct", "bb_pct", default=8.0),
        # Pitcher opposition — keyed from pitcher sub-dict
        "opp_xera":     _sf(pitcher, "sv_xera",  "fg_era",         default=4.50),
        "opp_k_pct":    _sf(pitcher, "fg_kpct", "sv_k_pct",        default=22.0),
        "opp_bb_pct":   _sf(pitcher, "fg_bbpct", "sv_bb_pct",      default=8.0),
        # opp_whiff = pitcher SwStr% — was always 0.0 before (key was missing)
        "opp_whiff":    _sf(pitcher, "sv_swstr_pct", "sv_whiff_pct",
                            "swstr_pct", "opp_whiff",              default=24.0),
        # Platoon flags
        "bats_L":       1.0 if bat_side == "L" else 0.0,
        "throws_R":     1.0 if pit_hand == "R" else 0.0,
        "platoon_adv":  float(platoon),
        # Rolling form
        "l7_hits":      _sf(prop, "l7_hits",    "_l7_hits",        default=1.5),
        "l7_hit_rate":  _sf(prop, "l7_hit_rate", "_l7_hit_rate",   default=0.50),
    }

    # Scale fractions → percent
    for pct_key in ("sv_ss_pct", "sv_brl_pct", "sv_hh_pct",
                    "sv_k_pct", "sv_bb_pct", "opp_k_pct", "opp_bb_pct", "opp_whiff"):
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
