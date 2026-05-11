"""
bp2vec_integration.py
======================
Wires the (batter|pitcher)2vec embeddings into PropIQ's prop evaluation chain.

HOW IT WORKS
------------
After all existing enrichment (PA model, XGBoost blend, park factors,
adjustment dampener, market validator), this module adds one more signal:
the matchup-specific tendency score from the embedding space.

The adjustment is small by design (capped at ±3pp). It's not trying to
override the formula — it's adding the one signal the formula structurally
can't capture: "this specific batter historically underperforms against this
specific pitcher archetype, independent of their individual stats."

WHAT "MATCHUP ARCHETYPE" MEANS
-------------------------------
Two pitchers can have identical K% and SwStr% but structurally different
effects on different batter types. A pitcher with elite spin but soft velocity
will dominate contact hitters but give up hard contact to power hitters —
stats don't distinguish these patterns but embeddings do.

The embedding learns this from 4 years of actual PA outcomes without being
told which stats drive it.

INTEGRATION POINT
-----------------
In prop_enrichment_layer.py, find the final adjustment assembly block —
the one that calls dampen_adjustments(). Add bp2vec BEFORE the dampener
so it participates in correlation gating.

   from bp2vec_integration import apply_bp2vec_adjustment

   # Existing adjustment collection:
   adjustments = [
       ("shadow_whiff", whiff_delta),
       ("zone_integrity", zone_delta),
       # ... other signals ...
   ]

   # Add bp2vec matchup signal:
   bp_adj = apply_bp2vec_adjustment(prop)
   if bp_adj != 0.0:
       adjustments.append(("bp2vec_matchup", bp_adj))

   # Then dampen as normal:
   final_prob_pct = dampen_adjustments(base_prob * 100, adjustments)

FALLBACK BEHAVIOR
-----------------
If models aren't trained yet, apply_bp2vec_adjustment() returns 0.0 silently.
No crashes, no log spam. The enrichment layer works exactly as before until
models exist.

BUILDING MODELS
---------------
From the PropIQ repo root:
   python bp2vec_train.py --train --seasons 2022 2023 2024 2025

Takes 15-25 minutes depending on RAM. Statcast data (~4M rows per season)
is downloaded via pybaseball and cached. Subsequent runs use the cache.

MONITORING
----------
After wiring, check logs for:
   [bp2vec] Loaded: 1247 batters, 892 pitchers, trained on [2022, 2023, 2024, 2025]
   [bp2vec_wire] Adj: Spencer Strider vs Aaron Judge strikeouts → +1.20pp

If you see "[bp2vec] Models not found" — models haven't been trained yet.
If you never see any [bp2vec_wire] lines — the prop dict is missing
  mlb_batter_id or mlb_pitcher_id fields (see ID RESOLUTION below).
"""

from __future__ import annotations

import logging
from typing import Optional

log = logging.getLogger("propiq.bp2vec_wire")

# Lazy import — only load models when first called
_scorer = None


def _get_scorer():
    global _scorer
    if _scorer is None:
        try:
            from bp2vec_train import BP2VecScorer
            _scorer = BP2VecScorer.get()
        except ImportError:
            log.debug("[bp2vec_wire] bp2vec_train not importable — scorer disabled")
            _scorer = _NullScorer()
    return _scorer


class _NullScorer:
    """Fallback when bp2vec models don't exist. Returns 0.0 for everything."""
    def ready(self): return False
    def get_matchup_adjustment_pp(self, *a, **kw): return 0.0


# ── ID Resolution ──────────────────────────────────────────────────────────────
#
# The embedding models use Statcast mlb_id integers (e.g. batter=669373 for
# Tarik Skubal). PropIQ's prop dicts should carry these as:
#   prop["mlb_batter_id"]  — the Statcast batter ID
#   prop["mlb_pitcher_id"] — the Statcast pitcher ID
#
# If your enrichment layer doesn't populate these yet, they can also come from:
#   prop["player_id"] (for single-player props)
#   prop["pitcher_id"] or prop["opp_pitcher_id"]
#
# The resolver below tries multiple key names in priority order.

def _resolve_batter_id(prop: dict) -> str:
    """Extract batter MLB ID from prop dict, trying multiple key names."""
    for key in ("mlb_batter_id", "batter_id", "mlb_id", "player_mlb_id"):
        v = prop.get(key)
        if v:
            return str(v).strip()
    return ""


def _resolve_pitcher_id(prop: dict) -> str:
    """Extract pitcher MLB ID from prop dict, trying multiple key names."""
    for key in ("mlb_pitcher_id", "pitcher_id", "opp_pitcher_mlb_id",
                "sp_mlb_id", "opposing_pitcher_id"):
        v = prop.get(key)
        if v:
            return str(v).strip()
    return ""


# ── Main integration function ─────────────────────────────────────────────────

def apply_bp2vec_adjustment(prop: dict) -> float:
    """
    Compute and return the (batter|pitcher)2vec matchup adjustment in pp.

    This is the single function to add to prop_enrichment_layer.py.
    Returns 0.0 in all error/unavailable cases — safe to call unconditionally.

    Args:
        prop: enriched PropIQ prop dict

    Returns:
        float: additive adjustment in percentage points (e.g. +1.8 or -2.1)
               Capped at [-3.0, +3.0]pp.
               0.0 if models not loaded or players not in embedding space.
    """
    scorer = _get_scorer()
    if not scorer.ready():
        return 0.0

    batter_id  = _resolve_batter_id(prop)
    pitcher_id = _resolve_pitcher_id(prop)
    prop_type  = (prop.get("prop_type") or "").lower()

    if not batter_id or not pitcher_id:
        return 0.0

    adj = scorer.get_matchup_adjustment_pp(batter_id, pitcher_id, prop_type)

    if adj != 0.0:
        log.info(
            "[bp2vec_wire] Adj: %s vs %s %s → %+.2fpp",
            prop.get("pitcher_name", pitcher_id),
            prop.get("player", batter_id),
            prop_type,
            adj,
        )

    return adj


def bp2vec_ready() -> bool:
    """Return True if bp2vec models are trained and loaded."""
    return _get_scorer().ready()


def bp2vec_status() -> dict:
    """Return status dict for health checks / Discord embed."""
    s = _get_scorer()
    if not s.ready():
        return {"ready": False}
    if hasattr(s, "status"):
        return s.status()
    return {"ready": True}


# ── Prop dict annotation ───────────────────────────────────────────────────────

def annotate_prop(prop: dict) -> dict:
    """
    Add bp2vec fields to prop dict without modifying model_prob.
    Useful for logging and Discord embeds before the actual adjustment is applied.

    Adds:
        prop["_bp2vec_k_adj"]     — K-prop adjustment
        prop["_bp2vec_hit_adj"]   — Hit-prop adjustment
        prop["_bp2vec_power_adj"] — HR/power adjustment
        prop["_bp2vec_available"] — True if both players found in embeddings

    Call this in the DataHub enrichment pass (not in the hot evaluation path).
    """
    scorer = _get_scorer()
    if not scorer.ready():
        prop["_bp2vec_available"] = False
        return prop

    batter_id  = _resolve_batter_id(prop)
    pitcher_id = _resolve_pitcher_id(prop)

    if not batter_id or not pitcher_id:
        prop["_bp2vec_available"] = False
        return prop

    prop["_bp2vec_k_adj"]     = scorer.get_matchup_adjustment_pp(batter_id, pitcher_id, "strikeouts")
    prop["_bp2vec_hit_adj"]   = scorer.get_matchup_adjustment_pp(batter_id, pitcher_id, "hits")
    prop["_bp2vec_power_adj"] = scorer.get_matchup_adjustment_pp(batter_id, pitcher_id, "home_runs")
    prop["_bp2vec_available"] = True
    return prop


# ── Nearest-neighbor lookup (for Discord research embeds) ─────────────────────

def similar_pitchers(pitcher_id: str, k: int = 5) -> list[dict]:
    """
    Return the k most similar pitchers to the given pitcher in embedding space.
    Useful for "pitchers like this one" context in Discord matchup embeds.

    Args:
        pitcher_id: Statcast pitcher mlb_id
        k:          number of neighbors to return

    Returns:
        list of {"name": str, "id": str, "similarity": float}
    """
    scorer = _get_scorer()
    if not scorer.ready():
        return []

    try:
        import numpy as np
        idx = scorer._pitcher_to_idx.get(str(pitcher_id))
        if idx is None:
            return []

        p_vec  = scorer._p_vecs[idx]
        norms  = np.linalg.norm(scorer._p_vecs, axis=1)
        p_norm = np.linalg.norm(p_vec)
        sims   = np.dot(scorer._p_vecs, p_vec) / (norms * p_norm + 1e-8)
        ranked = np.argsort(-sims)

        id_map    = {v: k for k, v in scorer._pitcher_to_idx.items()}
        id_to_name = scorer._meta.get("id_to_name", {})
        results = []
        for i in ranked[1:]:
            pid = id_map.get(int(i))
            if pid is None:
                continue
            results.append({
                "name":       id_to_name.get(pid, pid),
                "id":         pid,
                "similarity": round(float(sims[i]), 4),
            })
            if len(results) >= k:
                break
        return results

    except Exception as e:
        log.debug("[bp2vec_wire] similar_pitchers error: %s", e)
        return []


def similar_batters(batter_id: str, k: int = 5) -> list[dict]:
    """Return the k most similar batters to the given batter in embedding space."""
    scorer = _get_scorer()
    if not scorer.ready():
        return []

    try:
        import numpy as np
        idx = scorer._batter_to_idx.get(str(batter_id))
        if idx is None:
            return []

        b_vec  = scorer._b_vecs[idx]
        norms  = np.linalg.norm(scorer._b_vecs, axis=1)
        b_norm = np.linalg.norm(b_vec)
        sims   = np.dot(scorer._b_vecs, b_vec) / (norms * b_norm + 1e-8)
        ranked = np.argsort(-sims)

        id_map    = {v: k for k, v in scorer._batter_to_idx.items()}
        id_to_name = scorer._meta.get("id_to_name", {})
        results = []
        for i in ranked[1:]:
            bid = id_map.get(int(i))
            if bid is None:
                continue
            results.append({
                "name":       id_to_name.get(bid, bid),
                "id":         bid,
                "similarity": round(float(sims[i]), 4),
            })
            if len(results) >= k:
                break
        return results

    except Exception as e:
        log.debug("[bp2vec_wire] similar_batters error: %s", e)
        return []
